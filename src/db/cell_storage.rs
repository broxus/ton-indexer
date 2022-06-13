use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use ton_types::{ByteOrderRead, CellImpl, FxDashMap, UInt256};

use super::{columns, Column, Tree};

pub struct CellStorage {
    cells: Tree<columns::Cells>,
    cells_cache: Arc<FxDashMap<UInt256, Weak<StorageCell>>>,
}

impl CellStorage {
    pub fn new(db: &Arc<rocksdb::DB>) -> Result<Self> {
        let cache = Arc::new(FxDashMap::default());
        Ok(Self {
            cells: Tree::new(db)?,
            cells_cache: cache,
        })
    }

    pub fn store_dynamic_boc(
        &self,
        batch: &mut rocksdb::WriteBatch,
        marker: u8,
        root: ton_types::Cell,
    ) -> Result<usize> {
        let transaction = self.prepare_tree_of_cells(marker, root)?;
        let len = transaction.len();

        let cf = self.cells.get_cf();
        for (cell_id, data) in transaction {
            batch.put_cf(&cf, cell_id, data);
        }

        Ok(len)
    }

    pub fn load_cell(self: &Arc<Self>, hash: UInt256) -> Result<Arc<StorageCell>> {
        if let Some(cell) = self.cells_cache.get(&hash) {
            if let Some(cell) = cell.upgrade() {
                return Ok(cell);
            }
        }

        let cell = match self.cells.get(hash.as_slice())? {
            Some(value) => Arc::new(StorageCell::deserialize(self.clone(), &value)?),
            None => return Err(CellStorageError::CellNotFound.into()),
        };
        self.cells_cache.insert(hash, Arc::downgrade(&cell));

        Ok(cell)
    }

    pub async fn sweep_cells(&self, target_marker: u8) -> Result<usize> {
        let total = Arc::new(AtomicUsize::new(0));
        let mut tasks = FuturesUnordered::new();

        // Prepare one database snapshot for all shard states iterators
        let snapshot = self.cells.raw_db_handle().snapshot();

        for i in 0..8 {
            // iii00000, 00000000, ...
            let mut lower_bound = [0; 32];
            lower_bound[0] = i << 5;

            // iii11111, 11111111, ...
            let mut upper_bound = [0xff; 32];
            upper_bound[0] = (i << 5) | 0b00011111;

            // Prepare cells read options
            let mut read_options = rocksdb::ReadOptions::default();
            columns::Cells::read_options(&mut read_options);
            read_options.set_snapshot(&snapshot);
            read_options.set_iterate_upper_bound(upper_bound);

            let db = self.cells.raw_db_handle().clone();
            let total = total.clone();

            tasks.push(tokio::task::spawn_blocking(move || {
                let cells_cf = db.cf_handle(columns::Cells::NAME).context("No cf")?;

                // Prepare iterator
                let iter = db.iterator_cf_opt(
                    &cells_cf,
                    read_options,
                    rocksdb::IteratorMode::From(&lower_bound, rocksdb::Direction::Forward),
                );

                // Prepare cells write options
                let mut write_options = rocksdb::WriteOptions::default();
                columns::Cells::write_options(&mut write_options);

                // Iterate all cells in range
                let mut subtotal = 0;
                for (key, value) in iter {
                    if value.is_empty() {
                        continue;
                    }
                    let marker = value[0];

                    if marker > 0 && marker != target_marker {
                        db.delete_cf_opt(&cells_cf, key, &write_options)?;
                        subtotal += 1;
                    }
                }

                total.fetch_add(subtotal, Ordering::Relaxed);
                Result::<(), anyhow::Error>::Ok(())
            }));
        }

        // Wait for all tasks to complete
        while let Some(result) = tasks.next().await {
            result??
        }

        // Load counter
        Ok(total.load(Ordering::Relaxed))
    }

    pub fn mark_cells_tree(
        &self,
        root_cell: UInt256,
        target_marker: u8,
        force: bool,
    ) -> Result<usize> {
        // Prepare handles
        let cf = self.cells.get_cf();
        let read_config = self.cells.read_config();
        let write_config = self.cells.write_config();
        let db = self.cells.raw_db_handle();

        // NOTE: don't use WriteBatch here to prevent high memory usage.
        // Atomic write is not needed here

        // Start from the root cell
        let mut stack = SmallVec::<[_; 256]>::with_capacity(256);
        stack.push(root_cell);

        let mut total = 0;

        let mut buffer = SmallVec::<[u8; 512]>::with_capacity(512);

        // While some cells left
        while let Some(cell_id) = stack.pop() {
            // Load cell marker and references from the top of the stack
            let (persistent_cell, marker_changed, references) =
                match db.get_pinned_cf_opt(&cf, cell_id.as_slice(), read_config)? {
                    Some(value) => {
                        // NOTE: dereference value only once to prevent multiple ffi calls
                        let value = value.as_ref();

                        let (marker, references) =
                            StorageCell::deserialize_marker_and_references(value)?;

                        let persistent_cell = marker == 0;
                        let marker_changed = !persistent_cell && marker != target_marker;

                        // Update cell data if marker changed
                        if marker_changed {
                            buffer.clear();
                            buffer.extend_from_slice(value);
                            buffer[0] = target_marker;
                            db.put_cf_opt(&cf, cell_id.as_slice(), &buffer, write_config)?;
                        }

                        (persistent_cell, marker_changed, references)
                    }
                    None => {
                        return Err(CellStorageError::CellNotFound)
                            .with_context(|| format!("Child not found. Depth: {}", stack.len()))
                    }
                };

            total += marker_changed as usize;

            // Add all children
            if !persistent_cell && (marker_changed || force) {
                for cell_id in references {
                    stack.push(cell_id.hash());
                }
            }
        }

        Ok(total)
    }

    pub fn store_single_cell(&self, hash: &[u8], value: &[u8]) -> Result<()> {
        self.cells.insert(hash, value)
    }

    pub fn drop_cell(&self, hash: &UInt256) {
        self.cells_cache.remove(hash);
    }

    fn prepare_tree_of_cells(
        &self,
        marker: u8,
        cell: ton_types::Cell,
    ) -> Result<FxHashMap<UInt256, Vec<u8>>> {
        // Prepare handles
        let cf = self.cells.get_cf();
        let db = self.cells.raw_db_handle();
        let read_options = self.cells.read_config();

        let mut transaction = FxHashMap::default();

        // Check root cell
        let cell_id = cell.repr_hash();
        match db.get_pinned_cf_opt(&cf, &cell_id, read_options)? {
            Some(value) => {
                // NOTE: dereference value only once to prevent multiple ffi calls
                let value = value.as_ref();
                // Overwrite value if it has different marker
                if value.is_empty() {
                    // Empty cell is invalid
                    return Err(CellStorageError::InvalidCell.into());
                } else if value[0] > 0 && value[0] != marker {
                    // Proceed if cell was updated
                    let mut value = value.to_vec();
                    value[0] = marker;
                    transaction.insert(cell_id, value);
                } else {
                    // Cell already exists. Do nothing
                    return Ok(transaction);
                }
            }
            // Insert cell value if it doesn't exist
            None => {
                transaction.insert(cell_id, StorageCell::serialize(marker, &*cell)?);
            }
        }

        let mut stack = Vec::with_capacity(16);
        stack.push(cell);

        while let Some(current) = stack.pop() {
            for i in 0..current.references_count() {
                let cell = current.reference(i)?;
                let cell_id = cell.repr_hash();

                match db.get_pinned_cf_opt(&cf, &cell_id, read_options)? {
                    Some(value) => {
                        // NOTE: dereference value only once to prevent multiple ffi calls
                        let value = value.as_ref();
                        if value.is_empty() {
                            transaction.insert(cell_id, StorageCell::serialize(marker, &*cell)?);
                        } else if value[0] > 0 && value[0] != marker {
                            // Update cell if marker is different or value is invalid
                            let mut value = value.to_vec();
                            // SAFETY: bounds are checked above. Definitely removes bounds check
                            unsafe { *value.get_unchecked_mut(0) = marker };
                            transaction.insert(cell_id, value);
                        } else {
                            // Cell already exists
                            continue;
                        }
                    }
                    // Already inserting this cell
                    None if transaction.contains_key(&cell_id) => continue,
                    None => {
                        transaction.insert(cell_id, StorageCell::serialize(marker, &*cell)?);
                    }
                }

                stack.push(cell);
            }
        }

        Ok(transaction)
    }
}

#[derive(thiserror::Error, Debug)]
enum CellStorageError {
    #[error("Cell not found in cell db")]
    CellNotFound,
    #[error("Invalid cell")]
    InvalidCell,
}

pub struct StorageCell {
    _c: countme::Count<Self>,
    cell_storage: Arc<CellStorage>,
    cell_data: ton_types::CellData,
    references: RwLock<SmallVec<[StorageCellReference; 4]>>,
    tree_bits_count: u64,
    tree_cell_count: u64,
}

impl StorageCell {
    pub fn repr_hash(&self) -> UInt256 {
        self.hash(ton_types::MAX_LEVEL as usize)
    }

    pub fn deserialize(boc_db: Arc<CellStorage>, mut data: &[u8]) -> Result<Self> {
        // skip marker
        if data.is_empty() {
            return Err(StorageCellError::InvalidData.into());
        }
        data = &(*data)[1..];

        // deserialize cell
        let cell_data = ton_types::CellData::deserialize(&mut data)?;
        let references_count = data.read_byte()?;
        let mut references = SmallVec::with_capacity(references_count as usize);

        for _ in 0..references_count {
            let hash = UInt256::from(data.read_u256()?);
            references.push(StorageCellReference::Unloaded(hash));
        }

        let (tree_bits_count, tree_cell_count) = match data.read_le_u64() {
            Ok(tree_bits_count) => match data.read_le_u64() {
                Ok(tree_cell_count) => (tree_bits_count, tree_cell_count),
                Err(_) => (0, 0),
            },
            Err(_) => (0, 0),
        };

        Ok(Self {
            _c: Default::default(),
            cell_storage: boc_db,
            cell_data,
            references: RwLock::new(references),
            tree_bits_count,
            tree_cell_count,
        })
    }

    pub fn deserialize_marker_and_references(
        mut data: &[u8],
    ) -> Result<(u8, SmallVec<[StorageCellReference; 4]>)> {
        let reader = &mut data;

        // skip marker
        let mut marker = [0u8];
        reader.read_exact(&mut marker)?;

        // deserialize cell
        let _cell_data = ton_types::CellData::deserialize(reader)?;
        let references_count = reader.read_byte()?;
        let mut references = SmallVec::with_capacity(references_count as usize);

        for _ in 0..references_count {
            let hash = UInt256::from(reader.read_u256()?);
            references.push(StorageCellReference::Unloaded(hash));
        }
        Ok((marker[0], references))
    }

    pub fn serialize(marker: u8, cell: &dyn CellImpl) -> Result<Vec<u8>> {
        let mut data = SmallVec::<[u8; 512]>::with_capacity(512);

        // write marker
        data.write_all(&[marker])?;

        // serialize cell
        let references_count = cell.references_count() as u8;
        cell.cell_data().serialize(&mut data)?;
        data.write_all(&[references_count])?;

        for i in 0..references_count {
            data.write_all(cell.reference(i as usize)?.repr_hash().as_slice())?;
        }

        data.write_all(&cell.tree_bits_count().to_le_bytes())?;
        data.write_all(&cell.tree_cell_count().to_le_bytes())?;

        Ok(data.as_slice().to_vec())
    }

    pub fn reference(&self, index: usize) -> Result<Arc<StorageCell>> {
        let hash = match &self.references.read().get(index) {
            Some(StorageCellReference::Unloaded(hash)) => *hash,
            Some(StorageCellReference::Loaded(cell)) => return Ok(cell.clone()),
            None => return Err(StorageCellError::AccessingInvalidReference.into()),
        };

        let storage_cell = self.cell_storage.load_cell(hash)?;
        self.references.write()[index] = StorageCellReference::Loaded(storage_cell.clone());

        Ok(storage_cell)
    }
}

impl CellImpl for StorageCell {
    fn data(&self) -> &[u8] {
        self.cell_data.data()
    }

    fn cell_data(&self) -> &ton_types::CellData {
        &self.cell_data
    }

    fn bit_length(&self) -> usize {
        self.cell_data.bit_length() as usize
    }

    fn references_count(&self) -> usize {
        self.references.read().len()
    }

    fn reference(&self, index: usize) -> Result<ton_types::Cell> {
        Ok(ton_types::Cell::with_cell_impl_arc(self.reference(index)?))
    }

    fn cell_type(&self) -> ton_types::CellType {
        self.cell_data.cell_type()
    }

    fn level_mask(&self) -> ton_types::LevelMask {
        self.cell_data.level_mask()
    }

    fn hash(&self, index: usize) -> UInt256 {
        self.cell_data.hash(index)
    }

    fn depth(&self, index: usize) -> u16 {
        self.cell_data.depth(index)
    }

    fn store_hashes(&self) -> bool {
        self.cell_data.store_hashes()
    }

    fn tree_bits_count(&self) -> u64 {
        self.tree_bits_count
    }

    fn tree_cell_count(&self) -> u64 {
        self.tree_cell_count
    }
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        self.cell_storage.drop_cell(&self.repr_hash())
    }
}

impl PartialEq for StorageCell {
    fn eq(&self, other: &Self) -> bool {
        if self.cell_data != other.cell_data {
            return false;
        }

        let references = self.references.read();
        let other_references = self.references.read();
        references.len() == other_references.len()
            && same_references(&references, &other_references)
    }
}

fn same_references(
    references: &[StorageCellReference],
    other_references: &[StorageCellReference],
) -> bool {
    for i in 0..references.len() {
        if references[i].hash() != other_references[i].hash() {
            return false;
        }
    }
    true
}

#[derive(Clone, PartialEq)]
pub enum StorageCellReference {
    Loaded(Arc<StorageCell>),
    Unloaded(UInt256),
}

impl StorageCellReference {
    pub fn hash(&self) -> UInt256 {
        match self {
            Self::Loaded(cell) => cell.repr_hash(),
            Self::Unloaded(hash) => *hash,
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum StorageCellError {
    #[error("Invalid data")]
    InvalidData,
    #[error("Accessing invalid cell reference")]
    AccessingInvalidReference,
}
