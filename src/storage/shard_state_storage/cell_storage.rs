use std::collections::hash_map;
use std::sync::Arc;

use anyhow::Result;
use bumpalo::Bump;
use bytes::Bytes;
use histogram::Histogram;
use parking_lot::RwLock;
use smallvec::SmallVec;
use ton_types::{ByteOrderRead, CellImpl, UInt256};

use crate::db::*;
use crate::utils::{print_hist, FastHashMap, FastLruCache};

pub struct CellStorage {
    db: Arc<Db>,
    cells_cache: Arc<FastLruCache<UInt256, Arc<StorageCell>>>,
    raw_getter: CellStorageRawCached,
}

// Percentile 0.1%    from 96 to 127  => 1725119 count
// Percentile 10%     from 128 to 191  => 82838849 count
// Percentile 25%     from 128 to 191  => 82838849 count
// Percentile 50%     from 128 to 191  => 82838849 count
// Percentile 75%     from 128 to 191  => 82838849 count
// Percentile 90%     from 192 to 255  => 22775080 count
// Percentile 95%     from 192 to 255  => 22775080 count
// Percentile 99%     from 192 to 255  => 22775080 count
// Percentile 99.9%   from 256 to 383  => 484002 count
// Percentile 99.99%  from 256 to 383  => 484002 count
// Percentile 99.999% from 256 to 383  => 484002 count

// from 64  to 95  - 15_267
// from 96  to 127 - 1_725_119
// from 128 to 191 - 82_838_849
// from 192 to 255 - 22_775_080
// from 256 to 383 - 484_002

// we assume that 75% of cells are in range 128..191
// so we can use use 192 as size for value in cache

const MAX_CELL_SIZE: u64 = 192;
const KEY_SIZE: u64 = 32;

struct CellStorageRawCached {
    db: Arc<Db>,
    raw_cache: Arc<FastLruCache<[u8; 32], Bytes>>,
    hist: Arc<Histogram>,
}

impl CellStorageRawCached {
    fn new(db: Arc<Db>, size_in_bytes: u64) -> Self {
        let num_cells_in_cache = (size_in_bytes / (KEY_SIZE + MAX_CELL_SIZE)) as u32;
        tracing::info!("num_cells_in_cache: {}", num_cells_in_cache);
        const NSEC_IN_SEC: u64 = 1_000_000_000;
        let hist = Arc::new(
            Histogram::builder()
                .min_resolution(50) // 32 buckets
                .maximum_value(NSEC_IN_SEC)
                .build()
                .unwrap(),
        );

        {
            let hist = hist.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    print_hist(&hist);
                }
            });
        }

        Self {
            db,
            raw_cache: Arc::new(FastLruCache::new(num_cells_in_cache)),
            hist,
        }
    }

    fn get_raw(&self, key: &[u8; 32]) -> Result<Option<Bytes>, rocksdb::Error> {
        let start = std::time::Instant::now();
        if let Some(value) = self.raw_cache.get(key) {
            let end = start.elapsed().as_nanos();
            self.hist.increment(end as u64, 1).unwrap();
            return Ok(Some(value));
        }

        let value = self
            .db
            .cells
            .get(key)?
            .map(|v| Bytes::copy_from_slice(v.as_ref()));
        if let Some(value) = &value {
            self.raw_cache.insert(*key, value.clone());
        }
        let end = start.elapsed().as_nanos();
        self.hist
            .increment(
                if end > 1_000_000_000 {
                    1_000_000_000
                } else {
                    end
                } as u64,
                1,
            )
            .ok();

        Ok(value)
    }
}

impl CellStorage {
    pub fn new(db: Arc<Db>, cache_size_bytes: u64) -> Result<Arc<Self>> {
        let cache = Arc::new(FastLruCache::new(10_000)); //todo: make it configurable / measure memory usage
        {
            let cache = cache.clone();

            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    tracing::warn!("CellStorage cache size: {:?}", cache.stats());
                }
            });
        }

        Ok(Arc::new(Self {
            raw_getter: CellStorageRawCached::new(db.clone(), cache_size_bytes),
            db,
            cells_cache: cache,
        }))
    }

    pub fn store_cell(
        &self,
        batch: &mut rocksdb::WriteBatch,
        root: ton_types::Cell,
    ) -> Result<usize, CellStorageError> {
        struct CellWithRefs<'a> {
            rc: u32,
            data: Option<&'a [u8]>,
        }

        struct Context<'a> {
            cells_cf: &'a BoundedCfHandle<'a>,
            alloc: &'a Bump,
            transaction: FastHashMap<[u8; 32], CellWithRefs<'a>>,
            buffer: Vec<u8>,
        }

        impl Context<'_> {
            fn insert_cell<V>(
                &mut self,
                key: &[u8; 32],
                cell: &ton_types::Cell,
                value: Option<V>,
            ) -> Result<bool, CellStorageError>
            where
                V: AsRef<[u8]>,
            {
                Ok(match self.transaction.entry(*key) {
                    hash_map::Entry::Occupied(mut value) => {
                        value.get_mut().rc += 1;
                        false
                    }
                    hash_map::Entry::Vacant(entry) => {
                        let has_value =
                            matches!(value, Some(value) if refcount::has_value(value.as_ref()));

                        let data = if !has_value {
                            self.buffer.clear();
                            if StorageCell::serialize_to(&**cell, &mut self.buffer).is_err() {
                                return Err(CellStorageError::InvalidCell);
                            }
                            Some(self.alloc.alloc_slice_copy(self.buffer.as_slice()) as &[u8])
                        } else {
                            None
                        };
                        entry.insert(CellWithRefs { rc: 1, data });
                        !has_value
                    }
                })
            }

            fn finalize(mut self, batch: &mut rocksdb::WriteBatch) -> usize {
                let total = self.transaction.len();
                for (key, CellWithRefs { rc, data }) in self.transaction {
                    self.buffer.clear();
                    refcount::add_positive_refount(rc, data, &mut self.buffer);
                    batch.merge_cf(self.cells_cf, key.as_slice(), &self.buffer);
                }
                total
            }
        }

        // Prepare context and handles
        let alloc = Bump::new();
        let cells = &self.db.cells;
        let cells_cf = &cells.cf();

        let mut ctx = Context {
            cells_cf,
            alloc: &alloc,
            transaction: FastHashMap::with_capacity_and_hasher(128, Default::default()),
            buffer: Vec::with_capacity(512),
        };

        // Check root cell
        {
            let key = root.repr_hash();
            let key = key.as_array();

            match self.raw_getter.get_raw(key) {
                Ok(value) => {
                    if !ctx.insert_cell(key, &root, value.as_deref())? {
                        return Ok(0);
                    }
                }
                Err(e) => return Err(CellStorageError::Internal(e)),
            }
        }

        let mut stack = Vec::with_capacity(16);
        stack.push(root);

        // Check other cells
        while let Some(current) = stack.pop() {
            for i in 0..current.references_count() {
                let cell = match current.reference(i) {
                    Ok(cell) => cell,
                    Err(_) => return Err(CellStorageError::InvalidCell),
                };
                let key = cell.repr_hash();
                let key = key.as_array();

                match self.raw_getter.get_raw(key) {
                    Ok(value) => {
                        if !ctx.insert_cell(key, &cell, value.as_deref())? {
                            continue;
                        }
                    }
                    Err(e) => return Err(CellStorageError::Internal(e)),
                }

                stack.push(cell);
            }
        }

        // Clear big chunks of data before finalization
        drop(stack);

        // Write transaction to the `WriteBatch`
        Ok(ctx.finalize(batch))
    }

    pub fn load_cell(
        self: &Arc<Self>,
        hash: UInt256,
    ) -> Result<Arc<StorageCell>, CellStorageError> {
        if let Some(cell) = self.cells_cache.get(&hash) {
            return Ok(cell);
        }

        let cell = match self.raw_getter.get_raw(hash.as_slice()) {
            Ok(value) => 'cell: {
                if let Some(value) = value {
                    if let Some(value) = refcount::strip_refcount(&value) {
                        match StorageCell::deserialize(self.clone(), value) {
                            Ok(cell) => break 'cell Arc::new(cell),
                            Err(_) => return Err(CellStorageError::InvalidCell),
                        }
                    }
                }
                return Err(CellStorageError::CellNotFound);
            }
            Err(e) => return Err(CellStorageError::Internal(e)),
        };
        self.cells_cache.insert(hash, cell.clone());

        Ok(cell)
    }

    pub fn remove_cell(
        &self,
        batch: &mut rocksdb::WriteBatch,
        alloc: &Bump,
        hash: UInt256,
    ) -> Result<usize, CellStorageError> {
        #[derive(Clone, Copy)]
        struct CellState<'a> {
            rc: i64,
            removes: u32,
            refs: &'a [[u8; 32]],
        }

        impl<'a> CellState<'a> {
            fn remove(&mut self) -> Result<Option<&'a [[u8; 32]]>, CellStorageError> {
                self.removes += 1;
                if self.removes as i64 <= self.rc {
                    Ok(self.next_refs())
                } else {
                    Err(CellStorageError::CounterMismatch)
                }
            }

            fn next_refs(&self) -> Option<&'a [[u8; 32]]> {
                if self.rc > self.removes as i64 {
                    None
                } else {
                    Some(self.refs)
                }
            }
        }

        let cells = &self.db.cells;
        let cells_cf = &cells.cf();

        let mut transaction: FastHashMap<&[u8; 32], CellState> =
            FastHashMap::with_capacity_and_hasher(128, Default::default());
        let mut buffer = Vec::with_capacity(4);

        let mut stack = Vec::with_capacity(16);
        stack.push(hash.as_slice());

        // While some cells left
        while let Some(cell_id) = stack.pop() {
            let refs = match transaction.entry(cell_id) {
                hash_map::Entry::Occupied(mut v) => v.get_mut().remove()?,
                hash_map::Entry::Vacant(v) => {
                    let rc = match self.raw_getter.get_raw(cell_id) {
                        Ok(value) => 'rc: {
                            if let Some(value) = value {
                                buffer.clear();
                                if let (rc, Some(value)) = refcount::decode_value_with_rc(&value) {
                                    if StorageCell::deserialize_references(value, &mut buffer) {
                                        break 'rc rc;
                                    } else {
                                        return Err(CellStorageError::InvalidCell);
                                    }
                                }
                            }
                            return Err(CellStorageError::CellNotFound);
                        }
                        Err(e) => return Err(CellStorageError::Internal(e)),
                    };

                    v.insert(CellState {
                        rc,
                        removes: 1,
                        refs: alloc.alloc_slice_copy(buffer.as_slice()),
                    })
                    .next_refs()
                }
            };

            if let Some(refs) = refs {
                // Add all children
                for cell_id in refs {
                    // Unknown cell, push to the stack to process it
                    stack.push(cell_id);
                }
            }
        }

        // Clear big chunks of data before finalization
        drop(stack);

        // Write transaction to the `WriteBatch`
        let total = transaction.len();
        for (key, CellState { removes, .. }) in transaction {
            batch.merge_cf(
                cells_cf,
                key.as_slice(),
                refcount::encode_negative_refcount(removes),
            );
        }
        Ok(total)
    }

    pub fn drop_cell(&self, _hash: &UInt256) {
        // no op
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CellStorageError {
    #[error("Cell not found in cell db")]
    CellNotFound,
    #[error("Invalid cell")]
    InvalidCell,
    #[error("Cell counter mismatch")]
    CounterMismatch,
    #[error("Internal rocksdb error")]
    Internal(#[source] rocksdb::Error),
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
        self.hash(ton_types::MAX_LEVEL)
    }

    pub fn deserialize(boc_db: Arc<CellStorage>, mut data: &[u8]) -> Result<Self> {
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

    pub fn deserialize_references(mut data: &[u8], target: &mut Vec<[u8; 32]>) -> bool {
        let reader = &mut data;

        if ton_types::CellData::deserialize(reader).is_err() {
            return false;
        }
        let Ok(references_count) = reader.read_byte() else {
            return false;
        };

        for _ in 0..references_count {
            let Ok(hash) = reader.read_u256() else {
                return false;
            };
            target.push(hash);
        }

        true
    }

    pub fn serialize_to(cell: &dyn CellImpl, target: &mut Vec<u8>) -> Result<()> {
        target.clear();

        // serialize cell
        let references_count = cell.references_count() as u8;
        cell.cell_data().serialize(target)?;
        target.push(references_count);

        for i in 0..references_count {
            target.extend_from_slice(cell.reference(i as usize)?.repr_hash().as_slice());
        }

        target.extend_from_slice(&cell.tree_bits_count().to_le_bytes());
        target.extend_from_slice(&cell.tree_cell_count().to_le_bytes());

        Ok(())
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

#[derive(Clone)]
pub enum StorageCellReference {
    Loaded(Arc<StorageCell>),
    Unloaded(UInt256),
}

#[derive(thiserror::Error, Debug)]
enum StorageCellError {
    #[error("Accessing invalid cell reference")]
    AccessingInvalidReference,
}
