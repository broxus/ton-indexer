use std::collections::hash_map;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::Result;
use bumpalo::Bump;
use parking_lot::RwLock;
use quick_cache::sync::{Cache, DefaultLifecycle};
use smallvec::SmallVec;
use ton_types::{ByteOrderRead, CellImpl, UInt256};
use triomphe::ThinArc;

use crate::db::*;
use crate::utils::{CellExt, FastDashMap, FastHashMap, FastHasherState};

pub struct CellStorage {
    db: Arc<Db>,
    cells_cache: Arc<FastDashMap<UInt256, Weak<StorageCell>>>,
    raw_cells_cache: RawCellsCache,
}

impl CellStorage {
    pub fn new(db: Arc<Db>, cache_size_bytes: u64) -> Arc<Self> {
        let cells_cache = Default::default();
        let raw_cells_cache = RawCellsCache::new(cache_size_bytes);

        let this = Arc::new(Self {
            db,
            cells_cache,
            raw_cells_cache,
        });

        {
            // Cell storage metrics update loop
            const UPDATE_INTERVAL: Duration = Duration::from_secs(5);

            let this = Arc::downgrade(&this);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(UPDATE_INTERVAL);
                loop {
                    interval.tick().await;
                    let Some(this) = this.upgrade() else {
                        break;
                    };
                    this.update_metrics();
                }
            });
        }

        this
    }

    fn update_metrics(&self) {
        crate::set_metrics! {
            "cells_cache_hits" => self.raw_cells_cache.0.hits(),
            "cells_cache_requests" => self.raw_cells_cache.0.misses() + self.raw_cells_cache.0.hits(),
            "cells_cache_occupied" => self.raw_cells_cache.0.len() ,
            "cells_cache_size_bytes" => self.raw_cells_cache.0.weight(),
            "cells_cache_hits_ratio" => self.raw_cells_cache.hit_ratio(),
        }
    }

    pub fn store_cell(
        &self,
        batch: &mut rocksdb::WriteBatch,
        root: ton_types::Cell,
    ) -> Result<usize, CellStorageError> {
        struct CellWithRefs<'a> {
            rc: u32,
            // TODO: always copy data?
            data: Option<&'a [u8]>,
        }

        struct Context<'a> {
            db: &'a Db,
            raw_cache: &'a RawCellsCache,
            alloc: &'a Bump,
            transaction: FastHashMap<[u8; 32], CellWithRefs<'a>>,
            buffer: Vec<u8>,
        }

        impl Context<'_> {
            fn insert_cell(
                &mut self,
                key: &[u8; 32],
                cell: &ton_types::Cell,
                depth: usize,
            ) -> Result<bool, CellStorageError> {
                Ok(match self.transaction.entry(*key) {
                    hash_map::Entry::Occupied(mut value) => {
                        value.get_mut().rc += 1;
                        false
                    }
                    hash_map::Entry::Vacant(entry) => {
                        // A constant which tells since which depth we should start to use cache.
                        // This method is used mostly for inserting new states, so we can assume
                        // that first N levels will mostly be new.
                        //
                        // This value was chosen empirically.
                        const NEW_CELLS_DEPTH_THRESHOLD: usize = 4;

                        let (old_rc, has_value) = 'value: {
                            if depth >= NEW_CELLS_DEPTH_THRESHOLD {
                                // NOTE: `get` here is used to affect a "hotness" of the value, because
                                // there is a big chance that we will need it soon during state processing
                                if let Some(entry) = self.raw_cache.0.get(key) {
                                    let rc = entry.header.header.load(Ordering::Acquire);
                                    break 'value (rc, rc > 0);
                                }
                            }

                            match self.db.cells.get(key).map_err(CellStorageError::Internal)? {
                                Some(value) => {
                                    let (rc, value) =
                                        refcount::decode_value_with_rc(value.as_ref());
                                    (rc, value.is_some())
                                }
                                None => (0, false),
                            }
                        };

                        // TODO: lower to `debug_assert` when sure
                        assert!(has_value && old_rc > 0 || !has_value && old_rc == 0);

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
                let cells_cf = &self.db.cells.cf();
                for (key, CellWithRefs { rc, data }) in self.transaction {
                    self.buffer.clear();
                    refcount::add_positive_refount(rc, data, &mut self.buffer);
                    if let Some(data) = data {
                        self.raw_cache.insert(&key, rc, data);
                    } else {
                        self.raw_cache.add_refs(&key, rc);
                    }
                    batch.merge_cf(cells_cf, key.as_slice(), &self.buffer);
                }
                total
            }
        }

        // Prepare context and handles
        let alloc = Bump::new();

        let mut ctx = Context {
            db: &self.db,
            raw_cache: &self.raw_cells_cache,
            alloc: &alloc,
            transaction: FastHashMap::with_capacity_and_hasher(128, Default::default()),
            buffer: Vec::with_capacity(512),
        };

        // Check root cell
        {
            let key = root.repr_hash();
            let key = key.as_array();

            if !ctx.insert_cell(key, &root, 0)? {
                return Ok(0);
            }
        }

        let mut stack = Vec::with_capacity(16);
        stack.push(root.into_references());

        // Check other cells
        'outer: loop {
            let depth = stack.len();
            let Some(iter) = stack.last_mut() else {
                break;
            };

            for child in &mut *iter {
                let key = child.repr_hash();
                let key = key.as_array();

                if ctx.insert_cell(key, &child, depth)? {
                    stack.push(child.into_references());
                    continue 'outer;
                }
            }

            stack.pop();
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
            if let Some(cell) = cell.upgrade() {
                return Ok(cell);
            }
        }

        let cell = match self
            .raw_cells_cache
            .get_raw(self.db.as_ref(), hash.as_slice())
        {
            Ok(value) => 'cell: {
                if let Some(value) = value {
                    let rc = &value.header.header;
                    if rc.load(Ordering::Acquire) > 0 {
                        match StorageCell::deserialize(self.clone(), &value.slice) {
                            Ok(cell) => break 'cell Arc::new(cell),
                            Err(_) => return Err(CellStorageError::InvalidCell),
                        }
                    }
                }
                return Err(CellStorageError::CellNotFound);
            }
            Err(e) => return Err(CellStorageError::Internal(e)),
        };
        self.cells_cache.insert(hash, Arc::downgrade(&cell));

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
                    let rc =
                        self.raw_cells_cache
                            .get_raw_for_delete(&self.db, cell_id, &mut buffer)?;
                    debug_assert!(rc > 0);

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
            self.raw_cells_cache.remove_refs(key, removes);
            batch.merge_cf(
                cells_cf,
                key.as_slice(),
                refcount::encode_negative_refcount(removes),
            );
        }
        Ok(total)
    }

    pub fn drop_cell(&self, hash: &UInt256) {
        self.cells_cache.remove(hash);
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
        let references_count = cell_data.references_count();
        let mut references = SmallVec::with_capacity(references_count);

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

        let references_count = match ton_types::CellData::deserialize(reader) {
            Ok(data) => data.references_count(),
            Err(_) => return false,
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
        let references_count = cell.references_count();
        cell.cell_data().serialize(target)?;

        for i in 0..references_count {
            target.extend_from_slice(cell.reference(i)?.repr_hash().as_slice());
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

    fn raw_data(&self) -> ton_types::Result<&[u8]> {
        Ok(self.cell_data.raw_data())
    }

    fn cell_data(&self) -> &ton_types::CellData {
        &self.cell_data
    }

    fn bit_length(&self) -> usize {
        self.cell_data.bit_length()
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

struct RawCellsCache(Cache<[u8; 32], RawCellsCacheItem, CellSizeEstimator, FastHasherState>);

impl RawCellsCache {
    pub(crate) fn hit_ratio(&self) -> f64 {
        (if self.0.hits() > 0 {
            self.0.hits() as f64 / (self.0.hits() + self.0.misses()) as f64
        } else {
            0.0
        }) * 100.0
    }
}

type RawCellsCacheItem = ThinArc<AtomicI64, u8>;

#[derive(Clone, Copy)]
pub struct CellSizeEstimator;
impl quick_cache::Weighter<[u8; 32], RawCellsCacheItem> for CellSizeEstimator {
    fn weight(&self, key: &[u8; 32], val: &RawCellsCacheItem) -> u32 {
        const STATIC_SIZE: usize = std::mem::size_of::<RawCellsCacheItem>()
            + std::mem::size_of::<i64>()
            + std::mem::size_of::<usize>() * 2; // ArcInner refs + HeaderWithLength length

        let len = key.len() + val.slice.len() + STATIC_SIZE;
        len as u32
    }
}

impl RawCellsCache {
    fn new(size_in_bytes: u64) -> Self {
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

        let estimated_cell_cache_capacity = size_in_bytes / (KEY_SIZE + MAX_CELL_SIZE);
        tracing::info!(
            estimated_cell_cache_capacity,
            max_cell_cache_size = %bytesize::ByteSize(size_in_bytes),
        );

        let raw_cache = Cache::with(
            estimated_cell_cache_capacity as usize,
            size_in_bytes,
            CellSizeEstimator,
            FastHasherState::default(),
            DefaultLifecycle::default(),
        );

        Self(raw_cache)
    }

    fn get_raw(
        &self,
        db: &Db,
        key: &[u8; 32],
    ) -> Result<Option<RawCellsCacheItem>, rocksdb::Error> {
        use quick_cache::GuardResult;

        match self.0.get_value_or_guard(key, None) {
            GuardResult::Value(value) => Ok(Some(value)),
            GuardResult::Guard(g) => Ok(if let Some(value) = db.cells.get(key)? {
                let (rc, data) = refcount::decode_value_with_rc(value.as_ref());
                data.map(|value| {
                    let value = RawCellsCacheItem::from_header_and_slice(AtomicI64::new(rc), value);
                    _ = g.insert(value.clone());
                    value
                })
            } else {
                None
            }),
            GuardResult::Timeout => unreachable!(),
        }
    }

    fn get_raw_for_delete(
        &self,
        db: &Db,
        key: &[u8; 32],
        refs_buffer: &mut Vec<[u8; 32]>,
    ) -> Result<i64, CellStorageError> {
        refs_buffer.clear();

        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(value) = self.0.peek(key) {
            let rc = value.header.header.load(Ordering::Acquire);
            if rc <= 0 {
                return Err(CellStorageError::CellNotFound);
            }

            StorageCell::deserialize_references(&value.slice, refs_buffer)
                .then_some(rc)
                .ok_or(CellStorageError::InvalidCell)
        } else {
            match db.cells.get(key) {
                Ok(value) => {
                    if let Some(value) = value {
                        if let (rc, Some(value)) = refcount::decode_value_with_rc(&value) {
                            return StorageCell::deserialize_references(value, refs_buffer)
                                .then_some(rc)
                                .ok_or(CellStorageError::InvalidCell);
                        }
                    }

                    Err(CellStorageError::CellNotFound)
                }
                Err(e) => Err(CellStorageError::Internal(e)),
            }
        }
    }

    fn insert(&self, key: &[u8; 32], refs: u32, value: &[u8]) {
        let value = RawCellsCacheItem::from_header_and_slice(AtomicI64::new(refs as _), value);
        self.0.insert(*key, value);
    }

    fn add_refs(&self, key: &[u8; 32], refs: u32) {
        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(v) = self.0.peek(key) {
            v.header.header.fetch_add(refs as i64, Ordering::Release);
        }
    }

    fn remove_refs(&self, key: &[u8; 32], refs: u32) {
        // NOTE: `peek` here is used to avoid affecting a "hotness" of the value
        if let Some(v) = self.0.peek(key) {
            let old_refs = v.header.header.fetch_sub(refs as i64, Ordering::Release);
            debug_assert!(old_refs >= refs as i64);
        }
    }
}
