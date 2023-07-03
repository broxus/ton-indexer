use std::cell::UnsafeCell;
use std::collections::hash_map;
use std::mem::ManuallyDrop;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Weak};

use anyhow::Result;
use bumpalo::Bump;
use bytes::Bytes;
use everscale_types::cell::*;
use quick_cache::sync::Cache;

use crate::db::*;
use crate::utils::{CacheStats, FastDashMap, FastHashMap, FastHasherState};

pub struct CellStorage {
    db: Arc<Db>,
    cells_cache: Arc<FastDashMap<HashBytes, Weak<StorageCell>>>,
    raw_cells_cache: RawCellsCache,
}

impl CellStorage {
    pub fn new(db: Arc<Db>, cache_size_bytes: u64) -> Result<Arc<Self>> {
        let cells_cache = Default::default();
        let raw_cells_cache = RawCellsCache::new(cache_size_bytes);

        Ok(Arc::new(Self {
            db,
            cells_cache,
            raw_cells_cache,
        }))
    }

    pub fn store_cell(
        &self,
        batch: &mut rocksdb::WriteBatch,
        root: Cell,
    ) -> Result<usize, CellStorageError> {
        struct CellWithRefs<'a> {
            rc: u32,
            data: Option<&'a [u8]>,
        }

        struct Context<'a> {
            cells_cf: &'a BoundedCfHandle<'a>,
            alloc: &'a Bump,
            transaction: FastHashMap<HashBytes, CellWithRefs<'a>>,
            buffer: Vec<u8>,
        }

        impl Context<'_> {
            fn insert_cell<V>(
                &mut self,
                key: &HashBytes,
                cell: &DynCell,
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
                            if StorageCell::serialize_to(cell, &mut self.buffer).is_err() {
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

            fn finalize(
                mut self,
                batch: &mut rocksdb::WriteBatch,
                raw_cache: &RawCellsCache,
            ) -> usize {
                let total = self.transaction.len();
                for (key, CellWithRefs { rc, data }) in self.transaction {
                    self.buffer.clear();
                    refcount::add_positive_refount(rc, data, &mut self.buffer);
                    if data.is_some() {
                        raw_cache.insert(key, &self.buffer);
                    }
                    batch.merge_cf(self.cells_cf, &key.0, &self.buffer);
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
            match cells.get(&key.0) {
                Ok(value) => {
                    if !ctx.insert_cell(key, root.as_ref(), value.as_deref())? {
                        return Ok(0);
                    }
                }
                Err(e) => return Err(CellStorageError::Internal(e)),
            }
        }

        let mut stack = Vec::with_capacity(16);
        stack.push(root.as_ref());

        // Check other cells
        while let Some(current) = stack.pop() {
            for cell in current.references() {
                let key = cell.repr_hash();
                match cells.get(&key.0) {
                    Ok(value) => {
                        if !ctx.insert_cell(key, cell, value.as_deref())? {
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
        Ok(ctx.finalize(batch, &self.raw_cells_cache))
    }

    pub fn load_cell(
        self: &Arc<Self>,
        hash: &HashBytes,
    ) -> Result<Arc<StorageCell>, CellStorageError> {
        if let Some(cell) = self.cells_cache.get(hash) {
            if let Some(cell) = cell.upgrade() {
                return Ok(cell);
            }
        }

        let cell = match self.raw_cells_cache.get_raw(self.db.as_ref(), hash) {
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
        self.cells_cache.insert(*hash, Arc::downgrade(&cell));

        Ok(cell)
    }

    pub fn remove_cell(
        &self,
        batch: &mut rocksdb::WriteBatch,
        alloc: &Bump,
        hash: &HashBytes,
    ) -> Result<usize, CellStorageError> {
        #[derive(Clone, Copy)]
        struct CellState<'a> {
            rc: i64,
            removes: u32,
            refs: &'a [HashBytes],
        }

        impl<'a> CellState<'a> {
            fn remove(&mut self) -> Result<Option<&'a [HashBytes]>, CellStorageError> {
                self.removes += 1;
                if self.removes as i64 <= self.rc {
                    Ok(self.next_refs())
                } else {
                    Err(CellStorageError::CounterMismatch)
                }
            }

            fn next_refs(&self) -> Option<&'a [HashBytes]> {
                if self.rc > self.removes as i64 {
                    None
                } else {
                    Some(self.refs)
                }
            }
        }

        let cells = &self.db.cells;
        let cells_cf = &cells.cf();

        let mut transaction: FastHashMap<&HashBytes, CellState> =
            FastHashMap::with_capacity_and_hasher(128, Default::default());
        let mut buffer = Vec::with_capacity(4);

        let mut stack = Vec::with_capacity(16);
        stack.push(hash);

        // While some cells left
        while let Some(cell_id) = stack.pop() {
            let refs = match transaction.entry(cell_id) {
                hash_map::Entry::Occupied(mut v) => v.get_mut().remove()?,
                hash_map::Entry::Vacant(v) => {
                    let rc = match self.db.cells.get(&cell_id.0) {
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

    pub fn drop_cell(&self, hash: &HashBytes) {
        self.cells_cache.remove(hash);
    }

    pub fn cache_stats(&self) -> CacheStats {
        let hits = self.raw_cells_cache.0.hits();
        let misses = self.raw_cells_cache.0.misses();
        let occupied = self.raw_cells_cache.0.len() as u64;
        let weight = self.raw_cells_cache.0.weight();

        let hits_ratio = if hits > 0 {
            hits as f64 / (hits + misses) as f64
        } else {
            0.0
        } * 100.0;
        CacheStats {
            hits,
            misses,
            requests: hits + misses,
            occupied,
            hits_ratio,
            size_bytes: weight,
        }
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
    cell_storage: Arc<CellStorage>,
    bit_len: u16,
    descriptor: CellDescriptor,
    data: Vec<u8>,

    reference_states: [AtomicU8; 4],
    reference_data: [UnsafeCell<StorageCellReferenceData>; 4],
}

impl StorageCell {
    const REF_INCOMPLETE: u8 = 0x0;
    const REF_RUNNING: u8 = 0x1;
    const REF_COMPLETE: u8 = 0x2;

    pub fn deserialize(boc_db: Arc<CellStorage>, mut data: &[u8]) -> Result<Self> {
        // // deserialize cell
        // let cell_data = ton_types::CellData::deserialize(&mut data)?;
        // let references_count = cell_data.references_count();
        // let mut references = SmallVec::with_capacity(references_count);

        // for _ in 0..references_count {
        //     let hash = UInt256::from(data.read_u256()?);
        //     references.push(StorageCellReference::Unloaded(hash));
        // }

        // let (tree_bits_count, tree_cell_count) = match data.read_le_u64() {
        //     Ok(tree_bits_count) => match data.read_le_u64() {
        //         Ok(tree_cell_count) => (tree_bits_count, tree_cell_count),
        //         Err(_) => (0, 0),
        //     },
        //     Err(_) => (0, 0),
        // };

        // Ok(Self {
        //     _c: Default::default(),
        //     cell_storage: boc_db,
        //     cell_data,
        //     references: RwLock::new(references),
        //     tree_bits_count,
        //     tree_cell_count,
        // })

        todo!()
    }

    pub fn deserialize_references(mut data: &[u8], target: &mut Vec<HashBytes>) -> bool {
        // let reader = &mut data;

        // let references_count = match ton_types::CellData::deserialize(reader) {
        //     Ok(data) => data.references_count(),
        //     Err(_) => return false,
        // };

        // for _ in 0..references_count {
        //     let Ok(hash) = reader.read_u256() else {
        //         return false;
        //     };
        //     target.push(hash);
        // }

        // true

        todo!()
    }

    pub fn serialize_to(cell: &DynCell, target: &mut Vec<u8>) -> Result<()> {
        target.clear();

        // // serialize cell
        // let references_count = cell.references_count();
        // cell.cell_data().serialize(target)?;

        // for i in 0..references_count {
        //     target.extend_from_slice(cell.reference(i)?.repr_hash().as_slice());
        // }

        // target.extend_from_slice(&cell.tree_bits_count().to_le_bytes());
        // target.extend_from_slice(&cell.tree_cell_count().to_le_bytes());

        // Ok(())

        todo!()
    }

    pub fn reference_raw(&self, index: u8) -> Option<&Arc<StorageCell>> {
        if index > 3 || index >= self.descriptor.reference_count() {
            return None;
        }

        let state = &self.reference_states[index as usize];
        let slot = self.reference_data[index as usize].get();

        if state.load(Ordering::Acquire) == Self::REF_COMPLETE {
            return Some(unsafe { &(*slot).cell });
        }

        let mut res = Ok(());
        Self::initialize_inner(state, &mut || match self
            .cell_storage
            .load_cell(unsafe { &(*slot).hash })
        {
            Ok(cell) => unsafe {
                *slot = StorageCellReferenceData {
                    cell: ManuallyDrop::new(cell),
                };
                true
            },
            Err(err) => {
                res = Err(err);
                false
            }
        });

        // TODO: just return none?
        res.unwrap();

        Some(unsafe { &(*slot).cell })
    }

    // Note: this is intentionally monomorphic
    #[inline(never)]
    fn initialize_inner(state: &AtomicU8, init: &mut dyn FnMut() -> bool) {
        struct Guard<'a> {
            state: &'a AtomicU8,
            new_state: u8,
        }

        impl<'a> Drop for Guard<'a> {
            fn drop(&mut self) {
                self.state.store(self.new_state, Ordering::Release);
                unsafe {
                    let key = self.state as *const AtomicU8 as usize;
                    parking_lot_core::unpark_all(key, parking_lot_core::DEFAULT_UNPARK_TOKEN);
                }
            }
        }

        loop {
            let exchange = state.compare_exchange_weak(
                Self::REF_INCOMPLETE,
                Self::REF_RUNNING,
                Ordering::Acquire,
                Ordering::Acquire,
            );
            match exchange {
                Ok(_) => {
                    let mut guard = Guard {
                        state,
                        new_state: Self::REF_INCOMPLETE,
                    };
                    if init() {
                        guard.new_state = Self::REF_COMPLETE;
                    }
                    return;
                }
                Err(Self::REF_COMPLETE) => return,
                Err(Self::REF_RUNNING) => unsafe {
                    let key = state as *const AtomicU8 as usize;
                    parking_lot_core::park(
                        key,
                        || state.load(Ordering::Relaxed) == Self::REF_RUNNING,
                        || (),
                        |_, _| (),
                        parking_lot_core::DEFAULT_PARK_TOKEN,
                        None,
                    );
                },
                Err(Self::REF_INCOMPLETE) => (),
                Err(_) => debug_assert!(false),
            }
        }
    }
}

impl CellImpl for StorageCell {
    fn descriptor(&self) -> CellDescriptor {
        self.descriptor
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn bit_len(&self) -> u16 {
        self.bit_len
    }

    fn reference(&self, index: u8) -> Option<&DynCell> {
        Some(self.reference_raw(index)?.as_ref())
    }

    fn reference_cloned(&self, index: u8) -> Option<Cell> {
        Some(Cell::from(self.reference_raw(index)?.clone() as Arc<_>))
    }

    fn virtualize(&self) -> &DynCell {
        VirtualCellWrapper::wrap(self)
    }

    fn hash(&self, level: u8) -> &HashBytes {
        todo!()
    }

    fn depth(&self, level: u8) -> u16 {
        todo!()
    }

    fn take_first_child(&mut self) -> Option<Cell> {
        todo!()
    }

    fn replace_first_child(&mut self, parent: Cell) -> std::result::Result<Cell, Cell> {
        todo!()
    }

    fn take_next_child(&mut self) -> Option<Cell> {
        todo!()
    }
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        self.cell_storage.drop_cell(DynCell::repr_hash(self))
    }
}

unsafe impl Send for StorageCell {}
unsafe impl Sync for StorageCell {}

pub union StorageCellReferenceData {
    /// Incplmete state.
    hash: HashBytes,
    /// Complete state.
    cell: ManuallyDrop<Arc<StorageCell>>,
}

struct RawCellsCache(Cache<HashBytes, Bytes, CellSizeEstimator, FastHasherState>);

#[derive(Clone, Copy)]
struct CellSizeEstimator;
impl quick_cache::Weighter<HashBytes, (), Bytes> for CellSizeEstimator {
    fn weight(&self, _: &HashBytes, _: &(), val: &Bytes) -> NonZeroU32 {
        const BYTES_SIZE: usize = std::mem::size_of::<usize>() * 4;
        let len = 32 + val.len() + BYTES_SIZE;

        NonZeroU32::new(len as u32).expect("Key is not empty")
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
        );

        Self(raw_cache)
    }

    fn get_raw(&self, db: &Db, key: &HashBytes) -> Result<Option<Bytes>, rocksdb::Error> {
        if let Some(value) = self.0.get(key) {
            return Ok(Some(value));
        }

        let value = db
            .cells
            .get(&key.0)?
            .map(|v| Bytes::copy_from_slice(v.as_ref()));
        if let Some(value) = &value {
            self.0.insert(*key, value.clone());
        }

        Ok(value)
    }

    pub fn insert(&self, key: HashBytes, value: &[u8]) {
        let value = Bytes::copy_from_slice(value);
        self.0.insert(key, value);
    }
}
