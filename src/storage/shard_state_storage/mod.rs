use std::ops::Deref;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use itertools::Itertools;
use smallvec::SmallVec;
use tokio::sync::RwLockWriteGuard;
use ton_types::UInt256;

use self::cell_storage::*;
use self::files_context::FilesContext;
use self::gc_state_storage::{GcState, GcStateStorage, LastShardBlockKey, Step};
use self::marker::Marker;
use self::replace_transaction::ShardStateReplaceTransaction;
use super::{BlockHandle, BlockHandleStorage, BlockStorage};
use crate::db::*;
use crate::utils::*;

mod cell_storage;
mod cell_writer;
mod entries_buffer;
mod files_context;
mod gc_state_storage;
mod marker;
mod replace_transaction;
mod shard_state_reader;

pub struct ShardStateStorage {
    db: Arc<Db>,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,
    cell_storage: Arc<CellStorage>,
    gc_state_storage: Arc<GcStateStorage>,

    downloads_dir: Arc<PathBuf>,

    current_marker: tokio::sync::RwLock<Marker>,
    min_ref_mc_state: Arc<MinRefMcState>,
    max_new_mc_cell_count: AtomicUsize,
    max_new_sc_cell_count: AtomicUsize,
}

impl ShardStateStorage {
    pub async fn new(
        db: Arc<Db>,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_storage: Arc<BlockStorage>,
        file_db_path: PathBuf,
    ) -> Result<Self> {
        let downloads_dir = prepare_file_db_dir(file_db_path, "downloads").await?;
        // let persistent_dir = prepare_file_db_dir(&file_db_path, "persistent").await?;

        let cell_storage = CellStorage::new(db.clone())?;
        let gc_state_storage = GcStateStorage::new(db.clone())?;

        let res = Self {
            db,
            block_handle_storage,
            block_storage,
            cell_storage,
            gc_state_storage,
            downloads_dir,
            current_marker: Default::default(),
            min_ref_mc_state: Arc::new(Default::default()),
            max_new_mc_cell_count: AtomicUsize::new(0),
            max_new_sc_cell_count: AtomicUsize::new(0),
        };

        let gc_state = res.gc_state_storage.load()?;
        match &gc_state.step {
            None => {
                tracing::info!("shard state GC is pending");
                *res.current_marker.write().await = gc_state.current_marker;
            }
            Some(step) => {
                let target_marker = gc_state
                    .current_marker
                    .next()
                    .context("Invalid target marker")?;

                let mut target_marker_lock = res.current_marker.write().await;
                *target_marker_lock = target_marker;

                match step {
                    Step::Mark(top_blocks) => {
                        res.mark(
                            gc_state.current_marker,
                            target_marker_lock,
                            top_blocks,
                            true,
                        )
                        .await?;

                        let target_marker_lock = res.current_marker.write().await;
                        res.sweep_cells(gc_state.current_marker, target_marker_lock, top_blocks)
                            .await?;

                        res.sweep_block_states(target_marker, top_blocks).await?;
                    }
                    Step::SweepCells(top_blocks) => {
                        res.sweep_cells(gc_state.current_marker, target_marker_lock, top_blocks)
                            .await?;
                        res.sweep_block_states(target_marker, top_blocks).await?;
                    }
                    Step::SweepBlocks(top_blocks) => {
                        res.sweep_block_states(target_marker, top_blocks).await?;
                    }
                }
            }
        };

        // Trigger compaction on load
        // res.trigger_compaction();

        // Done
        Ok(res)
    }

    pub fn metrics(&self) -> ShardStateStorageMetrics {
        #[cfg(feature = "count-cells")]
        let storage_cell = countme::get::<StorageCell>();

        ShardStateStorageMetrics {
            #[cfg(feature = "count-cells")]
            storage_cell_live_count: storage_cell.live,
            #[cfg(feature = "count-cells")]
            storage_cell_max_live_count: storage_cell.max_live,
            max_new_mc_cell_count: self.max_new_mc_cell_count.swap(0, Ordering::AcqRel),
            max_new_sc_cell_count: self.max_new_sc_cell_count.swap(0, Ordering::AcqRel),
        }
    }

    pub fn min_ref_mc_state(&self) -> &Arc<MinRefMcState> {
        &self.min_ref_mc_state
    }

    pub async fn store_state(
        &self,
        handle: &Arc<BlockHandle>,
        state: &ShardStateStuff,
    ) -> Result<bool> {
        if handle.id() != state.block_id() {
            return Err(ShardStateStorageError::BlockHandleIdMismatch.into());
        }

        if handle.meta().has_state() {
            return Ok(false);
        }

        let block_id = handle.id();
        let cell_id = state.root_cell().repr_hash();

        let mut batch = rocksdb::WriteBatch::default();

        let current_marker = self.current_marker.read().await;
        let marker = match block_id.seq_no {
            // Mark zero state as persistent
            0 => Marker::PERSISTENT,
            // Mark all other states with current marker
            _ => *current_marker,
        };

        let len = self
            .cell_storage
            .store_cell(&mut batch, marker, state.root_cell().clone())?;

        if block_id.shard_id.is_masterchain() {
            self.max_new_mc_cell_count.fetch_max(len, Ordering::Release);
        } else {
            self.max_new_sc_cell_count.fetch_max(len, Ordering::Release);
        }

        let mut value = [0; 32 * 3];
        value[..32].copy_from_slice(cell_id.as_slice());
        value[32..64].copy_from_slice(block_id.root_hash.as_slice());
        value[64..96].copy_from_slice(block_id.file_hash.as_slice());

        batch.put_cf(
            &self.db.shard_states.cf(),
            (block_id.shard_id, block_id.seq_no).to_vec(),
            value,
        );

        self.db.raw().write(batch)?;

        Ok(if handle.meta().set_has_state() {
            self.block_handle_storage.store_handle(handle)?;
            true
        } else {
            false
        })
    }

    pub async fn load_state(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Arc<ShardStateStuff>> {
        let shard_states = &self.db.shard_states;
        let shard_state = shard_states.get((block_id.shard_id, block_id.seq_no).to_vec())?;
        match shard_state {
            Some(root) => {
                let cell_id = UInt256::from_be_bytes(&root);
                let cell = self.cell_storage.load_cell(cell_id)?;

                ShardStateStuff::new(
                    block_id.clone(),
                    ton_types::Cell::with_cell_impl_arc(cell),
                    &self.min_ref_mc_state,
                )
                .map(Arc::new)
            }
            None => Err(ShardStateStorageError::NotFound.into()),
        }
    }

    pub async fn begin_replace(
        &'_ self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<(ShardStateReplaceTransaction<'_>, FilesContext)> {
        let ctx = FilesContext::new(self.downloads_dir.as_ref(), block_id).await?;

        Ok((
            ShardStateReplaceTransaction::new(
                &self.db,
                &self.cell_storage,
                &self.min_ref_mc_state,
                Marker::PERSISTENT,
            ),
            ctx,
        ))
    }

    /// Searches for an edge with the least referenced masterchain block
    ///
    /// Returns `None` if all states are recent enough
    pub async fn compute_recent_blocks(&self, mut mc_seq_no: u32) -> Result<Option<TopBlocks>> {
        // 0. Adjust masterchain seqno with minimal referenced masterchain state
        if let Some(min_ref_mc_seqno) = self.min_ref_mc_state.seq_no() {
            if min_ref_mc_seqno < mc_seq_no {
                mc_seq_no = min_ref_mc_seqno;
            }
        }

        // 1. Find target block

        // Find block id using states table
        let mc_block_id = match self
            .find_mc_block_id(mc_seq_no)
            .context("Failed to find block id by seqno")?
        {
            Some(block_id) => block_id,
            None => return Ok(None),
        };

        // Find block handle
        let handle = match self.block_handle_storage.load_handle(&mc_block_id)? {
            Some(handle) if handle.meta().has_data() => handle,
            // Skip blocks without handle or data
            _ => return Ok(None),
        };

        // 2. Find minimal referenced masterchain block from the target block

        let block_data = self.block_storage.load_block_data(&handle).await?;
        let block_info = block_data
            .block()
            .read_info()
            .context("Failed to read target block info")?;

        // Find full min masterchain reference id
        let min_ref_mc_seqno = block_info.min_ref_mc_seqno();
        let min_ref_block_id = match self.find_mc_block_id(min_ref_mc_seqno)? {
            Some(block_id) => block_id,
            None => return Ok(None),
        };

        // Find block handle
        let min_ref_block_handle = match self
            .block_handle_storage
            .load_handle(&min_ref_block_id)
            .context("Failed to find min ref mc block handle")?
        {
            Some(handle) if handle.meta().has_data() => handle,
            // Skip blocks without handle or data
            _ => return Ok(None),
        };

        // Compute `TopBlocks` from block data
        self.block_storage
            .load_block_data(&min_ref_block_handle)
            .await
            .and_then(|block_data| TopBlocks::from_mc_block(&block_data))
            .map(Some)
    }

    pub async fn remove_outdated_states(&self, mc_seq_no: u32) -> Result<TopBlocks> {
        // Compute recent block ids for the specified masterchain seqno
        let top_blocks = self
            .compute_recent_blocks(mc_seq_no)
            .await?
            .context("Recent blocks edge not found")?;

        tracing::info!(
            block_id = %top_blocks.mc_block.display(),
            "starting shard states GC",
        );
        let instant = Instant::now();

        // Reset GC state and compute markers
        let (current_marker, target_marker) = {
            let gc_state = self.gc_state_storage.load()?;
            if gc_state.step.is_some() {
                tracing::warn!(?gc_state, "invalid stored GC state");
            }

            self.gc_state_storage
                .update(&GcState {
                    current_marker: gc_state.current_marker,
                    step: Some(Step::Mark(top_blocks.clone())),
                })
                .context("Failed to update gc state to 'Mark'")?;

            let target_marker = gc_state
                .current_marker
                .next()
                .context("Invalid target marker")?;
            (gc_state.current_marker, target_marker)
        };

        // Wait until all writes will be finished and take write lock of the current marker
        let mut target_marker_lock = self.current_marker.write().await;
        // Update current marker
        *target_marker_lock = target_marker;

        // Mark all blocks, referenced by `top_blocks`, with target marker
        self.mark(current_marker, target_marker_lock, &top_blocks, false)
            .await?;

        // Wait until all states are written and prevent writing new states
        let target_marker_lock = self.current_marker.write().await;

        // Remove all cells with different marker
        self.sweep_cells(current_marker, target_marker_lock, &top_blocks)
            .await?;
        // Remove all blocks which are not referenced by `top_blocks`
        self.sweep_block_states(target_marker, &top_blocks).await?;

        // Trigger compaction for touched column families
        // self.trigger_compaction();

        // Done
        tracing::info!(
            block_id = %top_blocks.mc_block.display(),
            elapsed_sec = instant.elapsed().as_secs_f64(),
            "finished shard states GC",
        );
        Ok(top_blocks)
    }

    async fn mark<'a>(
        &self,
        current_marker: Marker,
        target_marker_lock: RwLockWriteGuard<'a, Marker>,
        top_blocks: &TopBlocks,
        force: bool,
    ) -> Result<()> {
        let target_marker = *target_marker_lock;

        let time = Instant::now();

        // Load previous intermediate state
        let last_blocks = self
            .gc_state_storage
            .load_last_blocks()
            .context("Failed to load last shard blocks")?;

        #[derive(thiserror::Error, Debug)]
        enum MarkBlocksError {
            #[error("Internal rocksdb error")]
            Internal(#[source] rocksdb::Error),
            #[error("Invalid block id")]
            InvalidBlockId,
            #[error("Invalid cell")]
            MarkCellsTreeFailed,
        }

        let total = {
            let raw = self.db.raw();

            let total = Arc::new(AtomicUsize::new(0));
            let mut tasks = FuturesUnordered::new();

            // Prepare one database snapshot for all shard states iterators
            let snapshot = Arc::new(OwnedSnapshot::new(raw.clone()));

            let mut unique_shards = self
                .find_unique_shards(Some(&snapshot))
                .context("Failed to find unique shards")?;
            unique_shards.push(ton_block::ShardIdent::masterchain());

            // NOTE: `target_marker_lock` must be released after the rocksdb snapshot is made,
            // so that all new inserted cells will be marked with this marker and this method
            // will work only with old shard states
            drop(target_marker_lock);

            let shard_count = unique_shards.len();
            let shards_per_chunk = std::cmp::max(shard_count / num_cpus::get(), 1);

            // Iterate all shards
            for shard_idents in &unique_shards.into_iter().chunks(shards_per_chunk) {
                // Prepare context
                let snapshot = snapshot.clone();
                let top_blocks = top_blocks.clone();
                let cell_storage = self.cell_storage.clone();
                let total = total.clone();

                struct ShardTask {
                    last_block: Option<u32>,
                    upper_bound: [u8; 16],
                    read_options: rocksdb::ReadOptions,
                    last_shard_block_key:
                        SmallVec<<LastShardBlockKey as StoredValue>::OnStackSlice>,
                }

                // Prepare tasks
                let shard_tasks = shard_idents
                    .map(|shard_ident| {
                        let last_block = last_blocks.get(&shard_ident).cloned();

                        // Compute iteration bounds
                        let lower_bound = make_block_id_bound(&shard_ident, 0x00);
                        let upper_bound = make_block_id_bound(&shard_ident, 0xff);

                        // Prepare shard states read options
                        let mut read_options = self.db.shard_states.new_read_config();
                        read_options.set_iterate_lower_bound(lower_bound);

                        // Compute intermediate state key
                        let last_shard_block_key = LastShardBlockKey(shard_ident).to_vec();

                        ShardTask {
                            last_block,
                            upper_bound,
                            read_options,
                            last_shard_block_key,
                        }
                    })
                    .collect::<Vec<_>>();

                let shard_states_cf = self.db.shard_states.get_unbounded_cf();
                let node_states_cf = self.db.node_states.get_unbounded_cf();

                let write_options = self.db.node_states.new_write_config();

                // Spawn task
                tasks.push(tokio::task::spawn_blocking(move || {
                    let db = snapshot.db.as_ref();

                    // Prepare cf handles
                    let shard_states_cf = shard_states_cf.bound();
                    let node_states_cf = node_states_cf.bound();

                    for mut task in shard_tasks {
                        // Prepare reverse iterator
                        let mut iter =
                            snapshot.raw_iterator_cf_opt(&shard_states_cf, task.read_options);
                        iter.seek_for_prev(task.upper_bound.as_slice());

                        // Iterate all block states in shard starting from the latest
                        loop {
                            let (mut key, value) = match iter.item() {
                                Some(item) => item,
                                None => match iter.status() {
                                    Ok(()) => break,
                                    Err(e) => return Err(MarkBlocksError::Internal(e)),
                                },
                            };

                            let (shard_ident, seq_no) = match BlockIdShort::deserialize(&mut key) {
                                Ok(id) => id,
                                Err(_) => return Err(MarkBlocksError::InvalidBlockId),
                            };
                            // Stop iterating on first outdated block
                            if !top_blocks.contains_shard_seq_no(&shard_ident, seq_no) {
                                break;
                            }

                            let edge_block = match &task.last_block {
                                // Skip blocks which were definitely processed
                                Some(last_seq_no) if seq_no > *last_seq_no => {
                                    iter.prev();
                                    continue;
                                }
                                // Block may have been processed
                                Some(_) => {
                                    task.last_block = None;
                                    true
                                }
                                // Block is definitely processed first time
                                None => false,
                            };

                            // Update intermediate state for this shard to continue
                            // from this block on accidental restart
                            if let Err(e) = db.put_cf_opt(
                                &node_states_cf,
                                &task.last_shard_block_key,
                                seq_no.to_le_bytes(),
                                &write_options,
                            ) {
                                return Err(MarkBlocksError::Internal(e));
                            };

                            // Mark all cells of this state recursively with target marker
                            //
                            // NOTE: `mark_cells_tree` works with the current db instead of
                            // using the snapshot, so we can reduce the number of cells
                            // which will be marked (some new states can already be inserted
                            // and have overwritten old markers)
                            match cell_storage.mark_cells_tree_for_gc(
                                UInt256::from_be_bytes(value),
                                target_marker,
                                force && edge_block,
                            ) {
                                Ok(count) => total.fetch_add(count, Ordering::Relaxed),
                                Err(_) => return Err(MarkBlocksError::MarkCellsTreeFailed),
                            };

                            iter.prev();
                        }
                    }

                    Ok::<_, MarkBlocksError>(())
                }));
            }

            // Wait for all tasks to complete
            while let Some(result) = tasks.next().await {
                result??
            }

            // Load counter
            total.load(Ordering::Relaxed)
        };

        // Clear intermediate states
        self.gc_state_storage
            .clear_last_blocks()
            .context("Failed to reset last block")?;

        tracing::info!(
            marked_count = total,
            elapsed_ms = time.elapsed().as_millis(),
            "marked cells",
        );

        // Update gc state
        self.gc_state_storage
            .update(&GcState {
                current_marker,
                step: Some(Step::SweepCells(top_blocks.clone())),
            })
            .context("Failed to update gc state to 'Sweep'")
    }

    async fn sweep_cells<'a>(
        &self,
        current_marker: Marker,
        target_marker_lock: RwLockWriteGuard<'a, Marker>,
        top_blocks: &TopBlocks,
    ) -> Result<()> {
        let target_marker = *target_marker_lock;

        tracing::info!(?target_marker, "sweeping cells");
        let time = Instant::now();

        // Remove all unmarked cells
        let total = self
            .cell_storage
            .sweep_cells(target_marker)
            .await
            .context("Failed to sweep cells")?;

        // NOTE: target marker lock must be held during cells sweep.
        // Because there can be a situation when unmarked cell is
        // used by the newly inserted state, however it is being deleted
        // by this functions. So, unless better solution is found,
        // it will block the insertion of new states.
        drop(target_marker_lock);

        tracing::info!(
            swept_count = total,
            elapsed_ms = time.elapsed().as_millis(),
            "swept cells",
        );

        // Update gc state
        self.gc_state_storage
            .update(&GcState {
                current_marker,
                step: Some(Step::SweepBlocks(top_blocks.clone())),
            })
            .context("Failed to update gc state to 'SweepBlocks'")
    }

    async fn sweep_block_states(
        &self,
        target_marker: Marker,
        top_blocks: &TopBlocks,
    ) -> Result<()> {
        tracing::info!("sweeping block states");

        #[derive(thiserror::Error, Debug)]
        enum SweepBlocksError {
            #[error("internal rocksdb error")]
            Internal(#[source] rocksdb::Error),
            #[error("invalid block id")]
            InvalidBlockId,
        }

        let time = Instant::now();

        // Prepare context
        let db = self.db.clone();
        let top_blocks = top_blocks.clone();

        // Spawn blocking thread for iterator
        let total = tokio::task::spawn_blocking(move || {
            let raw = db.raw().as_ref();

            // Manually get required column factory and r/w options
            let shard_states_cf = db.shard_states.cf();
            let read_options = db.shard_states.new_read_config();
            let write_options = db.shard_states.write_config();

            // Create iterator
            let mut iter = raw.raw_iterator_cf_opt(&shard_states_cf, read_options);
            iter.seek_to_first();

            // Iterate all states and remove outdated
            let mut total = 0;
            loop {
                let key = match iter.key() {
                    Some(key) => key,
                    None => match iter.status() {
                        Ok(()) => break,
                        Err(e) => return Err(SweepBlocksError::Internal(e)),
                    },
                };

                let (shard_ident, seq_no) =
                    match BlockIdShort::deserialize(&mut std::convert::identity(key)) {
                        Ok(id) => id,
                        Err(_) => return Err(SweepBlocksError::InvalidBlockId),
                    };

                // Skip blocks from zero state and top blocks
                if seq_no == 0 || top_blocks.contains_shard_seq_no(&shard_ident, seq_no) {
                    iter.next();
                    continue;
                }

                if let Err(e) = raw.delete_cf_opt(&shard_states_cf, key, write_options) {
                    return Err(SweepBlocksError::Internal(e));
                }
                total += 1;

                iter.next();
            }

            Ok::<_, SweepBlocksError>(total)
        })
        .await??;

        tracing::info!(
            swept_count = total,
            elapsed_ms = time.elapsed().as_millis(),
            "swept block states",
        );

        // Update gc state
        self.gc_state_storage
            .update(&GcState {
                current_marker: target_marker,
                step: None,
            })
            .context("Failed to update gc state to 'Wait'")
    }

    fn find_mc_block_id(&self, mc_seq_no: u32) -> Result<Option<ton_block::BlockIdExt>> {
        let shard_states = &self.db.shard_states;
        Ok(shard_states
            .get((ton_block::ShardIdent::masterchain(), mc_seq_no).to_vec())?
            .and_then(|value| {
                let value = value.as_ref();
                if value.len() < 96 {
                    return None;
                }

                let root_hash: [u8; 32] = value[32..64].try_into().unwrap();
                let file_hash: [u8; 32] = value[64..96].try_into().unwrap();

                Some(ton_block::BlockIdExt {
                    shard_id: ton_block::ShardIdent::masterchain(),
                    seq_no: mc_seq_no,
                    root_hash: UInt256::from(root_hash),
                    file_hash: UInt256::from(file_hash),
                })
            }))
    }

    fn find_unique_shards(
        &self,
        snapshot: Option<&rocksdb::Snapshot>,
    ) -> Result<Vec<ton_block::ShardIdent>> {
        const BASE_WC_UPPER_BOUND: [u8; 16] = [
            0, 0, 0, 0, // workchain id
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // shard id
            0xff, 0xff, 0xff, 0xff, // seq no
        ];

        let raw = self.db.raw();
        let cf = self.db.shard_states.cf();
        let read_options = self.db.shard_states.new_read_config();

        // Prepare reverse iterator
        let mut iter = match snapshot {
            Some(snapshot) => snapshot.raw_iterator_cf_opt(&cf, read_options),
            None => raw.raw_iterator_cf_opt(&cf, read_options),
        };
        iter.seek_for_prev(BASE_WC_UPPER_BOUND.as_slice());

        let mut prev_shard_id = None;
        let mut shard_idents = Vec::new();
        while let Some(mut key) = iter.key() {
            let shard_id = ton_block::ShardIdent::deserialize(&mut key)?;
            if matches!(&prev_shard_id, Some(prev_shard_id) if prev_shard_id == &shard_id) {
                break;
            }

            prev_shard_id = Some(shard_id);
            shard_idents.push(shard_id);

            iter.seek_for_prev(make_block_id_bound(&shard_id, 0x00).as_slice());
        }

        Ok(shard_idents)
    }

    fn trigger_compaction(&self) {
        tracing::info!("shard states compaction started");
        let instant = Instant::now();
        self.db.shard_states.trigger_compaction();
        tracing::info!(
            elapsed_ms = instant.elapsed().as_millis(),
            "shard states compaction finished"
        );

        tracing::info!("cells compaction started");
        let instant = Instant::now();
        self.db.cells.trigger_compaction();
        tracing::info!(
            elapsed_ms = instant.elapsed().as_millis(),
            "cells compaction finished"
        );
    }
}

struct OwnedSnapshot {
    inner: rocksdb::Snapshot<'static>,
    db: Arc<rocksdb::DB>,
}

impl OwnedSnapshot {
    fn new(db: Arc<rocksdb::DB>) -> Self {
        use rocksdb::Snapshot;

        unsafe fn extend_lifetime<'a>(r: Snapshot<'a>) -> Snapshot<'static> {
            std::mem::transmute::<Snapshot<'a>, Snapshot<'static>>(r)
        }

        // SAFETY: `Snapshot` requires the same lifetime as `rocksdb::DB` but
        // `tokio::task::spawn_blocking` requires 'static. This object ensures
        // that `rocksdb::DB` object lifetime will exceed the lifetime of the snapshot
        //
        // See https://github.com/rust-rocksdb/rust-rocksdb/issues/673
        let inner = unsafe { extend_lifetime(db.as_ref().snapshot()) };
        Self { inner, db }
    }
}

impl Deref for OwnedSnapshot {
    type Target = rocksdb::Snapshot<'static>;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ShardStateStorageMetrics {
    #[cfg(feature = "count-cells")]
    pub storage_cell_live_count: usize,
    #[cfg(feature = "count-cells")]
    pub storage_cell_max_live_count: usize,
    pub max_new_mc_cell_count: usize,
    pub max_new_sc_cell_count: usize,
}

fn make_block_id_bound(shard_ident: &ton_block::ShardIdent, value: u8) -> [u8; 16] {
    let mut result = [value; 16];
    result[..4].copy_from_slice(&shard_ident.workchain_id().to_be_bytes());
    result[4..12].copy_from_slice(&shard_ident.shard_prefix_with_tag().to_be_bytes());
    result
}

async fn prepare_file_db_dir(file_db_path: PathBuf, folder: &str) -> Result<Arc<PathBuf>> {
    let dir = Arc::new(file_db_path.join(folder));
    tokio::fs::create_dir_all(dir.as_ref()).await?;
    Ok(dir)
}

#[derive(thiserror::Error, Debug)]
enum ShardStateStorageError {
    #[error("Not found")]
    NotFound,
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
}
