use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use itertools::Itertools;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;
use tokio::sync::RwLockWriteGuard;
use ton_block::HashmapAugType;
use ton_types::{ByteOrderRead, UInt256};

use self::files_context::*;
use self::replace_transaction::*;
use super::cell_storage::*;
use super::{
    columns, BlockHandle, BlockHandleStorage, BlockStorage, Column, StoredValue, StoredValueBuffer,
    TopBlocks, Tree,
};
use crate::utils::*;

mod entries_buffer;
mod files_context;
mod parser;
mod replace_transaction;

pub struct ShardStateStorage {
    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,

    downloads_dir: Arc<PathBuf>,
    state: Arc<ShardStateStorageState>,
    min_ref_mc_state: Arc<MinRefMcState>,
    max_new_mc_cell_count: AtomicUsize,
    max_new_sc_cell_count: AtomicUsize,
}

impl ShardStateStorage {
    pub async fn with_db<P>(
        db: &Arc<rocksdb::DB>,
        block_handle_storage: &Arc<BlockHandleStorage>,
        block_storage: &Arc<BlockStorage>,
        file_db_path: &P,
    ) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let downloads_dir = Arc::new(file_db_path.as_ref().join("downloads"));
        tokio::fs::create_dir_all(downloads_dir.as_ref()).await?;

        Ok(Self {
            block_handle_storage: block_handle_storage.clone(),
            block_storage: block_storage.clone(),
            downloads_dir,
            state: Arc::new(ShardStateStorageState::new(db).await?),
            min_ref_mc_state: Arc::new(Default::default()),
            max_new_mc_cell_count: AtomicUsize::new(0),
            max_new_sc_cell_count: AtomicUsize::new(0),
        })
    }

    pub fn metrics(&self) -> ShardStateStorageMetrics {
        ShardStateStorageMetrics {
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

        let current_marker = self.state.current_marker.read().await;

        let mut batch = rocksdb::WriteBatch::default();

        let marker = match block_id.seq_no {
            // Mark zero state as persistent
            0 => 0,
            // Mark all other states with current marker
            _ => *current_marker,
        };

        let len =
            self.state
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
            &self.state.shard_state_db.get_cf(),
            (block_id.shard_id, block_id.seq_no).to_vec(),
            value,
        );

        self.state.shard_state_db.raw_db_handle().write(batch)?;

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
        let shard_state = self
            .state
            .shard_state_db
            .get((block_id.shard_id, block_id.seq_no).to_vec())?;
        match shard_state {
            Some(root) => {
                let cell_id = UInt256::from_be_bytes(&root);
                let cell = self.state.cell_storage.load_cell(cell_id)?;

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
                &self.state.shard_state_db,
                &self.state.cell_storage,
                &self.min_ref_mc_state,
                PS_MARKER,
            ),
            ctx,
        ))
    }

    pub async fn remove_outdated_states(&self, mut mc_seq_no: u32) -> Result<TopBlocks> {
        if let Some(min_ref_mc_seqno) = self.min_ref_mc_state.seq_no() {
            if min_ref_mc_seqno < mc_seq_no {
                mc_seq_no = min_ref_mc_seqno;
            }
        }
        let mc_block_id = self
            .state
            .find_mc_block_id(mc_seq_no)
            .context("Failed to find block id by seqno")?
            .with_context(|| format!("Masterchain state with seqno {mc_seq_no} not found"))?;

        let top_blocks = match self.block_handle_storage.load_handle(&mc_block_id)? {
            Some(handle) => {
                if !handle.meta().has_state() {
                    return Err(ShardStateStorageError::NotFound)
                        .context("Target block has no state")?;
                }
                if !handle.meta().has_data() {
                    return Err(ShardStateStorageError::NotFound)
                        .context("Target block has no data")?;
                }

                let state = self
                    .load_state(handle.id())
                    .await
                    .context("Failed to load shard state for target block")?;
                let state_extra = state.shard_state_extra()?;

                let block_data = self.block_storage.load_block_data(&handle).await?;
                let block_info = block_data
                    .block()
                    .read_info()
                    .context("Failed to read target block info")?;

                let target_block_id = state_extra
                    .prev_blocks
                    .get(&block_info.min_ref_mc_seqno())
                    .context("Failed to find min ref mc block id")?
                    .context("Prev ref mc not found")?
                    .master_block_id()
                    .1;

                let target_block_handle = self
                    .block_handle_storage
                    .load_handle(&target_block_id)
                    .context("Failed to find min ref mc block handle")?
                    .context("Prev ref mc handle not found")?;

                if !target_block_handle.meta().has_data() {
                    return Err(ShardStateStorageError::NotFound)
                        .context("Min ref mc block has no data")?;
                }

                self.block_storage
                    .load_block_data(&target_block_handle)
                    .await
                    .and_then(|block_data| TopBlocks::from_mc_block(&block_data))?
            }
            None => return Err(ShardStateStorageError::NotFound).context("Target block not found"),
        };

        log::info!(
            "Starting shard states GC for mc block: {}",
            top_blocks.mc_block
        );
        let instant = Instant::now();

        self.state.gc(&top_blocks).await?;

        log::info!(
            "Finished shard states GC for mc block: {}. Took: {} ms",
            top_blocks.mc_block,
            instant.elapsed().as_millis()
        );

        Ok(top_blocks)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct ShardStateStorageMetrics {
    pub max_new_mc_cell_count: usize,
    pub max_new_sc_cell_count: usize,
}

struct ShardStateStorageState {
    current_marker: tokio::sync::RwLock<u8>,
    shard_state_db: Tree<columns::ShardStates>,
    gc_state_storage: Arc<GcStateStorage>,
    cell_storage: Arc<CellStorage>,
}

impl ShardStateStorageState {
    async fn new(db: &Arc<rocksdb::DB>) -> Result<Self> {
        let res = Self {
            current_marker: Default::default(),
            shard_state_db: Tree::new(db)?,
            gc_state_storage: Arc::new(GcStateStorage::new(db)?),
            cell_storage: Arc::new(CellStorage::new(db)?),
        };

        // res.cell_storage.check()?;

        let gc_state = res.gc_state_storage.load()?;

        match &gc_state.step {
            None => {
                log::info!("Shard state GC is pending");
            }
            Some(step) => {
                let mut target_marker_lock = res.current_marker.write().await;
                *target_marker_lock = gc_state.next_marker();
                let target_marker = *target_marker_lock;

                match step {
                    Step::Mark(top_blocks) => {
                        res.mark(
                            gc_state.current_marker,
                            target_marker_lock,
                            top_blocks,
                            true,
                        )
                        .await?;
                        res.sweep_cells(gc_state.current_marker, target_marker, top_blocks)
                            .await?;
                        res.sweep_blocks(target_marker, top_blocks).await?;
                    }
                    Step::SweepCells(top_blocks) => {
                        res.sweep_cells(gc_state.current_marker, target_marker, top_blocks)
                            .await?;
                        res.sweep_blocks(target_marker, top_blocks).await?;
                    }
                    Step::SweepBlocks(top_blocks) => {
                        res.sweep_blocks(target_marker, top_blocks).await?;
                    }
                }
            }
        };

        Ok(res)
    }

    async fn gc(self: &Arc<Self>, top_blocks: &TopBlocks) -> Result<()> {
        let gc_state = self.gc_state_storage.load()?;
        if gc_state.step.is_some() {
            log::info!("Invalid GC state: {gc_state:?}");
        };

        self.gc_state_storage
            .update(&GcState {
                current_marker: gc_state.current_marker,
                step: Some(Step::Mark(top_blocks.clone())),
            })
            .context("Failed to update gc state to 'Mark'")?;

        // NOTE: make sure that target marker lock is dropped after all gc steps are finished
        let mut target_marker_lock = self.current_marker.write().await;
        *target_marker_lock = gc_state.next_marker();
        let target_marker = *target_marker_lock;

        self.mark(
            gc_state.current_marker,
            target_marker_lock,
            top_blocks,
            false,
        )
        .await?;
        self.sweep_cells(gc_state.current_marker, target_marker, top_blocks)
            .await?;
        self.sweep_blocks(target_marker, top_blocks).await
    }

    // async fn update_persistent_state(
    //     self: &Arc<Self>,
    //     prev_ps_root: &UInt256,
    //     new_ps_root: &UInt256,
    // ) -> Result<()> {
    //     // Traverse new persistent state and mark it with `PS_TEMP_MARKER`.
    //     // NOTE: Top cell of the old persistent state will be marked
    //
    //     // Traverse old persistent state
    // }

    async fn mark<'a>(
        &self,
        current_marker: u8,
        target_marker_lock: RwLockWriteGuard<'a, u8>,
        top_blocks: &TopBlocks,
        force: bool,
    ) -> Result<()> {
        let time = Instant::now();

        // Load previous intermediate state
        let last_blocks = self
            .gc_state_storage
            .load_last_blocks()
            .context("Failed to load last shard blocks")?;

        let total = {
            let db = self.shard_state_db.raw_db_handle();

            let total = Arc::new(AtomicUsize::new(0));
            let mut tasks = FuturesUnordered::new();

            // Prepare one database snapshot for all shard states iterators
            let snapshot = db.snapshot();

            let mut unique_shards = self
                .find_unique_shards(Some(&snapshot))
                .context("Failed to find unique shards")?;
            unique_shards.push(ton_block::ShardIdent::masterchain());

            // NOTE: `target_marker_lock` must be released after the rocksdb snapshot is made,
            // so that all new inserted cells will be marked with this marker and this method
            // will work only with old shard states
            let target_marker = *target_marker_lock;
            drop(target_marker_lock);

            let shard_count = unique_shards.len();
            let shards_per_chunk = std::cmp::max(shard_count / num_cpus::get(), 1);

            // Iterate all shards
            for shard_idents in &unique_shards.into_iter().chunks(shards_per_chunk) {
                // Prepare context
                let db = db.clone();
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
                        let mut read_options = rocksdb::ReadOptions::default();
                        columns::ShardStates::read_options(&mut read_options);
                        read_options.set_snapshot(&snapshot);
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

                // Spawn task
                tasks.push(tokio::task::spawn_blocking(move || {
                    // Prepare cf handles
                    let shard_states_cf =
                        db.cf_handle(columns::ShardStates::NAME).context("No cf")?;
                    let node_states_cf =
                        db.cf_handle(columns::NodeStates::NAME).context("No cf")?;

                    // Prepare intermediate state write options
                    let mut write_options = rocksdb::WriteOptions::default();
                    columns::NodeStates::write_options(&mut write_options);

                    for mut task in shard_tasks {
                        // Prepare reverse iterator
                        let iter = db.iterator_cf_opt(
                            &shard_states_cf,
                            task.read_options,
                            rocksdb::IteratorMode::From(
                                &task.upper_bound,
                                rocksdb::Direction::Reverse,
                            ),
                        );

                        // Iterate all block states in shard starting from the latest
                        for (key, value) in iter {
                            let (shard_ident, seq_no) =
                                BlockIdShort::deserialize(&mut key.as_ref())?;
                            // Stop iterating on first outdated block
                            if !top_blocks.contains_shard_seq_no(&shard_ident, seq_no) {
                                break;
                            }

                            let edge_block = match &task.last_block {
                                // Skip blocks which were definitely processed
                                Some(last_seq_no) if seq_no > *last_seq_no => continue,
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
                            db.put_cf_opt(
                                &node_states_cf,
                                &task.last_shard_block_key,
                                seq_no.to_le_bytes(),
                                &write_options,
                            )
                            .context("Failed to update last block")?;

                            // Mark all cells of this state recursively with target marker
                            //
                            // NOTE: `mark_cells_tree` works with the current db instead of
                            // using the snapshot, so we can reduce the number of cells
                            // which will be marked (some new states can already be inserted
                            // and have overwritten old markers)
                            let count = cell_storage.mark_cells_tree(
                                UInt256::from_be_bytes(&value),
                                Marker::WhileDifferent {
                                    marker: target_marker,
                                    force: force && edge_block,
                                },
                            )?;
                            total.fetch_add(count, Ordering::Relaxed);
                        }
                    }

                    Ok::<_, anyhow::Error>(())
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

        log::info!(
            "Marked {total} cells. Took: {} ms",
            time.elapsed().as_millis()
        );

        // Update gc state
        self.gc_state_storage
            .update(&GcState {
                current_marker,
                step: Some(Step::SweepCells(top_blocks.clone())),
            })
            .context("Failed to update gc state to 'Sweep'")
    }

    async fn sweep_cells(
        &self,
        current_marker: u8,
        target_marker: u8,
        top_blocks: &TopBlocks,
    ) -> Result<()> {
        log::info!("Sweeping cells other than {target_marker}");

        let time = Instant::now();

        // Remove all unmarked cells
        let total = self
            .cell_storage
            .sweep_cells(target_marker)
            .await
            .context("Failed to sweep cells")?;

        log::info!(
            "Swept {total} cells. Took: {} ms",
            time.elapsed().as_millis()
        );

        // Update gc state
        self.gc_state_storage
            .update(&GcState {
                current_marker,
                step: Some(Step::SweepBlocks(top_blocks.clone())),
            })
            .context("Failed to update gc state to 'SweepBlocks'")
    }

    async fn sweep_blocks(&self, target_marker: u8, top_blocks: &TopBlocks) -> Result<()> {
        log::info!("Sweeping block states");

        let time = Instant::now();

        // Prepare context
        let db = self.shard_state_db.raw_db_handle().clone();
        let top_blocks = top_blocks.clone();

        // Spawn blocking thread for iterator
        let total = tokio::task::spawn_blocking(move || {
            // Manually get required column factory and r/w options
            let shard_state_cf = db.cf_handle(columns::ShardStates::NAME).context("No cf")?;

            let mut read_options = rocksdb::ReadOptions::default();
            columns::ShardStates::read_options(&mut read_options);

            let mut write_options = rocksdb::WriteOptions::default();
            columns::ShardStates::write_options(&mut write_options);

            // Create iterator
            let iter =
                db.iterator_cf_opt(&shard_state_cf, read_options, rocksdb::IteratorMode::Start);

            // Iterate all states and remove outdated
            let mut total = 0;
            for (key, _) in iter {
                let (shard_ident, seq_no) = BlockIdShort::deserialize(&mut key.as_ref())?;
                // Skip blocks from zero state and top blocks
                if seq_no == 0 || top_blocks.contains_shard_seq_no(&shard_ident, seq_no) {
                    continue;
                }

                db.delete_cf_opt(&shard_state_cf, key, &write_options)
                    .context("Failed to remove swept block")?;
                total += 1;
            }

            Ok::<_, anyhow::Error>(total)
        })
        .await??;

        log::info!(
            "Swept {} block states. Took: {} ms",
            total,
            time.elapsed().as_millis()
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
        Ok(self
            .shard_state_db
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

        let cf = self.shard_state_db.get_cf();

        let db = self.shard_state_db.raw_db_handle();
        let mut read_options = rocksdb::ReadOptions::default();
        columns::ShardStates::read_options(&mut read_options);

        if let Some(snapshot) = snapshot {
            read_options.set_snapshot(snapshot);
        }

        // Prepare reverse iterator
        let mut iter = db.raw_iterator_cf_opt(&cf, read_options);
        iter.seek_for_prev(&BASE_WC_UPPER_BOUND);

        let mut shard_idents = Vec::new();
        while let Some(mut key) = iter.key() {
            let shard_id = ton_block::ShardIdent::deserialize(&mut key)?;
            shard_idents.push(shard_id);

            iter.seek_for_prev(&make_block_id_bound(&shard_id, 0x00));
        }

        Ok(shard_idents)
    }
}

struct GcStateStorage {
    node_states: Tree<columns::NodeStates>,
}

impl GcStateStorage {
    fn new(db: &Arc<rocksdb::DB>) -> Result<Self> {
        let storage = Self {
            node_states: Tree::new(db)?,
        };
        let _ = storage.load()?;
        Ok(storage)
    }

    fn load(&self) -> Result<GcState> {
        Ok(match self.node_states.get(STATES_GC_STATE_KEY)? {
            Some(value) => {
                GcState::from_slice(&value).context("Failed to decode states GC state")?
            }
            None => {
                let state = GcState {
                    current_marker: 1, // NOTE: zero marker is reserved for persistent state
                    step: None,
                };
                self.update(&state)?;
                state
            }
        })
    }

    fn update(&self, state: &GcState) -> Result<()> {
        self.node_states
            .insert(STATES_GC_STATE_KEY, state.to_vec())
            .context("Failed to update shards GC state")
    }

    fn clear_last_blocks(&self) -> Result<()> {
        let iter = self.node_states.prefix_iterator(GC_LAST_BLOCK_KEY);
        for (key, _) in iter.filter(|(key, _)| key.starts_with(GC_LAST_BLOCK_KEY)) {
            self.node_states.remove(key)?
        }
        Ok(())
    }

    fn load_last_blocks(&self) -> Result<FxHashMap<ton_block::ShardIdent, u32>> {
        let mut result = FxHashMap::default();

        let iter = self.node_states.prefix_iterator(GC_LAST_BLOCK_KEY);
        for (key, value) in iter.filter(|(key, _)| key.starts_with(GC_LAST_BLOCK_KEY)) {
            let shard_ident = LastShardBlockKey::from_slice(&key)
                .context("Failed to load last shard id")?
                .0;
            let top_block = (&mut &*value)
                .read_le_u32()
                .context("Failed to load top block")?;
            result.insert(shard_ident, top_block);
        }

        Ok(result)
    }
}

fn make_block_id_bound(shard_ident: &ton_block::ShardIdent, value: u8) -> [u8; 16] {
    let mut result = [value; 16];
    result[..4].copy_from_slice(&shard_ident.workchain_id().to_be_bytes());
    result[4..12].copy_from_slice(&shard_ident.shard_prefix_with_tag().to_be_bytes());
    result
}

#[derive(Debug)]
struct GcState {
    current_marker: u8,
    step: Option<Step>,
}

impl StoredValue for GcState {
    const SIZE_HINT: usize = 512;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        let (step, blocks) = match &self.step {
            None => (0, None),
            Some(Step::Mark(blocks)) => (1, Some(blocks)),
            Some(Step::SweepCells(blocks)) => (2, Some(blocks)),
            Some(Step::SweepBlocks(blocks)) => (3, Some(blocks)),
        };
        buffer.write_raw_slice(&[self.current_marker, step]);

        if let Some(blocks) = blocks {
            blocks.serialize(buffer);
        }
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let mut data = [0u8; 2];
        reader.read_exact(&mut data)?;
        let step = match data[1] {
            0 => None,
            1 => Some(Step::Mark(TopBlocks::deserialize(reader)?)),
            2 => Some(Step::SweepCells(TopBlocks::deserialize(reader)?)),
            3 => Some(Step::SweepBlocks(TopBlocks::deserialize(reader)?)),
            _ => return Err(ShardStateStorageError::InvalidStatesGcStep.into()),
        };

        Ok(Self {
            current_marker: data[0],
            step,
        })
    }
}

impl GcState {
    /// 0x00 marker is used for persistent state
    /// 0xff marker is used for persistent state transition
    fn next_marker(&self) -> u8 {
        match self.current_marker {
            // Saturate marker
            254 | 255 => 1,
            // Increment marker otherwise
            marker => marker + 1,
        }
    }
}

#[derive(Debug)]
enum Step {
    Mark(TopBlocks),
    SweepCells(TopBlocks),
    SweepBlocks(TopBlocks),
}

#[derive(Debug)]
struct LastShardBlockKey(ton_block::ShardIdent);

impl StoredValue for LastShardBlockKey {
    const SIZE_HINT: usize = GC_LAST_BLOCK_KEY.len() + ton_block::ShardIdent::SIZE_HINT;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    #[inline(always)]
    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(GC_LAST_BLOCK_KEY);
        self.0.serialize(buffer);
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        if reader.len() > GC_LAST_BLOCK_KEY.len() {
            *reader = &(*reader)[GC_LAST_BLOCK_KEY.len()..];
        }

        ton_block::ShardIdent::deserialize(reader).map(Self)
    }
}

const STATES_GC_STATE_KEY: &[u8] = b"states_gc_state";
const GC_LAST_BLOCK_KEY: &[u8] = b"gc_last_block";

#[derive(thiserror::Error, Debug)]
enum ShardStateStorageError {
    #[error("Not found")]
    NotFound,
    #[error("Invalid states GC step")]
    InvalidStatesGcStep,
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fully_on_stack() {
        assert!(!LastShardBlockKey(ton_block::ShardIdent::default())
            .to_vec()
            .spilled());
    }

    #[test]
    fn correct_last_shared_block_key_repr() {
        let key = LastShardBlockKey(
            ton_block::ShardIdent::with_tagged_prefix(-1, ton_block::SHARD_FULL).unwrap(),
        );

        let mut data = Vec::new();
        key.serialize(&mut data);

        let deserialized_key = LastShardBlockKey::from_slice(&data).unwrap();
        assert_eq!(deserialized_key.0, key.0);
    }
}
