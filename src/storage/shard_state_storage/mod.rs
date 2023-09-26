use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use ton_types::UInt256;

use self::cell_storage::*;
use self::files_context::FilesContext;
use self::replace_transaction::ShardStateReplaceTransaction;
use super::{BlockHandle, BlockHandleStorage, BlockStorage};
use crate::db::*;
use crate::utils::*;

mod cell_storage;
mod entries_buffer;
mod files_context;
mod replace_transaction;
mod shard_state_reader;

pub struct ShardStateStorage {
    db: Arc<Db>,

    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,
    cell_storage: Arc<CellStorage>,
    downloads_dir: Arc<PathBuf>,

    gc_lock: tokio::sync::RwLock<()>,
    min_ref_mc_state: Arc<MinRefMcState>,
    max_new_mc_cell_count: AtomicUsize,
    max_new_sc_cell_count: AtomicUsize,

    gc_status: ArcSwapOption<ShardStatesGcStatus>,
}

impl ShardStateStorage {
    pub async fn new(
        db: Arc<Db>,
        block_handle_storage: Arc<BlockHandleStorage>,
        block_storage: Arc<BlockStorage>,
        file_db_path: PathBuf,
        cache_size_bytes: u64,
    ) -> Result<Self> {
        let downloads_dir = prepare_file_db_dir(file_db_path, "downloads").await?;

        let cell_storage = CellStorage::new(db.clone(), cache_size_bytes)?;

        let res = Self {
            db,
            block_handle_storage,
            block_storage,
            cell_storage,
            downloads_dir,
            gc_lock: Default::default(),
            min_ref_mc_state: Arc::new(Default::default()),
            max_new_mc_cell_count: AtomicUsize::new(0),
            max_new_sc_cell_count: AtomicUsize::new(0),
            gc_status: ArcSwapOption::default(),
        };

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
            gc_status: self.gc_status.load_full(),
        }
    }

    pub fn cache_metrics(&self) -> CacheStats {
        self.cell_storage.cache_stats()
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

        let _gc_lock = self.gc_lock.read().await;

        let len = self
            .cell_storage
            .store_cell(&mut batch, state.root_cell().clone())?;

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
        let cell_id = self.load_state_root(block_id.shard_id, block_id.seq_no)?;
        let cell = self.cell_storage.load_cell(cell_id)?;

        ShardStateStuff::new(
            block_id.clone(),
            ton_types::Cell::with_cell_impl_arc(cell),
            &self.min_ref_mc_state,
        )
        .map(Arc::new)
    }

    pub async fn begin_replace(
        &'_ self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<(ShardStateReplaceTransaction<'_>, FilesContext)> {
        let ctx = FilesContext::new(self.downloads_dir.as_ref(), block_id).await?;

        Ok((
            ShardStateReplaceTransaction::new(&self.db, &self.cell_storage, &self.min_ref_mc_state),
            ctx,
        ))
    }

    pub async fn remove_outdated_states(&self, mc_seq_no: u32) -> Result<TopBlocks> {
        struct ResetGcStatusOnDrop<'a>(&'a ShardStateStorage);

        impl Drop for ResetGcStatusOnDrop<'_> {
            fn drop(&mut self) {
                self.0.gc_status.store(None);
            }
        }

        let _compaction_guard = self.db.delay_compaction().await;
        let _gc_status_guard = ResetGcStatusOnDrop(self);

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

        let raw = self.db.raw();

        // Manually get required column factory and r/w options
        let snapshot = raw.snapshot();
        let shard_states_cf = self.db.shard_states.get_unbounded_cf();
        let mut states_read_options = self.db.shard_states.new_read_config();
        states_read_options.set_snapshot(&snapshot);

        let cells_write_options = self.db.cells.write_config();

        let mut last_gc_status = None::<Arc<ShardStatesGcStatus>>;

        let mut alloc = bumpalo::Bump::new();

        // Create iterator
        let mut iter = raw.raw_iterator_cf_opt(&shard_states_cf.bound(), states_read_options);
        iter.seek_to_first();

        // Iterate all states and remove outdated
        let mut removed_states = 0usize;
        let mut removed_cells = 0usize;
        loop {
            let (key, value) = match iter.item() {
                Some(item) => item,
                None => match iter.status() {
                    Ok(()) => break,
                    Err(e) => return Err(e.into()),
                },
            };

            let (shard_ident, seq_no) =
                BlockIdShort::deserialize(&mut std::convert::identity(key))?;
            let root_hash = UInt256::from_be_bytes(value);

            // Skip blocks from zero state and top blocks
            if seq_no == 0 {
                iter.next();
                continue;
            }

            // Compute target seqno
            let target_seqno = top_blocks.get_seqno(&shard_ident);
            if seq_no >= target_seqno {
                iter.next();
                continue;
            }

            // Remove state
            alloc.reset();
            let mut batch = rocksdb::WriteBatch::default();
            {
                let _guard = self.gc_lock.write().await;
                let total = self
                    .cell_storage
                    .remove_cell(&mut batch, &alloc, root_hash)?;
                batch.delete_cf(&shard_states_cf.bound(), key);
                raw.write_opt(batch, cells_write_options)?;

                removed_cells += total;
                tracing::debug!(
                    removed_cells = total,
                    block_id = %(shard_ident, seq_no).display(),
                );
            }

            // Update GC status for metrics
            'status: {
                if let Some(status) = &last_gc_status {
                    if shard_ident == status.current_shard {
                        status.current_seqno.store(seq_no, Ordering::Release);
                        break 'status;
                    }
                }

                let new_status = Arc::new(ShardStatesGcStatus {
                    current_shard: shard_ident,
                    start_seqno: seq_no,
                    end_seqno: target_seqno,
                    current_seqno: AtomicU32::new(seq_no),
                });
                self.gc_status.store(Some(new_status.clone()));
                last_gc_status = Some(new_status);
            }

            // Update iterator
            removed_states += 1;
            iter.next();
        }

        // Done
        tracing::info!(
            removed_states,
            removed_cells,
            block_id = %top_blocks.mc_block.display(),
            elapsed = %humantime::format_duration(instant.elapsed()),
            "finished shard states GC",
        );
        Ok(top_blocks)
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

    fn load_state_root(
        &self,
        shard_ident: ton_block::ShardIdent,
        seqno: u32,
    ) -> Result<ton_types::UInt256> {
        let shard_states = &self.db.shard_states;
        let shard_state = shard_states.get((shard_ident, seqno).to_vec())?;
        match shard_state {
            Some(root) => Ok(UInt256::from_be_bytes(&root)),
            None => Err(ShardStateStorageError::NotFound.into()),
        }
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
}

#[derive(Debug, Clone)]
pub struct ShardStateStorageMetrics {
    #[cfg(feature = "count-cells")]
    pub storage_cell_live_count: usize,
    #[cfg(feature = "count-cells")]
    pub storage_cell_max_live_count: usize,
    pub max_new_mc_cell_count: usize,
    pub max_new_sc_cell_count: usize,
    pub gc_status: Option<Arc<ShardStatesGcStatus>>,
}

#[derive(Debug)]
pub struct ShardStatesGcStatus {
    pub current_shard: ton_block::ShardIdent,
    pub start_seqno: u32,
    pub end_seqno: u32,
    pub current_seqno: AtomicU32,
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
