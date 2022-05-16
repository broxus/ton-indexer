use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use rlimit::Resource;
use rocksdb::perf::MemoryUsageStats;
use rocksdb::DBCompressionType;
use ton_block::HashmapAugType;

pub use self::block_connection_storage::*;
pub use self::block_handle::*;
use self::block_handle_storage::*;
pub use self::block_meta::*;
use self::block_storage::*;
use self::node_state_storage::*;
pub use self::runtime_storage::*;
use self::shard_state_storage::*;
use self::tree::*;
use crate::config::*;
use crate::utils::*;

mod block_connection_storage;
mod block_handle;
mod block_handle_storage;
mod block_meta;
mod block_storage;
mod cell_storage;
mod columns;
mod node_state_storage;
mod persistent_state_keeper;
mod runtime_storage;
mod shard_state_storage;
mod tree;

const CURRENT_VERSION: [u8; 3] = [2, 0, 6];

pub struct Db {
    file_db_path: PathBuf,
    block_handle_storage: BlockHandleStorage,
    block_connection_storage: BlockConnectionStorage,
    shard_state_storage: ShardStateStorage,
    archive_manager: BlockStorage,
    node_state_storage: NodeStateStorage,
    runtime_storage: RuntimeStorage,

    db: Arc<rocksdb::DB>,
    caches: DbCaches,
}

pub struct RocksdbStats {
    pub whole_db_stats: MemoryUsageStats,
    pub uncompressed_block_cache_usage: usize,
    pub uncompressed_block_cache_pined_usage: usize,
    pub compressed_block_cache_usage: usize,
    pub compressed_block_cache_pined_usage: usize,
}

impl Db {
    pub async fn new<PS, PF>(
        rocksdb_path: PS,
        file_db_path: PF,
        mem_limit: usize,
    ) -> Result<Arc<Self>>
    where
        PS: AsRef<Path>,
        PF: AsRef<Path>,
    {
        let limit = match fdlimit::raise_fd_limit() {
            // New fd limit
            Some(limit) => limit,
            // Current soft limit
            None => rlimit::getrlimit(Resource::NOFILE).unwrap_or((256, 0)).0,
        };

        let caches = DbCaches::with_capacity(mem_limit)?;

        let db = DbBuilder::new(rocksdb_path, &caches)
            .options(|opts, _| {
                opts.set_level_compaction_dynamic_level_bytes(true);

                // compression opts
                opts.set_zstd_max_train_bytes(32 * 1024 * 1024);
                opts.set_compression_type(DBCompressionType::Zstd);

                // io
                opts.set_max_open_files(limit as i32);

                // logging
                opts.set_log_level(rocksdb::LogLevel::Error);
                opts.set_keep_log_file_num(2);
                opts.set_recycle_log_file_num(2);

                // cf
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);

                // cpu
                opts.set_max_background_jobs(std::cmp::max((num_cpus::get() as i32) / 2, 2));
                opts.increase_parallelism(num_cpus::get() as i32);

                // debug
                // opts.enable_statistics();
                // opts.set_stats_dump_period_sec(30);
            })
            .column::<columns::Archives>()
            .column::<columns::BlockHandles>()
            .column::<columns::KeyBlocks>()
            .column::<columns::ShardStates>()
            .column::<columns::Cells>()
            .column::<columns::NodeStates>()
            .column::<columns::Prev1>()
            .column::<columns::Prev2>()
            .column::<columns::Next1>()
            .column::<columns::Next2>()
            .column::<columns::PackageEntries>()
            .build()
            .context("Failed building db")?;

        check_version(&db)?;

        let block_handle_storage = BlockHandleStorage::with_db(&db)?;
        let shard_state_storage = ShardStateStorage::with_db(&db, &file_db_path).await?;
        let archive_manager = BlockStorage::with_db(&db)?;
        let node_state_storage = NodeStateStorage::with_db(&db)?;
        let block_connection_storage = BlockConnectionStorage::with_db(&db)?;
        let runtime_storage = RuntimeStorage::default();

        Ok(Arc::new(Self {
            file_db_path: file_db_path.as_ref().to_path_buf(),
            block_handle_storage,
            block_connection_storage,
            shard_state_storage,
            archive_manager,
            node_state_storage,
            runtime_storage,
            db,
            caches,
        }))
    }

    #[inline(always)]
    pub fn file_db_path(&self) -> &Path {
        &self.file_db_path
    }

    #[inline(always)]
    pub fn block_handle_storage(&self) -> &BlockHandleStorage {
        &self.block_handle_storage
    }

    #[inline(always)]
    pub fn block_connection_storage(&self) -> &BlockConnectionStorage {
        &self.block_connection_storage
    }

    #[inline(always)]
    pub fn shard_state_storage(&self) -> &ShardStateStorage {
        &self.shard_state_storage
    }

    #[inline(always)]
    pub fn runtime_storage(&self) -> &RuntimeStorage {
        &self.runtime_storage
    }

    #[inline(always)]
    pub fn node_state(&self) -> &NodeStateStorage {
        &self.node_state_storage
    }

    pub fn metrics(&self) -> DbMetrics {
        DbMetrics {
            shard_state_storage: self.shard_state_storage.metrics(),
        }
    }

    pub fn get_memory_usage_stats(&self) -> Result<RocksdbStats> {
        let caches = &[
            &self.caches.block_cache,
            &self.caches.compressed_block_cache,
        ];
        let whole_db_stats =
            rocksdb::perf::get_memory_usage_stats(Some(&[&self.db]), Some(caches))?;

        let uncompressed_block_cache_usage = self.caches.block_cache.get_usage();
        let uncompressed_block_cache_pined_usage = self.caches.block_cache.get_pinned_usage();

        let compressed_block_cache_usage = self.caches.compressed_block_cache.get_usage();
        let compressed_block_cache_pined_usage =
            self.caches.compressed_block_cache.get_pinned_usage();

        Ok(RocksdbStats {
            whole_db_stats,
            uncompressed_block_cache_usage,
            uncompressed_block_cache_pined_usage,
            compressed_block_cache_usage,
            compressed_block_cache_pined_usage,
        })
    }

    pub async fn store_block_data(&self, block: &BlockStuffAug) -> Result<StoreBlockResult> {
        let (handle, status) = self.block_handle_storage.create_or_load_handle(
            block.id(),
            Some(block.block()),
            None,
        )?;

        let archive_id = PackageEntryId::Block(handle.id());
        let mut updated = false;
        if !handle.meta().has_data() || !self.archive_manager.has_data(&archive_id)? {
            let data = block.new_archive_data()?;

            let _lock = handle.block_data_lock().write().await;
            if !handle.meta().has_data() || !self.archive_manager.has_data(&archive_id)? {
                self.archive_manager.add_data(&archive_id, data)?;
                if handle.meta().set_has_data() {
                    self.block_handle_storage.store_handle(&handle)?;
                    updated = true;
                }
            }
        }

        if handle.id().shard_id.is_masterchain() && handle.is_key_block() {
            self.runtime_storage
                .update_last_known_key_block_seqno(handle.id().seq_no);
        }

        Ok(StoreBlockResult {
            handle,
            updated,
            new: status == HandleCreationStatus::Created,
        })
    }

    pub async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        let raw_block = self.load_block_data_raw_ref(handle).await?;
        BlockStuff::deserialize(handle.id().clone(), raw_block.as_ref())
    }

    pub async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        if !handle.meta().has_data() {
            return Err(DbError::BlockDataNotFound.into());
        }
        self.archive_manager
            .get_data(handle, &PackageEntryId::Block(handle.id()))
            .await
    }

    pub async fn load_block_data_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        if !handle.meta().has_data() {
            return Err(DbError::BlockDataNotFound.into());
        }
        self.archive_manager
            .get_data_ref(handle, &PackageEntryId::Block(handle.id()))
            .await
    }

    pub async fn store_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        handle: Option<Arc<BlockHandle>>,
        proof: &BlockProofStuffAug,
    ) -> Result<StoreBlockResult> {
        if matches!(&handle, Some(handle) if handle.id() != block_id) {
            return Err(DbError::BlockHandleIdMismatch.into());
        }

        if block_id != proof.id() {
            return Err(DbError::BlockProofIdMismatch.into());
        }

        let (handle, status) = match handle {
            Some(handle) => (handle, HandleCreationStatus::Fetched),
            None => self.block_handle_storage.create_or_load_handle(
                block_id,
                Some(&proof.virtualize_block()?.0),
                None,
            )?,
        };

        let mut updated = false;
        if proof.is_link() {
            let archive_id = PackageEntryId::ProofLink(block_id);
            if !handle.meta().has_proof_link() || !self.archive_manager.has_data(&archive_id)? {
                let data = proof.new_archive_data()?;

                let _lock = handle.proof_data_lock().write().await;
                if !handle.meta().has_proof_link() || !self.archive_manager.has_data(&archive_id)? {
                    self.archive_manager.add_data(&archive_id, data)?;
                    if handle.meta().set_has_proof_link() {
                        self.block_handle_storage.store_handle(&handle)?;
                        updated = true;
                    }
                }
            }
        } else {
            let archive_id = PackageEntryId::Proof(block_id);
            if !handle.meta().has_proof() || !self.archive_manager.has_data(&archive_id)? {
                let data = proof.new_archive_data()?;

                let _lock = handle.proof_data_lock().write().await;
                if !handle.meta().has_proof() || !self.archive_manager.has_data(&archive_id)? {
                    self.archive_manager.add_data(&archive_id, data)?;
                    if handle.meta().set_has_proof() {
                        self.block_handle_storage.store_handle(&handle)?;
                        updated = true;
                    }
                }
            }
        }

        Ok(StoreBlockResult {
            handle,
            updated,
            new: status == HandleCreationStatus::Created,
        })
    }

    pub async fn load_block_proof(
        &self,
        handle: &BlockHandle,
        is_link: bool,
    ) -> Result<BlockProofStuff> {
        let raw_proof = self.load_block_proof_raw_ref(handle, is_link).await?;
        BlockProofStuff::deserialize(handle.id().clone(), raw_proof.as_ref(), is_link)
    }

    pub async fn load_block_proof_raw(
        &self,
        handle: &BlockHandle,
        is_link: bool,
    ) -> Result<Vec<u8>> {
        let (archive_id, exists) = if is_link {
            (
                PackageEntryId::ProofLink(handle.id()),
                handle.meta().has_proof_link(),
            )
        } else {
            (
                PackageEntryId::Proof(handle.id()),
                handle.meta().has_proof(),
            )
        };

        if !exists {
            return Err(DbError::BlockProofNotFound.into());
        }

        self.archive_manager.get_data(handle, &archive_id).await
    }

    pub async fn load_block_proof_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
        is_link: bool,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        let (archive_id, exists) = if is_link {
            (
                PackageEntryId::ProofLink(handle.id()),
                handle.meta().has_proof_link(),
            )
        } else {
            (
                PackageEntryId::Proof(handle.id()),
                handle.meta().has_proof(),
            )
        };

        if !exists {
            return Err(DbError::BlockProofNotFound.into());
        }

        self.archive_manager.get_data_ref(handle, &archive_id).await
    }

    pub async fn store_shard_state(
        &self,
        handle: &Arc<BlockHandle>,
        state: &ShardStateStuff,
    ) -> Result<bool> {
        if handle.id() != state.block_id() {
            return Err(DbError::BlockHandleIdMismatch.into());
        }

        if !handle.meta().has_state() {
            self.shard_state_storage
                .store_state(state.block_id(), state.root_cell().clone())
                .await?;
            if handle.meta().set_has_state() {
                self.block_handle_storage.store_handle(handle)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub async fn load_shard_state(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Arc<ShardStateStuff>> {
        ShardStateStuff::new(
            block_id.clone(),
            self.shard_state_storage.load_state(block_id).await?,
        )
        .map(Arc::new)
    }

    pub fn store_block_applied(&self, handle: &Arc<BlockHandle>) -> Result<bool> {
        if handle.meta().set_is_applied() {
            self.block_handle_storage.store_handle(handle)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn archive_block(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        profl::span!("move_into_archive", {
            self.archive_manager.move_into_archive(handle).await
        })
    }

    pub fn archive_block_with_data(
        &self,
        handle: &Arc<BlockHandle>,
        block_data: &[u8],
        block_proof_data: &[u8],
    ) -> Result<()> {
        self.archive_manager
            .move_into_archive_with_data(handle, block_data, block_proof_data)
    }

    pub fn find_last_key_block(&self) -> Result<Arc<BlockHandle>> {
        self.block_handle_storage.find_last_key_block()
    }

    pub fn find_prev_persistent_key_block(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>> {
        self.block_handle_storage
            .find_prev_persistent_key_block(block_id.seq_no)
    }

    pub fn assign_mc_ref_seq_no(
        &self,
        handle: &Arc<BlockHandle>,
        mc_ref_seq_no: u32,
    ) -> Result<()> {
        if handle.set_masterchain_ref_seqno(mc_ref_seq_no)? {
            self.block_handle_storage.store_handle(handle)?;
        }
        Ok(())
    }

    pub fn get_archive_id(&self, mc_seq_no: u32) -> Option<u32> {
        self.archive_manager.get_archive_id(mc_seq_no)
    }

    #[allow(unused)]
    pub fn get_archives(
        &self,
        range: impl std::ops::RangeBounds<u32> + 'static,
    ) -> impl Iterator<Item = (u32, Vec<u8>)> + '_ {
        self.archive_manager.get_archives(range)
    }

    pub fn get_archive_slice(
        &self,
        id: u32,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.archive_manager.get_archive_slice(id, offset, limit)
    }

    pub fn background_sync_store(&self) -> &NodeStateStorage {
        &self.node_state_storage
    }

    pub fn remove_outdated_archives(&self, until_id: u32) -> Result<()> {
        self.archive_manager.remove_outdated_archives(until_id)
    }

    pub async fn remove_outdated_states(
        &self,
        mc_block_id: &ton_block::BlockIdExt,
    ) -> Result<TopBlocks> {
        let top_blocks = match self.block_handle_storage.load_handle(mc_block_id)? {
            Some(handle) => {
                let state = self
                    .load_shard_state(handle.id())
                    .await
                    .context("Failed to load shard state for top block")?;
                let state_extra = state.shard_state_extra()?;

                let block_data = self.load_block_data(&handle).await?;
                let block_info = block_data
                    .block()
                    .read_info()
                    .context("Failed to read block info")?;

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

                self.load_block_data(&target_block_handle)
                    .await
                    .and_then(|block_data| TopBlocks::from_mc_block(&block_data))?
            }
            None => return Err(DbError::BlockHandleNotFound.into()),
        };

        self.shard_state_storage.gc(&top_blocks).await?;

        Ok(top_blocks)
    }

    pub async fn remove_outdated_blocks(
        self: &Arc<Self>,
        key_block_id: &ton_block::BlockIdExt,
        max_blocks_per_batch: Option<usize>,
        gc_type: BlocksGcKind,
    ) -> Result<()> {
        // Find target block
        let target_block = match gc_type {
            BlocksGcKind::BeforePreviousKeyBlock => self
                .block_handle_storage
                .find_prev_key_block(key_block_id.seq_no)?,
            BlocksGcKind::BeforePreviousPersistentState => self
                .block_handle_storage
                .find_prev_persistent_key_block(key_block_id.seq_no)?,
        };

        // Load target block data
        let top_blocks = match target_block {
            Some(handle) if handle.meta().has_data() => {
                log::info!(
                    "Starting blocks GC for key block: {}. Target block: {}",
                    key_block_id,
                    handle.id()
                );
                self.load_block_data(&handle)
                    .await
                    .context("Failed to load target key block data")
                    .and_then(|block_data| TopBlocks::from_mc_block(&block_data))
                    .context("Failed to compute top blocks for target block")?
            }
            _ => {
                log::info!("Blocks GC skipped for key block: {}", key_block_id);
                return Ok(());
            }
        };

        // Remove all expired entries
        let total_cached_handles_removed = self.block_handle_storage.gc_handles_cache(&top_blocks);

        let db = self.clone();
        let stats = tokio::task::spawn_blocking(move || {
            db.archive_manager.gc(max_blocks_per_batch, &top_blocks)
        })
        .await??;

        log::info!(
            r#"Finished blocks GC for key block: {}
total_cached_handles_removed: {}
mc_package_entries_removed: {}
total_package_entries_removed: {}
total_handles_removed: {}
"#,
            key_block_id,
            total_cached_handles_removed,
            stats.mc_package_entries_removed,
            stats.total_package_entries_removed,
            stats.total_handles_removed,
        );

        // Done
        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct DbMetrics {
    pub shard_state_storage: ShardStateStorageMetrics,
}

fn check_version(db: &Arc<rocksdb::DB>) -> Result<()> {
    const DB_VERSION_KEY: &str = "db_version";

    let state = Tree::<columns::NodeStates>::new(db)?;
    let is_empty = state
        .iterator(rocksdb::IteratorMode::Start)
        .next()
        .is_none();

    let version = state.get(DB_VERSION_KEY)?.map(|v| v.to_vec());

    match version {
        Some(version) if version == CURRENT_VERSION => {
            log::info!("Stored DB version is compatible");
            Ok(())
        }
        Some(version) => Err(DbError::IncompatibleDbVersion).with_context(|| {
            format!(
                "Found version: {:?}. Expected version: {:?}",
                version, CURRENT_VERSION
            )
        }),
        None if is_empty => {
            log::info!("Starting with empty db");
            state
                .insert(DB_VERSION_KEY, CURRENT_VERSION)
                .context("Failed to save new DB version")?;
            Ok(())
        }
        None => Err(DbError::VersionNotFound.into()),
    }
}

pub struct StoreBlockResult {
    pub handle: Arc<BlockHandle>,
    pub updated: bool,
    pub new: bool,
}

#[derive(thiserror::Error, Debug)]
enum DbError {
    #[error("Incompatible DB version")]
    IncompatibleDbVersion,
    #[error("Existing DB version not found")]
    VersionNotFound,
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
    #[error("Block proof id mismatch")]
    BlockProofIdMismatch,
    #[error("Block data not found")]
    BlockDataNotFound,
    #[error("Block proof not found")]
    BlockProofNotFound,
    #[error("Block handle not found")]
    BlockHandleNotFound,
}
