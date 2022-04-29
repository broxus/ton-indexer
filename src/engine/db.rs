use std::ops::RangeBounds;
/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - slightly changed db structure
///
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use rlimit::Resource;
use rocksdb::perf::MemoryUsageStats;
use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, DataBlockIndexType};
use ton_block::HashmapAugType;

use crate::storage::*;
use crate::utils::*;
use crate::BlocksGcKind;

const CURRENT_VERSION: [u8; 3] = [2, 0, 6];

pub struct Db {
    file_db_path: PathBuf,
    block_handle_storage: BlockHandleStorage,
    shard_state_storage: ShardStateStorage,
    archive_manager: ArchiveManager,
    node_state_storage: NodeStateStorage,

    prev1_block_db: Tree<columns::Prev1>,
    prev2_block_db: Tree<columns::Prev2>,
    next1_block_db: Tree<columns::Next1>,
    next2_block_db: Tree<columns::Next2>,
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
                // opts.set_stats_dump_period_sec(300);
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
        let archive_manager = ArchiveManager::with_db(&db)?;
        let node_state_storage = NodeStateStorage::new(&db)?;

        Ok(Arc::new(Self {
            file_db_path: file_db_path.as_ref().to_path_buf(),
            block_handle_storage,
            shard_state_storage,
            archive_manager,
            node_state_storage,
            prev1_block_db: Tree::new(&db)?,
            prev2_block_db: Tree::new(&db)?,
            next1_block_db: Tree::new(&db)?,
            next2_block_db: Tree::new(&db)?,
            db,
            caches,
        }))
    }

    #[inline(always)]
    pub fn file_db_path(&self) -> &Path {
        &self.file_db_path
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

    pub fn create_or_load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: Option<&ton_block::Block>,
        utime: Option<u32>,
    ) -> Result<(Arc<BlockHandle>, HandleCreationStatus)> {
        if let Some(handle) = self.load_block_handle(block_id)? {
            return Ok((handle, HandleCreationStatus::Fetched));
        }

        let meta = match (block, utime) {
            (Some(block), _) => BlockMeta::from_block(block)?,
            (None, Some(utime)) if block_id.seq_no == 0 => BlockMeta::with_data(0, utime, 0),
            _ if block_id.seq_no == 0 => return Err(DbError::FailedToCreateZerostateHandle.into()),
            _ => return Err(DbError::FailedToCreateBlockHandle.into()),
        };

        if let Some(handle) = self
            .block_handle_storage
            .create_handle(block_id.clone(), meta)?
        {
            return Ok((handle, HandleCreationStatus::Created));
        }

        if let Some(handle) = self.load_block_handle(block_id)? {
            return Ok((handle, HandleCreationStatus::Fetched));
        }

        Err(DbError::FailedToCreateBlockHandle.into())
    }

    pub fn load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>> {
        self.block_handle_storage.load_handle(block_id)
    }

    pub fn load_key_block_handle(&self, seq_no: u32) -> Result<Arc<BlockHandle>> {
        self.block_handle_storage.load_key_block_handle(seq_no)
    }

    pub fn key_block_iterator(&self, since: Option<u32>) -> KeyBlocksIterator<'_> {
        self.block_handle_storage.key_block_iterator(since)
    }

    pub async fn store_block_data(&self, block: &BlockStuffAug) -> Result<StoreBlockResult> {
        let (handle, status) =
            self.create_or_load_block_handle(block.id(), Some(block.block()), None)?;

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
            None => self.create_or_load_block_handle(
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

    pub fn shard_state_storage(&self) -> &ShardStateStorage {
        &self.shard_state_storage
    }

    pub fn store_block_connection(
        &self,
        handle: &Arc<BlockHandle>,
        direction: BlockConnection,
        connected_block_id: &ton_block::BlockIdExt,
    ) -> Result<()> {
        // Use strange match because all columns have different types
        let store = match direction {
            BlockConnection::Prev1 => {
                if handle.meta().has_prev1() {
                    return Ok(());
                }
                store_block_connection_impl(&self.prev1_block_db, handle, connected_block_id)?;
                handle.meta().set_has_prev1()
            }
            BlockConnection::Prev2 => {
                if handle.meta().has_prev2() {
                    return Ok(());
                }
                store_block_connection_impl(&self.prev2_block_db, handle, connected_block_id)?;
                handle.meta().set_has_prev2()
            }
            BlockConnection::Next1 => {
                if handle.meta().has_next1() {
                    return Ok(());
                }
                store_block_connection_impl(&self.next1_block_db, handle, connected_block_id)?;
                handle.meta().set_has_next1()
            }
            BlockConnection::Next2 => {
                if handle.meta().has_next2() {
                    return Ok(());
                }
                store_block_connection_impl(&self.next2_block_db, handle, connected_block_id)?;
                handle.meta().set_has_next2()
            }
        };

        if store {
            self.block_handle_storage.store_handle(handle)?;
        }

        Ok(())
    }

    pub fn load_block_connection(
        &self,
        block_id: &ton_block::BlockIdExt,
        direction: BlockConnection,
    ) -> Result<ton_block::BlockIdExt> {
        match direction {
            BlockConnection::Prev1 => load_block_connection_impl(&self.prev1_block_db, block_id),
            BlockConnection::Prev2 => load_block_connection_impl(&self.prev2_block_db, block_id),
            BlockConnection::Next1 => load_block_connection_impl(&self.next1_block_db, block_id),
            BlockConnection::Next2 => load_block_connection_impl(&self.next2_block_db, block_id),
        }
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

    pub async fn archive_block_with_data(
        &self,
        handle: &Arc<BlockHandle>,
        block_data: &[u8],
        block_proof_data: &[u8],
    ) -> Result<()> {
        profl::span!("move_into_archive_with_data", {
            self.archive_manager
                .move_into_archive_with_data(handle, block_data, block_proof_data)
                .await
        })
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
        range: impl RangeBounds<u32> + 'static,
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
        let top_blocks = match self.load_block_handle(mc_block_id)? {
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

#[inline]
fn store_block_connection_impl<T>(
    db: &Tree<T>,
    handle: &BlockHandle,
    block_id: &ton_block::BlockIdExt,
) -> Result<()>
where
    T: Column,
{
    db.insert(
        handle.id().root_hash.as_slice(),
        write_block_id_le(block_id),
    )
}

#[inline]
fn load_block_connection_impl<T>(
    db: &Tree<T>,
    block_id: &ton_block::BlockIdExt,
) -> Result<ton_block::BlockIdExt>
where
    T: Column,
{
    match db.get(block_id.root_hash.as_slice())? {
        Some(value) => {
            read_block_id_le(value.as_ref()).ok_or_else(|| DbError::InvalidBlockId.into())
        }
        None => Err(DbError::NotFound.into()),
    }
}

pub struct StoreBlockResult {
    pub handle: Arc<BlockHandle>,
    pub updated: bool,
    pub new: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BlockConnection {
    Prev1,
    Prev2,
    Next1,
    Next2,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum HandleCreationStatus {
    Created,
    Fetched,
}

#[derive(thiserror::Error, Debug)]
enum DbError {
    #[error("Incompatible DB version")]
    IncompatibleDbVersion,
    #[error("Existing DB version not found")]
    VersionNotFound,
    #[error("Not found")]
    NotFound,
    #[error("Invalid block id")]
    InvalidBlockId,
    #[error("Failed to create zero state block handle")]
    FailedToCreateZerostateHandle,
    #[error("Failed to create block handle")]
    FailedToCreateBlockHandle,
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
