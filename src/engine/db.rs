/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - slightly changed db structure
///
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use rlimit::Resource;
use rocksdb::perf::MemoryUsageStats;
use rocksdb::{BlockBasedOptions, Cache, DBCompressionType};
use ton_api::ton;

use crate::storage::*;
use crate::utils::*;
use crate::BlocksGcKind;

const CURRENT_VERSION: [u8; 3] = [2, 0, 2];

pub struct Db {
    block_cache: Cache,
    uncompressed_block_cache: Cache,
    block_handle_storage: BlockHandleStorage,
    shard_state_storage: ShardStateStorage,
    archive_manager: ArchiveManager,
    block_index_db: BlockIndexDb,
    background_sync_meta_store: BackgroundSyncMetaStore,

    prev1_block_db: Tree<columns::Prev1>,
    prev2_block_db: Tree<columns::Prev2>,
    next1_block_db: Tree<columns::Next1>,
    next2_block_db: Tree<columns::Next2>,
    db: Arc<rocksdb::DB>,
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
        mut mem_limit: usize,
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

        let total_blocks_cache_size = (mem_limit * 2) / 3;
        mem_limit -= total_blocks_cache_size;
        let block_cache = Cache::new_lru_cache(total_blocks_cache_size / 2)?;
        let compressed_block_cache = Cache::new_lru_cache(total_blocks_cache_size / 2)?;

        let mut block_factory = BlockBasedOptions::default();
        block_factory.set_block_cache(&block_cache);
        block_factory.set_block_cache_compressed(&compressed_block_cache);
        block_factory.set_cache_index_and_filter_blocks(true);
        block_factory.set_pin_l0_filter_and_index_blocks_in_cache(true);
        block_factory.set_pin_top_level_index_and_filter(true);
        block_factory.set_block_size(32 * 1024); // reducing block size reduces index size
        block_factory.set_index_type(rocksdb::BlockBasedIndexType::HashSearch);

        let db = DbBuilder::new(rocksdb_path)
            .options(|opts| {
                // Memory consuming options
                // 8 mb
                // for direct io buffering
                opts.set_writable_file_max_buffer_size(1024 * 1024 * 8);
                // 8 mb
                // for direct io buffering
                opts.set_compaction_readahead_size(1024 * 1024 * 8);
                // 256
                // all metatables size
                opts.set_db_write_buffer_size(mem_limit);

                opts.set_block_based_table_factory(&block_factory);

                // compression opts
                opts.set_zstd_max_train_bytes(32 * 1024 * 1024);
                opts.set_compression_type(DBCompressionType::Lz4);
                // io
                opts.set_log_level(rocksdb::LogLevel::Error);
                opts.set_recycle_log_file_num(5);
                opts.set_max_open_files(limit as i32);

                // cf
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
                // cpu
                opts.set_max_background_jobs((num_cpus::get() as i32) / 2);
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
            .column::<columns::LtDesc>()
            .column::<columns::Lt>()
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
        let block_index_db = BlockIndexDb::with_db(&db)?;
        let background_sync_meta_store = BackgroundSyncMetaStore::new(&db)?;

        Ok(Arc::new(Self {
            block_cache,
            uncompressed_block_cache: compressed_block_cache,
            block_handle_storage,
            shard_state_storage,
            archive_manager,
            block_index_db,
            background_sync_meta_store,
            prev1_block_db: Tree::new(&db)?,
            prev2_block_db: Tree::new(&db)?,
            next1_block_db: Tree::new(&db)?,
            next2_block_db: Tree::new(&db)?,
            db,
        }))
    }

    pub fn raw(&self) -> &Arc<rocksdb::DB> {
        &self.db
    }

    pub fn get_memory_usage_stats(&self) -> Result<RocksdbStats> {
        let whole_db_stats = rocksdb::perf::get_memory_usage_stats(Some(&[&self.db]), None)?;
        let uncompressed_stats = self.uncompressed_block_cache.get_usage();
        let uncompressed_pined_stats = self.uncompressed_block_cache.get_pinned_usage();
        let compressed_stats = self.block_cache.get_usage();
        let compressed_pined_stats = self.block_cache.get_pinned_usage();

        Ok(RocksdbStats {
            whole_db_stats,
            uncompressed_block_cache_usage: uncompressed_stats,
            uncompressed_block_cache_pined_usage: uncompressed_pined_stats,
            compressed_block_cache_usage: compressed_stats,
            compressed_block_cache_pined_usage: compressed_pined_stats,
        })
    }

    pub fn create_or_load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: Option<&ton_block::Block>,
        utime: Option<u32>,
    ) -> Result<Arc<BlockHandle>> {
        if let Some(handle) = self.load_block_handle(block_id)? {
            return Ok(handle);
        }

        let meta = match (block, utime) {
            (Some(block), _) => BlockMeta::from_block(block)?,
            (None, Some(utime)) if block_id.seq_no == 0 => BlockMeta::with_data(0, utime, 0, 0),
            _ if block_id.seq_no == 0 => return Err(DbError::FailedToCreateZerostateHandle.into()),
            _ => return Err(DbError::FailedToCreateBlockHandle.into()),
        };

        if let Some(handle) = self
            .block_handle_storage
            .create_handle(block_id.clone(), meta)?
        {
            return Ok(handle);
        }

        if let Some(handle) = self.load_block_handle(block_id)? {
            return Ok(handle);
        }

        Err(DbError::FailedToCreateBlockHandle.into())
    }

    pub fn load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>> {
        self.block_handle_storage.load_handle(block_id)
    }

    pub fn key_block_iterator(&self, since: Option<u32>) -> Result<KeyBlocksIterator<'_>> {
        self.block_handle_storage.key_block_iterator(since)
    }

    pub async fn store_block_data(&self, block: &BlockStuff) -> Result<StoreBlockResult> {
        let handle = self.create_or_load_block_handle(block.id(), Some(block.block()), None)?;

        let archive_id = PackageEntryId::Block(handle.id());
        let mut already_existed = true;
        if !handle.meta().has_data() || !self.archive_manager.has_data(&archive_id)? {
            let _lock = handle.block_data_lock().write().await;
            if !handle.meta().has_data() || !self.archive_manager.has_data(&archive_id)? {
                self.archive_manager.add_data(&archive_id, block.data())?;
                if handle.meta().set_has_data() {
                    self.block_handle_storage.store_handle(&handle)?;
                    already_existed = false;
                }
            }
        }

        Ok(StoreBlockResult {
            handle,
            already_existed,
        })
    }

    pub async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        let raw_block = self.load_block_data_raw(handle).await?;
        BlockStuff::deserialize(handle.id().clone(), raw_block)
    }

    pub async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        if !handle.meta().has_data() {
            return Err(DbError::BlockDataNotFound.into());
        }
        self.archive_manager
            .get_data(handle, &PackageEntryId::Block(handle.id()))
            .await
    }

    pub async fn store_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        handle: Option<Arc<BlockHandle>>,
        proof: &BlockProofStuff,
    ) -> Result<StoreBlockResult> {
        if matches!(&handle, Some(handle) if handle.id() != block_id) {
            return Err(DbError::BlockHandleIdMismatch.into());
        }

        if block_id != proof.id() {
            return Err(DbError::BlockProofIdMismatch.into());
        }

        let handle = match handle {
            Some(handle) => handle,
            None => self.create_or_load_block_handle(
                block_id,
                Some(&proof.virtualize_block()?.0),
                None,
            )?,
        };

        let mut already_existed = true;
        if proof.is_link() {
            let archive_id = PackageEntryId::ProofLink(block_id);
            if !handle.meta().has_proof_link() || !self.archive_manager.has_data(&archive_id)? {
                let _lock = handle.proof_data_lock().write().await;
                if !handle.meta().has_proof_link() || !self.archive_manager.has_data(&archive_id)? {
                    self.archive_manager.add_data(&archive_id, proof.data())?;
                    if handle.meta().set_has_proof_link() {
                        self.block_handle_storage.store_handle(&handle)?;
                        already_existed = false;
                    }
                }
            }
        } else {
            let archive_id = PackageEntryId::Proof(block_id);
            if !handle.meta().has_proof() || !self.archive_manager.has_data(&archive_id)? {
                let _lock = handle.proof_data_lock().write().await;
                if !handle.meta().has_proof() || !self.archive_manager.has_data(&archive_id)? {
                    self.archive_manager.add_data(&archive_id, proof.data())?;
                    if handle.meta().set_has_proof() {
                        self.block_handle_storage.store_handle(&handle)?;
                        already_existed = false;
                    }
                }
            }
        }

        Ok(StoreBlockResult {
            handle,
            already_existed,
        })
    }

    pub async fn load_block_proof(
        &self,
        handle: &BlockHandle,
        is_link: bool,
    ) -> Result<BlockProofStuff> {
        let raw_proof = self.load_block_proof_raw(handle, is_link).await?;
        BlockProofStuff::deserialize(handle.id().clone(), raw_proof, is_link)
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
    ) -> Result<ShardStateStuff> {
        ShardStateStuff::new(
            block_id.clone(),
            self.shard_state_storage.load_state(block_id).await?,
        )
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

    pub fn find_block_by_seq_no(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        seq_no: u32,
    ) -> Result<Arc<BlockHandle>> {
        let block_id = self
            .block_index_db
            .get_block_by_seq_no(account_prefix, seq_no)?;

        self.block_handle_storage
            .load_handle(&block_id)?
            .ok_or_else(|| DbError::BlockHandleNotFound.into())
    }

    pub fn find_block_by_utime(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        utime: u32,
    ) -> Result<Arc<BlockHandle>> {
        let block_id = self
            .block_index_db
            .get_block_by_utime(account_prefix, utime)?;

        self.block_handle_storage
            .load_handle(&block_id)?
            .ok_or_else(|| DbError::BlockHandleNotFound.into())
    }

    pub fn find_block_by_lt(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        lt: u64,
    ) -> Result<Arc<BlockHandle>> {
        let block_id = self.block_index_db.get_block_by_lt(account_prefix, lt)?;

        self.block_handle_storage
            .load_handle(&block_id)?
            .ok_or_else(|| DbError::BlockHandleNotFound.into())
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
        self.archive_manager.move_into_archive(handle).await
    }

    pub fn find_last_key_block(&self) -> Result<Arc<BlockHandle>> {
        self.block_handle_storage.find_last_key_block()
    }

    pub fn index_handle(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        self.block_index_db.add_handle(handle)
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

    pub fn get_archive_id(&self, mc_seq_no: u32) -> Result<Option<u64>> {
        self.archive_manager.get_archive_id(mc_seq_no)
    }

    pub fn get_archive_slice(
        &self,
        id: u64,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        self.archive_manager.get_archive_slice(id, offset, limit)
    }

    pub fn background_sync_store(&self) -> &BackgroundSyncMetaStore {
        &self.background_sync_meta_store
    }

    pub async fn remove_outdated_states(
        &self,
        mc_block_id: &ton_block::BlockIdExt,
    ) -> Result<TopBlocks> {
        let top_blocks = match self.load_block_handle(mc_block_id)? {
            Some(handle) => self
                .load_block_data(&handle)
                .await
                .and_then(|block_data| TopBlocks::from_mc_block(&block_data))?,
            None => return Err(DbError::BlockHandleNotFound.into()),
        };

        self.shard_state_storage.gc(&top_blocks).await?;

        Ok(top_blocks)
    }

    pub async fn remove_outdated_blocks(
        &self,
        key_block_id: &ton_block::BlockIdExt,
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
        let stats = self.archive_manager.gc(&top_blocks).await?;

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

fn check_version(db: &Arc<rocksdb::DB>) -> Result<()> {
    const DB_VERSION_KEY: &str = "db_version";

    let state = Tree::<columns::NodeStates>::new(db)?;
    let is_empty = state
        .iterator(rocksdb::IteratorMode::Start)?
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
    let value = bincode::serialize(&convert_block_id_ext_blk2api(block_id))?;
    db.insert(handle.id().root_hash.as_slice(), value)
}

#[inline]
fn load_block_connection_impl<T>(
    db: &Tree<T>,
    block_id: &ton_block::BlockIdExt,
) -> Result<ton_block::BlockIdExt>
where
    T: Column,
{
    let value = match db.get(block_id.root_hash.as_slice())? {
        Some(value) => {
            bincode::deserialize::<ton::ton_node::blockidext::BlockIdExt>(value.as_ref())?
        }
        None => return Err(DbError::NotFound.into()),
    };

    convert_block_id_ext_api2blk(&value)
}

pub struct StoreBlockResult {
    pub handle: Arc<BlockHandle>,
    pub already_existed: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BlockConnection {
    Prev1,
    Prev2,
    Next1,
    Next2,
}

#[derive(thiserror::Error, Debug)]
enum DbError {
    #[error("Incompatible DB version")]
    IncompatibleDbVersion,
    #[error("Existing DB version not found")]
    VersionNotFound,
    #[error("Not found")]
    NotFound,
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
