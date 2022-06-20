use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use rlimit::Resource;
use rocksdb::perf::MemoryUsageStats;
use rocksdb::DBCompressionType;

pub use self::block_connection_storage::*;
pub use self::block_handle::*;
pub use self::block_handle_storage::*;
pub use self::block_meta::*;
use self::block_storage::*;
use self::node_state_storage::*;
pub use self::runtime_storage::*;
use self::shard_state_storage::*;
use self::tree::*;
use crate::utils::*;

mod block_connection_storage;
mod block_handle;
mod block_handle_storage;
mod block_meta;
mod block_storage;
mod cell_storage;
mod columns;
mod migrations;
mod node_state_storage;
mod persistent_state_keeper;
mod runtime_storage;
mod shard_state_storage;
mod tree;

pub struct Db {
    file_db_path: PathBuf,
    runtime_storage: Arc<RuntimeStorage>,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: ShardStateStorage,
    block_connection_storage: BlockConnectionStorage,
    node_state_storage: NodeStateStorage,

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

        migrations::apply(&db)
            .await
            .context("Failed to apply migrations")?;

        let runtime_storage = Arc::new(RuntimeStorage::default());
        let block_handle_storage = Arc::new(BlockHandleStorage::with_db(&db)?);
        let block_storage = Arc::new(BlockStorage::with_db(&db, &block_handle_storage)?);
        let shard_state_storage =
            ShardStateStorage::with_db(&db, &block_handle_storage, &block_storage, &file_db_path)
                .await?;
        let node_state_storage = NodeStateStorage::with_db(&db)?;
        let block_connection_storage = BlockConnectionStorage::with_db(&db)?;

        Ok(Arc::new(Self {
            file_db_path: file_db_path.as_ref().to_path_buf(),
            block_handle_storage,
            block_storage,
            shard_state_storage,
            block_connection_storage,
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
    pub fn runtime_storage(&self) -> &RuntimeStorage {
        self.runtime_storage.as_ref()
    }

    #[inline(always)]
    pub fn block_handle_storage(&self) -> &BlockHandleStorage {
        self.block_handle_storage.as_ref()
    }

    #[inline(always)]
    pub fn block_connection_storage(&self) -> &BlockConnectionStorage {
        &self.block_connection_storage
    }

    #[inline(always)]
    pub fn block_storage(&self) -> &BlockStorage {
        self.block_storage.as_ref()
    }

    #[inline(always)]
    pub fn shard_state_storage(&self) -> &ShardStateStorage {
        &self.shard_state_storage
    }

    #[inline(always)]
    pub fn node_state(&self) -> &NodeStateStorage {
        &self.node_state_storage
    }

    pub fn metrics(&self) -> DbMetrics {
        #[cfg(feature = "count-cells")]
        let storage_cell = countme::get::<cell_storage::StorageCell>();

        DbMetrics {
            #[cfg(feature = "count-cells")]
            storage_cell_live_count: storage_cell.live,
            #[cfg(feature = "count-cells")]
            storage_cell_max_live_count: storage_cell.max_live,
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
}

#[derive(Debug, Copy, Clone)]
pub struct DbMetrics {
    #[cfg(feature = "count-cells")]
    pub storage_cell_live_count: usize,
    #[cfg(feature = "count-cells")]
    pub storage_cell_max_live_count: usize,
    pub shard_state_storage: ShardStateStorageMetrics,
}
