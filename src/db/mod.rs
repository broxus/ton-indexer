use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use weedb::{Caches, WeeDb};

pub use weedb::Stats as RocksdbStats;
pub use weedb::{rocksdb, BoundedCfHandle, ColumnFamily, Table, UnboundedCfHandle};

use crate::config::DbOptions;

pub mod refcount;
pub mod tables;

mod migrations;

pub struct Db {
    pub archives: Table<tables::Archives>,
    pub block_handles: Table<tables::BlockHandles>,
    pub key_blocks: Table<tables::KeyBlocks>,
    pub package_entries: Table<tables::PackageEntries>,
    pub shard_states: Table<tables::ShardStates>,
    pub cells: Table<tables::Cells>,
    pub node_states: Table<tables::NodeStates>,
    pub prev1: Table<tables::Prev1>,
    pub prev2: Table<tables::Prev2>,
    pub next1: Table<tables::Next1>,
    pub next2: Table<tables::Next2>,

    compaction_lock: tokio::sync::RwLock<()>,
    inner: WeeDb,
}

impl Db {
    pub fn open(path: PathBuf, options: DbOptions) -> Result<Arc<Self>> {
        let limit = match fdlimit::raise_fd_limit() {
            // New fd limit
            Some(limit) => limit,
            // Current soft limit
            None => {
                rlimit::getrlimit(rlimit::Resource::NOFILE)
                    .unwrap_or((256, 0))
                    .0
            }
        };

        let caches_capacity =
            std::cmp::max(options.max_memory_usage / 3, options.min_caches_capacity);
        let compaction_memory_budget = std::cmp::max(
            options.max_memory_usage - options.max_memory_usage / 3,
            options.min_compaction_memory_budget,
        );

        let caches = Caches::with_capacity(caches_capacity);

        let inner = WeeDb::builder(path, caches)
            .options(|opts, _| {
                opts.set_level_compaction_dynamic_level_bytes(true);

                // compression opts
                opts.set_zstd_max_train_bytes(32 * 1024 * 1024);
                opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

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

                opts.optimize_level_style_compaction(compaction_memory_budget);

                // debug
                // opts.enable_statistics();
                // opts.set_stats_dump_period_sec(30);
            })
            .with_table::<tables::Archives>()
            .with_table::<tables::BlockHandles>()
            .with_table::<tables::KeyBlocks>()
            .with_table::<tables::ShardStates>()
            .with_table::<tables::Cells>()
            .with_table::<tables::NodeStates>()
            .with_table::<tables::Prev1>()
            .with_table::<tables::Prev2>()
            .with_table::<tables::Next1>()
            .with_table::<tables::Next2>()
            .with_table::<tables::PackageEntries>()
            .build()
            .context("Failed building db")?;

        migrations::apply(&inner).context("Failed to apply migrations")?;

        Ok(Arc::new(Self {
            archives: inner.instantiate_table(),
            block_handles: inner.instantiate_table(),
            key_blocks: inner.instantiate_table(),
            package_entries: inner.instantiate_table(),
            shard_states: inner.instantiate_table(),
            cells: inner.instantiate_table(),
            node_states: inner.instantiate_table(),
            prev1: inner.instantiate_table(),
            prev2: inner.instantiate_table(),
            next1: inner.instantiate_table(),
            next2: inner.instantiate_table(),
            compaction_lock: tokio::sync::RwLock::default(),
            inner,
        }))
    }

    #[inline]
    pub fn raw(&self) -> &Arc<rocksdb::DB> {
        self.inner.raw()
    }

    pub fn get_memory_usage_stats(&self) -> Result<RocksdbStats> {
        self.inner.get_memory_usage_stats().map_err(From::from)
    }

    pub async fn delay_compaction(&self) -> tokio::sync::RwLockReadGuard<'_, ()> {
        self.compaction_lock.read().await
    }

    pub async fn trigger_compaction(&self) {
        use std::time::Instant;

        let _compaction_guard = self.compaction_lock.write().await;

        let tables = [
            (self.block_handles.cf(), "block handles"),
            (self.package_entries.cf(), "package entries"),
            (self.archives.cf(), "archives"),
            (self.shard_states.cf(), "shard states"),
            (self.cells.cf(), "cells"),
        ];

        for (cf, title) in tables {
            tracing::info!("{title} compaction started");

            let instant = Instant::now();

            let bound = Option::<[u8; 0]>::None;
            self.raw().compact_range_cf(&cf, bound, bound);

            tracing::info!(
                elapsed_ms = instant.elapsed().as_millis(),
                "{title} compaction finished"
            );
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        self.raw().cancel_all_background_work(true)
    }
}
