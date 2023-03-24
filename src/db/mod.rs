use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};

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

    pub temp_cells: Arc<tokio::sync::RwLock<Table<tables::TempCells>>>,

    compaction_lock: tokio::sync::RwLock<()>,
    caches: Caches,
}

impl Db {
    pub fn open(path: PathBuf, mem_limit: usize) -> Result<Arc<Self>> {
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

        let caches = Caches::with_capacity(mem_limit)?;

        let raw = Builder::new(path, &caches)
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

                // debug
                // opts.enable_statistics();
                // opts.set_stats_dump_period_sec(30);
            })
            .column::<tables::Archives>()
            .column::<tables::BlockHandles>()
            .column::<tables::KeyBlocks>()
            .column::<tables::ShardStates>()
            .column::<tables::Cells>()
            .column::<tables::TempCells>()
            .column::<tables::NodeStates>()
            .column::<tables::Prev1>()
            .column::<tables::Prev2>()
            .column::<tables::Next1>()
            .column::<tables::Next2>()
            .column::<tables::PackageEntries>()
            .build()
            .context("Failed building db")?;

        let tables = TableAccessor { raw, caches };

        migrations::apply(&tables).context("Failed to apply migrations")?;

        let mut temp_cells = tables.get::<tables::TempCells>();
        temp_cells.clear().context("Failed to reset temp cells")?;

        Ok(Arc::new(Self {
            archives: tables.get(),
            block_handles: tables.get(),
            key_blocks: tables.get(),
            package_entries: tables.get(),
            shard_states: tables.get(),
            cells: tables.get(),
            node_states: tables.get(),
            prev1: tables.get(),
            prev2: tables.get(),
            next1: tables.get(),
            next2: tables.get(),
            temp_cells: Arc::new(tokio::sync::RwLock::new(temp_cells)),
            compaction_lock: tokio::sync::RwLock::default(),
            caches: tables.caches,
        }))
    }

    #[inline]
    pub fn raw(&self) -> &Arc<rocksdb::DB> {
        self.archives.db()
    }

    pub fn get_memory_usage_stats(&self) -> Result<RocksdbStats> {
        let caches = &[
            &self.caches.block_cache,
            &self.caches.compressed_block_cache,
        ];
        let whole_db_stats =
            rocksdb::perf::get_memory_usage_stats(Some(&[self.raw()]), Some(caches))?;

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

pub struct RocksdbStats {
    pub whole_db_stats: rocksdb::perf::MemoryUsageStats,
    pub uncompressed_block_cache_usage: usize,
    pub uncompressed_block_cache_pined_usage: usize,
    pub compressed_block_cache_usage: usize,
    pub compressed_block_cache_pined_usage: usize,
}

struct Builder<'a> {
    path: PathBuf,
    options: rocksdb::Options,
    caches: &'a Caches,
    descriptors: Vec<rocksdb::ColumnFamilyDescriptor>,
}

impl<'a> Builder<'a> {
    pub fn new(path: PathBuf, caches: &'a Caches) -> Self {
        Self {
            path,
            options: Default::default(),
            caches,
            descriptors: Default::default(),
        }
    }

    pub fn options<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(&mut rocksdb::Options, &Caches),
    {
        f(&mut self.options, self.caches);
        self
    }

    pub fn column<T>(mut self) -> Self
    where
        T: ColumnFamily,
    {
        let mut opts = Default::default();
        T::options(&mut opts, self.caches);
        self.descriptors
            .push(rocksdb::ColumnFamilyDescriptor::new(T::NAME, opts));
        self
    }

    pub fn build(self) -> Result<Arc<rocksdb::DB>, rocksdb::Error> {
        Ok(Arc::new(rocksdb::DB::open_cf_descriptors(
            &self.options,
            &self.path,
            self.descriptors,
        )?))
    }
}

#[derive(Clone)]
pub struct TableAccessor {
    raw: Arc<rocksdb::DB>,
    caches: Caches,
}

impl TableAccessor {
    fn get<T: ColumnFamily>(&self) -> Table<T> {
        Table::new(self.raw.clone(), self.caches.clone())
    }
}

pub trait ColumnFamily {
    const NAME: &'static str;

    fn options(opts: &mut rocksdb::Options, caches: &Caches) {
        let _unused = opts;
        let _unused = caches;
    }

    fn write_options(opts: &mut rocksdb::WriteOptions) {
        let _unused = opts;
    }

    fn read_options(opts: &mut rocksdb::ReadOptions) {
        let _unused = opts;
    }
}

#[derive(Clone)]
pub struct Caches {
    pub block_cache: rocksdb::Cache,
    pub compressed_block_cache: rocksdb::Cache,
}

impl Caches {
    pub fn with_capacity(capacity: usize) -> Result<Self, rocksdb::Error> {
        const MIN_CAPACITY: usize = 64 * 1024 * 1024;

        let block_cache_capacity = std::cmp::min(capacity * 2 / 3, MIN_CAPACITY);
        let compressed_block_cache_capacity =
            std::cmp::min(capacity.saturating_sub(block_cache_capacity), MIN_CAPACITY);

        Ok(Self {
            block_cache: rocksdb::Cache::new_lru_cache(block_cache_capacity)?,
            compressed_block_cache: rocksdb::Cache::new_lru_cache(compressed_block_cache_capacity)?,
        })
    }
}

pub struct Table<T> {
    cf: CfHandle,
    db: Arc<rocksdb::DB>,
    caches: Caches,
    write_config: rocksdb::WriteOptions,
    read_config: rocksdb::ReadOptions,
    _ty: PhantomData<T>,
}

impl<T> Table<T>
where
    T: ColumnFamily,
{
    pub fn new(db: Arc<rocksdb::DB>, caches: Caches) -> Self {
        use rocksdb::AsColumnFamilyRef;

        // Check that tree exists
        let cf = CfHandle(db.cf_handle(T::NAME).unwrap().inner());

        let mut write_config = Default::default();
        T::write_options(&mut write_config);

        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        Self {
            cf,
            db,
            caches,
            write_config,
            read_config,
            _ty: Default::default(),
        }
    }

    pub fn cf(&'_ self) -> BoundedCfHandle<'_> {
        BoundedCfHandle {
            inner: self.cf.0,
            _lifetime: PhantomData,
        }
    }

    pub fn get_unbounded_cf(&self) -> UnboundedCfHandle {
        UnboundedCfHandle {
            inner: self.cf.0,
            _db: self.db.clone(),
        }
    }

    #[inline]
    pub fn db(&self) -> &Arc<rocksdb::DB> {
        &self.db
    }

    #[inline]
    pub fn read_config(&self) -> &rocksdb::ReadOptions {
        &self.read_config
    }

    pub fn new_read_config(&self) -> rocksdb::ReadOptions {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);
        read_config
    }

    #[inline]
    pub fn write_config(&self) -> &rocksdb::WriteOptions {
        &self.write_config
    }

    pub fn new_write_config(&self) -> rocksdb::WriteOptions {
        let mut write_config = Default::default();
        T::write_options(&mut write_config);
        write_config
    }

    pub fn clear(&mut self) -> Result<(), rocksdb::Error> {
        use rocksdb::AsColumnFamilyRef;

        self.db.drop_cf(T::NAME)?;
        let mut opts = Default::default();
        T::options(&mut opts, &self.caches);
        self.db.create_cf(T::NAME, &opts)?;

        let cf = self.db.cf_handle(T::NAME).unwrap();
        self.cf = CfHandle(cf.inner());

        Ok(())
    }

    #[inline]
    pub fn get<K: AsRef<[u8]>>(
        &self,
        key: K,
    ) -> Result<Option<rocksdb::DBPinnableSlice>, rocksdb::Error> {
        fn db_get<'a>(
            db: &'a rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            readopts: &rocksdb::ReadOptions,
        ) -> Result<Option<rocksdb::DBPinnableSlice<'a>>, rocksdb::Error> {
            db.get_pinned_cf_opt(&cf, key, readopts)
        }
        db_get(self.db.as_ref(), self.cf, key.as_ref(), &self.read_config)
    }

    #[inline]
    pub fn insert<K, V>(&self, key: K, value: V) -> Result<(), rocksdb::Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        fn db_insert(
            db: &rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            value: &[u8],
            writeopts: &rocksdb::WriteOptions,
        ) -> Result<(), rocksdb::Error> {
            db.put_cf_opt(&cf, key, value, writeopts)
        }
        db_insert(
            self.db.as_ref(),
            self.cf,
            key.as_ref(),
            value.as_ref(),
            &self.write_config,
        )
    }

    #[inline]
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<(), rocksdb::Error> {
        fn db_remove(
            db: &rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            writeopts: &rocksdb::WriteOptions,
        ) -> Result<(), rocksdb::Error> {
            db.delete_cf_opt(&cf, key, writeopts)
        }
        db_remove(self.db.as_ref(), self.cf, key.as_ref(), &self.write_config)
    }

    #[inline]
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool, rocksdb::Error> {
        fn db_contains_key(
            db: &rocksdb::DB,
            cf: CfHandle,
            key: &[u8],
            readopts: &rocksdb::ReadOptions,
        ) -> Result<bool, rocksdb::Error> {
            match db.get_pinned_cf_opt(&cf, key, readopts) {
                Ok(value) => Ok(value.is_some()),
                Err(e) => Err(e),
            }
        }
        db_contains_key(self.db.as_ref(), self.cf, key.as_ref(), &self.read_config)
    }

    pub fn iterator(&'_ self, mode: rocksdb::IteratorMode) -> rocksdb::DBIterator<'_> {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        self.db.iterator_cf_opt(&self.cf, read_config, mode)
    }

    pub fn prefix_iterator<P>(&'_ self, prefix: P) -> rocksdb::DBRawIterator<'_>
    where
        P: AsRef<[u8]>,
    {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);
        read_config.set_prefix_same_as_start(true);

        let mut iter = self.db.raw_iterator_cf_opt(&self.cf, read_config);
        iter.seek(prefix.as_ref());

        iter
    }

    pub fn raw_iterator(&'_ self) -> rocksdb::DBRawIterator<'_> {
        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        self.db.raw_iterator_cf_opt(&self.cf, read_config)
    }
}

#[derive(Copy, Clone)]
pub struct BoundedCfHandle<'a> {
    inner: *mut librocksdb_sys::rocksdb_column_family_handle_t,
    _lifetime: PhantomData<&'a ()>,
}

impl rocksdb::AsColumnFamilyRef for BoundedCfHandle<'_> {
    #[inline]
    fn inner(&self) -> *mut librocksdb_sys::rocksdb_column_family_handle_t {
        self.inner
    }
}

unsafe impl Send for BoundedCfHandle<'_> {}

#[derive(Clone)]
pub struct UnboundedCfHandle {
    inner: *mut librocksdb_sys::rocksdb_column_family_handle_t,
    _db: Arc<rocksdb::DB>,
}

impl UnboundedCfHandle {
    #[inline]
    pub fn bound(&self) -> BoundedCfHandle<'_> {
        BoundedCfHandle {
            inner: self.inner,
            _lifetime: PhantomData,
        }
    }
}

unsafe impl Send for UnboundedCfHandle {}
unsafe impl Sync for UnboundedCfHandle {}

#[derive(Copy, Clone)]
#[repr(transparent)]
struct CfHandle(*mut librocksdb_sys::rocksdb_column_family_handle_t);

impl rocksdb::AsColumnFamilyRef for CfHandle {
    #[inline]
    fn inner(&self) -> *mut librocksdb_sys::rocksdb_column_family_handle_t {
        self.0
    }
}

unsafe impl Send for CfHandle {}
unsafe impl Sync for CfHandle {}
