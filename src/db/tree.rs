use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use rocksdb::{
    BoundColumnFamily, Cache, DBIterator, DBPinnableSlice, DBRawIterator, IteratorMode, Options,
    ReadOptions, WriteOptions, DB,
};

pub trait Column {
    const NAME: &'static str;

    fn options(opts: &mut Options, caches: &DbCaches) {
        let _unused = opts;
        let _unused = caches;
    }

    fn write_options(opts: &mut WriteOptions) {
        let _unused = opts;
    }

    fn read_options(opts: &mut ReadOptions) {
        let _unused = opts;
    }
}

pub struct DbCaches {
    pub block_cache: Cache,
    pub compressed_block_cache: Cache,
}

impl DbCaches {
    pub fn with_capacity(capacity: usize) -> Result<Self, rocksdb::Error> {
        const MIN_CAPACITY: usize = 64 * 1024 * 1024;

        let block_cache_capacity = std::cmp::min(capacity * 2 / 3, MIN_CAPACITY);
        let compressed_block_cache_capacity =
            std::cmp::min(capacity.saturating_sub(block_cache_capacity), MIN_CAPACITY);

        Ok(Self {
            block_cache: Cache::new_lru_cache(block_cache_capacity)?,
            compressed_block_cache: Cache::new_lru_cache(compressed_block_cache_capacity)?,
        })
    }
}

pub struct DbBuilder<'a> {
    path: PathBuf,
    options: Options,
    caches: &'a DbCaches,
    descriptors: Vec<rocksdb::ColumnFamilyDescriptor>,
}

impl<'a> DbBuilder<'a> {
    pub fn new<P>(path: P, caches: &'a DbCaches) -> Self
    where
        P: AsRef<Path>,
    {
        Self {
            path: path.as_ref().into(),
            options: Default::default(),
            caches,
            descriptors: Default::default(),
        }
    }

    pub fn options<F>(mut self, mut f: F) -> Self
    where
        F: FnMut(&mut Options, &DbCaches),
    {
        f(&mut self.options, self.caches);
        self
    }

    pub fn column<T>(mut self) -> Self
    where
        T: Column,
    {
        let mut opts = Default::default();
        T::options(&mut opts, self.caches);
        self.descriptors
            .push(rocksdb::ColumnFamilyDescriptor::new(T::NAME, opts));
        self
    }

    pub fn build(self) -> Result<Arc<DB>> {
        Ok(Arc::new(DB::open_cf_descriptors(
            &self.options,
            &self.path,
            self.descriptors,
        )?))
    }
}

pub struct Tree<T> {
    db: Arc<DB>,
    write_config: WriteOptions,
    read_config: ReadOptions,
    _column: std::marker::PhantomData<T>,
}

impl<T> Tree<T>
where
    T: Column,
{
    pub fn new(db: &Arc<DB>) -> Result<Self> {
        // Check that tree exists
        db.cf_handle(T::NAME)
            .with_context(|| format!("No cf for {}", T::NAME))?;

        let mut write_config = Default::default();
        T::write_options(&mut write_config);

        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        Ok(Self {
            db: db.clone(),
            write_config,
            read_config,
            _column: Default::default(),
        })
    }

    #[inline]
    pub fn read_config(&self) -> &ReadOptions {
        &self.read_config
    }

    #[inline]
    pub fn write_config(&self) -> &WriteOptions {
        &self.write_config
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<DBPinnableSlice>> {
        let cf = self.get_cf();
        Ok(self.db.get_pinned_cf_opt(&cf, key, &self.read_config)?)
    }

    #[inline]
    pub fn insert<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let cf = self.get_cf();
        Ok(self.db.put_cf_opt(&cf, key, value, &self.write_config)?)
    }

    #[allow(dead_code)]
    pub fn remove<K: AsRef<[u8]>>(&self, key: K) -> Result<()> {
        let cf = self.get_cf();
        Ok(self.db.delete_cf_opt(&cf, key, &self.write_config)?)
    }

    #[allow(dead_code)]
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> Result<bool> {
        let cf = self.get_cf();
        Ok(self
            .db
            .get_pinned_cf_opt(&cf, key, &self.read_config)?
            .is_some())
    }

    pub fn raw_db_handle(&self) -> &Arc<DB> {
        &self.db
    }

    /// Note. get_cf Usually took p999 511ns,
    /// So we are not storing it in any way
    pub fn get_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(T::NAME).expect("Shouldn't fail")
    }

    pub fn iterator(&'_ self, mode: IteratorMode) -> DBIterator {
        let cf = self.get_cf();

        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        self.db.iterator_cf_opt(&cf, read_config, mode)
    }

    pub fn prefix_iterator<P>(&'_ self, prefix: P) -> DBRawIterator
    where
        P: AsRef<[u8]>,
    {
        let cf = self.get_cf();

        let mut read_config = Default::default();
        T::read_options(&mut read_config);
        read_config.set_prefix_same_as_start(true);

        let mut iter = self.db.raw_iterator_cf_opt(&cf, read_config);
        iter.seek(prefix.as_ref());

        iter
    }

    pub fn raw_iterator(&'_ self) -> DBRawIterator {
        let cf = self.get_cf();

        let mut read_config = Default::default();
        T::read_options(&mut read_config);

        self.db.raw_iterator_cf_opt(&cf, read_config)
    }
}
