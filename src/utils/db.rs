use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rocksdb::{BoundColumnFamily, Options, DB};

const CF_NAMES: [&str; 4] = [
    "prev1_block_db",
    "prev2_block_db",
    "next1_block_db",
    "next2_block_db",
];

#[derive(Clone)]
pub struct Tree {
    db: Arc<DB>,
    name: String,
}

/// Note. get_cf Usually took p999 511ns,
/// So we are not storing it in any way
impl Tree {
    pub fn new<T: AsRef<Path>>(path: &T, name: &str) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let db = Arc::new(DB::open_cf(&opts, path, &CF_NAMES)?);
        let tree: Arc<BoundColumnFamily> = db.cf_handle(name).context("No cf")?;
        //checking that tree exists
        drop(tree);
        Ok(Self {
            db,
            name: name.to_string(),
        })
    }

    pub fn get<T: AsRef<[u8]>>(&self, key: T) -> Result<Option<Vec<u8>>> {
        let cf = self.get_cf()?;
        Ok(self.db.get_cf(&cf, key)?)
    }

    pub fn insert<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let cf = self.get_cf()?;
        Ok(self.db.put_cf(&cf, key, value)?)
    }

    pub fn remove<T: AsRef<[u8]>>(&self, key: T) -> Result<()> {
        let cf = self.get_cf()?;
        Ok(self.db.delete_cf(&cf, key)?)
    }

    pub fn clear(&self) -> Result<()> {
        Ok(self.db.drop_cf(&self.name)?)
    }

    pub fn contains_key<T: AsRef<[u8]>>(&self, key: T) -> Result<bool> {
        let cf = self.get_cf()?;
        Ok(self.db.key_may_exist_cf(&cf, key))
    }

    pub fn raw_db_handle(&self) -> &Arc<DB> {
        &self.db
    }

    pub fn get_cf(&self) -> Result<Arc<BoundColumnFamily>> {
        self.db.cf_handle(&self.name).context("No cf")
    }
}
