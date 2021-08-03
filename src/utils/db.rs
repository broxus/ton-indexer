use std::sync::Arc;

use anyhow::{Context, Result};
use rocksdb::{BoundColumnFamily, DB};
use std::path::Path;

#[derive(Clone)]
pub struct Tree {
    db: Arc<DB>,
    name: String,
}

/// Note. get_cf Usually took p999 511ns,
/// So we are not storing it in any way
impl Tree {
    pub fn new(db: Arc<DB>, name: &str) -> Result<Self> {
        println!("{}", name);
        let _handle: Arc<BoundColumnFamily> = db
            .cf_handle(name)
            .with_context(|| format!("No cf for {}", name))?;
        //checking that tree exists
        drop(_handle);
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

pub fn open_db<T: AsRef<Path>>(path: T) -> Result<Arc<DB>> {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    const CF_NAMES: [&str; 14] = [
        "prev1_block_db",
        "prev2_block_db",
        "next1_block_db",
        "next2_block_db",
        "block_handles",
        "shard_state_db",
        "cell_db",
        "node_state",
        "lt_desc_db",
        "lt_db",
        "prev1",
        "prev2",
        "next1",
        "next2",
    ];
    let db = Arc::new(rocksdb::DB::open_cf(&opts, &path, &CF_NAMES)?);
    Ok(db)
}
