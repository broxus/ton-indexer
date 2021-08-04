use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use rocksdb::{BoundColumnFamily, Options, DB};

#[derive(Clone)]
pub struct Tree {
    db: Arc<DB>,
    name: String,
}

/// Note. get_cf Usually took p999 511ns,
/// So we are not storing it in any way
impl Tree {
    pub fn new(db: Arc<DB>, name: &str) -> Result<Self> {
        // Check that tree exists
        {
            let handle: Arc<BoundColumnFamily> = db
                .cf_handle(name)
                .with_context(|| format!("No cf for {}", name))?;
            drop(handle);
        }

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

    #[allow(dead_code)]
    pub fn remove<T: AsRef<[u8]>>(&self, key: T) -> Result<()> {
        let cf = self.get_cf()?;
        Ok(self.db.delete_cf(&cf, key)?)
    }

    pub fn clear(&self) -> Result<()> {
        self.db.drop_cf(&self.name)?;
        self.db.create_cf(&self.name, &default_options())?;
        Ok(())
    }

    pub fn contains_key<T: AsRef<[u8]>>(&self, key: T) -> Result<bool> {
        let cf = self.get_cf()?;
        Ok(self.db.get_pinned_cf(&cf, key)?.is_some())
    }

    pub fn raw_db_handle(&self) -> &Arc<DB> {
        &self.db
    }

    pub fn get_cf(&self) -> Result<Arc<BoundColumnFamily>> {
        self.db.cf_handle(&self.name).context("No cf")
    }
}

pub fn open_db<T, I, N>(path: T, cfs: I) -> Result<Arc<DB>>
where
    T: AsRef<Path>,
    I: IntoIterator<Item = N>,
    N: AsRef<str>,
{
    let db = Arc::new(rocksdb::DB::open_cf(&default_options(), &path, cfs)?);
    Ok(db)
}

fn default_options() -> Options {
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts
}
