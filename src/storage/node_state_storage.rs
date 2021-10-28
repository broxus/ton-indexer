use std::sync::Arc;

use anyhow::Result;

use super::{columns, Tree};

pub struct NodeStateStorage {
    db: Tree<columns::NodeState>,
}

impl NodeStateStorage {
    pub fn with_db(db: &Arc<rocksdb::DB>) -> Result<Self> {
        Ok(Self { db: Tree::new(db)? })
    }

    pub fn store(&self, key: &'static str, value: Vec<u8>) -> Result<()> {
        self.db.insert(key.as_bytes(), value)?;
        Ok(())
    }

    pub fn load(&self, key: &'static str) -> Result<Vec<u8>> {
        match self.db.get(key.as_bytes())? {
            Some(value) => Ok(value.to_vec()),
            None => Err(NodeStateStorageError::NotFound.into()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum NodeStateStorageError {
    #[error("Not found")]
    NotFound,
}
