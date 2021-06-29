use anyhow::Result;

pub struct NodeStateStorage {
    db: sled::Tree,
}

impl NodeStateStorage {
    pub fn with_db(db: sled::Tree) -> Self {
        Self { db }
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
