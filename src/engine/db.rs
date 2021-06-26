use anyhow::Result;
use dashmap::DashMap;
use ton_api::ton;

pub trait Db: Send + Sync {
    fn store_node_state(&self, key: &'static str, value: Vec<u8>) -> Result<()>;
    fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>>;
}

#[derive(Default)]
pub struct InMemoryDb {
    node_state: DashMap<&'static str, Vec<u8>>,
}

impl InMemoryDb {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Db for InMemoryDb {
    fn store_node_state(&self, key: &'static str, value: Vec<u8>) -> Result<()> {
        self.node_state.insert(key, value);
        Ok(())
    }

    fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>> {
        let item = self.node_state.get(key).ok_or(DbError::NotFound)?;
        Ok(item.value().clone())
    }
}

#[derive(thiserror::Error, Debug)]
enum DbError {
    #[error("Not found")]
    NotFound,
}
