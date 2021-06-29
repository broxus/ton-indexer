use anyhow::Result;
use dashmap::DashMap;

//TODO: rewrite using dynamic cells

#[derive(Default)]
pub struct ShardStateStorage {
    states: DashMap<ton_block::BlockIdExt, ton_types::Cell>,
}

impl ShardStateStorage {
    pub fn store_state(
        &self,
        block_id: &ton_block::BlockIdExt,
        root: ton_types::Cell,
    ) -> Result<()> {
        self.states.insert(block_id.clone(), root);
        Ok(())
    }

    pub fn load_state(&self, block_id: &ton_block::BlockIdExt) -> Result<ton_types::Cell> {
        match self.states.get(block_id) {
            Some(root) => Ok(root.value().clone()),
            None => Err(ShardStateStorageError::NotFound.into()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum ShardStateStorageError {
    #[error("Not found")]
    NotFound,
}
