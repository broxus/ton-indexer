use anyhow::Result;
use dashmap::DashMap;

pub struct ShardStateStorage {
    shard_state_db: sled::Tree,
}

impl ShardStateStorage {
    pub fn with_db(shard_state_db: sled::Tree, cell_db_path: sled::Tree) -> Self {
        Self { shard_state_db }
    }

    pub fn store_state(
        &self,
        block_id: &ton_block::BlockIdExt,
        root: ton_types::Cell,
    ) -> Result<()> {
        todo!();
        //self.states.insert(block_id.clone(), root);
        Ok(())
    }

    pub fn load_state(&self, block_id: &ton_block::BlockIdExt) -> Result<ton_types::Cell> {
        // match self.states.get(block_id) {
        //     Some(root) => Ok(root.value().clone()),
        //     None => Err(ShardStateStorageError::NotFound.into()),
        // }
        todo!()
    }
}

#[derive(thiserror::Error, Debug)]
enum ShardStateStorageError {
    #[error("Not found")]
    NotFound,
}
