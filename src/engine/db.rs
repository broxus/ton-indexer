use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use ton_api::ton;

use crate::storage::*;
use crate::utils::*;

pub trait Db: Send + Sync {
    fn create_or_load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: Option<&ton_block::Block>,
        utime: Option<u32>,
    ) -> Result<Arc<BlockHandle>>;
    fn load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>>;

    fn store_block_data(&self, block: &BlockStuff) -> Result<StoreBlockResult>;
    fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff>;

    fn store_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        handle: Option<Arc<BlockHandle>>,
        proof: &BlockProofStuff,
    ) -> Result<StoreBlockResult>;
    fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff>;

    fn store_shard_state(&self, handle: &Arc<BlockHandle>, state: &ShardStateStuff)
        -> Result<bool>;
    fn load_shard_state(&self, block_id: &ton_block::BlockIdExt) -> Result<ShardStateStuff>;

    fn store_block_connection(
        &self,
        handle: &BlockHandle,
        direction: BlockConnection,
        connected_block_id: &ton_block::BlockIdExt,
    ) -> Result<()>;
    fn load_block_connection(
        &self,
        block_id: &ton_block::BlockIdExt,
        direction: BlockConnection,
    ) -> Result<ton_block::BlockIdExt>;

    fn store_node_state(&self, key: &'static str, value: Vec<u8>) -> Result<()>;
    fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>>;
}

pub struct StoreBlockResult {
    pub handle: Arc<BlockHandle>,
    pub already_existed: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BlockConnection {
    Prev1,
    Prev2,
    Next1,
    Next2,
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
    fn create_or_load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: Option<&ton_block::Block>,
        utime: Option<u32>,
    ) -> Result<Arc<BlockHandle>> {
        todo!()
    }

    fn load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>> {
        todo!()
    }

    fn store_block_data(&self, block: &BlockStuff) -> Result<StoreBlockResult> {
        todo!()
    }

    fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        todo!()
    }

    fn store_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        handle: Option<Arc<BlockHandle>>,
        proof: &BlockProofStuff,
    ) -> Result<StoreBlockResult> {
        todo!()
    }

    fn load_block_proof(&self, handle: &BlockHandle, is_link: bool) -> Result<BlockProofStuff> {
        todo!()
    }

    fn store_shard_state(
        &self,
        handle: &Arc<BlockHandle>,
        state: &ShardStateStuff,
    ) -> Result<bool> {
        todo!()
    }

    fn load_shard_state(&self, block_id: &ton_block::BlockIdExt) -> Result<ShardStateStuff> {
        todo!()
    }

    fn store_block_connection(
        &self,
        handle: &BlockHandle,
        direction: BlockConnection,
        connected_block_id: &ton_block::BlockIdExt,
    ) -> Result<()> {
        todo!()
    }

    fn load_block_connection(
        &self,
        block_id: &ton_block::BlockIdExt,
        direction: BlockConnection,
    ) -> Result<ton_block::BlockIdExt> {
        todo!()
    }

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
