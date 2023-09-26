use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;

pub use self::block_connection_storage::*;
pub use self::block_handle_storage::*;
pub use self::models::*;
pub use self::runtime_storage::*;
pub use self::shard_state_storage::{ShardStateStorageMetrics, ShardStatesGcStatus};

use self::block_storage::*;
use self::node_state_storage::*;
use self::shard_state_storage::ShardStateStorage;
use crate::db::Db;
use crate::storage::persistent_state_storage::PersistentStateStorage;
use crate::utils::CacheStats;

mod models;

mod block_connection_storage;
mod block_handle_storage;
mod block_storage;
mod node_state_storage;
mod persistent_state_storage;
mod runtime_storage;
mod shard_state_storage;

pub struct Storage {
    file_db_path: PathBuf,

    runtime_storage: Arc<RuntimeStorage>,
    block_handle_storage: Arc<BlockHandleStorage>,
    block_storage: Arc<BlockStorage>,
    shard_state_storage: ShardStateStorage,
    block_connection_storage: BlockConnectionStorage,
    node_state_storage: NodeStateStorage,
    persistent_state_storage: PersistentStateStorage,
}

impl Storage {
    pub async fn new(
        db: Arc<Db>,
        file_db_path: PathBuf,
        max_cell_cache_size_bytes: u64,
    ) -> Result<Arc<Self>> {
        let block_handle_storage = Arc::new(BlockHandleStorage::new(db.clone())?);
        let runtime_storage = Arc::new(RuntimeStorage::new(block_handle_storage.clone()));
        let block_storage = Arc::new(BlockStorage::new(db.clone(), block_handle_storage.clone())?);
        let shard_state_storage = ShardStateStorage::new(
            db.clone(),
            block_handle_storage.clone(),
            block_storage.clone(),
            file_db_path.clone(),
            max_cell_cache_size_bytes,
        )
        .await?;
        let persistent_state_storage = PersistentStateStorage::new(
            file_db_path.clone(),
            db.clone(),
            block_handle_storage.clone(),
        )
        .await?;
        let node_state_storage = NodeStateStorage::new(db.clone())?;
        let block_connection_storage = BlockConnectionStorage::new(db)?;

        Ok(Arc::new(Self {
            file_db_path,

            block_handle_storage,
            block_storage,
            shard_state_storage,
            persistent_state_storage,
            block_connection_storage,
            node_state_storage,
            runtime_storage,
        }))
    }

    #[inline(always)]
    pub fn file_db_path(&self) -> &Path {
        &self.file_db_path
    }

    #[inline(always)]
    pub fn runtime_storage(&self) -> &RuntimeStorage {
        self.runtime_storage.as_ref()
    }

    #[inline(always)]
    pub fn persistent_state_storage(&self) -> &PersistentStateStorage {
        &self.persistent_state_storage
    }

    #[inline(always)]
    pub fn block_handle_storage(&self) -> &BlockHandleStorage {
        self.block_handle_storage.as_ref()
    }

    #[inline(always)]
    pub fn block_connection_storage(&self) -> &BlockConnectionStorage {
        &self.block_connection_storage
    }

    #[inline(always)]
    pub fn block_storage(&self) -> &BlockStorage {
        self.block_storage.as_ref()
    }

    #[inline(always)]
    pub fn shard_state_storage(&self) -> &ShardStateStorage {
        &self.shard_state_storage
    }

    #[inline(always)]
    pub fn node_state(&self) -> &NodeStateStorage {
        &self.node_state_storage
    }

    pub fn metrics(&self) -> DbMetrics {
        DbMetrics {
            shard_state_storage: self.shard_state_storage.metrics(),
        }
    }

    pub fn cells_cache_stats(&self) -> CacheStats {
        self.shard_state_storage.cache_metrics()
    }
}

#[derive(Debug, Clone)]
pub struct DbMetrics {
    pub shard_state_storage: ShardStateStorageMetrics,
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.persistent_state_storage().cancel();
    }
}
