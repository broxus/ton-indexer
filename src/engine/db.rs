use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use ton_api::ton;

use crate::storage::*;
use crate::utils::*;

#[async_trait::async_trait]
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

    async fn store_block_data(&self, block: &BlockStuff) -> Result<StoreBlockResult>;
    async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>>;
    async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        let raw_block = self.load_block_data_raw(handle).await?;
        BlockStuff::deserialize(handle.id().clone(), raw_block)
    }

    async fn store_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        handle: Option<Arc<BlockHandle>>,
        proof: &BlockProofStuff,
    ) -> Result<StoreBlockResult>;
    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>>;
    async fn load_block_proof(
        &self,
        handle: &BlockHandle,
        is_link: bool,
    ) -> Result<BlockProofStuff> {
        let raw_proof = self.load_block_proof_raw(handle, is_link).await?;
        BlockProofStuff::deserialize(handle.id().clone(), raw_proof, is_link)
    }

    fn store_shard_state(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<bool>;
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

pub struct SledDb {
    block_handle_storage: BlockHandleStorage,
    node_state_storage: NodeStateStorage,
    archive_manager: ArchiveManager,
    db: sled::Db,
}

impl SledDb {
    pub async fn new() -> Result<Arc<Self>> {
        let db = sled::Config::new().temporary(true).open()?;

        Ok(Arc::new(Self {
            block_handle_storage: BlockHandleStorage::with_db(db.open_tree("block_handles")?),
            node_state_storage: NodeStateStorage::with_db(db.open_tree("node_state")?),
            archive_manager: ArchiveManager::with_root_dir("test").await?,
            db,
        }))
    }
}

#[async_trait::async_trait]
impl Db for SledDb {
    fn create_or_load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: Option<&ton_block::Block>,
        utime: Option<u32>,
    ) -> Result<Arc<BlockHandle>> {
        if let Some(handle) = self.load_block_handle(block_id)? {
            return Ok(handle);
        }

        let meta = match (block, utime) {
            (Some(block), _) => BlockMeta::from_block(block)?,
            (None, Some(utime)) if block_id.seq_no == 0 => BlockMeta::with_data(0, utime, 0, 0),
            _ if block_id.seq_no == 0 => return Err(DbError::FailedToCreateZerostateHandle.into()),
            _ => return Err(DbError::FailedToCreateBlockHandle.into()),
        };

        if let Some(handle) = self
            .block_handle_storage
            .create_handle(block_id.clone(), meta)?
        {
            return Ok(handle);
        }

        if let Some(handle) = self.load_block_handle(block_id)? {
            return Ok(handle);
        }

        Err(DbError::FailedToCreateBlockHandle.into())
    }

    fn load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>> {
        self.block_handle_storage.load_handle(block_id)
    }

    async fn store_block_data(&self, block: &BlockStuff) -> Result<StoreBlockResult> {
        let handle = self.create_or_load_block_handle(block.id(), Some(block.block()), None)?;

        let archive_id = PackageEntryId::Block(handle.id());
        let mut already_existed = true;
        if !handle.meta().has_data() || !self.archive_manager.has_file(&archive_id) {
            let _lock = handle.block_file_lock().write().await;
            if !handle.meta().has_data() || !self.archive_manager.has_file(&archive_id) {
                self.archive_manager
                    .add_file(&archive_id, block.data())
                    .await?;
                if handle.meta().set_has_data() {
                    self.block_handle_storage.store_handle(&handle)?;
                    already_existed = false;
                }
            }
        }

        Ok(StoreBlockResult {
            handle,
            already_existed,
        })
    }

    async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        if !handle.meta().has_data() {
            return Err(DbError::BlockDataNotFound.into());
        }
        self.archive_manager
            .get_file(handle, &PackageEntryId::Block(handle.id()))
            .await
    }

    async fn store_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        handle: Option<Arc<BlockHandle>>,
        proof: &BlockProofStuff,
    ) -> Result<StoreBlockResult> {
        if matches!(&handle, Some(handle) if handle.id() != block_id) {
            return Err(DbError::BlockHandleIdMismatch.into());
        }

        if block_id != proof.id() {
            return Err(DbError::BlockProofIdMismatch.into());
        }

        let handle = match handle {
            Some(handle) => handle,
            None => self.create_or_load_block_handle(
                block_id,
                Some(&proof.virtualize_block()?.0),
                None,
            )?,
        };

        let mut already_existed = true;
        if proof.is_link() {
            let archive_id = PackageEntryId::ProofLink(block_id);
            if !handle.meta().has_proof_link() || !self.archive_manager.has_file(&archive_id) {
                let _lock = handle.proof_file_lock().write().await;
                if !handle.meta().has_proof_link() || !self.archive_manager.has_file(&archive_id) {
                    self.archive_manager
                        .add_file(&archive_id, proof.data())
                        .await?;
                    if handle.meta().set_has_proof_link() {
                        self.block_handle_storage.store_handle(&handle)?;
                        already_existed = false;
                    }
                }
            }
        } else {
            let archive_id = PackageEntryId::Proof(block_id);
            if !handle.meta().has_proof() || !self.archive_manager.has_file(&archive_id) {
                let _lock = handle.proof_file_lock().write().await;
                if !handle.meta().has_proof() || !self.archive_manager.has_file(&archive_id) {
                    self.archive_manager
                        .add_file(&archive_id, proof.data())
                        .await?;
                    if handle.meta().set_has_proof() {
                        self.block_handle_storage.store_handle(&handle)?;
                        already_existed = false;
                    }
                }
            }
        }

        Ok(StoreBlockResult {
            handle,
            already_existed,
        })
    }

    async fn load_block_proof_raw(&self, handle: &BlockHandle, is_link: bool) -> Result<Vec<u8>> {
        let (archive_id, exists) = if is_link {
            (
                PackageEntryId::ProofLink(handle.id()),
                handle.meta().has_proof_link(),
            )
        } else {
            (
                PackageEntryId::Proof(handle.id()),
                handle.meta().has_proof(),
            )
        };

        if !exists {
            return Err(DbError::BlockProofNotFound.into());
        }

        self.archive_manager.get_file(handle, &archive_id).await
    }

    fn store_shard_state(&self, handle: &BlockHandle, state: &ShardStateStuff) -> Result<bool> {
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
        self.node_state_storage.store(key, value)
    }

    fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>> {
        self.node_state_storage.load(key)
    }
}

#[derive(thiserror::Error, Debug)]
enum DbError {
    #[error("Not found")]
    NotFound,
    #[error("Failed to create zero state block handle")]
    FailedToCreateZerostateHandle,
    #[error("Failed to create block handle")]
    FailedToCreateBlockHandle,
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
    #[error("Block proof id mismatch")]
    BlockProofIdMismatch,
    #[error("Block data not found")]
    BlockDataNotFound,
    #[error("Block proof not found")]
    BlockProofNotFound,
}
