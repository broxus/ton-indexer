use std::path::Path;
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

    fn store_shard_state(&self, handle: &Arc<BlockHandle>, state: &ShardStateStuff)
        -> Result<bool>;
    fn load_shard_state(&self, block_id: &ton_block::BlockIdExt) -> Result<ShardStateStuff>;

    fn store_block_connection(
        &self,
        handle: &Arc<BlockHandle>,
        direction: BlockConnection,
        connected_block_id: &ton_block::BlockIdExt,
    ) -> Result<()>;
    fn load_block_connection(
        &self,
        block_id: &ton_block::BlockIdExt,
        direction: BlockConnection,
    ) -> Result<ton_block::BlockIdExt>;

    fn find_block_by_seq_no(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        seqno: u32,
    ) -> Result<Arc<BlockHandle>>;
    fn find_block_by_utime(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        utime: u32,
    ) -> Result<Arc<BlockHandle>>;
    fn find_block_by_lt(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        lt: u64,
    ) -> Result<Arc<BlockHandle>>;

    fn store_node_state(&self, key: &'static str, value: Vec<u8>) -> Result<()>;
    fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>>;

    fn index_handle(&self, handle: &Arc<BlockHandle>) -> Result<()>;
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
    shard_state_storage: ShardStateStorage,
    node_state_storage: NodeStateStorage,
    archive_manager: ArchiveManager,
    block_index_db: BlockIndexDb,

    prev1_block_db: sled::Tree,
    prev2_block_db: sled::Tree,
    next1_block_db: sled::Tree,
    next2_block_db: sled::Tree,
    db: sled::Db,
}

impl SledDb {
    pub async fn new<PS, PF>(sled_db_path: PS, file_db_path: PF) -> Result<Arc<Self>>
    where
        PS: AsRef<Path>,
        PF: AsRef<Path>,
    {
        let db = sled::open(sled_db_path)?;

        Ok(Arc::new(Self {
            block_handle_storage: BlockHandleStorage::with_db(db.open_tree("block_handles")?),
            shard_state_storage: ShardStateStorage::with_db(
                db.open_tree("shard_state_db")?,
                db.open_tree("cell_db")?,
            ),
            node_state_storage: NodeStateStorage::with_db(db.open_tree("node_state")?),
            archive_manager: ArchiveManager::with_root_dir(file_db_path).await?,
            block_index_db: BlockIndexDb::with_db(
                db.open_tree("lt_desc_db")?,
                db.open_tree("lt_db")?,
            ),
            prev1_block_db: db.open_tree("prev1")?,
            prev2_block_db: db.open_tree("prev2")?,
            next1_block_db: db.open_tree("next1")?,
            next2_block_db: db.open_tree("next2")?,
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

    fn store_shard_state(
        &self,
        handle: &Arc<BlockHandle>,
        state: &ShardStateStuff,
    ) -> Result<bool> {
        if handle.id() != state.block_id() {
            return Err(DbError::BlockHandleIdMismatch.into());
        }

        if !handle.meta().has_state() {
            self.shard_state_storage
                .store_state(state.block_id(), state.root_cell().clone())?;
            if handle.meta().set_has_state() {
                self.block_handle_storage.store_handle(handle)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn load_shard_state(&self, block_id: &ton_block::BlockIdExt) -> Result<ShardStateStuff> {
        ShardStateStuff::new(
            block_id.clone(),
            self.shard_state_storage.load_state(block_id)?,
        )
    }

    fn store_block_connection(
        &self,
        handle: &Arc<BlockHandle>,
        direction: BlockConnection,
        connected_block_id: &ton_block::BlockIdExt,
    ) -> Result<()> {
        let (db, exists) = match direction {
            BlockConnection::Prev1 => (&self.prev1_block_db, handle.meta().has_prev1()),
            BlockConnection::Prev2 => (&self.prev2_block_db, handle.meta().has_prev2()),
            BlockConnection::Next1 => (&self.next1_block_db, handle.meta().has_next1()),
            BlockConnection::Next2 => (&self.next2_block_db, handle.meta().has_next2()),
        };

        if !exists {
            let value = bincode::serialize(&convert_block_id_ext_blk2api(connected_block_id))?;
            db.insert(handle.id().root_hash.as_slice(), value)?;
            if handle.meta().set_has_prev1() {
                self.block_handle_storage.store_handle(handle)?;
            }
        }

        Ok(())
    }

    fn load_block_connection(
        &self,
        block_id: &ton_block::BlockIdExt,
        direction: BlockConnection,
    ) -> Result<ton_block::BlockIdExt> {
        let db = match direction {
            BlockConnection::Prev1 => &self.prev1_block_db,
            BlockConnection::Prev2 => &self.prev2_block_db,
            BlockConnection::Next1 => &self.next1_block_db,
            BlockConnection::Next2 => &self.next2_block_db,
        };

        let value = match db.get(block_id.root_hash.as_slice())? {
            Some(value) => bincode::deserialize::<ton::ton_node::blockidext::BlockIdExt>(&value)?,
            None => return Err(DbError::NotFound.into()),
        };

        convert_block_id_ext_api2blk(&value)
    }

    fn find_block_by_seq_no(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        seq_no: u32,
    ) -> Result<Arc<BlockHandle>> {
        let block_id = self
            .block_index_db
            .get_block_by_seq_no(account_prefix, seq_no)?;

        self.block_handle_storage
            .load_handle(&block_id)?
            .ok_or_else(|| DbError::BlockHandleNotFound.into())
    }

    fn find_block_by_utime(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        utime: u32,
    ) -> Result<Arc<BlockHandle>> {
        let block_id = self
            .block_index_db
            .get_block_by_utime(account_prefix, utime)?;

        self.block_handle_storage
            .load_handle(&block_id)?
            .ok_or_else(|| DbError::BlockHandleNotFound.into())
    }

    fn find_block_by_lt(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        lt: u64,
    ) -> Result<Arc<BlockHandle>> {
        let block_id = self.block_index_db.get_block_by_lt(account_prefix, lt)?;

        self.block_handle_storage
            .load_handle(&block_id)?
            .ok_or_else(|| DbError::BlockHandleNotFound.into())
    }

    fn store_node_state(&self, key: &'static str, value: Vec<u8>) -> Result<()> {
        self.node_state_storage.store(key, value)
    }

    fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>> {
        self.node_state_storage.load(key)
    }

    fn index_handle(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        self.block_index_db.add_handle(handle)
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
    #[error("Block handle not found")]
    BlockHandleNotFound,
}
