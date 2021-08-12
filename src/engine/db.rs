use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use rocksdb::DBCompressionType;
use ton_api::ton;

use crate::storage::*;
use crate::utils::*;

pub struct Db {
    block_handle_storage: BlockHandleStorage,
    shard_state_storage: ShardStateStorage,
    node_state_storage: NodeStateStorage,
    archive_manager: ArchiveManager,
    block_index_db: BlockIndexDb,

    prev1_block_db: Tree<columns::Prev1>,
    prev2_block_db: Tree<columns::Prev2>,
    next1_block_db: Tree<columns::Next1>,
    next2_block_db: Tree<columns::Next2>,
    _db: Arc<rocksdb::DB>,
}

impl Db {
    pub async fn new<PS, PF>(sled_db_path: PS, file_db_path: PF) -> Result<Arc<Self>>
    where
        PS: AsRef<Path>,
        PF: AsRef<Path>,
    {
        let db = DbBuilder::new(sled_db_path)
            .options(|opts| {
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
                opts.set_max_background_jobs((num_cpus::get() as i32) / 2);
                opts.increase_parallelism(num_cpus::get() as i32);
                opts.set_compression_type(DBCompressionType::Zstd);
                opts.set_zstd_max_train_bytes(2 << 24);
                opts.set_stats_dump_period_sec(300);
            })
            .column::<columns::BlockHandles>()
            .column::<columns::ShardStateDb>()
            .column::<columns::CellDb>()
            .column::<columns::NodeState>()
            .column::<columns::LtDesc>()
            .column::<columns::Lt>()
            .column::<columns::Prev1>()
            .column::<columns::Prev2>()
            .column::<columns::Next1>()
            .column::<columns::Next2>()
            .column::<columns::ArchiveManagerDb>()
            .build()?;

        let block_handle_storage = BlockHandleStorage::with_db(Tree::new(db.clone())?);
        let shard_state_storage = ShardStateStorage::with_db(
            Tree::new(db.clone())?,
            Tree::new(db.clone())?,
            &file_db_path,
        )
        .await?;
        let node_state_storage = NodeStateStorage::with_db(Tree::new(db.clone())?);
        let archive_manager = ArchiveManager::with_db(Tree::new(db.clone())?);
        let block_index_db = BlockIndexDb::with_db(Tree::new(db.clone())?, Tree::new(db.clone())?);

        Ok(Arc::new(Self {
            block_handle_storage,
            shard_state_storage,
            node_state_storage,
            archive_manager,
            block_index_db,
            prev1_block_db: Tree::new(db.clone())?,
            prev2_block_db: Tree::new(db.clone())?,
            next1_block_db: Tree::new(db.clone())?,
            next2_block_db: Tree::new(db.clone())?,
            _db: db,
        }))
    }

    pub fn create_or_load_block_handle(
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

    pub fn load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>> {
        self.block_handle_storage.load_handle(block_id)
    }

    pub async fn store_block_data(&self, block: &BlockStuff) -> Result<StoreBlockResult> {
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

    pub async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        let raw_block = self.load_block_data_raw(handle).await?;
        BlockStuff::deserialize(handle.id().clone(), raw_block)
    }

    pub async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        if !handle.meta().has_data() {
            return Err(DbError::BlockDataNotFound.into());
        }
        self.archive_manager
            .get_file(handle, &PackageEntryId::Block(handle.id()))
            .await
            .map(|x| x.to_vec())
    }

    pub async fn store_block_proof(
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

    pub async fn load_block_proof(
        &self,
        handle: &BlockHandle,
        is_link: bool,
    ) -> Result<BlockProofStuff> {
        let raw_proof = self.load_block_proof_raw(handle, is_link).await?;
        BlockProofStuff::deserialize(handle.id().clone(), raw_proof, is_link)
    }

    pub async fn load_block_proof_raw(
        &self,
        handle: &BlockHandle,
        is_link: bool,
    ) -> Result<Vec<u8>> {
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

        self.archive_manager
            .get_file(handle, &archive_id)
            .await
            .map(|x| x.to_vec())
    }

    pub async fn store_shard_state(
        &self,
        handle: &Arc<BlockHandle>,
        state: &ShardStateStuff,
    ) -> Result<bool> {
        if handle.id() != state.block_id() {
            return Err(DbError::BlockHandleIdMismatch.into());
        }

        if !handle.meta().has_state() {
            self.shard_state_storage
                .store_state(state.block_id(), state.root_cell().clone())
                .await?;
            if handle.meta().set_has_state() {
                self.block_handle_storage.store_handle(handle)?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub async fn load_shard_state(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<ShardStateStuff> {
        ShardStateStuff::new(
            block_id.clone(),
            self.shard_state_storage.load_state(block_id).await?,
        )
    }

    pub fn shard_state_storage(&self) -> &ShardStateStorage {
        &self.shard_state_storage
    }

    pub fn store_block_connection(
        &self,
        handle: &Arc<BlockHandle>,
        direction: BlockConnection,
        connected_block_id: &ton_block::BlockIdExt,
    ) -> Result<()> {
        // Use strange match because all columns have different types
        let store = match direction {
            BlockConnection::Prev1 => {
                if handle.meta().has_prev1() {
                    return Ok(());
                }
                store_block_connection_impl(&self.prev1_block_db, handle, connected_block_id)?;
                handle.meta().set_has_prev1()
            }
            BlockConnection::Prev2 => {
                if handle.meta().has_prev2() {
                    return Ok(());
                }
                store_block_connection_impl(&self.prev2_block_db, handle, connected_block_id)?;
                handle.meta().set_has_prev2()
            }
            BlockConnection::Next1 => {
                if handle.meta().has_next1() {
                    return Ok(());
                }
                store_block_connection_impl(&self.next1_block_db, handle, connected_block_id)?;
                handle.meta().set_has_next1()
            }
            BlockConnection::Next2 => {
                if handle.meta().has_next2() {
                    return Ok(());
                }
                store_block_connection_impl(&self.next2_block_db, handle, connected_block_id)?;
                handle.meta().set_has_next2()
            }
        };

        if store {
            self.block_handle_storage.store_handle(handle)?;
        }

        Ok(())
    }

    pub fn load_block_connection(
        &self,
        block_id: &ton_block::BlockIdExt,
        direction: BlockConnection,
    ) -> Result<ton_block::BlockIdExt> {
        match direction {
            BlockConnection::Prev1 => load_block_connection_impl(&self.prev1_block_db, block_id),
            BlockConnection::Prev2 => load_block_connection_impl(&self.prev2_block_db, block_id),
            BlockConnection::Next1 => load_block_connection_impl(&self.next1_block_db, block_id),
            BlockConnection::Next2 => load_block_connection_impl(&self.next2_block_db, block_id),
        }
    }

    pub fn find_block_by_seq_no(
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

    pub fn find_block_by_utime(
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

    pub fn find_block_by_lt(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        lt: u64,
    ) -> Result<Arc<BlockHandle>> {
        let block_id = self.block_index_db.get_block_by_lt(account_prefix, lt)?;

        self.block_handle_storage
            .load_handle(&block_id)?
            .ok_or_else(|| DbError::BlockHandleNotFound.into())
    }

    pub fn store_block_applied(&self, handle: &Arc<BlockHandle>) -> Result<bool> {
        if handle.meta().set_is_applied() {
            self.block_handle_storage.store_handle(handle)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn store_node_state(&self, key: &'static str, value: Vec<u8>) -> Result<()> {
        self.node_state_storage.store(key, value)
    }

    pub fn load_node_state(&self, key: &'static str) -> Result<Vec<u8>> {
        self.node_state_storage.load(key)
    }

    pub fn index_handle(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        self.block_index_db.add_handle(handle)
    }

    pub fn assign_mc_ref_seq_no(
        &self,
        handle: &Arc<BlockHandle>,
        mc_ref_seq_no: u32,
    ) -> Result<()> {
        if handle.set_masterchain_ref_seqno(mc_ref_seq_no)? {
            self.block_handle_storage.store_handle(handle)?;
        }
        Ok(())
    }
}

#[inline]
fn store_block_connection_impl<T>(
    db: &Tree<T>,
    handle: &BlockHandle,
    block_id: &ton_block::BlockIdExt,
) -> Result<()>
where
    T: Column,
{
    let value = bincode::serialize(&convert_block_id_ext_blk2api(block_id))?;
    db.insert(handle.id().root_hash.as_slice(), value)
}

#[inline]
fn load_block_connection_impl<T>(
    db: &Tree<T>,
    block_id: &ton_block::BlockIdExt,
) -> Result<ton_block::BlockIdExt>
where
    T: Column,
{
    let value = match db.get(block_id.root_hash.as_slice())? {
        Some(value) => {
            bincode::deserialize::<ton::ton_node::blockidext::BlockIdExt>(value.as_ref())?
        }
        None => return Err(DbError::NotFound.into()),
    };

    convert_block_id_ext_api2blk(&value)
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
