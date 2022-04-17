use std::sync::Arc;

use anyhow::Result;
use parking_lot::Mutex;

use crate::storage::{columns, StoredValue, Tree};

pub struct NodeStateStorage {
    db: Tree<columns::NodeStates>,
    last_mc_block_id: BlockIdCache,
    init_mc_block_id: BlockIdCache,
    shards_client_mc_block_id: BlockIdCache,
}

impl NodeStateStorage {
    pub fn new(db: &Arc<rocksdb::DB>) -> Result<Self> {
        Ok(Self {
            db: Tree::new(db)?,
            last_mc_block_id: (Default::default(), LAST_MC_BLOCK_ID),
            init_mc_block_id: (Default::default(), INIT_MC_BLOCK_ID),
            shards_client_mc_block_id: (Default::default(), SHARDS_CLIENT_MC_BLOCK_ID),
        })
    }

    pub fn store_background_sync_start(&self, id: &ton_block::BlockIdExt) -> Result<()> {
        self.db.insert(BACKGROUND_SYNC_LOW, id.to_vec())
    }

    pub fn load_background_sync_start(&self) -> Result<Option<ton_block::BlockIdExt>> {
        Ok(match self.db.get(BACKGROUND_SYNC_LOW)? {
            Some(data) => Some(ton_block::BlockIdExt::from_slice(data.as_ref())?),
            None => None,
        })
    }

    pub fn store_background_sync_end(&self, id: &ton_block::BlockIdExt) -> Result<()> {
        self.db.insert(BACKGROUND_SYNC_HIGH, id.to_vec())
    }

    pub fn load_background_sync_end(&self) -> Result<ton_block::BlockIdExt> {
        let data = self
            .db
            .get(BACKGROUND_SYNC_HIGH)?
            .ok_or(NodeStateStoreError::HighBlockNotFound)?;
        ton_block::BlockIdExt::from_slice(data.as_ref())
    }

    #[allow(unused)]
    pub fn store_last_uploaded_archive(&self, archive_id: u32) -> Result<()> {
        self.db
            .insert(LAST_UPLOADED_ARCHIVE, archive_id.to_le_bytes())
    }

    #[allow(unused)]
    pub fn load_last_uploaded_archive(&self) -> Result<Option<u32>> {
        Ok(match self.db.get(LAST_UPLOADED_ARCHIVE)? {
            Some(data) if data.len() >= 4 => {
                Some(u32::from_le_bytes(data[..4].try_into().unwrap()))
            }
            _ => None,
        })
    }

    pub fn store_last_mc_block_id(&self, id: &ton_block::BlockIdExt) -> Result<()> {
        self.store_block_id(&self.last_mc_block_id, id)
    }

    pub fn load_last_mc_block_id(&self) -> Result<ton_block::BlockIdExt> {
        self.load_block_id(&self.last_mc_block_id)
    }

    pub fn store_init_mc_block_id(&self, id: &ton_block::BlockIdExt) -> Result<()> {
        self.store_block_id(&self.init_mc_block_id, id)
    }

    pub fn load_init_mc_block_id(&self) -> Result<ton_block::BlockIdExt> {
        self.load_block_id(&self.init_mc_block_id)
    }

    pub fn store_shards_client_mc_block_id(&self, id: &ton_block::BlockIdExt) -> Result<()> {
        self.store_block_id(&self.shards_client_mc_block_id, id)
    }

    pub fn load_shards_client_mc_block_id(&self) -> Result<ton_block::BlockIdExt> {
        self.load_block_id(&self.shards_client_mc_block_id)
    }

    #[inline(always)]
    fn store_block_id(
        &self,
        (cache, key): &BlockIdCache,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<()> {
        self.db.insert(key, write_block_id_le(block_id))?;
        *cache.lock() = Some(block_id.clone());
        Ok(())
    }

    #[inline(always)]
    fn load_block_id(&self, (cache, key): &BlockIdCache) -> Result<ton_block::BlockIdExt> {
        if let Some(a) = &*cache.lock() {
            return Ok(a.clone());
        }

        let value = match self.db.get(key)? {
            Some(data) => read_block_id_le(&data)?,
            None => return Err(NodeStateStoreError::ParamNotFound.into()),
        };
        *cache.lock() = Some(value.clone());
        Ok(value)
    }
}

/// Writes BlockIdExt in little-endian format
fn write_block_id_le(block_id: &ton_block::BlockIdExt) -> [u8; 80] {
    let mut bytes = [0u8; 80];
    bytes[..4].copy_from_slice(&block_id.shard_id.workchain_id().to_le_bytes());
    bytes[4..12].copy_from_slice(&block_id.shard_id.shard_prefix_with_tag().to_le_bytes());
    bytes[12..16].copy_from_slice(&block_id.seq_no.to_le_bytes());
    bytes[16..48].copy_from_slice(block_id.root_hash.as_slice());
    bytes[48..80].copy_from_slice(block_id.file_hash.as_slice());
    bytes
}

/// Reads BlockIdExt in little-endian format
fn read_block_id_le(data: &[u8]) -> Result<ton_block::BlockIdExt, NodeStateStoreError> {
    if data.len() < 80 {
        return Err(NodeStateStoreError::InvalidBlockId);
    }

    let mut workchain_id = [0; 4];
    workchain_id.copy_from_slice(&data[0..4]);
    let workchain_id = i32::from_le_bytes(workchain_id);

    let mut shard_id = [0; 8];
    shard_id.copy_from_slice(&data[4..12]);
    let shard_id = u64::from_le_bytes(shard_id);

    let mut seq_no = [0; 4];
    seq_no.copy_from_slice(&data[12..16]);
    let seq_no = u32::from_le_bytes(seq_no);

    let mut root_hash = [0; 32];
    root_hash.copy_from_slice(&data[16..48]);

    let mut file_hash = [0; 32];
    file_hash.copy_from_slice(&data[48..80]);

    let shard_id =
        unsafe { ton_block::ShardIdent::with_tagged_prefix_unchecked(workchain_id, shard_id) };

    Ok(ton_block::BlockIdExt {
        shard_id,
        seq_no,
        root_hash: root_hash.into(),
        file_hash: file_hash.into(),
    })
}

#[derive(thiserror::Error, Debug)]
enum NodeStateStoreError {
    #[error("High block not found")]
    HighBlockNotFound,
    #[error("Not found")]
    ParamNotFound,
    #[error("Invalid block id")]
    InvalidBlockId,
}

type BlockIdCache = (Mutex<Option<ton_block::BlockIdExt>>, &'static [u8]);

const BACKGROUND_SYNC_LOW: &[u8] = b"background_sync_low";
const BACKGROUND_SYNC_HIGH: &[u8] = b"background_sync_high";

const LAST_UPLOADED_ARCHIVE: &[u8] = b"last_uploaded_archive";

const LAST_MC_BLOCK_ID: &[u8] = b"LastMcBlockId";
const INIT_MC_BLOCK_ID: &[u8] = b"InitMcBlockId";
const SHARDS_CLIENT_MC_BLOCK_ID: &[u8] = b"ShardsClientMcBlockId";
