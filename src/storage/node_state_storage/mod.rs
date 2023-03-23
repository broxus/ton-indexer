use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::Mutex;

use crate::db::*;
use crate::utils::{read_block_id_le, write_block_id_le, StoredValue};

pub struct NodeStateStorage {
    db: Arc<Db>,
    last_mc_block_id: BlockIdCache,
    init_mc_block_id: BlockIdCache,
    shards_client_mc_block_id: BlockIdCache,
}

impl NodeStateStorage {
    pub fn new(db: Arc<Db>) -> Result<Self> {
        Ok(Self {
            db,
            last_mc_block_id: (Default::default(), LAST_MC_BLOCK_ID),
            init_mc_block_id: (Default::default(), INIT_MC_BLOCK_ID),
            shards_client_mc_block_id: (Default::default(), SHARDS_CLIENT_MC_BLOCK_ID),
        })
    }

    pub fn store_historical_sync_start(&self, id: &ton_block::BlockIdExt) -> Result<()> {
        let node_states = &self.db.node_states;
        node_states.insert(HISTORICAL_SYNC_LOW, id.to_vec())?;
        Ok(())
    }

    pub fn load_historical_sync_start(&self) -> Result<Option<ton_block::BlockIdExt>> {
        Ok(match self.db.node_states.get(HISTORICAL_SYNC_LOW)? {
            Some(data) => Some(ton_block::BlockIdExt::from_slice(data.as_ref())?),
            None => None,
        })
    }

    pub fn store_historical_sync_end(&self, id: &ton_block::BlockIdExt) -> Result<()> {
        let node_states = &self.db.node_states;
        node_states.insert(HISTORICAL_SYNC_HIGH, id.to_vec())?;
        Ok(())
    }

    pub fn load_historical_sync_end(&self) -> Result<ton_block::BlockIdExt> {
        let node_states = &self.db.node_states;
        let data = node_states
            .get(HISTORICAL_SYNC_HIGH)?
            .ok_or(NodeStateStorageError::HighBlockNotFound)?;
        ton_block::BlockIdExt::from_slice(data.as_ref())
    }

    #[allow(unused)]
    pub fn store_last_uploaded_archive(&self, archive_id: u32) -> Result<()> {
        let node_states = &self.db.node_states;
        node_states.insert(LAST_UPLOADED_ARCHIVE, archive_id.to_le_bytes())?;
        Ok(())
    }

    #[allow(unused)]
    pub fn load_last_uploaded_archive(&self) -> Result<Option<u32>> {
        Ok(match self.db.node_states.get(LAST_UPLOADED_ARCHIVE)? {
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

    pub fn load_prev_persistent_state_roots(&self) -> Result<Vec<ton_types::UInt256>> {
        self.load_hashes(PS_ROOTS_CURR)
    }

    pub fn load_current_persistent_state_roots(&self) -> Result<Vec<ton_types::UInt256>> {
        self.load_hashes(PS_ROOTS_CURR)
    }

    pub fn reset_persistent_state_roots(&self, roots: &[ton_types::UInt256]) -> Result<()> {
        let cf = &self.db.node_states.cf();

        let mut buffer = Vec::with_capacity(roots.len() * 32);
        for hash in roots {
            buffer.extend_from_slice(hash.as_array());
        }

        let mut write_batch = rocksdb::WriteBatch::default();
        write_batch.put_cf(cf, PS_ROOTS_PREV, &buffer);
        write_batch.put_cf(cf, PS_ROOTS_CURR, &buffer);

        let raw = self.db.raw();
        raw.write(write_batch)
            .context("Failed to reset persistent state roots")
    }

    pub fn update_persistent_state_roots(
        &self,
        current_roots: &[ton_types::UInt256],
    ) -> Result<()> {
        let cf = &self.db.node_states.cf();

        let current_roots = {
            let mut buffer = Vec::with_capacity(current_roots.len() * 32);
            for hash in current_roots {
                buffer.extend_from_slice(hash.as_array());
            }
            buffer
        };

        let prev_roots = self.db.node_states.get(PS_ROOTS_CURR)?;

        let mut write_batch = rocksdb::WriteBatch::default();
        if let Some(prev_roots) = prev_roots {
            write_batch.put_cf(cf, PS_ROOTS_PREV, prev_roots);
        }
        write_batch.put_cf(cf, PS_ROOTS_CURR, current_roots);

        let raw = self.db.raw();
        raw.write(write_batch)
            .context("Failed to update persistent state roots")
    }

    #[inline(always)]
    fn store_block_id(
        &self,
        (cache, key): &BlockIdCache,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<()> {
        let node_states = &self.db.node_states;
        node_states.insert(key, write_block_id_le(block_id))?;
        *cache.lock() = Some(block_id.clone());
        Ok(())
    }

    #[inline(always)]
    fn load_block_id(&self, (cache, key): &BlockIdCache) -> Result<ton_block::BlockIdExt> {
        if let Some(a) = &*cache.lock() {
            return Ok(a.clone());
        }

        let value = match self.db.node_states.get(key)? {
            Some(data) => read_block_id_le(&data).ok_or(NodeStateStorageError::InvalidBlockId)?,
            None => return Err(NodeStateStorageError::ParamNotFound.into()),
        };
        *cache.lock() = Some(value.clone());
        Ok(value)
    }

    fn load_hashes(&self, key: &[u8]) -> Result<Vec<ton_types::UInt256>> {
        let data = match self.db.node_states.get(key)? {
            Some(data) => data,
            None => return Ok(Vec::default()),
        };
        let data = data.as_ref();
        anyhow::ensure!(data.len() % 32 == 0, "Invalid hashes array");

        let mut result = Vec::with_capacity(data.len() / 32);
        for hash in data.chunks_exact(32) {
            result.push(ton_types::UInt256::from_slice(hash));
        }
        Ok(result)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum NodeStateStorageError {
    #[error("High block not found")]
    HighBlockNotFound,
    #[error("Not found")]
    ParamNotFound,
    #[error("Invalid block id")]
    InvalidBlockId,
}

type BlockIdCache = (Mutex<Option<ton_block::BlockIdExt>>, &'static [u8]);

const HISTORICAL_SYNC_LOW: &[u8] = b"background_sync_low";
const HISTORICAL_SYNC_HIGH: &[u8] = b"background_sync_high";

const LAST_UPLOADED_ARCHIVE: &[u8] = b"last_uploaded_archive";

const LAST_MC_BLOCK_ID: &[u8] = b"LastMcBlockId";
const INIT_MC_BLOCK_ID: &[u8] = b"InitMcBlockId";
const SHARDS_CLIENT_MC_BLOCK_ID: &[u8] = b"ShardsClientMcBlockId";

const PS_ROOTS_PREV: &[u8] = b"ps_roots_prev";
const PS_ROOTS_CURR: &[u8] = b"ps_roots_curr";
