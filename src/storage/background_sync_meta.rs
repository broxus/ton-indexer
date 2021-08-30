use anyhow::Result;
use ton_block::BlockIdExt;

use crate::storage::{StoredValue, Tree};

const LOW_ID: &str = "low";
const HIGH_ID: &str = "high";

pub struct BackgroundSyncMetaStore {
    db: Tree<super::columns::BackgroundSyncMeta>,
}

impl BackgroundSyncMetaStore {
    pub fn new(db: Tree<super::columns::BackgroundSyncMeta>) -> Self {
        Self { db }
    }

    pub fn store_low_key_block(&self, id: &BlockIdExt) -> Result<()> {
        self.db.insert(LOW_ID, id.to_vec()?)
    }

    pub fn load_low_key_block(&self) -> Result<Option<BlockIdExt>> {
        Ok(match self.db.get(LOW_ID)? {
            Some(data) => Some(BlockIdExt::from_slice(data.as_ref())?),
            None => None,
        })
    }

    pub fn store_high_key_block(&self, id: &BlockIdExt) -> Result<()> {
        self.db.insert(HIGH_ID, id.to_vec()?)
    }

    pub fn load_high_key_block(&self) -> Result<BlockIdExt> {
        let data = self
            .db
            .get(HIGH_ID)?
            .ok_or(BackgroundSyncMetaError::HighBlockNotFound)?;
        BlockIdExt::from_slice(data.as_ref())
    }
}

#[derive(thiserror::Error, Debug)]
enum BackgroundSyncMetaError {
    #[error("High block not found")]
    HighBlockNotFound,
}
