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

    pub fn commit_low_key_block(&self, id: &BlockIdExt) -> Result<()> {
        self.db.insert(LOW_ID, id.to_vec()?)
    }

    pub fn commit_high_key_block(&self, id: &BlockIdExt) -> Result<()> {
        self.db.insert(HIGH_ID, id.to_vec()?)
    }

    /// Returns Low and High key blocks
    pub fn get_committed_blocks(&self) -> Result<Option<(BlockIdExt, BlockIdExt)>> {
        let low = self
            .db
            .get(LOW_ID)?
            .map(|x| BlockIdExt::from_slice(x.as_ref()).ok())
            .flatten();
        let high = self
            .db
            .get(HIGH_ID)?
            .map(|x| BlockIdExt::from_slice(x.as_ref()).ok())
            .flatten();
        match (low, high) {
            (Some(a), Some(b)) => Ok(Some((a, b))),
            _ => Ok(None),
        }
    }
}
