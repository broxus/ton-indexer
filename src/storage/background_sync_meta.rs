use crate::storage::{StoredValue, Tree};
use anyhow::Result;
use std::convert::TryInto;
use ton_block::BlockIdExt;

const LOW_ID: &str = "low";
const HIGH_ID: &str = "high";
const SEQ_NO: &str = "seqno";

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

    pub fn commit_seq_no(&self, seq_no: u32) -> Result<()> {
        self.db.insert(SEQ_NO, seq_no.to_le_bytes())
    }

    /// Returns Low and High key blocks
    pub fn get_committed_blocks(&self) -> Result<(BlockIdExt, BlockIdExt)> {
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
            (Some(a), Some(b)) => Ok((a, b)),
            _ => anyhow::bail!("Blocks haven't been committed"),
        }
    }

    pub fn get_seqno(&self) -> Option<u32> {
        match self.db.get(&SEQ_NO).ok().flatten() {
            Some(a) => {
                let bytes: [u8; std::mem::size_of::<u32>()] = a.as_ref().try_into().ok()?;
                Some(u32::from_le_bytes(bytes))
            }
            None => None,
        }
    }
}
