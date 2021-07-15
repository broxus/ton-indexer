use std::io::Write;
use std::sync::{Arc, Weak};

use anyhow::Result;
use dashmap::DashMap;
use tokio::sync::RwLock;

use super::block_meta::BlockMeta;
use super::StoredValue;

pub struct BlockHandle {
    id: ton_block::BlockIdExt,
    meta: BlockMeta,
    block_file_lock: RwLock<()>,
    proof_file_block: RwLock<()>,
    cache: Arc<DashMap<ton_block::BlockIdExt, Weak<BlockHandle>>>,
}

impl BlockHandle {
    pub fn new(
        id: ton_block::BlockIdExt,
        cache: Arc<DashMap<ton_block::BlockIdExt, Weak<BlockHandle>>>,
    ) -> Self {
        Self::with_values(id, BlockMeta::default(), cache)
    }

    pub fn with_values(
        id: ton_block::BlockIdExt,
        meta: BlockMeta,
        cache: Arc<DashMap<ton_block::BlockIdExt, Weak<BlockHandle>>>,
    ) -> Self {
        Self {
            id,
            meta,
            block_file_lock: Default::default(),
            proof_file_block: Default::default(),
            cache,
        }
    }

    pub fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.meta.serialize(writer)
    }

    pub fn id(&self) -> &ton_block::BlockIdExt {
        &self.id
    }

    pub fn meta(&self) -> &BlockMeta {
        &self.meta
    }

    pub fn block_file_lock(&self) -> &RwLock<()> {
        &self.block_file_lock
    }

    pub fn proof_file_lock(&self) -> &RwLock<()> {
        &self.proof_file_block
    }

    pub fn has_proof_or_link(&self, is_link: &mut bool) -> bool {
        *is_link = !self.id.shard().is_masterchain();
        if *is_link {
            self.meta.has_proof_link()
        } else {
            self.meta.has_proof()
        }
    }

    pub fn masterchain_ref_seqno(&self) -> u32 {
        if self.id.shard().is_masterchain() {
            self.id.seq_no()
        } else {
            self.meta.masterchain_ref_seqno()
        }
    }

    pub fn set_masterchain_ref_seqno(&self, masterchain_ref_seqno: u32) -> Result<bool> {
        match self.meta.set_masterchain_ref_seqno(masterchain_ref_seqno) {
            0 => Ok(true),
            prev_seqno if prev_seqno == masterchain_ref_seqno => Ok(false),
            _ => Err(BlockHandleError::RefSeqnoAlreadySet.into()),
        }
    }
}

impl Drop for BlockHandle {
    fn drop(&mut self) {
        self.cache
            .remove_if(&self.id, |_, weak| weak.strong_count() == 0);
    }
}

#[derive(thiserror::Error, Debug)]
enum BlockHandleError {
    #[error("Different masterchain ref seqno has already been set")]
    RefSeqnoAlreadySet,
}
