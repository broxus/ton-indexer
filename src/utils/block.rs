/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use anyhow::{anyhow, Context, Result};
use everscale_types::models::*;
use everscale_types::prelude::*;
use sha2::Digest;

use crate::utils::*;

pub type BlockStuffAug = WithArchiveData<BlockStuff>;

#[derive(Clone)]
pub struct BlockStuff {
    id: BlockId,
    block: Block,
}

impl BlockStuff {
    pub fn deserialize_checked(id: BlockId, data: &[u8]) -> Result<Self> {
        let file_hash = sha2::Sha256::digest(data);
        if id.file_hash.as_slice() != file_hash.as_slice() {
            Err(anyhow!("wrong file_hash for {id}"))
        } else {
            Self::deserialize(id, data)
        }
    }

    pub fn deserialize(id: BlockId, data: &[u8]) -> Result<Self> {
        let root = Boc::decode(data)?;
        if &id.root_hash != root.repr_hash() {
            return Err(anyhow!("wrong root hash for {id}"));
        }

        let block = root.parse::<Block>()?;
        Ok(Self { id, block })
    }

    #[inline(always)]
    pub fn block(&self) -> &Block {
        &self.block
    }

    pub fn into_block(self) -> Block {
        self.block
    }

    #[inline(always)]
    pub fn id(&self) -> &BlockId {
        &self.id
    }

    pub fn construct_prev_id(&self) -> Result<(BlockId, Option<BlockId>)> {
        let header = self.block.load_info()?;
        match header.load_prev_ref()? {
            PrevBlockRef::Single(prev) => {
                let shard = if header.after_split {
                    header.shard.merge().context("Failed to merge shard")?
                } else {
                    header.shard
                };

                let id = BlockId {
                    shard,
                    seqno: prev.seqno,
                    root_hash: prev.root_hash,
                    file_hash: prev.file_hash,
                };

                Ok((id, None))
            }
            PrevBlockRef::AfterMerge { left, right } => {
                let (left_shard, right_shard) =
                    header.shard.split().context("Failed to split shard")?;

                let id1 = BlockId {
                    shard: left_shard,
                    seqno: left.seqno,
                    root_hash: left.root_hash,
                    file_hash: left.file_hash,
                };

                let id2 = BlockId {
                    shard: right_shard,
                    seqno: right.seqno,
                    root_hash: right.root_hash,
                    file_hash: right.file_hash,
                };

                Ok((id1, Some(id2)))
            }
        }
    }

    pub fn shard_blocks(&self) -> Result<FastHashMap<ShardIdent, BlockId>> {
        self.block
            .load_extra()?
            .load_custom()?
            .context("Given block is not a master block.")?
            .shards
            .latest_blocks()
            .map(|id| id.map(|id| (id.shard, id)).map_err(From::from))
            .collect()
    }

    pub fn shard_blocks_seq_no(&self) -> Result<FastHashMap<ShardIdent, u32>> {
        self.block
            .load_extra()?
            .load_custom()?
            .context("Given block is not a master block.")?
            .shards
            .latest_blocks()
            .map(|id| id.map(|id| (id.shard, id.seqno)).map_err(From::from))
            .collect()
    }
}

#[derive(Debug, Copy, Clone)]
pub struct BriefBlockInfo {
    pub is_key_block: bool,
    pub gen_utime: u32,
    pub after_split: bool,
}

impl From<&BlockInfo> for BriefBlockInfo {
    fn from(info: &BlockInfo) -> Self {
        Self {
            is_key_block: info.key_block,
            gen_utime: info.gen_utime,
            after_split: info.after_split,
        }
    }
}
