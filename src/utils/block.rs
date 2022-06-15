/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use anyhow::{anyhow, Context, Result};
use rustc_hash::FxHashMap;
use ton_block::Deserializable;
use ton_types::UInt256;

use crate::utils::*;

pub type BlockStuffAug = WithArchiveData<BlockStuff>;

#[derive(Clone)]
pub struct BlockStuff {
    id: ton_block::BlockIdExt,
    block: ton_block::Block,
}

impl BlockStuff {
    pub fn deserialize_checked(id: ton_block::BlockIdExt, data: &[u8]) -> Result<Self> {
        let file_hash = UInt256::calc_file_hash(data);
        if id.file_hash() != file_hash {
            Err(anyhow!("wrong file_hash for {id}"))
        } else {
            Self::deserialize(id, data)
        }
    }

    pub fn deserialize(id: ton_block::BlockIdExt, mut data: &[u8]) -> Result<Self> {
        let root = ton_types::deserialize_tree_of_cells(&mut data)?;
        if id.root_hash != root.repr_hash() {
            return Err(anyhow!("wrong root hash for {id}"));
        }

        let block = ton_block::Block::construct_from(&mut root.into())?;
        Ok(Self { id, block })
    }

    #[inline(always)]
    pub fn block(&self) -> &ton_block::Block {
        &self.block
    }

    pub fn into_block(self) -> ton_block::Block {
        self.block
    }

    #[inline(always)]
    pub fn id(&self) -> &ton_block::BlockIdExt {
        &self.id
    }

    pub fn construct_prev_id(
        &self,
    ) -> Result<(ton_block::BlockIdExt, Option<ton_block::BlockIdExt>)> {
        let header = self.block.read_info()?;
        match header.read_prev_ref()? {
            ton_block::BlkPrevInfo::Block { prev } => {
                let shard_id = if header.after_split() {
                    header.shard().merge()?
                } else {
                    *header.shard()
                };

                let id = ton_block::BlockIdExt {
                    shard_id,
                    seq_no: prev.seq_no,
                    root_hash: prev.root_hash,
                    file_hash: prev.file_hash,
                };

                Ok((id, None))
            }
            ton_block::BlkPrevInfo::Blocks { prev1, prev2 } => {
                let prev1 = prev1.read_struct()?;
                let prev2 = prev2.read_struct()?;
                let (shard1, shard2) = header.shard().split()?;

                let id1 = ton_block::BlockIdExt {
                    shard_id: shard1,
                    seq_no: prev1.seq_no,
                    root_hash: prev1.root_hash,
                    file_hash: prev1.file_hash,
                };

                let id2 = ton_block::BlockIdExt {
                    shard_id: shard2,
                    seq_no: prev2.seq_no,
                    root_hash: prev2.root_hash,
                    file_hash: prev2.file_hash,
                };

                Ok((id1, Some(id2)))
            }
        }
    }

    pub fn shard_blocks(&self) -> Result<FxHashMap<ton_block::ShardIdent, ton_block::BlockIdExt>> {
        let mut shards = FxHashMap::default();
        self.block()
            .read_extra()?
            .read_custom()?
            .context("Given block is not a master block.")?
            .hashes()
            .iterate_shards(|ident, descr| {
                let last_shard_block = ton_block::BlockIdExt {
                    shard_id: ident,
                    seq_no: descr.seq_no,
                    root_hash: descr.root_hash,
                    file_hash: descr.file_hash,
                };
                shards.insert(ident, last_shard_block);
                Ok(true)
            })?;

        Ok(shards)
    }

    pub fn shard_blocks_seq_no(&self) -> Result<FxHashMap<ton_block::ShardIdent, u32>> {
        let mut shards = FxHashMap::default();
        self.block()
            .read_extra()?
            .read_custom()?
            .context("Given block is not a master block")?
            .hashes()
            .iterate_shards(|ident, descr| {
                shards.insert(ident, descr.seq_no);
                Ok(true)
            })?;

        Ok(shards)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct BriefBlockInfo {
    pub is_key_block: bool,
    pub gen_utime: u32,
    pub after_split: bool,
}

impl From<&ton_block::BlockInfo> for BriefBlockInfo {
    fn from(info: &ton_block::BlockInfo) -> Self {
        Self {
            is_key_block: info.key_block(),
            gen_utime: info.gen_utime().0,
            after_split: info.after_split(),
        }
    }
}

pub trait BlockIdExtExtension {
    fn is_masterchain(&self) -> bool;
}

impl BlockIdExtExtension for ton_block::BlockIdExt {
    fn is_masterchain(&self) -> bool {
        self.shard().is_masterchain()
    }
}
