/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use std::collections::HashMap;

use anyhow::{anyhow, Result};
use ton_api::ton;
use ton_block::Deserializable;
use ton_types::{Cell, UInt256};

use crate::utils::*;

pub type BlockStuffAug = WithArchiveData<BlockStuff>;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct BlockStuff {
    id: ton_block::BlockIdExt,
    block: ton_block::Block,
    root: Cell,
}

impl BlockStuff {
    pub fn deserialize_checked(id: ton_block::BlockIdExt, data: &[u8]) -> Result<Self> {
        let file_hash = UInt256::calc_file_hash(data);
        if id.file_hash() != file_hash {
            Err(anyhow!("wrong file_hash for {}", id))
        } else {
            Self::deserialize(id, data)
        }
    }

    pub fn deserialize(id: ton_block::BlockIdExt, mut data: &[u8]) -> Result<Self> {
        let root = ton_types::deserialize_tree_of_cells(&mut data)?;
        if id.root_hash != root.repr_hash() {
            return Err(anyhow!("wrong root hash for {}", id));
        }

        let block = ton_block::Block::construct_from(&mut root.clone().into())?;
        Ok(Self { id, block, root })
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

    pub fn shard_blocks(&self) -> Result<HashMap<ton_block::ShardIdent, ton_block::BlockIdExt>> {
        let mut shards = HashMap::new();
        self.block()
            .read_extra()?
            .read_custom()?
            .ok_or_else(|| anyhow!("Given block is not a master block."))?
            .hashes()
            .iterate_shards(
                |ident: ton_block::ShardIdent, descr: ton_block::ShardDescr| {
                    let last_shard_block = ton_block::BlockIdExt {
                        shard_id: ident,
                        seq_no: descr.seq_no,
                        root_hash: descr.root_hash,
                        file_hash: descr.file_hash,
                    };
                    shards.insert(ident, last_shard_block);
                    Ok(true)
                },
            )?;

        Ok(shards)
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

pub fn convert_block_id_ext_api2blk(
    id: &ton::ton_node::blockidext::BlockIdExt,
) -> Result<ton_block::BlockIdExt> {
    Ok(ton_block::BlockIdExt::with_params(
        ton_block::ShardIdent::with_tagged_prefix(id.workchain, id.shard as u64)?,
        id.seqno as u32,
        UInt256::from(&id.root_hash.0),
        UInt256::from(&id.file_hash.0),
    ))
}

pub fn convert_block_id_ext_blk2api(
    id: &ton_block::BlockIdExt,
) -> ton::ton_node::blockidext::BlockIdExt {
    ton::ton_node::blockidext::BlockIdExt {
        workchain: id.shard_id.workchain_id(),
        shard: id.shard_id.shard_prefix_with_tag() as i64,
        seqno: id.seq_no as i32,
        root_hash: ton::int256(id.root_hash.as_slice().to_owned()),
        file_hash: ton::int256(id.file_hash.as_slice().to_owned()),
    }
}

pub fn compare_block_ids(
    id: &ton_block::BlockIdExt,
    id_api: &ton::ton_node::blockidext::BlockIdExt,
) -> bool {
    id.shard_id.shard_prefix_with_tag() == id_api.shard as u64
        && id.shard_id.workchain_id() == id_api.workchain
        && id.root_hash.as_slice() == &id_api.root_hash.0
        && id.file_hash.as_slice() == &id_api.file_hash.0
}
