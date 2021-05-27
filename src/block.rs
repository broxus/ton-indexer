use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use ton_block::{BlkPrevInfo, Block, BlockIdExt, Deserializable, ShardDescr, ShardIdent};
use ton_types::{types::UInt256, Cell};

use crate::utils::*;

pub struct BlockPrevStuff {
    pub mc_block_id: BlockIdExt,
    pub prev: Vec<BlockIdExt>,
    pub after_split: bool,
}

/// It is a wrapper around various block's representations and properties.
/// # Remark
/// Because of no deterministic of a bag of cells's serialization need to store `data`
/// to make deserialization and serialization functions symmetric.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct BlockStuff {
    id: BlockIdExt,
    block: Block,
    root: Cell,
    data: Arc<Vec<u8>>, // Arc is used to make cloning more lightweight
}

impl BlockStuff {
    pub fn new(id: BlockIdExt, data: Vec<u8>) -> Result<Self> {
        let file_hash = UInt256::calc_file_hash(&data);
        if file_hash != id.file_hash {
            return Err(anyhow!(
                "block candidate has invalid file hash: declared {}, actual {}",
                id.file_hash.to_hex_string(),
                file_hash.to_hex_string()
            ));
        }

        let root =
            ton_types::deserialize_tree_of_cells(&mut std::io::Cursor::new(&data)).convert()?;
        if root.repr_hash() != id.root_hash {
            return Err(anyhow!(
                "block candidate has invalid root hash: declared {}, actual {}",
                id.root_hash.to_hex_string(),
                root.repr_hash().to_hex_string()
            ));
        }

        let block = Block::construct_from(&mut root.clone().into()).convert()?;
        Ok(Self {
            id,
            block,
            root,
            data: Arc::new(data),
        })
    }

    pub fn deserialize_checked(id: BlockIdExt, data: Vec<u8>) -> Result<Self> {
        let file_hash = UInt256::calc_file_hash(&data);
        if id.file_hash() != file_hash {
            Err(anyhow!("wrong file_hash for {}", id))
        } else {
            Self::deserialize(id, data)
        }
    }

    pub fn deserialize(id: BlockIdExt, data: Vec<u8>) -> Result<Self> {
        let root =
            ton_types::deserialize_tree_of_cells(&mut std::io::Cursor::new(&data)).convert()?;
        if id.root_hash != root.repr_hash() {
            return Err(anyhow!("wrong root hash for {}", id));
        }

        let block = Block::construct_from(&mut root.clone().into()).convert()?;
        Ok(Self {
            id,
            block,
            root,
            data: Arc::new(data),
        })
    }

    pub fn block(&self) -> &Block {
        &self.block
    }

    pub fn id(&self) -> &BlockIdExt {
        &self.id
    }

    pub fn root_cell(&self) -> &Cell {
        &self.root
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn gen_utime(&self) -> Result<u32> {
        Ok(self.block.read_info().convert()?.gen_utime().0)
    }

    pub fn construct_prev_id(&self) -> Result<(BlockIdExt, Option<BlockIdExt>)> {
        let header = self.block.read_info().convert()?;
        match header.read_prev_ref().convert()? {
            BlkPrevInfo::Block { prev } => {
                let shard_id = if header.after_split() {
                    header.shard().merge().convert()?
                } else {
                    *header.shard()
                };
                let id = BlockIdExt {
                    shard_id,
                    seq_no: prev.seq_no,
                    root_hash: prev.root_hash,
                    file_hash: prev.file_hash,
                };

                Ok((id, None))
            }
            BlkPrevInfo::Blocks { prev1, prev2 } => {
                let prev1 = prev1.read_struct().convert()?;
                let prev2 = prev2.read_struct().convert()?;
                let (shard1, shard2) = header.shard().split().convert()?;
                let id1 = BlockIdExt {
                    shard_id: shard1,
                    seq_no: prev1.seq_no,
                    root_hash: prev1.root_hash,
                    file_hash: prev1.file_hash,
                };
                let id2 = BlockIdExt {
                    shard_id: shard2,
                    seq_no: prev2.seq_no,
                    root_hash: prev2.root_hash,
                    file_hash: prev2.file_hash,
                };
                Ok((id1, Some(id2)))
            }
        }
    }

    pub fn construct_master_id(&self) -> Result<BlockIdExt> {
        let mc_id = self
            .block
            .read_info()
            .convert()?
            .read_master_ref()
            .convert()?
            .ok_or_else(|| anyhow!("Can't get master ref: given block is a master block"))?
            .master;

        Ok(BlockIdExt {
            shard_id: ShardIdent::masterchain(),
            seq_no: mc_id.seq_no,
            root_hash: mc_id.root_hash,
            file_hash: mc_id.file_hash,
        })
    }

    pub fn write_to<T: Write>(&self, dst: &mut T) -> Result<()> {
        dst.write_all(&self.data)?;
        Ok(())
    }

    pub fn shards_blocks(&self) -> Result<HashMap<ShardIdent, BlockIdExt>> {
        let mut shards = HashMap::new();
        self.block()
            .read_extra()
            .convert()?
            .read_custom()
            .convert()?
            .ok_or_else(|| anyhow!("Given block is not a master block."))?
            .hashes()
            .iterate_shards(|ident: ShardIdent, descr: ShardDescr| {
                let last_shard_block = BlockIdExt {
                    shard_id: ident,
                    seq_no: descr.seq_no,
                    root_hash: descr.root_hash,
                    file_hash: descr.file_hash,
                };
                shards.insert(ident, last_shard_block);
                Ok(true)
            })
            .convert()?;

        Ok(shards)
    }
}

pub trait BlockIdExtExtension {
    fn is_masterchain(&self) -> bool;
}

impl BlockIdExtExtension for BlockIdExt {
    fn is_masterchain(&self) -> bool {
        self.shard().is_masterchain()
    }
}

pub fn convert_block_id_ext_api2blk(
    id: &ton_api::ton::ton_node::blockidext::BlockIdExt,
) -> Result<BlockIdExt> {
    Ok(BlockIdExt::with_params(
        ton_block::ShardIdent::with_tagged_prefix(id.workchain, id.shard as u64).convert()?,
        id.seqno as u32,
        UInt256::from(&id.root_hash.0),
        UInt256::from(&id.file_hash.0),
    ))
}

#[allow(dead_code)]
pub fn convert_block_id_ext_blk2api(
    id: &BlockIdExt,
) -> ton_api::ton::ton_node::blockidext::BlockIdExt {
    ton_api::ton::ton_node::blockidext::BlockIdExt {
        workchain: id.shard_id.workchain_id(),
        shard: id.shard_id.shard_prefix_with_tag() as i64, // FIXME: why signed int???
        seqno: if id.seq_no <= i32::MAX as u32 {
            id.seq_no as i32
        } else {
            panic!("too big block seq_no") // FIXME: what to do?
        },
        root_hash: ton_api::ton::int256(id.root_hash.as_slice().to_owned()),
        file_hash: ton_api::ton::int256(id.file_hash.as_slice().to_owned()),
    }
}

#[allow(dead_code)]
pub fn convert_block_id_ext_blk_vec(
    vec: &[BlockIdExt],
) -> Vec<ton_api::ton::ton_node::blockidext::BlockIdExt> {
    vec.iter().fold(Vec::new(), |mut vec, item| {
        vec.push(convert_block_id_ext_blk2api(item));
        vec
    })
}

#[allow(dead_code)]
pub fn compare_block_ids(
    id: &BlockIdExt,
    id_api: &ton_api::ton::ton_node::blockidext::BlockIdExt,
) -> bool {
    id.shard_id.shard_prefix_with_tag() == id_api.shard as u64
        && id.shard_id.workchain_id() == id_api.workchain
        && id.root_hash.as_slice() == &id_api.root_hash.0
        && id.file_hash.as_slice() == &id_api.file_hash.0
}

// unpack_block_prev_blk_try in t-node
pub fn construct_and_check_prev_stuff(
    block_root: &Cell,
    id: &BlockIdExt,
    fetch_blkid: bool,
) -> Result<(BlockIdExt, BlockPrevStuff)> {
    let block = Block::construct_from(&mut block_root.into()).convert()?;
    let info = block.read_info().convert()?;

    if info.version() != 0 {
        return Err(anyhow!(
            "Block -> info -> version should be zero (found {})",
            info.version()
        ));
    }

    let out_block_id = if fetch_blkid {
        BlockIdExt {
            shard_id: *info.shard(),
            seq_no: info.seq_no(),
            root_hash: block_root.repr_hash(),
            file_hash: UInt256::default(),
        }
    } else {
        if id.shard() != info.shard() {
            return Err(anyhow!(
                "block header contains shard ident: {}, but expected: {}",
                info.shard(),
                id.shard()
            ));
        }
        if id.seq_no() != info.seq_no() {
            return Err(anyhow!(
                "block header contains seq_no: {}, but expected: {}",
                info.seq_no(),
                id.seq_no()
            ));
        }
        if *id.root_hash() != block_root.repr_hash() {
            return Err(anyhow!(
                "block header has incorrect root hash: {}, but expected: {}",
                block_root.repr_hash().to_hex_string(),
                id.root_hash().to_hex_string()
            ));
        }
        BlockIdExt::default()
    };

    let master_ref = info.read_master_ref().convert()?;
    if master_ref.is_some() == info.shard().is_masterchain() {
        return Err(anyhow!(
            "Block info: `info.is_master()` and `info.shard().is_masterchain()` mismatch"
        ));
    }

    let out_after_split = info.after_split();

    let mut out_prev = Vec::new();
    let prev_seqno;
    match info.read_prev_ref().convert()? {
        BlkPrevInfo::Block { prev } => {
            out_prev.push(BlockIdExt {
                shard_id: if info.after_split() {
                    info.shard().merge().convert()?
                } else {
                    *info.shard()
                },
                seq_no: prev.seq_no,
                root_hash: prev.root_hash,
                file_hash: prev.file_hash,
            });
            prev_seqno = prev.seq_no;
        }
        BlkPrevInfo::Blocks { prev1, prev2 } => {
            if info.after_split() {
                return Err(anyhow!(
                    "shardchains cannot be simultaneously split and merged at the same block"
                ));
            }
            let prev1 = prev1.read_struct().convert()?;
            let prev2 = prev2.read_struct().convert()?;
            if prev1.seq_no == 0 || prev2.seq_no == 0 {
                return Err(anyhow!(
                    "shardchains cannot be merged immediately after initial state"
                ));
            }
            let (shard1, shard2) = info.shard().split().convert()?;
            out_prev.push(BlockIdExt {
                shard_id: shard1,
                seq_no: prev1.seq_no,
                root_hash: prev1.root_hash,
                file_hash: prev1.file_hash,
            });
            out_prev.push(BlockIdExt {
                shard_id: shard2,
                seq_no: prev2.seq_no,
                root_hash: prev2.root_hash,
                file_hash: prev2.file_hash,
            });
            prev_seqno = std::cmp::max(prev1.seq_no, prev2.seq_no);
        }
    }

    if id.seq_no() != prev_seqno + 1 {
        return Err(anyhow!(
            "new block has invalid seqno (not equal to one plus maximum of seqnos of its ancestors)"
        ));
    }

    let out_mc_block_id = if info.shard().is_masterchain() {
        out_prev[0].clone()
    } else {
        let master_ref = master_ref
            .ok_or_else(|| anyhow!("non masterchain block doesn't contain mc block ref"))?;
        BlockIdExt {
            shard_id: ShardIdent::masterchain(),
            seq_no: master_ref.master.seq_no,
            root_hash: master_ref.master.root_hash,
            file_hash: master_ref.master.file_hash,
        }
    };

    if info.shard().is_masterchain() && (info.vert_seqno_incr() != 0) && !info.key_block() {
        return Err(anyhow!(
            "non-key masterchain block cannot have vert_seqno_incr set"
        ));
    }

    Ok((
        out_block_id,
        BlockPrevStuff {
            mc_block_id: out_mc_block_id,
            prev: out_prev,
            after_split: out_after_split,
        },
    ))
}
