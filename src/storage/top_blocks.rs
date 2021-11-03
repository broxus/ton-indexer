use std::io::{Read, Write};

use anyhow::Result;
use tiny_adnl::utils::FxHashMap;
use ton_types::ByteOrderRead;

use super::StoredValue;
use crate::utils::*;

#[derive(Debug, Clone)]
pub struct TopBlocks {
    pub target_mc_block: ton_block::BlockIdExt,
    pub shard_heights: FxHashMap<ton_block::ShardIdent, u32>,
}

impl TopBlocks {
    pub fn from_mc_block(block_data: &BlockStuff) -> Result<Self> {
        Ok(Self {
            target_mc_block: block_data.id().clone(),
            shard_heights: block_data
                .shards_blocks()?
                .into_iter()
                .map(|(key, value)| (key, value.seq_no))
                .collect(),
        })
    }

    pub fn contains(&self, block_id: &ton_block::BlockIdExt) -> Result<bool> {
        Ok(if block_id.shard_id.is_masterchain() {
            block_id.seq_no >= self.target_mc_block.seq_no
        } else {
            match self.shard_heights.get(&block_id.shard_id) {
                Some(&top_seq_no) => block_id.seq_no >= top_seq_no,
                None => return Err(TopBlocksError::TopBlocksShardNotFound.into()),
            }
        })
    }
}

impl StoredValue for TopBlocks {
    const SIZE_HINT: usize = 512;

    type OnStackSlice = [u8; 512];

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.target_mc_block.serialize(writer)?;

        writer.write_all(&(self.shard_heights.len() as u32).to_le_bytes())?;
        for (shard, top_block) in &self.shard_heights {
            shard.serialize(writer)?;
            writer.write_all(&top_block.to_le_bytes())?;
        }

        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let target_mc_block = ton_block::BlockIdExt::deserialize(reader)?;

        let top_blocks_len = reader.read_le_u32()? as usize;
        let mut top_blocks =
            FxHashMap::with_capacity_and_hasher(top_blocks_len, Default::default());

        for _ in 0..top_blocks_len {
            let shard = ton_block::ShardIdent::deserialize(reader)?;
            let top_block = reader.read_le_u32()?;
            top_blocks.insert(shard, top_block);
        }

        Ok(Self {
            target_mc_block,
            shard_heights: top_blocks,
        })
    }
}

#[derive(thiserror::Error, Debug)]
enum TopBlocksError {
    #[error("Top blocks shard not found")]
    TopBlocksShardNotFound,
}
