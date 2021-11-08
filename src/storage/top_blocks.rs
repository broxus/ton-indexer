use std::io::{Read, Seek, Write};

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

    pub fn contains(&self, block_id: &ton_block::BlockIdExt) -> bool {
        self.contains_shard_seq_no(&block_id.shard_id, block_id.seq_no)
    }

    pub fn contains_shard_seq_no(&self, shard_ident: &ton_block::ShardIdent, seq_no: u32) -> bool {
        if shard_ident.is_masterchain() {
            seq_no >= self.target_mc_block.seq_no
        } else {
            match self.shard_heights.get(shard_ident) {
                Some(&top_seq_no) => seq_no >= top_seq_no,
                None => self
                    .shard_heights
                    .iter()
                    .find(|&(shard, _)| shard_ident.intersect_with(shard))
                    .map(|(_, &top_seq_no)| seq_no >= top_seq_no)
                    .unwrap_or_default(),
            }
        }
    }

    pub fn iter_shards(&'_ self) -> impl Iterator<Item = &'_ ton_block::ShardIdent> {
        std::iter::once(&self.target_mc_block.shard_id).chain(self.shard_heights.keys())
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

    fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self>
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_shards() {
        let mut shard_heights = FxHashMap::default();

        let main_shard =
            ton_block::ShardIdent::with_tagged_prefix(0, ton_block::SHARD_FULL).unwrap();

        let (left_shard, right_shard) = main_shard.split().unwrap();
        shard_heights.insert(left_shard, 1000);
        shard_heights.insert(right_shard, 1001);

        let top_blocks = TopBlocks {
            target_mc_block: ton_block::BlockIdExt {
                shard_id: ton_block::ShardIdent::masterchain(),
                seq_no: 100,
                root_hash: Default::default(),
                file_hash: Default::default(),
            },
            shard_heights,
        };

        assert!(!top_blocks.contains(&ton_block::BlockIdExt {
            shard_id: right_shard,
            seq_no: 100,
            ..Default::default()
        }));

        // Merged shard test
        assert!(!top_blocks.contains(&ton_block::BlockIdExt {
            shard_id: main_shard,
            seq_no: 100,
            ..Default::default()
        }));
        assert!(top_blocks.contains(&ton_block::BlockIdExt {
            shard_id: main_shard,
            seq_no: 10000,
            ..Default::default()
        }));

        // Split shard test
        let (right_left_shard, _) = right_shard.split().unwrap();
        assert!(!top_blocks.contains(&ton_block::BlockIdExt {
            shard_id: right_left_shard,
            seq_no: 100,
            ..Default::default()
        }));
        assert!(top_blocks.contains(&ton_block::BlockIdExt {
            shard_id: right_left_shard,
            seq_no: 10000,
            ..Default::default()
        }));
    }
}
