use anyhow::Result;
use ton_types::ByteOrderRead;

use super::{
    BlockIdShort, BlockStuff, FastHashMap, FastHasherState, StoredValue, StoredValueBuffer,
};

/// Stores last blocks for each workchain and shard
#[derive(Debug, Clone)]
pub struct TopBlocks {
    pub mc_block: (ton_block::ShardIdent, u32),
    pub shard_heights: FastHashMap<ton_block::ShardIdent, u32>,
}

impl TopBlocks {
    /// Constructs this structure for the zerostate
    pub fn zerostate() -> Self {
        Self {
            mc_block: (ton_block::ShardIdent::masterchain(), 0),
            shard_heights: FastHashMap::from_iter([(ton_block::ShardIdent::full(0), 0u32)]),
        }
    }

    /// Extracts last blocks for each workchain and shard from the given masterchain block
    pub fn from_mc_block(mc_block_data: &BlockStuff) -> Result<Self> {
        let block_id = mc_block_data.id();
        debug_assert!(block_id.shard_id.is_masterchain());

        Ok(Self {
            mc_block: (block_id.shard_id, block_id.seq_no),
            shard_heights: mc_block_data.shard_blocks_seq_no()?,
        })
    }

    /// Checks whether the given block is equal to or greater than
    /// the last block for the given shard
    pub fn contains(&self, block_id: &ton_block::BlockIdExt) -> bool {
        self.contains_shard_seq_no(&block_id.shard_id, block_id.seq_no)
    }

    /// Checks whether the given pair of [`ton_block::ShardIdent`] and seqno
    /// is equal to or greater than the last block for the given shard.
    ///
    /// NOTE: Specified shard could be split or merged
    pub fn contains_shard_seq_no(&self, shard_ident: &ton_block::ShardIdent, seq_no: u32) -> bool {
        if shard_ident.is_masterchain() {
            seq_no >= self.mc_block.1
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

    /// Returns the block seqno for the specified shard from this edge.
    ///
    /// NOTE: Specified shard could be split or merged
    pub fn get_seqno(&self, shard_ident: &ton_block::ShardIdent) -> u32 {
        if shard_ident.is_masterchain() {
            self.mc_block.1
        } else {
            match self.shard_heights.get(shard_ident) {
                Some(&top_seq_no) => top_seq_no,
                None => self
                    .shard_heights
                    .iter()
                    .find(|&(shard, _)| shard_ident.intersect_with(shard))
                    .map(|(_, &top_seq_no)| top_seq_no)
                    .unwrap_or_default(),
            }
        }
    }

    /// Returns an iterator over the short ids of the latest blocks.
    pub fn short_ids(&self) -> TopBlocksShortIdsIter<'_> {
        TopBlocksShortIdsIter {
            top_blocks: self,
            shards_iter: None,
        }
    }

    /// Returns block count (including masterchain).
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        1 + self.shard_heights.len()
    }

    /// Masterchain block seqno
    pub fn seqno(&self) -> u32 {
        self.mc_block.1
    }
}

pub struct TopBlocksShortIdsIter<'a> {
    top_blocks: &'a TopBlocks,
    shards_iter: Option<std::collections::hash_map::Iter<'a, ton_block::ShardIdent, u32>>,
}

impl<'a> Iterator for TopBlocksShortIdsIter<'a> {
    type Item = (ton_block::ShardIdent, u32);

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.shards_iter {
            None => {
                self.shards_iter = Some(self.top_blocks.shard_heights.iter());
                Some(self.top_blocks.mc_block)
            }
            Some(iter) => {
                let (shard_ident, seqno) = iter.next()?;
                Some((*shard_ident, *seqno))
            }
        }
    }
}

impl StoredValue for TopBlocks {
    const SIZE_HINT: usize = 512;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        self.mc_block.serialize(buffer);

        buffer.write_raw_slice(&(self.shard_heights.len() as u32).to_le_bytes());
        for (shard, top_block) in &self.shard_heights {
            shard.serialize(buffer);
            buffer.write_raw_slice(&top_block.to_le_bytes());
        }
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let mc_block = BlockIdShort::deserialize(reader)?;

        let top_blocks_len = reader.read_le_u32()? as usize;
        let mut top_blocks =
            FastHashMap::with_capacity_and_hasher(top_blocks_len, FastHasherState::new());

        for _ in 0..top_blocks_len {
            let shard = ton_block::ShardIdent::deserialize(reader)?;
            let top_block = reader.read_le_u32()?;
            top_blocks.insert(shard, top_block);
        }

        Ok(Self {
            mc_block,
            shard_heights: top_blocks,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_shards() {
        let mut shard_heights = FastHashMap::default();

        let main_shard = ton_block::ShardIdent::full(0);

        let (left_shard, right_shard) = main_shard.split().unwrap();
        shard_heights.insert(left_shard, 1000);
        shard_heights.insert(right_shard, 1001);

        let top_blocks = TopBlocks {
            mc_block: (ton_block::ShardIdent::masterchain(), 100),
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
