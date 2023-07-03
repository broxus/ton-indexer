use anyhow::Result;
use bytes::Buf;
use everscale_types::models::*;

use super::{BlockStuff, FastHashMap, FastHasherState, StoredValue, StoredValueBuffer};

/// Stores last blocks for each workchain and shard
#[derive(Debug, Clone)]
pub struct TopBlocks {
    pub mc_block: BlockIdShort,
    pub shard_heights: FastHashMap<ShardIdent, u32>,
}

impl TopBlocks {
    /// Constructs this structure for the zerostate
    pub fn zerostate() -> Self {
        Self {
            mc_block: BlockIdShort::from((ShardIdent::MASTERCHAIN, 0)),
            shard_heights: FastHashMap::from_iter([(ShardIdent::BASECHAIN, 0u32)]),
        }
    }

    /// Extracts last blocks for each workchain and shard from the given masterchain block
    pub fn from_mc_block(mc_block_data: &BlockStuff) -> Result<Self> {
        let block_id = mc_block_data.id();
        debug_assert!(block_id.shard.is_masterchain());

        Ok(Self {
            mc_block: block_id.as_short_id(),
            shard_heights: mc_block_data.shard_blocks_seq_no()?,
        })
    }

    /// Checks whether the given block is equal to or greater than
    /// the last block for the given shard
    pub fn contains(&self, block_id: &BlockId) -> bool {
        self.contains_shard_seqno(&block_id.shard, block_id.seqno)
    }

    /// Checks whether the given pair of [`ton_block::ShardIdent`] and seqno
    /// is equal to or greater than the last block for the given shard.
    ///
    /// NOTE: Specified shard could be split or merged
    pub fn contains_shard_seqno(&self, shard_ident: &ShardIdent, seq_no: u32) -> bool {
        if shard_ident.is_masterchain() {
            seq_no >= self.mc_block.seqno
        } else {
            match self.shard_heights.get(shard_ident) {
                Some(&top_seq_no) => seq_no >= top_seq_no,
                None => self
                    .shard_heights
                    .iter()
                    .find(|&(shard, _)| shard_ident.intersects(shard))
                    .map(|(_, &top_seq_no)| seq_no >= top_seq_no)
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
        self.mc_block.seqno
    }
}

pub struct TopBlocksShortIdsIter<'a> {
    top_blocks: &'a TopBlocks,
    shards_iter: Option<std::collections::hash_map::Iter<'a, ShardIdent, u32>>,
}

impl<'a> Iterator for TopBlocksShortIdsIter<'a> {
    type Item = BlockIdShort;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.shards_iter {
            None => {
                self.shards_iter = Some(self.top_blocks.shard_heights.iter());
                Some(self.top_blocks.mc_block)
            }
            Some(iter) => {
                let (shard_ident, seqno) = iter.next()?;
                Some(BlockIdShort::from((*shard_ident, *seqno)))
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

        let top_blocks_len = reader.get_u32_le() as usize;
        let mut top_blocks =
            FastHashMap::with_capacity_and_hasher(top_blocks_len, FastHasherState::new());

        for _ in 0..top_blocks_len {
            let shard = ShardIdent::deserialize(reader)?;
            let top_block = reader.get_u32_le();
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

        let main_shard = ShardIdent::new_full(0);

        let (left_shard, right_shard) = main_shard.split().unwrap();
        shard_heights.insert(left_shard, 1000);
        shard_heights.insert(right_shard, 1001);

        let top_blocks = TopBlocks {
            mc_block: (ShardIdent::MASTERCHAIN, 100).into(),
            shard_heights,
        };

        assert!(!top_blocks.contains(&BlockId {
            shard: right_shard,
            seqno: 100,
            ..Default::default()
        }));

        // Merged shard test
        assert!(!top_blocks.contains(&BlockId {
            shard: main_shard,
            seqno: 100,
            ..Default::default()
        }));
        assert!(top_blocks.contains(&BlockId {
            shard: main_shard,
            seqno: 10000,
            ..Default::default()
        }));

        // Split shard test
        let (right_left_shard, _) = right_shard.split().unwrap();
        assert!(!top_blocks.contains(&BlockId {
            shard: right_left_shard,
            seqno: 100,
            ..Default::default()
        }));
        assert!(top_blocks.contains(&BlockId {
            shard: right_left_shard,
            seqno: 10000,
            ..Default::default()
        }));
    }
}
