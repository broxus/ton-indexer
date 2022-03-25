use std::hash::BuildHasherDefault;
use std::sync::Arc;

use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use tiny_adnl::utils::*;

use crate::storage::*;
use crate::utils::*;

#[derive(Default)]
pub struct BlockMaps {
    pub mc_block_ids: BTreeMap<u32, ton_block::BlockIdExt>,
    pub blocks: BTreeMap<ton_block::BlockIdExt, BlockMapsEntry>,
}

impl BlockMaps {
    pub fn new(data: &[u8]) -> Result<Arc<Self>> {
        let mut reader = ArchivePackageViewReader::new(data)?;

        let mut maps = BlockMaps::default();

        while let Some(entry) = reader.read_next()? {
            match PackageEntryId::from_filename(entry.name)? {
                PackageEntryId::Block(id) => {
                    maps.blocks
                        .entry(id.clone())
                        .or_insert_with(BlockMapsEntry::default)
                        .block = Some(BlockStuff::deserialize_checked(
                        id.clone(),
                        entry.data.to_vec(),
                    )?);
                    if id.is_masterchain() {
                        maps.mc_block_ids.insert(id.seq_no, id);
                    }
                }
                PackageEntryId::Proof(id) if id.is_masterchain() => {
                    maps.blocks
                        .entry(id.clone())
                        .or_insert_with(BlockMapsEntry::default)
                        .proof = Some(BlockProofStuff::deserialize(
                        id.clone(),
                        entry.data.to_vec(),
                        false,
                    )?);
                    maps.mc_block_ids.insert(id.seq_no, id);
                }
                PackageEntryId::ProofLink(id) if !id.is_masterchain() => {
                    maps.blocks
                        .entry(id.clone())
                        .or_insert_with(BlockMapsEntry::default)
                        .proof = Some(BlockProofStuff::deserialize(
                        id.clone(),
                        entry.data.to_vec(),
                        true,
                    )?);
                }
                _ => continue,
            }
        }

        Ok(Arc::new(maps))
    }

    pub fn get_first_utime(&self) -> Option<u32> {
        for block in &self.blocks {
            if let Some(a) = &block.1.block {
                return Some(a.block().info.read_struct().ok()?.gen_utime().0);
            }
        }
        None
    }

    pub fn lowest_mc_id(&self) -> Option<&ton_block::BlockIdExt> {
        self.mc_block_ids.values().next()
    }

    pub fn highest_mc_id(&self) -> Option<&ton_block::BlockIdExt> {
        self.mc_block_ids.values().rev().next()
    }

    pub fn check(&self, archive_seqno: u32) -> Result<(), BlockMapsError> {
        let mc_block_count = self.mc_block_ids.len();

        let (left, right) = match (self.lowest_mc_id(), self.highest_mc_id()) {
            (Some(left), Some(right)) => {
                log::info!(
                    "Archive {}. Blocks in masterchain: {} (from {} to {}). Total: {}",
                    archive_seqno,
                    self.mc_block_ids.len(),
                    left.seq_no,
                    right.seq_no,
                    self.blocks.len()
                );
                (left.seq_no, right.seq_no)
            }
            _ => return Err(BlockMapsError::EmptyArchive),
        };

        // NOTE: blocks are stored in BTreeSet so keys are ordered integers
        if (left as usize) + mc_block_count != (right as usize) + 1 {
            log::error!("LEFT {}, COUNT: {}, RIGHT: {}", left, mc_block_count, right);
            return Err(BlockMapsError::InconsistentMasterchainBlocks);
        }

        // Group all block ids by shards
        let mut map = FxHashMap::with_capacity_and_hasher(16, BuildHasherDefault::default());
        for block_id in self.blocks.keys() {
            map.entry(block_id.shard_id)
                .or_insert_with(BTreeSet::new)
                .insert(block_id.seq_no);
        }

        // Check consistency
        for (shard_ident, blocks) in &map {
            let mut block_seqnos = blocks.iter();

            // Skip empty shards
            let mut prev = match block_seqnos.next() {
                Some(seqno) => *seqno,
                None => continue,
            };

            // Iterate through all blocks in shard
            for &seqno in block_seqnos {
                // Search either for the previous known block in the same shard
                // or in other shards in case of merge/split
                if seqno != prev + 1 && !contains_previous_block(&map, shard_ident, seqno - 1) {
                    return Err(BlockMapsError::InconsistentShardchainBlock {
                        shard_ident: *shard_ident,
                        seqno,
                    });
                }
                // Update last known seqno for this shard
                prev = seqno;
            }
        }

        // Archive is not empty and all blocks are contiguous
        Ok(())
    }

    pub fn find_intersection(&self, prev: &Self) -> BlockMapsIntersection {
        let prev_highest_seqno = prev.mc_block_ids.keys().rev().next();

        let lowest_seqno = self.mc_block_ids.keys().next();
        let highest_seqno = self.mc_block_ids.keys().rev().next();

        match (prev_highest_seqno, lowest_seqno, highest_seqno) {
            // |-----------|
            //  ..------|
            (Some(prev_highest_seqno), _, Some(highest_seqno))
                if highest_seqno <= prev_highest_seqno =>
            {
                BlockMapsIntersection::FullOverlap
            }
            // |-----------|
            //          |--------|
            (Some(prev_highest_seqno), Some(lowest_seqno), _)
                if prev_highest_seqno + 1 >= *lowest_seqno =>
            {
                BlockMapsIntersection::Contiguous
            }
            // |-----------|    |--------|
            (Some(prev_highest_seqno), Some(lowest_seqno), _) => BlockMapsIntersection::Gap {
                from: prev_highest_seqno + 1,
                to: lowest_seqno - 1,
            },
            // One of archives is empty
            _ => BlockMapsIntersection::Undefined,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BlockMapsIntersection {
    /// Archive is fully included into other
    FullOverlap,
    /// Archive has some new blocks
    Contiguous,
    /// There is a gap between archives
    Gap { from: u32, to: u32 },
    /// Archives are invalid (mostly due to emptiness)
    Undefined,
}

#[derive(Default)]
pub struct BlockMapsEntry {
    pub block: Option<BlockStuff>,
    pub proof: Option<BlockProofStuff>,
}

impl BlockMapsEntry {
    pub fn get_data(&self) -> Result<(&BlockStuff, &BlockProofStuff), BlockMapsError> {
        let block = match &self.block {
            Some(block) => block,
            None => return Err(BlockMapsError::BlockDataNotFound),
        };
        let block_proof = match &self.proof {
            Some(proof) => proof,
            None => return Err(BlockMapsError::BlockProofNotFound),
        };
        Ok((block, block_proof))
    }
}

fn contains_previous_block(
    map: &FxHashMap<ton_block::ShardIdent, BTreeSet<u32>>,
    shard_ident: &ton_block::ShardIdent,
    prev_seqno: u32,
) -> bool {
    if let Ok((left, right)) = shard_ident.split() {
        // Check case after merge in the same archive in the left child
        if let Some(ids) = map.get(&left) {
            // Search prev seqno in the left shard
            if ids.contains(&prev_seqno) {
                return true;
            }
        }

        // Check case after merge in the same archive in the right child
        if let Some(ids) = map.get(&right) {
            // Search prev seqno in the right shard
            if ids.contains(&prev_seqno) {
                return true;
            }
        }
    }

    if let Ok(parent) = shard_ident.merge() {
        // Check case after second split in the same archive
        if let Some(ids) = map.get(&parent) {
            // Search prev shard in the parent shard
            if ids.contains(&prev_seqno) {
                return true;
            }
        }
    }

    false
}

#[derive(thiserror::Error, Debug)]
pub enum BlockMapsError {
    #[error("Empty archive")]
    EmptyArchive,
    #[error("Inconsistent masterchain blocks")]
    InconsistentMasterchainBlocks,
    #[error("Inconsistent masterchain block {shard_ident}:{seqno}")]
    InconsistentShardchainBlock {
        shard_ident: ton_block::ShardIdent,
        seqno: u32,
    },
    #[error("Block not found in archive")]
    BlockDataNotFound,
    #[error("Block proof not found in archive")]
    BlockProofNotFound,
}

pub const ARCHIVE_SLICE: u32 = 100;
