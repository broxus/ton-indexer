use std::collections::{BTreeMap, BTreeSet};
use std::hash::BuildHasherDefault;
use std::sync::Arc;

use anyhow::Result;
use tiny_adnl::utils::*;

use crate::storage::*;
use crate::utils::*;

pub struct BlockMaps {
    pub mc_block_ids: BTreeMap<u32, ton_block::BlockIdExt>,
    pub blocks: BTreeMap<ton_block::BlockIdExt, BlockMapsEntry>,
}

impl BlockMaps {
    pub const MAX_MC_BLOCK_COUNT: usize = 100;

    pub fn new(data: &[u8]) -> Result<Arc<Self>> {
        let mut reader = ArchivePackageViewReader::new(data)?;

        let mut maps = BlockMaps {
            mc_block_ids: Default::default(),
            blocks: Default::default(),
        };

        while let Some(entry) = reader.read_next()? {
            match PackageEntryId::from_filename(entry.name)? {
                PackageEntryId::Block(id) => {
                    let block = BlockStuff::deserialize_checked(id.clone(), entry.data)?;

                    maps.blocks
                        .entry(id.clone())
                        .or_insert_with(BlockMapsEntry::default)
                        .block = Some(BlockStuffAug::new(block, entry.data.to_vec()));
                    if id.is_masterchain() {
                        maps.mc_block_ids.insert(id.seq_no, id);
                    }
                }
                PackageEntryId::Proof(id) if id.is_masterchain() => {
                    let proof = BlockProofStuff::deserialize(id.clone(), entry.data, false)?;

                    maps.blocks
                        .entry(id.clone())
                        .or_insert_with(BlockMapsEntry::default)
                        .proof = Some(BlockProofStuffAug::new(proof, entry.data.to_vec()));
                    maps.mc_block_ids.insert(id.seq_no, id);
                }
                PackageEntryId::ProofLink(id) if !id.is_masterchain() => {
                    let proof = BlockProofStuff::deserialize(id.clone(), entry.data, true)?;

                    maps.blocks
                        .entry(id.clone())
                        .or_insert_with(BlockMapsEntry::default)
                        .proof = Some(BlockProofStuffAug::new(proof, entry.data.to_vec()));
                }
                _ => continue,
            }
        }

        Ok(Arc::new(maps))
    }

    pub fn lowest_mc_id(&self) -> Option<&ton_block::BlockIdExt> {
        self.mc_block_ids.values().next()
    }

    pub fn highest_mc_id(&self) -> Option<&ton_block::BlockIdExt> {
        self.mc_block_ids.values().rev().next()
    }

    pub fn check(&self, index: u32, edge: &Option<BlockMapsEdge>) -> Result<(), BlockMapsError> {
        let mc_block_count = self.mc_block_ids.len();

        let (left, right) = match (self.lowest_mc_id(), self.highest_mc_id()) {
            (Some(left), Some(right)) => {
                log::info!(
                    "Archive {index} [{}..{}]. Blocks in masterchain: {}. Total: {}",
                    left.seq_no,
                    right.seq_no,
                    mc_block_count,
                    self.blocks.len()
                );
                (left.seq_no, right.seq_no)
            }
            _ => return Err(BlockMapsError::EmptyArchive),
        };

        // NOTE: blocks are stored in BTreeSet so keys are ordered integers
        if (left as usize) + mc_block_count != (right as usize) + 1 {
            return Err(BlockMapsError::InconsistentMasterchainBlocks);
        }

        // Group all block ids by shards
        let mut map = FxHashMap::with_capacity_and_hasher(16, BuildHasherDefault::default());
        for block_id in self.blocks.keys() {
            map.entry(block_id.shard_id)
                .or_insert_with(BTreeSet::new)
                .insert(block_id.seq_no);
        }

        let mut possible_edge = BlockMapsEdgeVerification::new(edge);

        // Check consistency
        for (shard_ident, blocks) in &map {
            let mut edge_verification = possible_edge.begin_shard(shard_ident);

            let mut block_seqnos = blocks
                .iter()
                .map(|&seq_no| edge_verification.update(seq_no).map(|_| seq_no));

            // Skip empty shards
            let mut prev = match block_seqnos.next().transpose()? {
                Some(seqno) => seqno,
                None => {
                    edge_verification.end()?;
                    continue;
                }
            };

            // Iterate through all blocks in shard
            while let Some(seqno) = block_seqnos.next().transpose()? {
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

            edge_verification.end()?;
        }

        // Try resolve edge
        possible_edge.final_check()?;

        // Archive is not empty and all blocks are contiguous
        Ok(())
    }
}

#[derive(Default)]
pub struct BlockMapsEntry {
    pub block: Option<BlockStuffAug>,
    pub proof: Option<BlockProofStuffAug>,
}

impl BlockMapsEntry {
    pub fn get_data(&self) -> Result<(&BlockStuffAug, &BlockProofStuffAug), BlockMapsError> {
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

/// Represents last known block seqno in each shard in the archive
#[derive(Debug, Clone)]
pub struct BlockMapsEdge {
    /// Last masterchain block seqno
    pub mc_block_seq_no: u32,
    /// Top blocks seqnos in shards for [BlockMapsEdge::mc_block_seq_no]
    pub top_shard_blocks: FxHashMap<ton_block::ShardIdent, u32>,
}

impl BlockMapsEdge {
    fn find_prev(&self, shard_ident: &ton_block::ShardIdent) -> Option<u32> {
        // Special case for masterchain
        if shard_ident.is_masterchain() {
            return Some(self.mc_block_seq_no + 1);
        }

        // Simple case when we just need next block:
        // --------------B--B--
        //              /    \
        // stored prev ^      ^ block we need
        if let Some(seqno) = self.top_shard_blocks.get(shard_ident) {
            return Some(seqno + 1);
        }

        // Complex case when we need to find block after split
        //                  B-------
        //                 / \
        // --------------B   : blocks we need
        //              / \ /
        // stored prev ^   B-------
        if let Ok(merged) = shard_ident.merge() {
            if let Some(seqno) = self.top_shard_blocks.get(&merged) {
                return Some(seqno + 1);
            }
        }

        // Most complex case when we need to find block after merge
        // ----------------B   . block we need
        //                / \ /
        // stored prevs :   B-------
        //               \ /
        // -------------- B
        if let Ok((left, right)) = shard_ident.split() {
            // Remove
            if let (Some(left), Some(right)) = (
                self.top_shard_blocks.get(&left),
                self.top_shard_blocks.get(&right),
            ) {
                return Some(std::cmp::max(left, right) + 1);
            }
        }

        None
    }
}

#[derive(Debug, Clone)]
struct BlockMapsEdgeVerification<'a> {
    edge: &'a Option<BlockMapsEdge>,
    touches_mc_block: bool,
    top_shard_blocks: FxHashMap<ton_block::ShardIdent, EdgeBlockStatus>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum EdgeBlockStatus {
    NotFound,
    Found,
}

impl<'a> BlockMapsEdgeVerification<'a> {
    fn new(edge: &'a Option<BlockMapsEdge>) -> Self {
        let top_shard_blocks = match edge {
            Some(edge) => edge
                .top_shard_blocks
                .iter()
                .map(|(shard_ident, _)| (shard_ident, EdgeBlockStatus::NotFound))
                .collect(),
            None => Default::default(),
        };

        Self {
            edge,
            touches_mc_block: false,
            top_shard_blocks,
        }
    }

    /// Starts shard verification
    fn begin_shard<'b>(
        &'b mut self,
        shard_ident: &'b ton_block::ShardIdent,
    ) -> BlockMapsEdgeShardVerification<'a, 'b> {
        BlockMapsEdgeShardVerification {
            target_seq_no: self
                .edge
                .as_ref()
                .and_then(|edge| edge.find_prev(shard_ident)),
            possible_edge: self,
            shard_ident,
            empty: true,
            found: false,
        }
    }

    /// Finish edge verification
    fn final_check(mut self) -> Result<(), PossibleBlockMapsEdgeError> {
        let edge = match self.edge {
            Some(edge) => edge,
            None => return Ok(()),
        };

        if !self.touches_mc_block {
            return Err(PossibleBlockMapsEdgeError::NextMasterchainBlockNotFound);
        }

        for (shard_ident, prev) in &edge.top_shard_blocks {
            todo!()
        }

        Ok(())
    }
}

struct BlockMapsEdgeShardVerification<'a, 'b> {
    target_seq_no: Option<u32>,
    possible_edge: &'b mut BlockMapsEdgeVerification<'a>,
    shard_ident: &'b ton_block::ShardIdent,
    empty: bool,
    found: bool,
}

impl BlockMapsEdgeShardVerification<'_, '_> {
    /// Fills possible block
    fn update(&mut self, seq_no: u32) -> Result<(), PossibleBlockMapsEdgeError> {
        // There is at least one block in shard
        self.empty = false;

        match self.target_seq_no {
            // Special case for masterchain block edge
            Some(target_seq_no) if self.shard_ident.is_masterchain() && seq_no == target_seq_no => {
                if self.possible_edge.touches_mc_block {
                    Err(PossibleBlockMapsEdgeError::DuplicateMasterchainBlock)
                } else {
                    self.possible_edge.touches_mc_block = true;
                    self.found = true;
                    Ok(())
                }
            }
            // Store any possible edge block
            Some(target_seq_no) if seq_no == target_seq_no => {
                match self
                    .possible_edge
                    .top_shard_blocks
                    .insert(*self.shard_ident, EdgeBlockStatus::Found)
                {
                    // New shard was added or we added edge block to empty shard
                    None | Some(EdgeBlockStatus::NotFound) => {
                        self.found = true;
                        Ok(())
                    }
                    _ => Err(PossibleBlockMapsEdgeError::AmbiguousEdgeBlock),
                }
            }
            _ => Ok(()),
        }
    }

    /// Finish intermediate shard verification
    fn end(self) -> Result<(), PossibleBlockMapsEdgeError> {
        match self.target_seq_no {
            // Target seqno was set, there definitely were some blocks, but we didn't find next
            Some(_) if !self.empty && !self.found => Err(if self.shard_ident.is_masterchain() {
                PossibleBlockMapsEdgeError::NextMasterchainBlockNotFound
            } else {
                PossibleBlockMapsEdgeError::NextBlockNotFound
            }),
            // Skip verification if we don't have edge block or the shard is empty
            _ => Ok(()),
        }
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
    #[error("Invalid block maps edge")]
    InvalidBlockMapsEdge(#[from] PossibleBlockMapsEdgeError),
}

#[derive(Debug, thiserror::Error)]
pub enum PossibleBlockMapsEdgeError {
    #[error("Duplicate masterchain block")]
    DuplicateMasterchainBlock,
    #[error("Found ambiguous edge block")]
    AmbiguousEdgeBlock,
    #[error("Next masterchain block not found")]
    NextMasterchainBlockNotFound,
    #[error("Next shard block not found")]
    NextBlockNotFound,
}
