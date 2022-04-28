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

    pub fn build_block_maps_edge(
        &self,
        id: &ton_block::BlockIdExt,
    ) -> Result<BlockMapsEdge, BlockMapsError> {
        let entry = self.blocks.get(id).ok_or(BlockMapsError::BlockNotFound)?;
        let block = &entry
            .block
            .as_ref()
            .ok_or(BlockMapsError::BlockDataNotFound)?
            .data;

        Ok(BlockMapsEdge {
            mc_block_seq_no: id.seq_no,
            top_shard_blocks: block
                .shard_blocks_seq_no()
                .map_err(|_| BlockMapsError::InvalidMasterchainBlock)?,
        })
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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum EdgeBlockStatus {
    /// Blocks of this shard are not presented in the archive
    Empty,
    /// No next block found in the archive
    NotFound,
    /// Found next block in the same shard
    FoundNext,
    /// Found
    FoundSplit {
        left: EdgeBlockSplitStatus,
        right: EdgeBlockSplitStatus,
    },
    FoundMerge,
}

impl EdgeBlockStatus {
    fn is_empty(&self) -> bool {
        matches!(self, EdgeBlockStatus::Empty | EdgeBlockStatus::NotFound)
    }

    fn found_split(side: AfterSplitSide) -> Self {
        Self::FoundSplit {
            left: if side == AfterSplitSide::Left {
                EdgeBlockSplitStatus::Found
            } else {
                EdgeBlockSplitStatus::Empty
            },
            right: if side == AfterSplitSide::Right {
                EdgeBlockSplitStatus::Found
            } else {
                EdgeBlockSplitStatus::Empty
            },
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum EdgeBlockSplitStatus {
    /// Blocks of this shard are not presented in the archive
    Empty,
    /// No next block found in the archive
    NotFound,
    /// Found next block in this shard
    Found,
}

#[derive(Debug, Copy, Clone)]
enum TargetSeqNo {
    /// ```text
    /// ──B──B(seq_no)──
    /// ```
    Next {
        seq_no: u32,
        shard: ton_block::ShardIdent,
    },
    /// ```text
    /// ──B─┐ <- left shard
    ///     ├─B(seq_no)──
    /// ──B─┘ <- right shard
    /// ```
    AfterMerge {
        seq_no: u32,
        left: ton_block::ShardIdent,
        right: ton_block::ShardIdent,
    },
    /// ```text
    ///    ┌─B(seq_no)── AfterSplitSide::Left
    /// ──B┤ <-parent shard
    ///    └─B(seq_no)── AfterSplitSide::Right
    /// ```
    AfterSplit {
        seq_no: u32,
        parent: ton_block::ShardIdent,
        side: AfterSplitSide,
    },
    /// No particular known seq no, but the shard must be marked
    Ancestor {
        after_seq_no: u32,
        shard: ton_block::ShardIdent,
    },
}

impl TargetSeqNo {
    fn seq_no(&self) -> u32 {
        match self {
            Self::Next { seq_no, .. }
            | Self::AfterMerge { seq_no, .. }
            | Self::AfterSplit { seq_no, .. } => *seq_no,
            Self::Ancestor { .. } => u32::MAX,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum AfterSplitSide {
    Left,
    Right,
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
    pub fn is_before(&self, id: &ton_block::BlockIdExt) -> bool {
        if id.shard_id.is_masterchain() {
            id.seq_no > self.mc_block_seq_no
        } else {
            match self.top_shard_blocks.get(&id.shard_id) {
                Some(&top_seq_no) => id.seq_no > top_seq_no,
                None => self
                    .top_shard_blocks
                    .iter()
                    .find(|&(shard, _)| id.shard_id.intersect_with(shard))
                    .map(|(_, &top_seq_no)| id.seq_no > top_seq_no)
                    .unwrap_or_default(),
            }
        }
    }

    fn find_target_seq_no(&self, shard_ident: &ton_block::ShardIdent) -> Option<TargetSeqNo> {
        // Special case for masterchain
        if shard_ident.is_masterchain() {
            return Some(TargetSeqNo::Next {
                seq_no: self.mc_block_seq_no + 1,
                shard: *shard_ident,
            });
        }

        // Simple case when we just need next block:
        // ────────────B──B──
        // stored prev ^  ^ block we need
        if let Some(seq_no) = self.top_shard_blocks.get(shard_ident) {
            return Some(TargetSeqNo::Next {
                seq_no: seq_no + 1,
                shard: *shard_ident,
            });
        }

        // Complex case when we need to find block after split
        //                 ┌─B──────
        //                 │  \
        // ──────────────B─┤   : blocks we need
        //   stored prev ^ │  /
        //                 └─B──────
        if let Ok(merged) = shard_ident.merge() {
            if let Some(seq_no) = self.top_shard_blocks.get(&merged) {
                return Some(TargetSeqNo::AfterSplit {
                    seq_no: seq_no + 1,
                    parent: merged,
                    side: if shard_ident.is_right_child() {
                        AfterSplitSide::Right
                    } else {
                        AfterSplitSide::Left
                    },
                });
            }
        }

        // Most complex case when we need to find block after merge
        // ─────────────B─┐   . block we need
        //              | │  /
        // stored prevs : ├─B─────
        //              | │
        // ─────────────B─┘
        if let Ok((left, right)) = shard_ident.split() {
            // Next block could be merged only if there are two parent blocks presented
            if let (Some(left_seq_no), Some(right_seq_no)) = (
                self.top_shard_blocks.get(&left),
                self.top_shard_blocks.get(&right),
            ) {
                return Some(TargetSeqNo::AfterMerge {
                    seq_no: std::cmp::max(left_seq_no, right_seq_no) + 1,
                    left,
                    right,
                });
            }
        }

        // If we are here, we could search an ancestor
        for (shard, seq_no) in &self.top_shard_blocks {
            if shard.is_ancestor_for(shard_ident) {
                return Some(TargetSeqNo::Ancestor {
                    after_seq_no: *seq_no,
                    shard: *shard,
                });
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

impl<'a> BlockMapsEdgeVerification<'a> {
    fn new(edge: &'a Option<BlockMapsEdge>) -> Self {
        let top_shard_blocks = match edge {
            Some(edge) => {
                let mut top_shard_blocks = FxHashMap::with_capacity_and_hasher(
                    edge.top_shard_blocks.len(),
                    Default::default(),
                );
                for shard_ident in edge.top_shard_blocks.keys() {
                    top_shard_blocks.insert(*shard_ident, EdgeBlockStatus::Empty);
                }
                top_shard_blocks
            }
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
        shard_ident: &ton_block::ShardIdent,
    ) -> BlockMapsEdgeShardVerification<'a, 'b> {
        BlockMapsEdgeShardVerification {
            target: self
                .edge
                .as_ref()
                .and_then(|edge| edge.find_target_seq_no(shard_ident)),
            possible_edge: self,
            empty: true,
            found: false,
        }
    }

    /// Finish edge verification
    fn final_check(self) -> Result<(), BlockMapsEdgeVerificationError> {
        if self.edge.is_none() {
            return Ok(());
        };

        if !self.touches_mc_block {
            return Err(BlockMapsEdgeVerificationError::NextMasterchainBlockNotFound);
        }

        for status in self.top_shard_blocks.into_values() {
            if matches!(
                status,
                EdgeBlockStatus::NotFound
                    | EdgeBlockStatus::FoundSplit {
                        left: EdgeBlockSplitStatus::NotFound,
                        ..
                    }
                    | EdgeBlockStatus::FoundSplit {
                        right: EdgeBlockSplitStatus::NotFound,
                        ..
                    }
            ) {
                return Err(BlockMapsEdgeVerificationError::NextBlockNotFound);
            }
        }

        Ok(())
    }
}

struct BlockMapsEdgeShardVerification<'a, 'b> {
    target: Option<TargetSeqNo>,
    possible_edge: &'b mut BlockMapsEdgeVerification<'a>,
    empty: bool,
    found: bool,
}

impl BlockMapsEdgeShardVerification<'_, '_> {
    /// Fills possible block
    fn update(&mut self, seq_no: u32) -> Result<(), BlockMapsEdgeVerificationError> {
        use std::collections::hash_map::Entry;

        // There is at least one block in shard
        self.empty = match &self.target {
            Some(TargetSeqNo::Ancestor { after_seq_no, .. }) => seq_no < *after_seq_no,
            Some(target) => seq_no < target.seq_no(),
            None => false,
        };

        match &self.target {
            // Any shard after the edge means that it had to split
            Some(TargetSeqNo::Ancestor {
                after_seq_no,
                shard,
            }) if seq_no >= *after_seq_no => {
                match self.possible_edge.top_shard_blocks.entry(*shard) {
                    Entry::Occupied(mut entry) => {
                        if entry.get().is_empty() {
                            entry.insert(EdgeBlockStatus::NotFound);
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(EdgeBlockStatus::NotFound);
                    }
                }
                Ok(())
            }
            // Store any possible edge block
            Some(target) if seq_no == target.seq_no() => {
                let state = &mut self.possible_edge.top_shard_blocks;

                match target {
                    // Special case for masterchain block edge
                    TargetSeqNo::Next { shard, .. } if shard.is_masterchain() => {
                        if !self.possible_edge.touches_mc_block {
                            self.possible_edge.touches_mc_block = true;
                        } else {
                            return Err(BlockMapsEdgeVerificationError::DuplicateMasterchainBlock);
                        }
                    }
                    // Direct next block found
                    TargetSeqNo::Next { shard, .. } => {
                        match state.insert(*shard, EdgeBlockStatus::FoundNext) {
                            // Previous status was empty or not found
                            Some(status) if status.is_empty() => {}
                            None => {
                                return Err(BlockMapsEdgeVerificationError::ParentBlockNotFound)
                            }
                            _ => return Err(BlockMapsEdgeVerificationError::AmbiguousEdgeBlock),
                        }
                    }
                    // Merged block found
                    TargetSeqNo::AfterMerge { left, right, .. } => {
                        match (
                            state.insert(*left, EdgeBlockStatus::FoundMerge),
                            state.insert(*right, EdgeBlockStatus::FoundMerge),
                        ) {
                            // Previous status in each parent shard was empty or not found
                            (Some(left), Some(right)) if left.is_empty() && right.is_empty() => {}
                            (None, _) | (_, None) => {
                                return Err(BlockMapsEdgeVerificationError::ParentBlockNotFound)
                            }
                            _ => return Err(BlockMapsEdgeVerificationError::AmbiguousEdgeBlock),
                        }
                    }
                    // Split block found
                    TargetSeqNo::AfterSplit { parent, side, .. } => match state.entry(*parent) {
                        Entry::Occupied(mut entry) => match entry.get_mut() {
                            // Previous parent status was empty or not found
                            status if status.is_empty() => {
                                *status = EdgeBlockStatus::found_split(*side);
                            }
                            // Merge status
                            EdgeBlockStatus::FoundSplit { left, right, .. } => match side {
                                AfterSplitSide::Left => *left = EdgeBlockSplitStatus::Found,
                                AfterSplitSide::Right => *right = EdgeBlockSplitStatus::Found,
                            },
                            _ => return Err(BlockMapsEdgeVerificationError::AmbiguousEdgeBlock),
                        },
                        Entry::Vacant(_) => {
                            return Err(BlockMapsEdgeVerificationError::ParentBlockNotFound)
                        }
                    },
                    // In most cases is unreachable, but must not panic
                    TargetSeqNo::Ancestor { .. } => {}
                }

                self.found = true;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Finish intermediate shard verification
    fn end(self) -> Result<(), BlockMapsEdgeVerificationError> {
        use std::collections::hash_map::Entry;

        // Handle case when nothing was found for this shard

        let target = match self.target {
            Some(target) if !self.empty && !self.found => target,
            _ => return Ok(()),
        };

        let edge = self.possible_edge;

        match target {
            // This shard is in the same shard as the edge block
            TargetSeqNo::Next { shard, .. } => {
                if shard.is_masterchain() {
                    // Special case for masterchain block
                    if !edge.touches_mc_block {
                        return Err(BlockMapsEdgeVerificationError::NextMasterchainBlockNotFound);
                    }
                } else if let Entry::Occupied(mut entry) = edge.top_shard_blocks.entry(shard) {
                    if entry.get().is_empty() {
                        entry.insert(EdgeBlockStatus::NotFound);
                    }
                }
            }
            // This shard is in the on of the split shards
            TargetSeqNo::AfterSplit { parent, side, .. } => {
                if let Entry::Occupied(mut entry) = edge.top_shard_blocks.entry(parent) {
                    match entry.get_mut() {
                        entry if entry.is_empty() => {
                            *entry = EdgeBlockStatus::NotFound;
                        }
                        EdgeBlockStatus::FoundSplit { left, right } => match side {
                            AfterSplitSide::Left => *left = EdgeBlockSplitStatus::NotFound,
                            AfterSplitSide::Right => *right = EdgeBlockSplitStatus::NotFound,
                        },
                        _ => { /* Do nothing */ }
                    }
                }
            }
            _ => {}
        }

        Ok(())
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
    #[error("Block not found")]
    BlockNotFound,
    #[error("Block not found in archive")]
    BlockDataNotFound,
    #[error("Block proof not found in archive")]
    BlockProofNotFound,
    #[error("Invalid masterchain block")]
    InvalidMasterchainBlock,
    #[error("Invalid block maps edge")]
    InvalidBlockMapsEdge(#[from] BlockMapsEdgeVerificationError),
}

#[derive(Debug, thiserror::Error)]
pub enum BlockMapsEdgeVerificationError {
    #[error("Duplicate masterchain block")]
    DuplicateMasterchainBlock,
    #[error("Found ambiguous edge block")]
    AmbiguousEdgeBlock,
    #[error("Parent block not found")]
    ParentBlockNotFound,
    #[error("Next masterchain block not found")]
    NextMasterchainBlockNotFound,
    #[error("Next shard block not found")]
    NextBlockNotFound,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unusual_byte_groupings)]

    use super::*;

    #[test]
    fn correct_block_maps_edge() {
        let edge = Some(make_edge(5, [(0b0_100, 5), (0b1_100, 5)]));

        // 1. Normal cases

        // 1.1. Next block in the same shard
        check_block_maps(
            [
                make_masterchain(0..10),
                make_shard(0b0_100, 0..10),
                make_shard(0b1_100, 0..10),
            ],
            &edge,
        )
        .unwrap();

        // 1.2. Next split block
        check_block_maps(
            [
                make_masterchain(0..10),
                make_shard(0b0_100, 0..10),
                make_shard(0b10_10, 6..10),
                make_shard(0b1_100, 0..5),
                make_shard(0b11_10, 6..10),
            ],
            &edge,
        )
        .unwrap();

        // 1.3. Next merged block
        check_block_maps([make_masterchain(0..10), make_shard(0b_1000, 6..10)], &edge).unwrap();

        // 2. Cases with empty shards

        // 2.1.1. One shard is missing completely
        check_block_maps([make_masterchain(0..10), make_shard(0b0_100, 0..10)], &edge).unwrap();

        // 2.1.2. One shard is missing partially
        check_block_maps(
            [
                make_masterchain(0..10),
                make_shard(0b0_100, 0..10),
                make_shard(0b1_100, 0..5),
            ],
            &edge,
        )
        .unwrap();

        // 2.2.1. Split shard is missing partially (left)
        check_block_maps(
            [
                make_masterchain(0..10),
                make_shard(0b0_100, 0..10),
                make_shard(0b10_10, 6..10),
            ],
            &edge,
        )
        .unwrap();

        // 2.2.2. Split shard is missing partially (right)
        check_block_maps(
            [
                make_masterchain(0..10),
                make_shard(0b0_100, 0..10),
                make_shard(0b11_10, 6..10),
            ],
            &edge,
        )
        .unwrap();

        // 2.2.2. Split shard is missing partially (right)
        check_block_maps(
            [
                make_masterchain(0..10),
                make_shard(0b0_100, 0..10),
                make_shard(0b11_10, 6..10),
            ],
            &edge,
        )
        .unwrap();

        // 2.2.3. Several splits after one shard
        check_block_maps(
            [
                make_masterchain(0..10),
                make_shard(0b0_100, 0..10),
                make_shard(0b100_1, 7..10),
                make_shard(0b101_1, 7..10),
                make_shard(0b10_10, 6..7),
                make_shard(0b1_100, 0..5),
                make_shard(0b11_10, 6..7),
            ],
            &edge,
        )
        .unwrap();
    }

    #[test]
    fn incorrect_block_maps_edge() {
        let edge = Some(make_edge(5, [(0b0_100, 5), (0b1_100, 5)]));

        // 1. Simple cases

        // 1.1. Next block is missing in first shard
        assert!(matches!(
            check_block_maps(
                [
                    make_masterchain(0..10),
                    make_shard(0b0_100, 7..10),
                    make_shard(0b1_100, 0..10),
                ],
                &edge,
            ),
            Err(BlockMapsEdgeVerificationError::NextBlockNotFound)
        ));

        // 1.2. Next block is missing in second shard
        assert!(matches!(
            check_block_maps(
                [
                    make_masterchain(0..10),
                    make_shard(0b0_100, 0..10),
                    make_shard(0b1_100, 7..10),
                ],
                &edge,
            ),
            Err(BlockMapsEdgeVerificationError::NextBlockNotFound)
        ));

        // 1.3. Next block is missing as a sequence hole
        assert!(matches!(
            check_block_maps(
                [
                    make_masterchain(0..10),
                    make_shard(0b0_100, 0..10),
                    make_shard(0b1_100, (0..5).chain(7..10)),
                ],
                &edge,
            ),
            Err(BlockMapsEdgeVerificationError::NextBlockNotFound)
        ));

        // 1.4. There is no next block, but there is a child shard
        assert!(matches!(
            check_block_maps(
                [
                    make_masterchain(0..10),
                    make_shard(0b0_100, 0..10),
                    make_shard(0b10_10, 8..10),
                ],
                &edge,
            ),
            Err(BlockMapsEdgeVerificationError::NextBlockNotFound)
        ));

        // 1.5. Far child
        assert!(matches!(
            check_block_maps(
                [
                    make_masterchain(0..10),
                    make_shard(0b0_100, 0..10),
                    make_shard(0b100_1, 8..10),
                ],
                &edge,
            ),
            Err(BlockMapsEdgeVerificationError::NextBlockNotFound)
        ));
    }

    fn make_masterchain(
        seqnos: impl IntoIterator<Item = u32>,
    ) -> (ton_block::ShardIdent, BTreeSet<u32>) {
        (
            ton_block::ShardIdent::masterchain(),
            seqnos.into_iter().collect(),
        )
    }

    fn make_shard(
        id: u64,
        seqnos: impl IntoIterator<Item = u32>,
    ) -> (ton_block::ShardIdent, BTreeSet<u32>) {
        let shard_ident = ton_block::ShardIdent::with_tagged_prefix(0, id << 28).unwrap();
        (shard_ident, seqnos.into_iter().collect())
    }

    fn make_edge(mc_seq_no: u32, shards: impl IntoIterator<Item = (u64, u32)>) -> BlockMapsEdge {
        BlockMapsEdge {
            mc_block_seq_no: mc_seq_no,
            top_shard_blocks: shards
                .into_iter()
                .map(|(id, seq_no)| {
                    (
                        ton_block::ShardIdent::with_tagged_prefix(0, id << 28).unwrap(),
                        seq_no,
                    )
                })
                .collect(),
        }
    }

    fn check_block_maps(
        shards: impl IntoIterator<Item = (ton_block::ShardIdent, BTreeSet<u32>)>,
        edge: &Option<BlockMapsEdge>,
    ) -> Result<(), BlockMapsEdgeVerificationError> {
        let mut possible_edge = BlockMapsEdgeVerification::new(edge);

        let shards = shards.into_iter().collect::<BTreeMap<_, _>>();
        for (shard_ident, ids) in shards.into_iter() {
            let mut possible_edge = possible_edge.begin_shard(&shard_ident);
            for seqno in ids {
                possible_edge.update(seqno)?;
            }
            possible_edge.end()?;
        }
        possible_edge.final_check()?;
        Ok(())
    }
}
