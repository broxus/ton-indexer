use std::hash::BuildHasherDefault;
use std::sync::Arc;

use anyhow::Result;
use std::collections::{BTreeMap, HashSet};
use tiny_adnl::utils::*;

use crate::storage::*;
use crate::utils::*;

pub const ARCHIVE_SLICE: u32 = 100;

pub fn parse_archive(data: Vec<u8>) -> Result<Arc<BlockMaps>> {
    let mut reader = ArchivePackageViewReader::new(&data)?;

    let mut maps = BlockMaps::default();

    while let Some(entry) = reader.read_next()? {
        match PackageEntryId::from_filename(entry.name)? {
            PackageEntryId::Block(id) => {
                maps.blocks
                    .entry(id.clone())
                    .or_insert_with(BlocksEntry::default)
                    .block = Some(BlockStuff::deserialize_checked(
                    id.clone(),
                    entry.data.to_vec(),
                )?);
                if id.is_masterchain() {
                    maps.mc_block_ids.insert(id.seq_no, id);
                }
            }
            PackageEntryId::Proof(id) => {
                if !id.is_masterchain() {
                    continue;
                }
                maps.blocks
                    .entry(id.clone())
                    .or_insert_with(BlocksEntry::default)
                    .proof = Some(BlockProofStuff::deserialize(
                    id.clone(),
                    entry.data.to_vec(),
                    false,
                )?);
                maps.mc_block_ids.insert(id.seq_no, id);
            }
            PackageEntryId::ProofLink(id) => {
                if id.is_masterchain() {
                    continue;
                }
                maps.blocks
                    .entry(id.clone())
                    .or_insert_with(BlocksEntry::default)
                    .proof = Some(BlockProofStuff::deserialize(
                    id.clone(),
                    entry.data.to_vec(),
                    true,
                )?);
            }
        }
    }
    Ok(Arc::new(maps))
}

#[derive(Default)]
pub struct BlockMaps {
    pub mc_block_ids: BTreeMap<u32, ton_block::BlockIdExt>,
    pub blocks: BTreeMap<ton_block::BlockIdExt, BlocksEntry>,
}

impl PartialEq for BlockMaps {
    fn eq(&self, other: &Self) -> bool {
        if self.mc_block_ids.is_empty() || other.mc_block_ids.is_empty() {
            return false;
        }
        //checked upper
        self.lowest_id().unwrap() == other.lowest_id().unwrap()
    }
}

impl Eq for BlockMaps {}

impl BlockMaps {
    pub fn get_first_utime(&self) -> Option<u32> {
        for block in &self.blocks {
            if let Some(a) = &block.1.block {
                return Some(a.block().info.read_struct().ok()?.gen_utime().0);
            }
        }
        None
    }

    pub fn is_valid(&self, archive_seqno: u32) -> Option<()> {
        log::info!(
            "BLOCKS IN MASTERCHAIN: {}. Total: {}",
            self.mc_block_ids.len(),
            self.blocks.len()
        );
        if self.mc_block_ids.is_empty() {
            log::error!(
                "Expected archive len: {}. Got: {}",
                ARCHIVE_SLICE - 1,
                self.mc_block_ids.len()
            );
            return None;
        }
        let left = self.lowest_id()?.seq_no;
        let right = self.highest_id()?.seq_no;
        log::info!(
            "Archive_id: {}. Left: {}. Right: {}",
            archive_seqno,
            left,
            right
        );
        let mc_blocks = self.mc_block_ids.iter().map(|x| x.1.seq_no);
        for (expected, got) in (left..right).zip(mc_blocks) {
            if expected != got {
                log::error!("Bad mc blocks {}", archive_seqno);
                return None;
            }
        }

        let mut map = FxHashMap::with_capacity_and_hasher(16, BuildHasherDefault::default());
        for blk in self.blocks.keys() {
            map.entry(blk.shard_id)
                .or_insert_with(Vec::new)
                .push(blk.seq_no);
        }

        for (_, mut blocks) in map {
            blocks.sort_unstable();
            let mut block_seqnos = blocks.into_iter();
            let mut prev = block_seqnos.next()?;
            for seqno in block_seqnos {
                if seqno != prev + 1 {
                    log::error!(
                        "Bad shard blocks {}. Prev: {}, block: {}",
                        seqno,
                        prev,
                        seqno
                    );
                    return None;
                }
                prev = seqno;
            }
        }
        Some(())
    }

    pub fn lowest_id(&self) -> Option<&ton_block::BlockIdExt> {
        self.mc_block_ids.iter().map(|x| x.1).min()
    }

    pub fn highest_id(&self) -> Option<&ton_block::BlockIdExt> {
        self.mc_block_ids.iter().map(|x| x.1).max()
    }

    pub fn is_contiguous(left: &Self, right: &Self) -> bool {
        let left_blocks: HashSet<u32> = left.mc_block_ids.iter().map(|x| x.1.seq_no).collect();
        let right_blocks: HashSet<u32> = right.mc_block_ids.iter().map(|x| x.1.seq_no).collect();
        if left_blocks.intersection(&right_blocks).next().is_some() {
            log::info!("Intersects");
            return true;
        }

        match (left.highest_id(), right.lowest_id()) {
            (Some(left), Some(right)) => left.seq_no + 1 >= right.seq_no,
            _ => true,
        }
    }

    pub fn distance_to(&self, right: &Self) -> Option<(u32, u32)> {
        let left_highest = self.highest_id()?.seq_no;
        let right_lowest = right.lowest_id()?.seq_no;
        Some((left_highest, right_lowest))
    }
}

#[derive(Default)]
pub struct BlocksEntry {
    pub block: Option<BlockStuff>,
    pub proof: Option<BlockProofStuff>,
}

impl BlocksEntry {
    pub fn get_data(&self) -> Result<(&BlockStuff, &BlockProofStuff)> {
        let block = match &self.block {
            Some(block) => block,
            None => return Err(SyncError::BlockNotFound.into()),
        };
        let block_proof = match &self.proof {
            Some(proof) => proof,
            None => return Err(SyncError::BlockProofNotFound.into()),
        };
        Ok((block, block_proof))
    }
}

#[derive(thiserror::Error, Debug)]
enum SyncError {
    #[error("Block not found in archive")]
    BlockNotFound,
    #[error("Block proof not found in archive")]
    BlockProofNotFound,
}
