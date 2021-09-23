use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::hash::BuildHasherDefault;
use std::option::Option::Some;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;
use tiny_adnl::utils::FxHashMap;
use ton_block::BlockIdExt;

use crate::engine::complex_operations::sync::archive_downloader::{
    download_archive_maps, download_archive_or_die, start_download, ARCHIVE_SLICE,
};
use crate::engine::Engine;
use crate::storage::*;
use crate::utils::*;

mod archive_downloader;

async fn last_applied_block(engine: &Engine) -> Result<ton_block::BlockIdExt> {
    let last_mc_block_id = {
        let mc_block_id = engine.load_last_applied_mc_block_id().await?;
        let sc_block_id = engine.load_shards_client_mc_block_id().await?;
        log::info!("sync: Last applied block id: {}", mc_block_id);
        log::info!("sync: Last shards client block id: {}", sc_block_id);

        match (mc_block_id, sc_block_id) {
            (mc_block_id, sc_block_id) if mc_block_id.seq_no > sc_block_id.seq_no => sc_block_id,
            (mc_block_id, _) => mc_block_id,
        }
    };
    Ok(last_mc_block_id)
}

pub async fn sync(engine: &Arc<Engine>) -> Result<()> {
    log::info!("Started sync");

    let active_peers = Arc::new(ActivePeers::default());
    let last_applied = engine.load_last_applied_mc_block_id().await?.seq_no;
    let theoretical_high_seqno = u32::MAX;
    log::info!(
        "Creating archives stream from {} to {}",
        last_applied,
        theoretical_high_seqno
    );
    let archives = start_download(
        engine.clone(),
        active_peers.clone(),
        ARCHIVE_SLICE,
        last_applied,
        theoretical_high_seqno,
    )
    .await;
    if let Some(mut archives) = archives {
        log::info!("Starting fast sync");
        let mut prev_step = std::time::Instant::now();
        while let Some(archive) = archives.next().await {
            log::info!(
                "Time from prev step = {}",
                (std::time::Instant::now() - prev_step).as_millis()
            );
            prev_step = std::time::Instant::now();
            match archive.get_first_utime() {
                Some(a) => {
                    if tiny_adnl::utils::now() as u32 - a < 3600 {
                        log::info!("Stopping fast sync");
                        break;
                    }
                }
                None => break,
            }
            let mc_block_id = last_applied_block(engine).await?;
            let now = std::time::Instant::now();
            match import_package(engine, archive, &mc_block_id).await {
                Ok(()) => {
                    log::info!(
                        "sync: Imported archive package for block {}. Took: {}",
                        mc_block_id.seq_no,
                        (std::time::Instant::now() - now).as_millis()
                    );
                }
                Err(e) => {
                    log::error!(
                        "sync: Failed to apply queued archive for block {}: {:?}",
                        mc_block_id.seq_no,
                        e
                    );
                }
            }

            log::info!(
                "Full cycle took: {}",
                (std::time::Instant::now() - prev_step).as_millis()
            );
        }
    }
    log::info!("Finished fast sync");
    'outer: while !engine.is_synced().await? {
        let last_mc_block_id = {
            let mc_block_id = engine.load_last_applied_mc_block_id().await?;
            let sc_block_id = engine.load_shards_client_mc_block_id().await?;
            log::info!("sync: Last applied block id: {}", mc_block_id);
            log::info!("sync: Last shards client block id: {}", sc_block_id);

            match (mc_block_id, sc_block_id) {
                (mc_block_id, sc_block_id) if mc_block_id.seq_no > sc_block_id.seq_no => {
                    sc_block_id
                }
                (mc_block_id, _) => mc_block_id,
            }
        };

        log::info!(
            "sync: Start iteration for last masterchain block id: {}",
            last_mc_block_id.seq_no
        );

        let next_mc_seq_no = last_mc_block_id.seq_no + 1;

        let data =
            download_archive_or_die(engine.clone(), active_peers.clone(), next_mc_seq_no).await;
        match apply(engine, &last_mc_block_id, next_mc_seq_no, data).await {
            Ok(()) => continue 'outer,
            Err(e) => {
                log::error!(
                    "sync: Failed to apply queued archive for block {}: {:?}",
                    next_mc_seq_no,
                    e
                );
            }
        }
    }

    Ok(())
}

async fn apply(
    engine: &Arc<Engine>,
    last_mc_block_id: &ton_block::BlockIdExt,
    mc_seq_no: u32,
    data: Vec<u8>,
) -> Result<()> {
    log::info!("sync: Parsing archive for block {}", mc_seq_no);
    let maps = parse_archive(data)?;
    log::info!(
        "sync: Parsed {} masterchain blocks, {} blocks total",
        maps.mc_block_ids.len(),
        maps.blocks.len()
    );
    import_package(engine, maps, last_mc_block_id).await?;
    log::info!("sync: Imported archive package for block {}", mc_seq_no);
    Ok(())
}

async fn import_package(
    engine: &Arc<Engine>,
    maps: Arc<BlockMaps>,
    last_mc_block_id: &ton_block::BlockIdExt,
) -> Result<()> {
    if maps.mc_block_ids.is_empty() {
        return Err(SyncError::EmptyArchivePackage.into());
    }

    import_mc_blocks(engine, maps.clone(), last_mc_block_id).await?;
    import_shard_blocks(engine, maps).await?;

    Ok(())
}

async fn import_mc_blocks(
    engine: &Arc<Engine>,
    maps: Arc<BlockMaps>,
    mut last_mc_block_id: &ton_block::BlockIdExt,
) -> Result<()> {
    for id in maps.mc_block_ids.values() {
        if id.seq_no <= last_mc_block_id.seq_no {
            if id.seq_no == last_mc_block_id.seq_no && last_mc_block_id != id {
                return Err(SyncError::MasterchainBlockIdMismatch.into());
            }
            continue;
        }

        if id.seq_no != last_mc_block_id.seq_no + 1 {
            log::error!(
                "Seqno: {}, expected: {}",
                id.seq_no,
                last_mc_block_id.seq_no + 1
            );
            return Err(SyncError::BlocksSkippedInArchive.into());
        }

        last_mc_block_id = id;
        if let Some(handle) = engine.load_block_handle(last_mc_block_id)? {
            if handle.meta().is_applied() {
                continue;
            }
        }

        let entry = maps.blocks.get(last_mc_block_id).unwrap();

        let (block, block_proof) = entry.get_data()?;
        let handle = save_block(engine, last_mc_block_id, block, block_proof, None).await?;

        engine
            .apply_block_ext(&handle, block, last_mc_block_id.seq_no, false, 0)
            .await?;
    }

    log::info!("Last applied masterchain block id: {}", last_mc_block_id);
    Ok(())
}

async fn import_shard_blocks(engine: &Arc<Engine>, maps: Arc<BlockMaps>) -> Result<()> {
    for (id, entry) in &maps.blocks {
        if !id.shard_id.is_masterchain() {
            let (block, block_proof) = entry.get_data()?;
            save_block(engine, id, block, block_proof, None).await?;
        }
    }

    let mut last_applied_mc_block_id = engine.load_shards_client_mc_block_id().await?;
    for mc_block_id in maps.mc_block_ids.values() {
        let mc_seq_no = mc_block_id.seq_no;
        if mc_seq_no <= last_applied_mc_block_id.seq_no {
            continue;
        }

        let masterchain_handle = engine
            .load_block_handle(mc_block_id)?
            .ok_or(SyncError::MasterchainBlockNotFound)?;
        let masterchain_block = engine.load_block_data(&masterchain_handle).await?;
        let shard_blocks = masterchain_block.shards_blocks()?;

        let mut tasks = Vec::with_capacity(shard_blocks.len());
        for (_, id) in shard_blocks {
            let engine = engine.clone();
            let maps = maps.clone();
            tasks.push(tokio::spawn(async move {
                let handle = engine
                    .load_block_handle(&id)?
                    .ok_or(SyncError::ShardchainBlockHandleNotFound)?;
                if handle.meta().is_applied() {
                    return Ok(());
                }

                if id.seq_no == 0 {
                    super::boot::download_zero_state(&engine, &id).await?;
                    return Ok(());
                }

                let block = match maps.blocks.get(&id) {
                    Some(entry) => match &entry.block {
                        Some(block) => Some(Cow::Borrowed(block)),
                        None => engine.load_block_data(&handle).await.ok().map(Cow::Owned),
                    },
                    None => engine.load_block_data(&handle).await.ok().map(Cow::Owned),
                };

                match block {
                    Some(block) => {
                        engine
                            .apply_block_ext(&handle, block.as_ref(), mc_seq_no, false, 0)
                            .await
                    }
                    None => {
                        log::info!("Downloading sc block for {}", mc_seq_no);
                        engine
                            .download_and_apply_block(handle.id(), mc_seq_no, false, 0)
                            .await
                    }
                }
            }));
        }

        futures::future::try_join_all(tasks)
            .await?
            .into_iter()
            .find(|item| item.is_err())
            .unwrap_or(Ok(()))?;

        engine.store_shards_client_mc_block_id(mc_block_id).await?;
        last_applied_mc_block_id = mc_block_id.clone();
    }

    Ok(())
}

async fn save_block(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
    block: &BlockStuff,
    block_proof: &BlockProofStuff,
    prev_key_block_id: Option<&ton_block::BlockIdExt>,
) -> Result<Arc<BlockHandle>> {
    if block_proof.is_link() {
        block_proof.check_proof_link()?;
    } else {
        let (virt_block, virt_block_info) = block_proof.pre_check_block_proof()?;

        let handle = match prev_key_block_id {
            Some(block_id) => engine
                .db
                .load_block_handle(block_id)?
                .context("Prev key block not found")?,
            None => {
                let prev_key_block_seqno = virt_block_info.prev_key_block_seqno();

                let masterchain_prefix = ton_block::AccountIdPrefixFull::any_masterchain();
                engine
                    .db
                    .find_block_by_seq_no(&masterchain_prefix, prev_key_block_seqno)?
            }
        };

        let prev_key_block_proof = engine.load_block_proof(&handle, false).await?;

        check_with_prev_key_block_proof(
            block_proof,
            &prev_key_block_proof,
            &virt_block,
            &virt_block_info,
        )?;
    }

    let handle = engine.store_block_data(block).await?.handle;
    let handle = engine
        .store_block_proof(block_id, Some(handle), block_proof)
        .await?;
    Ok(handle)
}

fn parse_archive(data: Vec<u8>) -> Result<Arc<BlockMaps>> {
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
    mc_block_ids: BTreeMap<u32, ton_block::BlockIdExt>,
    blocks: BTreeMap<ton_block::BlockIdExt, BlocksEntry>,
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

    pub fn is_contiguous(left: &Self, right: &Self) -> Option<bool> {
        let left_blocks: HashSet<u32> = left.mc_block_ids.iter().map(|x| x.1.seq_no).collect();
        let right_blocks: HashSet<u32> = right.mc_block_ids.iter().map(|x| x.1.seq_no).collect();
        if left_blocks.intersection(&right_blocks).next().is_some() {
            log::info!("Intersects");
            return Some(true);
        }
        let left_highest = left.highest_id()?.seq_no;
        let right_lowest = right.lowest_id()?.seq_no;
        Some(right_lowest - left_highest <= 1)
    }

    pub fn get_distance(left: &Self, right: &Self) -> Option<(u32, u32)> {
        let left_highest = left.highest_id()?.seq_no;
        let right_lowest = right.lowest_id()?.seq_no;
        Some((left_highest, right_lowest))
    }
}

#[derive(Default)]
struct BlocksEntry {
    block: Option<BlockStuff>,
    proof: Option<BlockProofStuff>,
}

impl BlocksEntry {
    fn get_data(&self) -> Result<(&BlockStuff, &BlockProofStuff)> {
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

pub async fn background_sync(engine: &Arc<Engine>, from_seqno: u32) -> Result<()> {
    let store = engine.db.background_sync_store();

    log::info!("Download previous history sync since block {}", from_seqno);

    let high = store.load_high_key_block()?.seq_no;
    let low = match store.load_low_key_block()? {
        Some(low) => {
            log::warn!(
                "Ignoring `from_seqno` param, using saved seqno: {}",
                low.seq_no
            );
            low.seq_no
        }
        None => from_seqno,
    };

    let prev_key_block = engine.load_prev_key_block(low).await?;

    log::info!("Started background sync from {} to {}", low, high);
    download_archives(engine, low, high, prev_key_block)
        .await
        .context("Failed downloading archives")?;

    log::info!("Background sync complete");
    Ok(())
}

async fn download_archives(
    engine: &Arc<Engine>,
    low_seqno: u32,
    high_seqno: u32,
    mut prev_key_block_id: ton_block::BlockIdExt,
) -> Result<()> {
    let mut context = SaveContext {
        peers: Arc::new(Default::default()),
        high_seqno,
        prev_key_block_id: &mut prev_key_block_id,
    };
    let mut stream = start_download(
        engine.clone(),
        context.peers.clone(),
        ARCHIVE_SLICE,
        low_seqno,
        high_seqno,
    )
    .await
    .context("To small bounds")?;
    while let Some(a) = stream.next().await {
        let res = save_archive(engine, a, &mut context).await?;
        if let SyncStatus::Done = res {
            return Ok(());
        }
    }

    Ok(())
}

enum SyncStatus {
    Done,
    InProgress,
    NoBlocksInArchive,
}

struct SaveContext<'a> {
    peers: Arc<ActivePeers>,
    high_seqno: u32,
    prev_key_block_id: &'a mut ton_block::BlockIdExt,
}

#[async_recursion::async_recursion]
async fn save_archive(
    engine: &Arc<Engine>,
    maps: Arc<BlockMaps>,
    context: &mut SaveContext<'_>,
) -> Result<SyncStatus> {
    //todo store block maps in db
    let lowest_id = match maps.lowest_id() {
        Some(a) => a,
        None => return Ok(SyncStatus::NoBlocksInArchive),
    };
    let highest_id = match maps.highest_id() {
        Some(a) => a,
        None => return Ok(SyncStatus::NoBlocksInArchive),
    };
    log::debug!(
        "Saving archive. Low id: {}. High id: {}",
        lowest_id,
        highest_id.seq_no
    );

    for (id, entry) in &maps.blocks {
        let (block, proof) = entry.get_data()?;

        let is_key_block = match engine.load_block_handle(block.id())? {
            Some(handle) => {
                if handle.meta().is_key_block() {
                    true
                } else {
                    // block already saved
                    continue;
                }
            }
            None => {
                let handle = save_block(engine, id, block, proof, Some(context.prev_key_block_id))
                    .await
                    .context("Failed saving block")?;
                handle.meta().is_key_block()
            }
        };

        if is_key_block {
            *context.prev_key_block_id = id.clone();
        }
    }

    Ok({
        engine
            .db
            .background_sync_store()
            .store_low_key_block(highest_id)?;
        log::info!(
            "Background sync: Saved archive from {} to {}",
            lowest_id,
            highest_id
        );

        if highest_id.seq_no >= context.high_seqno {
            SyncStatus::Done
        } else {
            SyncStatus::InProgress
        }
    })
}

#[derive(thiserror::Error, Debug)]
enum SyncError {
    #[error("Empty archive package")]
    EmptyArchivePackage,
    #[error("Masterchain block id mismatch")]
    MasterchainBlockIdMismatch,
    #[error("Some blocks are missing in archive")]
    BlocksSkippedInArchive,
    #[error("Block not found in archive")]
    BlockNotFound,
    #[error("Block proof not found in archive")]
    BlockProofNotFound,
    #[error("Masterchain block not found")]
    MasterchainBlockNotFound,
    #[error("Shardchain block handle not found")]
    ShardchainBlockHandleNotFound,
}
