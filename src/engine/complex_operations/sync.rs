use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result};
use tiny_adnl::utils::*;

use crate::engine::Engine;
use crate::storage::*;
use crate::utils::*;

const MAX_CONCURRENCY: usize = 8;

pub async fn sync(engine: &Arc<Engine>) -> Result<()> {
    log::info!("Started sync");

    let active_peers = Arc::new(ActivePeers::default());
    let mut queue = Queue::new(MAX_CONCURRENCY);
    let mut response_collector = ResponseCollector::new();
    let mut concurrency = 1;

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

        // Apply downloaded blocks
        loop {
            start_downloads(
                engine,
                &mut queue,
                &active_peers,
                &mut response_collector,
                concurrency,
                next_mc_seq_no,
            )
            .await?;

            match queue.finish_download(next_mc_seq_no) {
                Some((seq_no, data)) => {
                    match apply(engine, &last_mc_block_id, seq_no, data).await {
                        Ok(()) => continue 'outer,
                        Err(e) => {
                            log::error!(
                                "sync: Failed to apply queued archive for block {}: {:?}",
                                seq_no,
                                e
                            );
                        }
                    }
                }
                None => break,
            }
        }

        log::info!(
            "sync: Continue iteration for last masterchain block id: {}",
            last_mc_block_id.seq_no
        );

        // Process queue
        while !engine.is_synced().await? {
            start_downloads(
                engine,
                &mut queue,
                &active_peers,
                &mut response_collector,
                concurrency,
                next_mc_seq_no,
            )
            .await?;

            match response_collector.wait(false).await.flatten() {
                Some((seq_no, Ok(data))) => {
                    let data = match data {
                        Some(data) => data,
                        None => {
                            queue
                                .set_status(seq_no, ArchiveStatus::NotFound)
                                .context("Broken queue")?;
                            retry_downloading_not_found_archives(
                                engine,
                                &mut queue,
                                &active_peers,
                                &mut response_collector,
                            )
                            .await?;
                            continue;
                        }
                    };

                    if seq_no <= next_mc_seq_no {
                        match apply(engine, &last_mc_block_id, seq_no, data).await {
                            Ok(_) => {
                                queue.0.remove(&seq_no);
                                concurrency = MAX_CONCURRENCY;
                                break;
                            }
                            Err(e) => {
                                log::error!(
                                    "Failed to apply downloaded archive for block {}: {:?}",
                                    seq_no,
                                    e
                                );
                                start_download(
                                    engine,
                                    &active_peers,
                                    &mut response_collector,
                                    seq_no,
                                );
                            }
                        }
                    } else {
                        queue
                            .set_status(seq_no, ArchiveStatus::Downloaded(data))
                            .context("Broken queue")?;
                        retry_downloading_not_found_archives(
                            engine,
                            &mut queue,
                            &active_peers,
                            &mut response_collector,
                        )
                        .await?;
                    }
                }
                Some((seq_no, Err(e))) => {
                    log::error!(
                        "sync: Failed to download archive for block {}: {:?}",
                        seq_no,
                        e
                    );
                    start_download(engine, &active_peers, &mut response_collector, seq_no);
                }
                _ => return Err(SyncError::BrokenQueue.into()),
            }
        }
    }

    Ok(())
}

async fn start_downloads(
    engine: &Arc<Engine>,
    queue: &mut Queue,
    active_peers: &Arc<ActivePeers>,
    response_collector: &mut ResponseCollector<ArchiveResponse>,
    concurrency: usize,
    mut mc_seq_no: u32,
) -> Result<()> {
    retry_downloading_not_found_archives(engine, queue, active_peers, response_collector).await?;

    while response_collector.count_pending() < concurrency {
        if queue.0.len() > concurrency {
            break;
        }
        if queue.0.get(&mc_seq_no).is_none() {
            queue.0.insert(mc_seq_no, ArchiveStatus::Downloading);
            start_download(engine, active_peers, response_collector, mc_seq_no);
        }

        mc_seq_no += BLOCKS_IN_ARCHIVE;
    }

    Ok(())
}

async fn retry_downloading_not_found_archives(
    engine: &Arc<Engine>,
    queue: &mut Queue,
    active_peers: &Arc<ActivePeers>,
    response_collector: &mut ResponseCollector<ArchiveResponse>,
) -> Result<()> {
    let mut latest = None;
    for (seq_no, status) in queue.0.iter() {
        if !matches!(status, ArchiveStatus::Downloaded(_))
            || matches!(latest, Some(latest) if latest >= *seq_no)
        {
            continue;
        }
        latest = Some(*seq_no);
    }

    match latest {
        Some(latest) => {
            for (seq_no, status) in queue.0.iter_mut() {
                if latest < *seq_no {
                    continue;
                }

                if let ArchiveStatus::NotFound = status {
                    *status = ArchiveStatus::Downloading;
                    start_download(engine, active_peers, response_collector, *seq_no);
                }
            }
        }
        None if !engine.is_synced().await? => {
            let mut earliest = None;
            for (seq_no, status) in queue.0.iter_mut() {
                match status {
                    ArchiveStatus::NotFound if matches!(earliest, Some(earliest) if earliest <= *seq_no) => {
                        continue
                    }
                    ArchiveStatus::NotFound => earliest = Some(*seq_no),
                    _ => return Ok(()),
                }
            }

            let earliest =
                earliest.and_then(|earliest| queue.0.get_mut(&earliest).map(|x| (earliest, x)));

            if let Some((seq_no, status)) = earliest {
                *status = ArchiveStatus::Downloading;
                start_download(engine, active_peers, response_collector, seq_no);
            }
        }
        None => { /* do nothing */ }
    }

    Ok(())
}

fn start_download(
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    response_collector: &mut ResponseCollector<ArchiveResponse>,
    mc_seq_no: u32,
) {
    tokio::spawn({
        let engine = engine.clone();
        let active_peers = active_peers.clone();
        let response = response_collector.make_request();
        async move {
            let result = download_archive(&engine, &active_peers, mc_seq_no).await;
            response.send(Some((mc_seq_no, result)));
        }
    });
}

async fn download_archive(
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    mc_seq_no: u32,
) -> Result<Option<Vec<u8>>> {
    log::info!(
        "sync: Start downloading archive for masterchain block {}",
        mc_seq_no
    );

    match engine.download_archive(mc_seq_no, active_peers).await {
        Ok(Some(data)) => {
            log::info!(
                "sync: Downloaded archive for block {}, size {} bytes",
                mc_seq_no,
                data.len()
            );
            Ok(Some(data))
        }
        Ok(None) => {
            log::info!("sync: No archive found for block {}", mc_seq_no);
            Ok(None)
        }
        e => e,
    }
}

enum ArchiveStatus {
    Downloading,
    NotFound,
    Downloaded(Vec<u8>),
}

impl std::fmt::Debug for ArchiveStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArchiveStatus::Downloading => {
                writeln!(f, "Downloading")
            }
            ArchiveStatus::NotFound => {
                writeln!(f, "NotFound")
            }
            ArchiveStatus::Downloaded(a) => {
                writeln!(f, "Downloaded {} bytes", a.len())
            }
        }
    }
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
struct BlockMaps {
    mc_block_ids: BTreeMap<u32, ton_block::BlockIdExt>,
    blocks: BTreeMap<ton_block::BlockIdExt, BlocksEntry>,
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

    let current_block = engine.load_last_applied_mc_block_id().await?;
    let current_shard_state = engine.load_state(&current_block).await?;
    let prev_blocks = &current_shard_state.shard_state_extra()?.prev_blocks;

    let possible_ref_blk = prev_blocks
        .get_prev_key_block(low)?
        .context("No keyblock found")?;

    let ref_blk = match possible_ref_blk.seq_no {
        seqno if seqno < low => possible_ref_blk,
        seqno if seqno == low => prev_blocks
            .get_prev_key_block(seqno)?
            .context("No keyblock found")?,
        _ => anyhow::bail!("Failed to find previous key block"),
    };

    let prev_key_block = ton_block::BlockIdExt {
        shard_id: ton_block::ShardIdent::masterchain(),
        seq_no: ref_blk.seq_no,
        root_hash: ref_blk.root_hash,
        file_hash: ref_blk.file_hash,
    };

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
    async fn save_archive(
        engine: &Arc<Engine>,
        archive: Vec<u8>,
        high_seqno: u32,
        prev_key_block_id: &mut ton_block::BlockIdExt,
        last_mc_seq_no: &mut u32,
    ) -> Result<bool> {
        let maps = parse_archive(archive)?;
        for (id, entry) in &maps.blocks {
            let (block, proof) = entry.get_data()?;

            let is_key_block = match engine.load_block_handle(block.id())? {
                Some(handle) => handle.meta().is_key_block(),
                None => {
                    let handle = save_block(engine, id, block, proof, Some(prev_key_block_id))
                        .await
                        .context("Failed saving block")?;
                    handle.meta().is_key_block()
                }
            };

            if is_key_block {
                *prev_key_block_id = id.clone();
            }
        }

        Ok(if let Some(max_id) = maps.mc_block_ids.values().max() {
            engine
                .db
                .background_sync_store()
                .store_low_key_block(max_id)?;
            log::info!("Background sync: Saved archive for {}", max_id.seq_no);

            *last_mc_seq_no = max_id.seq_no;
            max_id.seq_no >= high_seqno
        } else {
            false
        })
    }

    let active_peers = Arc::new(ActivePeers::default());
    let mut queue = Queue::new(MAX_CONCURRENCY);
    let mut response_collector = ResponseCollector::new();
    let mut concurrency = 1;
    let mut last_mc_seqno = low_seqno;

    'outer: while last_mc_seqno < high_seqno {
        let next_mc_seq_no = last_mc_seqno + 1;

        // Save downloaded blocks
        start_downloads(
            engine,
            &mut queue,
            &active_peers,
            &mut response_collector,
            concurrency,
            next_mc_seq_no,
        )
        .await?;

        if let Some((_, data)) = queue.finish_download(next_mc_seq_no) {
            if save_archive(
                engine,
                data,
                high_seqno,
                &mut prev_key_block_id,
                &mut last_mc_seqno,
            )
            .await
            .context("Failed saving archive")?
            {
                return Ok(());
            }
            continue 'outer;
        }

        // Process queue
        loop {
            start_downloads(
                engine,
                &mut queue,
                &active_peers,
                &mut response_collector,
                concurrency,
                next_mc_seq_no,
            )
            .await?;

            match response_collector.wait(false).await.flatten() {
                Some((seq_no, Ok(data))) => {
                    log::info!("Background sync: downloaded {}", seq_no);
                    let data = match data {
                        Some(data) => data,
                        None => {
                            queue
                                .set_status(seq_no, ArchiveStatus::NotFound)
                                .context("Broken queue while not found")?;
                            retry_downloading_not_found_archives(
                                engine,
                                &mut queue,
                                &active_peers,
                                &mut response_collector,
                            )
                            .await?;
                            continue;
                        }
                    };

                    if seq_no <= next_mc_seq_no {
                        match save_archive(
                            engine,
                            data,
                            high_seqno,
                            &mut prev_key_block_id,
                            &mut last_mc_seqno,
                        )
                        .await
                        {
                            Ok(finished) => {
                                queue.0.remove(&seq_no);
                                if finished {
                                    return Ok(());
                                }
                                concurrency = MAX_CONCURRENCY;
                                break;
                            }
                            Err(e) => {
                                log::error!(
                                    "Background sync: failed to save downloaded archive for block {}: {:?}",
                                    seq_no,
                                    e
                                );
                                start_download(
                                    engine,
                                    &active_peers,
                                    &mut response_collector,
                                    seq_no,
                                );
                            }
                        }
                    } else {
                        queue
                            .set_status(seq_no, ArchiveStatus::Downloaded(data))
                            .context("Broken queue while downloaded")?;
                        retry_downloading_not_found_archives(
                            engine,
                            &mut queue,
                            &active_peers,
                            &mut response_collector,
                        )
                        .await?;
                    }
                }
                Some((seq_no, Err(e))) => {
                    log::error!(
                        "Background sync: Failed to download archive for block {}: {:?}",
                        seq_no,
                        e
                    );
                    start_download(engine, &active_peers, &mut response_collector, seq_no);
                }
                _ => {
                    log::error!("Background queue: {:?}", queue.0);
                    return Err(SyncError::BrokenQueue.into());
                }
            }
        }
    }

    Ok(())
}

struct Queue(HashMap<u32, ArchiveStatus>);

impl Queue {
    fn new(size: usize) -> Self {
        Self(HashMap::with_capacity(size))
    }

    fn set_status(&mut self, seq_no: u32, status: ArchiveStatus) -> Option<()> {
        *self.0.get_mut(&seq_no)? = status;
        Some(())
    }

    fn finish_download(&mut self, id: u32) -> Option<(u32, Vec<u8>)> {
        let seq_no = self
            .0
            .iter()
            .find(
                |(seq_no, status)| matches!(status, ArchiveStatus::Downloaded(_) if **seq_no <= id),
            )
            .map(|a| *a.0)?;
        match self.0.remove(&seq_no) {
            Some(ArchiveStatus::Downloaded(a)) => Some((seq_no, a)),
            _ => None,
        }
    }
}

type ArchiveResponse = (u32, Result<Option<Vec<u8>>>);

const BLOCKS_IN_ARCHIVE: u32 = 100;

#[derive(thiserror::Error, Debug)]
enum SyncError {
    #[error("Broken queue")]
    BrokenQueue,
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
