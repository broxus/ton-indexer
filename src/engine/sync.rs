use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;
use dashmap::DashSet;
use tiny_adnl::utils::*;
use ton_types::ByteOrderRead;

use super::Engine;
use crate::storage::*;
use crate::utils::*;

pub async fn sync(engine: &Arc<Engine>) -> Result<()> {
    const MAX_CONCURRENCY: usize = 8;

    log::info!("Started sync");

    let active_peers = Arc::new(DashSet::new());
    let mut queue: Vec<(u32, ArchiveStatus)> = Vec::with_capacity(MAX_CONCURRENCY);
    let mut response_collector = ResponseCollector::new();
    let mut concurrency = 1;

    'outer: while !engine.check_sync().await? {
        let last_mc_block_id = match (
            engine.load_last_applied_mc_block_id().await?,
            engine.load_shards_client_mc_block_id().await?,
        ) {
            (mc_block_id, sc_block_id) if mc_block_id.seq_no > sc_block_id.seq_no => sc_block_id,
            (mc_block_id, _) => mc_block_id,
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

            match queue.iter().position(|(seq_no, status)| matches!(status, ArchiveStatus::Downloaded(_) if *seq_no <= next_mc_seq_no)) {
                Some(downloaded_item) => {
                    let (seq_no, data) = match queue.remove(downloaded_item) {
                        (seq_no, ArchiveStatus::Downloaded(data)) => (seq_no, data),
                        _ => unreachable!()
                    };

                    match apply(engine, &last_mc_block_id, seq_no, data).await {
                        Ok(()) => continue 'outer,
                        Err(e) => {
                            log::error!("sync: Failed to apply queued archive for block {}: {:?}", seq_no, e);
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
        while !engine.check_sync().await? {
            start_downloads(
                engine,
                &mut queue,
                &active_peers,
                &mut response_collector,
                concurrency,
                next_mc_seq_no,
            )
            .await?;

            log::info!("sync: ------ loop");

            match response_collector.wait(false).await {
                Some(Some((seq_no, Ok(data)))) => {
                    let queue_index = match queue.iter().position(|(queue_seq_no, status)| matches!(status, ArchiveStatus::Downloading if *queue_seq_no == seq_no)) {
                        Some(index) => index,
                        None => {
                            log::error!("AAAAA");
                            return Err(SyncError::BrokenQueue.into())
                        }
                    };

                    let data = match data {
                        Some(data) => data,
                        None => {
                            let (_, status) = &mut queue[queue_index];
                            *status = ArchiveStatus::NotFound;
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

                    if seq_no <= last_mc_block_id.seq_no + 1 {
                        match apply(engine, &last_mc_block_id, seq_no, data).await {
                            Ok(ok) => {
                                queue.remove(queue_index);
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
                        let (_, status) = &mut queue[queue_index];
                        *status = ArchiveStatus::Downloaded(data);
                        retry_downloading_not_found_archives(
                            engine,
                            &mut queue,
                            &active_peers,
                            &mut response_collector,
                        )
                        .await?;
                    }
                }
                Some(Some((seq_no, Err(e)))) => {
                    log::error!(
                        "sync: Failed to download archive for block {}: {:?}",
                        seq_no,
                        e
                    );
                    start_download(engine, &active_peers, &mut response_collector, seq_no);
                }
                e => {
                    log::error!("EEEE: {:?}", e);
                    return Err(SyncError::BrokenQueue.into());
                }
            }
        }
    }

    Ok(())
}

async fn start_downloads(
    engine: &Arc<Engine>,
    queue: &mut Vec<(u32, ArchiveStatus)>,
    active_peers: &Arc<ActivePeers>,
    response_collector: &mut ResponseCollector<ArchiveResponse>,
    concurrency: usize,
    mut mc_seq_no: u32,
) -> Result<()> {
    retry_downloading_not_found_archives(engine, queue, active_peers, response_collector).await?;

    while response_collector.count() < concurrency {
        if queue.len() > concurrency {
            break;
        }

        if queue.iter().all(|(seq_no, _)| *seq_no != mc_seq_no) {
            queue.push((mc_seq_no, ArchiveStatus::Downloading));
            start_download(engine, active_peers, response_collector, mc_seq_no);
        }

        mc_seq_no += BLOCKS_IN_ARCHIVE;
    }

    Ok(())
}

async fn retry_downloading_not_found_archives(
    engine: &Arc<Engine>,
    queue: &mut Vec<(u32, ArchiveStatus)>,
    active_peers: &Arc<ActivePeers>,
    response_collector: &mut ResponseCollector<ArchiveResponse>,
) -> Result<()> {
    let mut latest = None;
    for (seq_no, status) in queue.iter() {
        if !matches!(status, ArchiveStatus::Downloaded(_))
            || matches!(latest, Some(latest) if latest >= *seq_no)
        {
            continue;
        }
        latest = Some(*seq_no);
    }

    match latest {
        Some(latest) => {
            for (seq_no, status) in queue.iter_mut() {
                if latest < *seq_no {
                    continue;
                }

                if let ArchiveStatus::NotFound = status {
                    *status = ArchiveStatus::Downloading;
                    start_download(engine, active_peers, response_collector, *seq_no);
                }
            }
        }
        None if !engine.check_sync().await? => {
            let mut earliest = None;
            for (seq_no, status) in queue.iter_mut() {
                match status {
                    ArchiveStatus::NotFound if matches!(earliest, Some(earliest) if earliest <= *seq_no) => {
                        continue
                    }
                    ArchiveStatus::NotFound => earliest = Some(*seq_no),
                    _ => return Ok(()),
                }
            }

            let earliest = earliest
                .and_then(|earliest| queue.iter_mut().find(|(seq_no, _)| *seq_no == earliest));

            if let Some((seq_no, status)) = earliest {
                *status = ArchiveStatus::Downloading;
                start_download(engine, active_peers, response_collector, *seq_no);
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
    maps: BlockMaps,
    mut last_mc_block_id: &ton_block::BlockIdExt,
) -> Result<()> {
    if maps.mc_block_ids.is_empty() {
        return Err(SyncError::EmptyArchivePackage.into());
    }

    for id in maps.mc_block_ids.values() {
        if id.seq_no <= last_mc_block_id.seq_no {
            if last_mc_block_id != id {
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
        let handle = save_block(engine, last_mc_block_id, entry).await?;

        // TODO: apply
        engine.db.index_handle(&handle)?;

        engine
            .store_last_applied_mc_block_id(last_mc_block_id)
            .await?;
    }

    log::info!("Last applied masterchain block id: {}", last_mc_block_id);
    Ok(())
}

async fn save_block(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
    entry: &BlocksEntry,
) -> Result<(Arc<BlockHandle>)> {
    let block = match &entry.block {
        Some(block) => block,
        None => return Err(SyncError::BlockNotFound.into()),
    };
    let block_proof = match &entry.proof {
        Some(proof) => proof,
        None => return Err(SyncError::BlockProofNotFound.into()),
    };

    if block_proof.is_link() {
        block_proof.check_proof_link()?;
    } else {
        let now = std::time::Instant::now();
        log::info!("Checking proof for block: {}", block_id);

        let (virt_block, virt_block_info) = block_proof.pre_check_block_proof()?;
        let prev_key_block_seqno = virt_block_info.prev_key_block_seqno();

        log::info!("Prev key block seqno: {}", prev_key_block_seqno);

        let masterchain_prefix = ton_block::AccountIdPrefixFull::any_masterchain();
        let handle = engine
            .db
            .find_block_by_seq_no(&masterchain_prefix, prev_key_block_seqno)?;
        let prev_key_block_proof = engine.load_block_proof(&handle, false).await?;

        check_with_prev_key_block_proof(
            &block_proof,
            &prev_key_block_proof,
            &virt_block,
            &virt_block_info,
        )?;

        log::info!(
            "Checked proof for block: {}. TIME = {} ms",
            block_id,
            now.elapsed().as_millis()
        );
    }

    let handle = engine.store_block_data(block).await?.handle;
    let handle = engine
        .store_block_proof(block_id, Some(handle), block_proof)
        .await?;
    Ok(handle)
}

fn parse_archive(data: Vec<u8>) -> Result<BlockMaps> {
    let mut reader = ArchivePackageViewReader::new(&data)?;

    let mut maps = BlockMaps::default();

    while let Some(entry) = reader.read_next()? {
        log::info!(
            "sync: Processing archive package entry: {} ({} bytes)",
            entry.name,
            entry.data.len()
        );

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
                if !id.is_masterchain() {
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
                maps.mc_block_ids.insert(id.seq_no, id);
            }
        }
    }

    Ok(maps)
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

type ActivePeers = DashSet<AdnlNodeIdShort>;
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
}
