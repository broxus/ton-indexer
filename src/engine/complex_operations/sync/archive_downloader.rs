use std::sync::Arc;

use anyhow::Result;
use futures::{SinkExt, StreamExt};

use crate::engine::Engine;
use crate::utils::*;

use super::parse_archive;
use super::BlockMaps;
use futures::stream::BoxStream;

#[async_recursion::async_recursion]
pub async fn start_download(
    engine: Arc<Engine>,
    active_peers: Arc<ActivePeers>,
    step: u32,
    from: u32,
    to: u32,
) -> Option<BoxStream<'static, Arc<BlockMaps>>> {
    let num_tasks = engine.parallel_tasks.get();
    let map_engine = engine.clone();
    let map_peers = active_peers.clone();
    let stream = futures::stream::iter((from..to).step_by(step as usize))
        .inspect(|x| log::info!("Downloading {} arch", x))
        .map(move |x| (x, engine.clone(), active_peers.clone()))
        .map(|(x, engine, peers)| async move { download_archive_maps(engine, peers, x).await })
        .buffered(num_tasks);
    process_maps(stream.boxed(), num_tasks, map_engine, map_peers).await
}

#[async_recursion::async_recursion]
async fn process_maps(
    mut stream: BoxStream<'static, Arc<BlockMaps>>,
    num_tasks: usize,
    engine: Arc<Engine>,
    peers: Arc<ActivePeers>,
) -> Option<BoxStream<'static, Arc<BlockMaps>>> {
    let (mut tx, rx) = futures::channel::mpsc::channel(num_tasks);
    let mut left: Arc<BlockMaps> = stream.next().await?;
    tokio::spawn(async move {
        while let Some(map) = stream.next().await {
            let right: Arc<BlockMaps> = map;
            if BlockMaps::is_contiguous(&left, &right)
                .expect("download_archive_maps produces non empty archives")
            {
                tx.send(left).await.unwrap();
            } else {
                let (start, stop) = BlockMaps::get_distance(&left, &right)
                    .expect("download_archive_maps produces non empty archives");
                let archives = gaps_handler(start, stop, engine.clone(), peers.clone()).await;
                for arch in archives {
                    tx.send(arch).await.unwrap();
                }
            }
            left = right
        }
    });
    Some(rx.boxed())
}

#[async_recursion::async_recursion]
async fn gaps_handler(
    gap_start: u32,
    gap_end: u32,
    engine: Arc<Engine>,
    peers: Arc<ActivePeers>,
) -> Vec<Arc<BlockMaps>> {
    if gap_start > gap_end {
        return vec![];
    }
    log::info!("Need to fill gap between {} and {}", gap_start, gap_end);
    let mut archives: Vec<Arc<BlockMaps>> = match start_download(
        engine.clone(),
        peers,
        (ARCHIVE_SLICE / 2) - 1,
        gap_start,
        gap_end,
    )
    .await
    {
        Some(a) => a,
        None => return vec![],
    }
    .collect()
    .await;
    log::info!("Downloaded {} archives for gap handling", archives.len());
    archives.dedup_by(|a, b| a == b);
    log::info!("After dedup: {}", archives.len());
    archives
}

pub async fn download_archive_maps(
    engine: Arc<Engine>,
    active_peers: Arc<ActivePeers>,
    mc_seq_no: u32,
) -> Arc<BlockMaps> {
    loop {
        let arch = download_archive_or_die(engine.clone(), active_peers.clone(), mc_seq_no).await;
        match parse_archive(arch) {
            Ok(a) if !a.mc_block_ids.is_empty() => break a,
            Err(e) => {
                log::error!("Failed parsing archive: {}", e);
            }
            _ => {
                log::error!("Empty archive {}", mc_seq_no);
            }
        };
    }
}

pub async fn download_archive_or_die(
    engine: Arc<Engine>,
    active_peers: Arc<ActivePeers>,
    mc_seq_no: u32,
) -> Vec<u8> {
    log::info!("Start downloading {}", mc_seq_no);
    loop {
        let res = download_archive(engine.clone(), active_peers.clone(), mc_seq_no).await;
        match res {
            Ok(Some(a)) => return a,
            _ => {
                continue;
            }
        }
    }
}
async fn download_archive(
    engine: Arc<Engine>,
    active_peers: Arc<ActivePeers>,
    mc_seq_no: u32,
) -> Result<Option<Vec<u8>>> {
    match engine.download_archive(mc_seq_no, &active_peers).await {
        Ok(Some(data)) => {
            log::info!(
                "sync: Downloaded archive for block {}, size {} bytes",
                mc_seq_no,
                data.len()
            );
            Ok(Some(data))
        }
        Ok(None) => {
            log::trace!("sync: No archive found for block {}", mc_seq_no);
            Ok(None)
        }
        e => e,
    }
}

pub const ARCHIVE_SLICE: u32 = 100;
