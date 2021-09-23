use std::sync::Arc;

use anyhow::Result;

use futures::stream::BoxStream;
use futures::{SinkExt, StreamExt};

use crate::engine::Engine;
use crate::utils::*;

use super::parse_archive;
use super::BlockMaps;

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
    process_maps(stream.boxed(), map_engine, map_peers).await
}

async fn process_maps(
    mut stream: BoxStream<'static, Arc<BlockMaps>>,
    engine: Arc<Engine>,
    peers: Arc<ActivePeers>,
) -> Option<BoxStream<'static, Arc<BlockMaps>>> {
    let (mut tx, rx) = futures::channel::mpsc::channel(1);
    let mut left: Arc<BlockMaps> = match stream.next().await {
        Some(a) => a,
        None => {
            log::warn!("Stream is empty");
            return None;
        }
    };
    tokio::spawn(async move {
        while let Some(map) = stream.next().await {
            let right: Arc<BlockMaps> = map;
            if BlockMaps::is_contiguous(&left, &right)
                .expect("download_archive_maps produces non empty archives")
            {
                if let Err(e) = tx.send(left).await {
                    log::error!("Failed sending: {}", e);
                    break;
                }
            } else {
                let (prev, next) = BlockMaps::get_distance(&left, &right)
                    .expect("download_archive_maps produces non empty archives");
                let gaps = gaps_handler(prev, next, engine.clone(), peers.clone()).await;

                if let Err(e) = tx.send(left).await {
                    log::error!("Failed sending: {}", e);
                    break;
                }
                for arch in gaps {
                    if let Err(e) = tx.send(arch).await {
                        log::error!("Failed sending: {}", e);
                        break;
                    }
                }
            }
            left = right
        }
    });
    Some(rx.boxed())
}

async fn gaps_handler(
    mut prev: u32,
    next: u32,
    engine: Arc<Engine>,
    peers: Arc<ActivePeers>,
) -> Vec<Arc<BlockMaps>> {
    log::warn!("It's a gap. {}..{}", prev, next);
    let mut arhives = vec![];
    while next - prev > 1 {
        let arch = download_archive_maps(engine.clone(), peers.clone(), prev + 1).await;
        prev = arch.highest_id().unwrap().seq_no;
        arhives.push(arch);
    }
    arhives
}

pub async fn download_archive_maps(
    engine: Arc<Engine>,
    active_peers: Arc<ActivePeers>,
    mc_seq_no: u32,
) -> Arc<BlockMaps> {
    loop {
        let start = std::time::Instant::now();
        let arch = download_archive_or_die(engine.clone(), active_peers.clone(), mc_seq_no).await;
        let took = std::time::Instant::now() - start;
        log::info!("Download took: {}", took.as_millis());
        match parse_archive(arch) {
            Ok(a) if a.is_valid(mc_seq_no).is_some() => break a,
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
