use std::sync::Arc;

use anyhow::Result;
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt};

use super::block_maps::*;
use crate::engine::Engine;
use crate::utils::*;

pub async fn start_download<I>(
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    step: u32,
    range: I,
) -> Option<impl Stream<Item = Arc<BlockMaps>>>
where
    I: IntoIterator<Item = u32> + Send + 'static,
    <I as IntoIterator>::IntoIter: Send,
{
    let num_tasks = engine.parallel_archive_downloads;

    let engine_clone = engine.clone();
    let active_peers_clone = active_peers.clone();
    let stream = futures::stream::iter(range.into_iter().step_by(step as usize))
        .inspect(|x: &u32| log::info!("Downloading {} arch", x))
        .map(move |x| (x, engine_clone.clone(), active_peers_clone.clone()))
        .map(|(x, engine, peers)| async move { download_archive_maps(&engine, &peers, x).await })
        .buffered(num_tasks as usize);

    process_maps(stream, engine, active_peers).await
}

async fn process_maps<S>(
    mut stream: S,
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
) -> Option<impl Stream<Item = Arc<BlockMaps>>>
where
    S: Stream<Item = Arc<BlockMaps>> + Send + Unpin + 'static,
{
    let (mut tx, rx) = mpsc::channel(1);
    let mut left: Arc<BlockMaps> = match stream.next().await {
        Some(a) => a,
        None => {
            log::warn!("Archives stream is empty");
            return None;
        }
    };

    let engine = engine.clone();
    let active_peers = active_peers.clone();

    tokio::spawn(async move {
        while let Some(right) = stream.next().await {
            // Check if there are some gaps between two archives
            if BlockMaps::is_contiguous(&left, &right) {
                // Send previous archive
                if tx.send(left).await.is_err() {
                    log::error!("Archive stream closed");
                    return;
                }
            } else {
                // Find gaps
                let (prev, next) = left
                    .distance_to(&right)
                    .expect("download_archive_maps produces non empty archives");

                // Download gaps
                let gaps = download_gaps(prev, next, &engine, &active_peers).await;

                // Send previous archive
                if tx.send(left).await.is_err() {
                    log::error!("Archive stream closed");
                    return;
                }

                // Send archives for gaps
                for arch in gaps {
                    if tx.send(arch).await.is_err() {
                        log::error!("Archive stream closed");
                        return;
                    }
                }
            }

            left = right
        }

        if tx.send(left).await.is_err() {
            log::error!("Archive stream closed");
        }
    });

    Some(rx)
}

async fn download_gaps(
    mut prev: u32,
    next: u32,
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
) -> Vec<Arc<BlockMaps>> {
    log::warn!("Finding archive for the gap {}..{}", prev, next);

    let mut arhives = Vec::with_capacity(1);
    while prev + 1 < next {
        let arch = download_archive_maps(engine, active_peers, prev + 1).await;
        prev = arch.highest_id().unwrap().seq_no;
        arhives.push(arch);
    }

    arhives
}

pub async fn download_archive_maps(
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    mc_seq_no: u32,
) -> Arc<BlockMaps> {
    loop {
        let start = std::time::Instant::now();
        let data = download_archive_or_die(engine, active_peers, mc_seq_no).await;
        log::info!("Download took: {} ms", start.elapsed().as_millis());

        match parse_archive(data) {
            Ok(map) if map.is_valid(mc_seq_no).is_some() => break map,
            Err(e) => {
                log::error!("Failed to parse archive: {:?}", e);
            }
            _ => {
                log::error!("Empty archive {}", mc_seq_no);
            }
        };
    }
}

pub async fn download_archive_or_die(
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    mc_seq_no: u32,
) -> Vec<u8> {
    log::info!("Start downloading {}", mc_seq_no);
    loop {
        if let Ok(Some(data)) = download_archive(engine, active_peers, mc_seq_no).await {
            break data;
        }
    }
}

async fn download_archive(
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    mc_seq_no: u32,
) -> Result<Option<Vec<u8>>> {
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
            log::trace!("sync: No archive found for block {}", mc_seq_no);
            Ok(None)
        }
        e => e,
    }
}
