use std::fmt::Formatter;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::stream::{BoxStream, FuturesOrdered};
use tiny_adnl::utils::*;
use tokio::sync::mpsc;

use crate::engine::Engine;
use crate::utils::*;
use futures::{Stream, StreamExt, TryStreamExt};

pub fn start_download(
    engine: Arc<Engine>,
    active_peers: Arc<ActivePeers>,
    step: u32,
    from: u32,
    to: u32,
    num_workers: NonZeroUsize,
) -> impl Stream<Item = Vec<u8>> {
    futures::stream::iter((from..to).step_by(step as usize))
        .inspect(|x| log::info!("Downloading {} arch", x))
        .map(move |x| (x, engine.clone(), active_peers.clone()))
        .map(|(x, engine, peers)| async move { download_archive_or_die(engine, peers, x).await })
        .buffered(num_workers.get())
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
                tokio::time::sleep(Duration::from_secs(1)).await;
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
