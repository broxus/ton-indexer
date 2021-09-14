use std::sync::Arc;

use anyhow::Result;
use futures::{Stream, StreamExt};

use crate::engine::Engine;
use crate::utils::*;

use super::BlockMaps;

pub enum ArchiveStatus {
    Done { maps: Arc<BlockMaps>, seqno: u32 },
    ParsingFailed(u32),
}

impl ArchiveStatus {
    fn seqno(&self) -> u32 {
        match self {
            ArchiveStatus::ParsingFailed(a) => *a,
            ArchiveStatus::Done { seqno, .. } => *seqno,
        }
    }
}

impl Eq for ArchiveStatus {}

impl PartialEq for ArchiveStatus {
    fn eq(&self, other: &Self) -> bool {
        self.seqno() == other.seqno()
    }
}

pub fn start_download(
    engine: Arc<Engine>,
    active_peers: Arc<ActivePeers>,
    step: u32,
    from: u32,
    to: u32,
) -> impl Stream<Item = ArchiveStatus> {
    let num_tasks = engine.parallel_tasks.get();
    futures::stream::iter((from..to).step_by(step as usize))
        .inspect(|x| log::info!("Downloading {} arch", x))
        .map(move |x| (x, engine.clone(), active_peers.clone()))
        .map(|(x, engine, peers)| async move {(x, download_archive_or_die(engine, peers, x).await)
        })
        .buffered(num_tasks)
        .map(|(seq_no, archive)| (seq_no, super::parse_archive(archive)))
        .map(|(seq_no, archive)| match archive {
            Ok(a) => ArchiveStatus::Done {
                maps: a,
                seqno: seq_no,
            },
            Err(e) => {
                log::error!("Failed parsing archive {}. Downloading again: {}", e,seq_no);
                ArchiveStatus::ParsingFailed(seq_no)
            }
        })
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
