use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tiny_adnl::utils::*;
use tokio::sync::mpsc;

use crate::engine::Engine;
use crate::utils::*;

pub struct ArchiveDownloader {
    ctx: Arc<DownloadContext>,
    worker_count: usize,
    seqno_txs: Vec<SeqNoTx>,
    pending_archives: Vec<(u32, ArchiveStatus)>,
    archives_rx: ArchivesRx,
    complete_trigger: Trigger,
}

impl ArchiveDownloader {
    pub fn new(engine: Arc<Engine>, active_peers: Arc<ActivePeers>, worker_count: usize) -> Self {
        let (archives_tx, archives_rx) = mpsc::channel(worker_count);
        let (complete_trigger, complete_signal) = trigger();

        let ctx = Arc::new(DownloadContext {
            engine,
            active_peers,
            archives_tx,
            complete: Arc::new(AtomicBool::new(false)),
            complete_signal,
        });

        Self {
            ctx,
            worker_count,
            seqno_txs: Vec::with_capacity(worker_count),
            pending_archives: Vec::with_capacity(worker_count),
            archives_rx,
            complete_trigger,
        }
    }

    pub async fn wait_next_archive(&mut self, seqno: u32) -> Result<Vec<u8>> {
        if self.pending_archives.is_empty() {
            self.start_workers(seqno).await?;
        }

        loop {
            // log::info!("--- loop {}", QueueWrapper(&self.pending_archives));

            if let Some(data) = self.find_next_downloaded_archive(seqno).await? {
                return Ok(data);
            }

            let (received_seqno, data) = match self.archives_rx.recv().await {
                Some((seqno, data)) => (seqno, data),
                None => {
                    return Err(ArchiveDownloaderError::ArchivesChannelClosed.into());
                }
            };

            match self
                .pending_archives
                .iter_mut()
                .find(|(seqno, _)| seqno == &received_seqno)
            {
                Some((_, status @ ArchiveStatus::Downloading)) => {
                    *status = ArchiveStatus::Downloaded(data);
                }
                Some(_) => {
                    return Err(ArchiveDownloaderError::SchedulerError)
                        .context("Received data for already delivered archive")
                }
                None => {
                    return Err(ArchiveDownloaderError::SchedulerError).with_context(|| {
                        format!("Slot not found for response seqno: {}", received_seqno)
                    })
                }
            }
        }
    }

    async fn find_next_downloaded_archive(
        &mut self,
        current_seqno: u32,
    ) -> Result<Option<Vec<u8>>> {
        for (worker_id, (seqno, status)) in self.pending_archives.iter_mut().enumerate() {
            if seqno.saturating_sub(current_seqno) >= ARCHIVE_SLICE {
                continue;
            }

            let data = match std::mem::replace(status, ArchiveStatus::Downloading) {
                ArchiveStatus::Downloading => continue,
                ArchiveStatus::Downloaded(data) => data,
            };

            *seqno = current_seqno + ARCHIVE_SLICE * self.seqno_txs.len() as u32;
            self.seqno_txs[worker_id]
                .send(*seqno)
                .await
                .context("Worker closed")?;

            log::info!("--- Found archive for {}", current_seqno);
            return Ok(Some(data));
        }

        Ok(None)
    }

    async fn start_workers(&mut self, seqno: u32) -> Result<()> {
        let mut next_seqno = seqno;
        for _ in 0..self.worker_count {
            let (seqno_tx, seqno_rx) = mpsc::channel(1);
            tokio::spawn(download_archive_worker(self.ctx.clone(), seqno_rx));

            self.pending_archives
                .push((next_seqno, ArchiveStatus::Downloading));
            self.seqno_txs.push(seqno_tx);

            next_seqno += ARCHIVE_SLICE;
        }

        next_seqno = seqno;
        for tx in &self.seqno_txs {
            tx.send(next_seqno).await?;
            next_seqno += ARCHIVE_SLICE;
        }

        Ok(())
    }
}

impl Drop for ArchiveDownloader {
    fn drop(&mut self) {
        self.ctx.complete.store(true, Ordering::Release);
        self.complete_trigger.trigger();
    }
}

struct DownloadContext {
    engine: Arc<Engine>,
    active_peers: Arc<ActivePeers>,
    archives_tx: ArchivesTx,
    complete: Arc<AtomicBool>,
    complete_signal: TriggerReceiver,
}

async fn download_archive_worker(ctx: Arc<DownloadContext>, mut seqno_rx: SeqNoRx) {
    'tasks: while let Some(seqno) = seqno_rx.recv().await {
        log::info!(
            "sync: Start downloading archive for masterchain block {}",
            seqno
        );

        let data = loop {
            if ctx.complete.load(Ordering::Acquire) {
                break 'tasks;
            }

            let archive_fut = download_archive(&ctx.engine, &ctx.active_peers, seqno);

            let result = tokio::select! {
                data = archive_fut => data,
                _ = ctx.complete_signal.clone() => {
                    log::trace!("Received complete signal");
                    continue;
                }
            };

            match result {
                Ok(Some(data)) => break data,
                Ok(None) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                Err(e) => {
                    log::warn!("Failed to download archive {}: {:?}", seqno, e);
                    continue;
                }
            }
        };

        if ctx.archives_tx.send((seqno, data)).await.is_err() {
            break 'tasks;
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

const ARCHIVE_SLICE: u32 = 100;

// struct QueueWrapper<'a>(&'a [(u32, ArchiveStatus)]);
//
// impl std::fmt::Display for QueueWrapper<'_> {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.write_str("[\n")?;
//         for (seqno, item) in self.0 {
//             match item {
//                 ArchiveStatus::Downloading => {
//                     f.write_fmt(format_args!("    ({}, downloading...),\n", seqno))?
//                 }
//                 ArchiveStatus::Downloaded(_) => {
//                     f.write_fmt(format_args!("    ({}, downloaded)\n", seqno))?
//                 }
//             }
//         }
//         f.write_str("]")
//     }
// }

enum ArchiveStatus {
    Downloading,
    Downloaded(Vec<u8>),
}

type ArchiveItem = (u32, Vec<u8>);

type SeqNoRx = mpsc::Receiver<u32>;
type SeqNoTx = mpsc::Sender<u32>;

type ArchivesTx = mpsc::Sender<ArchiveItem>;
type ArchivesRx = mpsc::Receiver<ArchiveItem>;

#[derive(thiserror::Error, Debug)]
enum ArchiveDownloaderError {
    #[error("Archives channel closed")]
    ArchivesChannelClosed,
    #[error("Scheduler error")]
    SchedulerError,
}
