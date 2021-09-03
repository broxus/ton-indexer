use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tiny_adnl::utils::*;
use tiny_adnl::Neighbour;
use tokio::sync::{mpsc, oneshot};

use crate::engine::Engine;
use crate::network::*;
use crate::utils::*;

const PROCESSING_QUEUE_LEN: usize = 6;
const DOWNLOADING_QUEUE_LEN: usize = 3;
const PACKET_SIZE: usize = 1 << 20; // 1 MB

pub async fn download_state(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
    masterchain_block_id: &ton_block::BlockIdExt,
    clear_db: bool,
    active_peers: &Arc<ActivePeers>,
) -> Result<Arc<ShardStateStuff>> {
    let overlay = engine
        .get_full_node_overlay(
            block_id.shard_id.workchain_id(),
            block_id.shard_id.shard_prefix_with_tag(),
        )
        .await?;

    let neighbour = loop {
        match overlay
            .check_persistent_state(block_id, masterchain_block_id, active_peers)
            .await
        {
            Ok(Some(peer)) => break peer,
            Ok(None) => {
                log::trace!("Failed to download state: state not found");
            }
            Err(e) => {
                log::trace!("Failed to download state: {}", e);
            }
        };
    };

    let (result_tx, result_rx) = oneshot::channel();
    let (packets_tx, packets_rx) = mpsc::channel(PROCESSING_QUEUE_LEN);

    tokio::spawn({
        let engine = engine.clone();
        let block_id = block_id.clone();
        async move { result_tx.send(background_process(&engine, block_id, clear_db, packets_rx).await) }
    });

    {
        let mut scheduler = Scheduler::with_slots(
            overlay,
            block_id.clone(),
            masterchain_block_id.clone(),
            neighbour,
            DOWNLOADING_QUEUE_LEN,
            PACKET_SIZE,
        )
        .await?;

        let mut total_bytes = 0;
        while let Some(packet) = scheduler.wait_next_packet().await? {
            total_bytes += packet.len();
            log::info!("--- Queueing packet");
            if packets_tx.send(packet).await.is_err() {
                break;
            }
        }

        log::info!("DOWNLOADED: {} bytes", total_bytes);
    }

    result_rx.await?
}

async fn background_process(
    engine: &Arc<Engine>,
    block_id: ton_block::BlockIdExt,
    clear_db: bool,
    mut packets_rx: PacketsRx,
) -> Result<Arc<ShardStateStuff>> {
    let (mut transaction, mut ctx) = engine
        .db
        .shard_state_storage()
        .begin_replace(&block_id, clear_db)
        .await?;

    let mut full = false;
    while let Some(packet) = packets_rx.recv().await {
        match transaction.process_packet(&mut ctx, packet).await {
            Ok(true) => {
                full = true;
                break;
            }
            Ok(false) => continue,
            Err(e) => {
                ctx.clear().await?;
                return Err(e);
            }
        }
    }

    packets_rx.close();
    while packets_rx.recv().await.is_some() {}

    if !full {
        ctx.clear().await?;
        return Err(DownloadStateError::UnexpectedEof.into());
    }

    let result = transaction.finalize(&mut ctx, block_id).await;
    ctx.clear().await?;
    result
}

struct Scheduler {
    offset_txs: Vec<OffsetsTx>,
    pending_packets: Vec<(usize, PacketStatus)>,
    response_rx: ResponseRx,
    max_size: usize,
    current_offset: usize,
    finished: bool,
    got_last_part: Arc<AtomicBool>,
    last_part_trigger: Trigger,
}

impl Scheduler {
    async fn with_slots(
        overlay: Arc<dyn FullNodeOverlayClient>,
        block_id: ton_block::BlockIdExt,
        masterchain_block_id: ton_block::BlockIdExt,
        neighbour: Arc<Neighbour>,
        worker_count: usize,
        max_size: usize,
    ) -> Result<Self> {
        let (response_tx, response_rx) = mpsc::channel(worker_count);

        let got_last_part = Arc::new(AtomicBool::new(false));
        let (last_part_trigger, last_part_signal) = trigger();

        let ctx = Arc::new(DownloadContext {
            overlay,
            block_id,
            masterchain_block_id,
            neighbour,
            max_size,
            response_tx,
            peer_attempt: AtomicU32::new(0),
            got_last_part: got_last_part.clone(),
            last_part_signal,
        });

        let mut offset_txs = Vec::with_capacity(worker_count);
        let mut pending_packets = Vec::with_capacity(worker_count);

        let mut offset = 0;
        for _ in 0..worker_count {
            let (offsets_tx, offsets_rx) = mpsc::channel(1);
            tokio::spawn(download_packet_worker(ctx.clone(), offsets_rx));

            pending_packets.push((offset, PacketStatus::Downloading));
            offsets_tx.send(offset).await?;
            offset_txs.push(offsets_tx);

            offset += max_size;
        }

        Ok(Self {
            offset_txs,
            pending_packets,
            response_rx,
            max_size,
            current_offset: 0,
            finished: false,
            got_last_part,
            last_part_trigger,
        })
    }

    async fn wait_next_packet(&mut self) -> Result<Option<Vec<u8>>> {
        if self.finished {
            return Ok(None);
        }

        loop {
            log::info!("--- loop {}", QueueWrapper(&self.pending_packets));

            if let Some(data) = self.find_next_downloaded_packet().await? {
                return Ok(Some(data));
            }

            let (offset, data) = match self.response_rx.recv().await {
                Some((offset, Ok(data))) => (offset, data),
                Some((_, Err(e))) => return Err(e),
                None => {
                    return Err(DownloadStateError::SchedulerError)
                        .context("Response channel closed")
                }
            };

            if data.len() < self.max_size {
                self.got_last_part.store(true, Ordering::Release);
                self.last_part_trigger.trigger();
            }

            match self
                .pending_packets
                .iter_mut()
                .find(|(packet_offset, _)| packet_offset == &offset)
            {
                Some((_, status @ PacketStatus::Downloading)) => {
                    *status = PacketStatus::Downloaded(data);
                }
                Some(_) => {
                    return Err(DownloadStateError::SchedulerError)
                        .context("Received data for already delivered packet")
                }
                None => {
                    return Err(DownloadStateError::SchedulerError)
                        .context("Slot not found for response offset")
                }
            };
        }
    }

    async fn find_next_downloaded_packet(&mut self) -> Result<Option<Vec<u8>>> {
        for (worker_id, (offset, packet)) in self.pending_packets.iter_mut().enumerate() {
            if offset != &self.current_offset {
                continue;
            }

            let data = match std::mem::replace(packet, PacketStatus::Downloading) {
                PacketStatus::Downloading => continue,
                PacketStatus::Downloaded(data) => data,
                PacketStatus::Done => return Ok(None),
            };

            if data.len() < self.max_size {
                *packet = PacketStatus::Done;
                self.finished = true;
            } else {
                *offset += self.max_size * self.offset_txs.len();
                self.offset_txs[worker_id]
                    .send(*offset)
                    .await
                    .context("Worker closed")?;
            }

            log::info!("--- Found packet for {}", self.current_offset);

            self.current_offset += data.len();
            return Ok(Some(data));
        }

        Ok(None)
    }
}

struct DownloadContext {
    overlay: Arc<dyn FullNodeOverlayClient>,
    block_id: ton_block::BlockIdExt,
    masterchain_block_id: ton_block::BlockIdExt,
    neighbour: Arc<Neighbour>,
    max_size: usize,
    response_tx: ResponseTx,
    peer_attempt: AtomicU32,
    got_last_part: Arc<AtomicBool>,
    last_part_signal: TriggerReceiver,
}

async fn download_packet_worker(ctx: Arc<DownloadContext>, mut offsets_rx: OffsetsRx) {
    'tasks: while let Some(offset) = offsets_rx.recv().await {
        let mut part_attempt = 0;
        loop {
            if ctx.got_last_part.load(Ordering::Acquire) {
                break 'tasks;
            }

            let recv_fut = ctx.overlay.download_persistent_state_part(
                &ctx.block_id,
                &ctx.masterchain_block_id,
                offset,
                ctx.max_size,
                ctx.neighbour.clone(),
                ctx.peer_attempt.load(Ordering::Acquire),
            );

            let result = tokio::select! {
                data = recv_fut => data,
                _ = ctx.last_part_signal.clone() => {
                    continue;
                }
            };

            match result {
                Ok(part) => {
                    if ctx.response_tx.send((offset, Ok(part))).await.is_err() {
                        break 'tasks;
                    }
                    continue 'tasks;
                }
                Err(e) => {
                    part_attempt += 1;
                    ctx.peer_attempt.fetch_add(1, Ordering::Release);

                    if !ctx.got_last_part.load(Ordering::Acquire) {
                        log::error!("Failed to download persistent state part {}: {}", offset, e);
                    }

                    if part_attempt > 10 {
                        offsets_rx.close();
                        while offsets_rx.recv().await.is_some() {}

                        let _ = ctx
                            .response_tx
                            .send((offset, Err(DownloadStateError::RanOutOfAttempts.into())))
                            .await;
                        break 'tasks;
                    }

                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

struct QueueWrapper<'a>(&'a [(usize, PacketStatus)]);

impl std::fmt::Display for QueueWrapper<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[\n")?;
        for (id, item) in self.0 {
            match item {
                PacketStatus::Downloading => {
                    f.write_fmt(format_args!("    ({}, downloading...),\n", id))?
                }
                PacketStatus::Downloaded(_) => {
                    f.write_fmt(format_args!("    ({}, downloaded)\n", id))?
                }
                PacketStatus::Done => f.write_fmt(format_args!("    ({}, done)\n", id))?,
            }
        }
        f.write_str("]")
    }
}

enum PacketStatus {
    Downloading,
    Downloaded(Vec<u8>),
    Done,
}

type ResponseRx = mpsc::Receiver<(usize, Result<Vec<u8>>)>;
type ResponseTx = mpsc::Sender<(usize, Result<Vec<u8>>)>;

type OffsetsRx = mpsc::Receiver<usize>;
type OffsetsTx = mpsc::Sender<usize>;

type PacketsRx = mpsc::Receiver<Vec<u8>>;

#[derive(thiserror::Error, Debug)]
enum DownloadStateError {
    #[error("Ran out of attempts")]
    RanOutOfAttempts,
    #[error("Unexpected eof")]
    UnexpectedEof,
    #[error("State downloader scheduler error")]
    SchedulerError,
}
