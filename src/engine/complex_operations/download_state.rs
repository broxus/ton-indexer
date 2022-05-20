/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - optimized state downloading and processing
///
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tiny_adnl::Neighbour;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::engine::{Engine, NodeRpcClient};
use crate::utils::*;

const PROCESSING_QUEUE_LEN: usize = 10;
const DOWNLOADING_QUEUE_LEN: usize = 10;
const PACKET_SIZE: usize = 1 << 20; // 1 MB

pub async fn download_state(
    engine: &Arc<Engine>,
    full_state_id: FullStateId,
) -> Result<Arc<ShardStateStuff>> {
    let mc_client = engine.masterchain_client.clone();

    let neighbour = loop {
        match mc_client.find_persistent_state(&full_state_id).await {
            Ok(Some(peer)) => break peer,
            Ok(None) => {
                log::trace!("Failed to download state: state not found");
            }
            Err(e) => {
                log::trace!("Failed to download state: {e:?}");
            }
        };
    };

    let (result_tx, result_rx) = oneshot::channel();
    let (packets_tx, packets_rx) = mpsc::channel(PROCESSING_QUEUE_LEN);

    let (_completion_trigger, completion_signal) = trigger_on_drop();

    tokio::spawn({
        let engine = engine.clone();
        let block_id = full_state_id.block_id.clone();
        async move { result_tx.send(background_process(&engine, block_id, packets_rx).await) }
    });

    let downloader = async move {
        let mut scheduler = Scheduler::with_slots(
            mc_client,
            full_state_id,
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

        Result::<_, anyhow::Error>::Ok(total_bytes)
    };

    tokio::spawn(async move {
        tokio::select! {
            result = downloader => match result {
                Ok(total_bytes) => {
                    log::info!(
                        "Persistent state downloader finished. Total size: {} bytes",
                        total_bytes
                    );
                },
                Err(e) => {
                    log::error!("Persistent state downloader failed: {:?}", e);
                },
            },
            _ = completion_signal.cancelled() => {}
        }
    });

    result_rx.await?
}

async fn background_process(
    engine: &Arc<Engine>,
    block_id: ton_block::BlockIdExt,
    mut packets_rx: PacketsRx,
) -> Result<Arc<ShardStateStuff>> {
    let (mut transaction, mut ctx) = engine
        .db
        .shard_state_storage()
        .begin_replace(&block_id)
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
    complete: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
}

impl Scheduler {
    async fn with_slots(
        mc_client: NodeRpcClient,
        full_state_id: FullStateId,
        neighbour: Arc<Neighbour>,
        worker_count: usize,
        max_size: usize,
    ) -> Result<Self> {
        let (response_tx, response_rx) = mpsc::channel(worker_count);

        let complete = Arc::new(AtomicBool::new(false));
        let cancellation_token = CancellationToken::new();

        let ctx = Arc::new(DownloadContext {
            mc_client,
            full_state_id,
            neighbour,
            max_size,
            response_tx,
            peer_attempt: AtomicU32::new(0),
            complete: complete.clone(),
            cancellation_token: cancellation_token.clone(),
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
            complete,
            cancellation_token,
        })
    }

    async fn wait_next_packet(&mut self) -> Result<Option<Vec<u8>>> {
        if self.complete.load(Ordering::Acquire) {
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
                self.complete.store(true, Ordering::Release);
                self.cancellation_token.cancel();
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
    mc_client: NodeRpcClient,
    full_state_id: FullStateId,
    neighbour: Arc<Neighbour>,
    max_size: usize,
    response_tx: ResponseTx,
    peer_attempt: AtomicU32,
    complete: Arc<AtomicBool>,
    cancellation_token: CancellationToken,
}

async fn download_packet_worker(ctx: Arc<DownloadContext>, mut offsets_rx: OffsetsRx) {
    tokio::pin!(let complete_signal = ctx.cancellation_token.cancelled(););

    'tasks: while let Some(offset) = offsets_rx.recv().await {
        let mut part_attempt = 0;
        loop {
            if ctx.complete.load(Ordering::Acquire) {
                break 'tasks;
            }

            let recv_fut = ctx.mc_client.download_persistent_state_part(
                &ctx.full_state_id,
                offset,
                ctx.max_size,
                ctx.neighbour.clone(),
                ctx.peer_attempt.load(Ordering::Acquire),
            );

            let result = tokio::select! {
                data = recv_fut => data,
                _ = &mut complete_signal => {
                    log::warn!("Got last_part_signal: {}", offset);
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

                    if !ctx.complete.load(Ordering::Acquire) {
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
