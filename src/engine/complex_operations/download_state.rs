use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::engine::{Engine, NodeRpcClient};
use crate::network::Neighbour;
use crate::utils::*;

const PROCESSING_QUEUE_LEN: usize = 10;
const DOWNLOADING_QUEUE_LEN: usize = 10;
const PACKET_SIZE: usize = 1 << 20; // 1 MB

pub async fn download_state(
    engine: &Arc<Engine>,
    full_state_id: &FullStateId,
) -> Result<Arc<ShardStateStuff>> {
    let mc_client = engine.masterchain_client.clone();

    let neighbour = loop {
        match mc_client.find_persistent_state(full_state_id).await {
            Ok(Some(peer)) => break peer,
            Ok(None) => {
                tracing::trace!(
                    block_id = %full_state_id.block_id.display(),
                    "failed to download state: state not found"
                );
            }
            Err(e) => {
                tracing::trace!(
                    block_id = %full_state_id.block_id.display(),
                    "failed to download state: {e:?}"
                );
            }
        };
    };

    let (packets_tx, packets_rx) = mpsc::channel(PROCESSING_QUEUE_LEN);

    let processing_fut = tokio::spawn({
        let engine = engine.clone();
        let block_id = full_state_id.block_id.clone();
        async move { background_process(&engine, block_id, packets_rx).await }
    });

    let downloader_fut = tokio::spawn({
        let full_state_id = full_state_id.clone();
        async move {
            let mut scheduler = Scheduler::with_slots(
                mc_client,
                full_state_id,
                neighbour,
                DOWNLOADING_QUEUE_LEN,
                PACKET_SIZE,
            )
            .await;

            while let Some(packet) = scheduler.wait_next_packet().await? {
                if packets_tx.send(packet).await.is_err() {
                    anyhow::bail!("Packets receiver closed");
                }
            }
            Ok::<_, anyhow::Error>(())
        }
    });

    match futures_util::future::select(processing_fut, downloader_fut).await {
        // State processing finished first - ignore downloader result, return processing result
        futures_util::future::Either::Left((result, _)) => result.unwrap(),
        // Downloader finished first
        futures_util::future::Either::Right((downloader_result, result_fut)) => {
            match downloader_result.unwrap() {
                // Downloader finished - wait for processing result and return it
                Ok(()) => result_fut.await.unwrap(),
                // Downloader failed - cancel processing and return downloader error
                Err(e) => Err(e),
            }
        }
    }
}

async fn background_process(
    engine: &Arc<Engine>,
    block_id: ton_block::BlockIdExt,
    mut packets_rx: PacketsRx,
) -> Result<Arc<ShardStateStuff>> {
    let (mut transaction, mut ctx) = engine
        .storage
        .shard_state_storage()
        .begin_replace(&block_id)
        .await?;

    let mut pg = ProgressBar::builder()
        .exact_unit("cells")
        .build(|msg| tracing::info!("downloading state... {msg}"));

    let mut full = false;

    let mut received_bytes = 0;
    while let Some(packet) = packets_rx.recv().await {
        received_bytes += packet.len();
        if let Some(header) = transaction.header() {
            anyhow::ensure!(
                received_bytes <= header.total_size as usize,
                "Received more data than expected"
            );
        }

        match transaction.process_packet(&mut ctx, packet, &mut pg).await {
            Ok(true) => {
                full = true;
                break;
            }
            Ok(false) => continue,
            Err(e) => {
                return Err(e);
            }
        }
    }

    if !full {
        return Err(DownloadStateError::UnexpectedEof.into());
    }

    packets_rx.close();
    while packets_rx.recv().await.is_some() {}

    tracing::info!(
        size_bytes = received_bytes,
        size = %bytesize::ByteSize::b(received_bytes as _),
        "downloaded persistent state",
    );

    let mut pg = ProgressBar::builder()
        .with_mapper(|x| bytesize::to_string(x, false))
        .build(|msg| tracing::info!("processing state... {msg}"));

    transaction.finalize(&mut ctx, block_id, &mut pg).await
}

struct Scheduler {
    offset_txs: Vec<OffsetsTx>,
    pending_packets: Vec<(usize, PacketStatus)>,
    response_rx: ResponseRx,
    packet_size: usize,
    current_offset: usize,
    complete: Arc<AtomicBool>,
    join_set: JoinSet<()>,
}

impl Scheduler {
    async fn with_slots(
        mc_client: NodeRpcClient,
        full_state_id: FullStateId,
        neighbour: Arc<Neighbour>,
        worker_count: usize,
        packet_size: usize,
    ) -> Self {
        let (response_tx, response_rx) = mpsc::channel(worker_count);

        let complete = Arc::new(AtomicBool::new(false));

        let ctx = Arc::new(DownloadContext {
            mc_client,
            full_state_id,
            neighbour,
            packet_size,
            response_tx,
            complete: complete.clone(),
        });

        let mut offset_txs = Vec::with_capacity(worker_count);
        let mut pending_packets = Vec::with_capacity(worker_count);
        let mut join_set = tokio::task::JoinSet::new();

        let mut offset = 0;
        for _ in 0..worker_count {
            let (offsets_tx, offsets_rx) = mpsc::channel(1);

            pending_packets.push((offset, PacketStatus::Downloading));
            offsets_tx.send(offset).await.ok();
            offset_txs.push(offsets_tx);

            offset += packet_size;

            join_set.spawn(download_packet_worker(ctx.clone(), offsets_rx));
        }

        Self {
            offset_txs,
            pending_packets,
            response_rx,
            packet_size,
            current_offset: 0,
            complete,
            join_set,
        }
    }

    async fn wait_next_packet(&mut self) -> Result<Option<Vec<u8>>> {
        if self.complete.load(Ordering::Acquire) {
            return Ok(None);
        }

        loop {
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

            if data.len() < self.packet_size {
                *packet = PacketStatus::Done;
                self.complete.store(true, Ordering::Release);
                self.join_set.abort_all();
            } else {
                *offset += self.packet_size * self.offset_txs.len();
                self.offset_txs[worker_id]
                    .send(*offset)
                    .await
                    .context("Worker closed")?;
            }

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
    packet_size: usize,
    response_tx: ResponseTx,
    complete: Arc<AtomicBool>,
}

async fn download_packet_worker(ctx: Arc<DownloadContext>, mut offsets_rx: OffsetsRx) {
    const MAX_ATTEMPTS: u32 = 10;
    const RETRY_INTERVAL: Duration = Duration::from_millis(100);

    'tasks: while let Some(offset) = offsets_rx.recv().await {
        let mut attempt = 0;
        loop {
            if ctx.complete.load(Ordering::Acquire) {
                break 'tasks;
            }

            match ctx
                .mc_client
                .download_persistent_state_part(
                    &ctx.full_state_id,
                    offset,
                    ctx.packet_size,
                    ctx.neighbour.clone(),
                    attempt,
                )
                .await
            {
                Ok(part) => {
                    tracing::debug!(
                        offset,
                        part_size = part.len(),
                        "downloaded persistent state part"
                    );

                    if ctx.response_tx.send((offset, Ok(part))).await.is_err() {
                        break 'tasks;
                    }
                    continue 'tasks;
                }
                Err(e) => {
                    attempt += 1;

                    if attempt > MAX_ATTEMPTS {
                        let _ = ctx
                            .response_tx
                            .send((
                                offset,
                                Err(DownloadStateError::RanOutOfAttempts).with_context(|| {
                                    format!("Failed to download persistent state part at offset {offset}: {e:?}")
                                }),
                            ))
                            .await;
                        break 'tasks;
                    }

                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        }
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
