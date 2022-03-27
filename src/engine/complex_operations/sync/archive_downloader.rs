use std::cmp::{Ordering, Reverse};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::sync::Arc;

use anyhow::Result;
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt};
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::block_maps::*;
use crate::engine::Engine;
use crate::utils::*;

pub struct ArchiveDownloader {
    engine: Arc<Engine>,
    active_peers: Arc<ActivePeers>,
    pending_archives: BinaryHeap<PendingBlockMaps>,
    new_archive_notification: Arc<Notify>,
    cancellation_token: CancellationToken,
    running: bool,
    step: usize,
    next_mc_seq_no: u32,
    to: Option<u32>,
}

impl ArchiveDownloader {
    pub fn builder() -> ArchiveDownloaderBuilder {
        ArchiveDownloaderBuilder {
            step: ARCHIVE_SLICE as usize,
            from: 0,
            to: None,
        }
    }

    pub async fn recv(&'_ mut self) -> Option<ReceivedBlockMaps<'_>> {
        if !self.running {
            return None;
        }

        let block_maps = loop {
            // Get pending archive with max priority
            let notified = match self.pending_archives.peek_mut() {
                // Process if this is an archive with required seq_no
                Some(item) if item.index <= self.next_mc_seq_no => {
                    // Acquire pending archive lock
                    let data = item.block_maps.lock().take();
                    if let Some(data) = data {
                        // Remove this item from the queue
                        PeekMut::pop(item);
                        // Result item is found
                        break data;
                    }

                    // Create `Notified` future while lock is still acquired
                    self.new_archive_notification.notified()
                }
                // Queue is empty or there is a gap
                item => {
                    // Drop `PeekMut` with pending archive
                    drop(item);

                    // Start downloading an archive with required seq_no
                    self.start_downloading(self.next_mc_seq_no);
                    continue;
                }
            };

            // Wait until next archive is available
            notified.await;
        };

        Some(ReceivedBlockMaps {
            downloader: self,
            block_maps,
            accepted: false,
        })
    }

    pub async fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.running = false;
    }

    fn start_downloading(&mut self, mc_block_seq_no: u32) {
        let block_maps = Arc::new(Mutex::new(None));

        // Add pending archive
        self.pending_archives.push(PendingBlockMaps {
            index: mc_block_seq_no,
            block_maps: block_maps.clone(),
        });

        // Prepare context
        let engine = self.engine.clone();
        let active_peers = self.active_peers.clone();
        let cancellation_token = self.cancellation_token.clone();
        let new_archive_notification = self.new_archive_notification.clone();

        // Spawn downloader
        tokio::spawn(async move {
            if let Some(result) =
                download_archive_maps(&engine, &active_peers, &cancellation_token, mc_block_seq_no)
                    .await
            {
                *block_maps.lock() = Some(result);
                new_archive_notification.notify_waiters();
            }
        });
    }
}

impl Drop for ArchiveDownloader {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
    }
}

pub struct ArchiveDownloaderBuilder {
    step: usize,
    from: u32,
    to: Option<u32>,
}

impl ArchiveDownloaderBuilder {
    pub fn step(mut self, step: usize) -> Self {
        self.step = step;
        self
    }

    pub fn range(mut self, range: impl RangeBounds<u32>) -> Self {
        self.from = match range.start_bound() {
            Bound::Included(&from) => from,
            Bound::Excluded(&from) => from + 1,
            Bound::Unbounded => 0,
        };

        self.to = match range.end_bound() {
            Bound::Included(&to) => Some(to),
            Bound::Excluded(&to) if to > 0 => Some(to - 1),
            Bound::Excluded(&to) => Some(0),
            Bound::Unbounded => None,
        };

        if let Some(to) = &mut self.to {
            *to = std::cmp::max(*to, self.from);
        }

        self
    }

    pub fn start(self, engine: &Arc<Engine>, active_peers: &Arc<ActivePeers>) -> ArchiveDownloader {
        let mut downloader = ArchiveDownloader {
            engine: engine.clone(),
            active_peers: active_peers.clone(),
            pending_archives: Default::default(),
            new_archive_notification: Default::default(),
            cancellation_token: Default::default(),
            running: true,
            step: self.step,
            next_mc_seq_no: self.from,
            to: self.to,
        };

        for mc_seq_no in (downloader.next_mc_seq_no..)
            .step_by(downloader.step)
            .take(engine.parallel_archive_downloads)
        {
            downloader.start_downloading(mc_seq_no);
        }

        downloader
    }
}

struct PendingBlockMaps {
    index: u32,
    block_maps: Arc<Mutex<Option<Arc<BlockMaps>>>>,
}

impl PartialEq for PendingBlockMaps {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl Eq for PendingBlockMaps {}

impl PartialOrd for PendingBlockMaps {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingBlockMaps {
    fn cmp(&self, other: &Self) -> Ordering {
        // NOTE: reverse comparison here because `BinaryHeap` is a max-heap
        other.index.cmp(&self.index)
    }
}

pub struct ReceivedBlockMaps<'a> {
    downloader: &'a mut ArchiveDownloader,
    block_maps: Arc<BlockMaps>,
    accepted: bool,
}

impl ReceivedBlockMaps<'_> {
    pub fn accept(mut self) {
        self.accepted = true;
        if let Some(highest_mc_id) = self.block_maps.highest_mc_id() {
            self.downloader.next_mc_seq_no = highest_mc_id.seq_no + 1;
        }
    }
}

impl Deref for ReceivedBlockMaps<'_> {
    type Target = Arc<BlockMaps>;

    fn deref(&self) -> &Self::Target {
        &self.block_maps
    }
}

impl DerefMut for ReceivedBlockMaps<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.block_maps
    }
}

impl Drop for ReceivedBlockMaps<'_> {
    fn drop(&mut self) {
        if !self.accepted {
            self.downloader.start_downloading(self.index);
        }
    }
}

pub async fn start_download<I>(
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    step: u32,
    range: I,
) -> Option<(impl Stream<Item = Arc<BlockMaps>>, TriggerOnDrop)>
where
    I: IntoIterator<Item = u32> + Send + 'static,
    <I as IntoIterator>::IntoIter: Send,
{
    let (trigger, signal) = trigger_on_drop();

    let num_tasks = engine.parallel_archive_downloads;

    let engine_clone = engine.clone();
    let active_peers_clone = active_peers.clone();
    let signal_clone = signal.clone();
    let stream = futures::stream::iter(range.into_iter().step_by(step as usize))
        .map(move |x| {
            let engine = engine_clone.clone();
            let active_peers = active_peers_clone.clone();
            let signal = signal_clone.clone();
            async move { download_archive_maps(&engine, &active_peers, &signal, x).await }
        })
        .buffered(num_tasks as usize)
        .while_some();

    process_maps(stream, engine, active_peers, signal)
        .await
        .map(|stream| (stream, trigger))
}

async fn process_maps<S>(
    mut stream: S,
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    signal: CancellationToken,
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
            match right.find_intersection(&left) {
                BlockMapsIntersection::Contiguous => {
                    // Send previous archive
                    if tx.send(left).await.is_err() {
                        log::warn!("Archive stream closed");
                        return;
                    }
                }
                BlockMapsIntersection::Gap { from, to } => {
                    // Download gaps
                    let gaps = match download_gaps(from, to, &engine, &active_peers, &signal).await
                    {
                        Some(gaps) => gaps,
                        None => return,
                    };

                    // Send previous archive
                    if tx.send(left).await.is_err() {
                        log::warn!("Archive stream closed");
                        return;
                    }

                    // Send archives for gaps
                    for arch in gaps {
                        if tx.send(arch).await.is_err() {
                            log::warn!("Archive stream closed");
                            return;
                        }
                    }
                }
                _ => continue,
            }

            left = right
        }

        if tx.send(left).await.is_err() {
            log::warn!("Archive stream closed");
        }
    });

    Some(rx)
}

async fn download_gaps(
    mut from: u32,
    to: u32,
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    signal: &CancellationToken,
) -> Option<Vec<Arc<BlockMaps>>> {
    log::warn!("Finding archive for the gap {}..{}", from, to);

    let mut arhives = Vec::with_capacity(1);
    while from < to {
        let archive = download_archive_maps(engine, active_peers, signal, from).await?;

        from = archive.highest_mc_id().unwrap().seq_no + 1;
        arhives.push(archive);
    }

    Some(arhives)
}

pub async fn download_archive_maps(
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    signal: &CancellationToken,
    mc_seq_no: u32,
) -> Option<Arc<BlockMaps>> {
    tokio::pin!(
        let signal = signal.cancelled();
    );

    loop {
        let start = std::time::Instant::now();
        let data = tokio::select! {
            data = download_archive(engine, active_peers, mc_seq_no) => data,
            _ = (&mut signal) => return None,
        };
        log::info!("Download took: {} ms", start.elapsed().as_millis());

        match BlockMaps::new(mc_seq_no, &data) {
            Ok(map) => match map.check() {
                Ok(()) => break Some(map),
                Err(e) => log::error!("Invalid archive: {:?}", e),
            },
            Err(e) => {
                log::error!("Failed to parse archive: {:?}", e);
            }
        };
    }
}

pub async fn download_archive(
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    mc_seq_no: u32,
) -> Vec<u8> {
    log::info!("sync: Downloading archive for block {mc_seq_no}");

    loop {
        match engine.download_archive(mc_seq_no, active_peers).await {
            Ok(Some(data)) => {
                let len = data.len();
                log::info!("sync: Downloaded archive for block {mc_seq_no}, size {len} bytes");
                break data;
            }
            Ok(None) => {
                log::trace!("sync: No archive found for block {mc_seq_no}");
            }
            Err(e) => {
                log::warn!("sync: Failed to download archive for block {mc_seq_no}: {e:?}")
            }
        }
    }
}
