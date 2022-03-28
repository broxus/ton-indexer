use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::sync::Arc;

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
    step: u32,
    next_mc_seq_no: u32,
    max_mc_seq_no: u32,
    to: Option<u32>,
}

impl ArchiveDownloader {
    pub fn builder() -> ArchiveDownloaderBuilder {
        ArchiveDownloaderBuilder {
            step: ARCHIVE_SLICE,
            from: 0,
            to: None,
        }
    }

    pub async fn recv(&'_ mut self) -> Option<ReceivedBlockMaps<'_>> {
        if !self.running {
            return None;
        }

        let mut has_gap = false;

        let block_maps = loop {
            let next_index = self.next_mc_seq_no;

            // Force fill gap
            if has_gap {
                self.start_downloading(next_index);
                has_gap = false;
                continue;
            }

            // Get pending archive with max priority
            let notified = match self.pending_archives.peek_mut() {
                // Process if this is an archive with required seq_no
                Some(item) if item.index < next_index + self.step => {
                    let data = {
                        let mut data = item.block_maps.lock();

                        // Check lowest id without taking inner data
                        if let Some(maps) = &*data {
                            if matches!(maps.lowest_mc_id(), Some(id) if id.seq_no > next_index) {
                                // Drop acquired lock and `PeekMut` object
                                has_gap = true;
                                continue;
                            }
                        }

                        data.take()
                    };

                    if let Some(data) = data {
                        // Remove this item from the queue
                        PeekMut::pop(item);

                        if let Err(e) = data.check() {
                            log::error!("Retrying invalid archive {next_index}: {e:?}");
                        } else {
                            // Result item was found
                            break data;
                        }
                    }

                    // Create `Notified` future while lock is still acquired
                    self.new_archive_notification.notified()
                }
                // Queue is empty or there is a gap
                item => {
                    // Drop `PeekMut` with pending archive
                    drop(item);

                    log::info!("GAP: {next_index}");

                    // Start downloading an archive with required seq_no
                    self.start_downloading(next_index);
                    continue;
                }
            };

            // Wait until next archive is available
            notified.await;
        };

        while self.pending_archives.len() < self.engine.parallel_archive_downloads
            && !matches!(self.to, Some(to) if self.max_mc_seq_no + self.step > to)
        {
            self.start_downloading(self.max_mc_seq_no + self.step);
        }

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
        self.max_mc_seq_no = std::cmp::max(self.max_mc_seq_no, mc_block_seq_no);

        // Prepare context
        let engine = self.engine.clone();
        let active_peers = self.active_peers.clone();
        let cancellation_token = self.cancellation_token.clone();
        let new_archive_notification = self.new_archive_notification.clone();

        // Spawn downloader
        tokio::spawn(async move {
            if let Some(result) =
                download_archive(&engine, &active_peers, &cancellation_token, mc_block_seq_no).await
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
    step: u32,
    from: u32,
    to: Option<u32>,
}

impl ArchiveDownloaderBuilder {
    pub fn step(mut self, step: u32) -> Self {
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
            Bound::Excluded(_) => Some(0),
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
            max_mc_seq_no: 0,
            to: self.to,
        };

        for mc_seq_no in (downloader.next_mc_seq_no..)
            .step_by(downloader.step as usize)
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

pub async fn download_archive(
    engine: &Arc<Engine>,
    active_peers: &Arc<ActivePeers>,
    signal: &CancellationToken,
    mc_seq_no: u32,
) -> Option<Arc<BlockMaps>> {
    tokio::pin!(
        let signal = signal.cancelled();
    );

    log::info!("sync: Downloading archive for block {mc_seq_no}");

    loop {
        let start = std::time::Instant::now();
        let result = tokio::select! {
            data = engine.download_archive(mc_seq_no, active_peers) => data,
            _ = (&mut signal) => return None,
        };
        log::info!("sync: Download took: {} ms", start.elapsed().as_millis());

        match result {
            Ok(Some(data)) => {
                let len = data.len();
                log::info!("sync: Downloaded archive for block {mc_seq_no}, size {len} bytes");

                match BlockMaps::new(mc_seq_no, &data) {
                    Ok(data) => break Some(data),
                    Err(e) => {
                        log::error!("sync: Failed to parse archive: {e:?}");
                    }
                }
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
