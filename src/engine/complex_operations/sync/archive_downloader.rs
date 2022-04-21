use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::Mutex;
use tiny_adnl::utils::*;
use tiny_adnl::Neighbour;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::archive_writers_pool::*;
use super::block_maps::*;
use crate::engine::Engine;
use crate::network::ArchiveDownloadStatus;

pub struct ArchiveDownloader {
    ctx: Arc<DownloaderContext>,
    pending_archives: BinaryHeap<PendingBlockMaps>,
    prefetch_enabled: bool,
    next_mc_seq_no: u32,
    last_blocks: Option<BlockMapsEdge>,
    /// The seq_no of the last archive that started downloading
    max_mc_seq_no: u32,
    to: Option<u32>,
}

impl ArchiveDownloader {
    /// `last_blocks` is a map of shard_id -> block_id of the last known shard block.
    /// Each of blocks of the first downloaded archive is expected to start with
    /// the next block of the last known shard block.
    ///
    /// NOTE: this check is disabled when `last_blocks` is `None`
    pub fn new(
        engine: &Arc<Engine>,
        range: impl RangeBounds<u32>,
        last_blocks: Option<BlockMapsEdge>,
    ) -> ArchiveDownloader {
        let from = match range.start_bound() {
            Bound::Included(&from) => from,
            Bound::Excluded(&from) => from + 1,
            Bound::Unbounded => 0,
        };

        // Archive stream cannot start from zero block
        let from = std::cmp::max(from, 1);

        let mut to = match range.end_bound() {
            Bound::Included(&to) => Some(to),
            Bound::Excluded(&to) if to > 0 => Some(to - 1),
            Bound::Excluded(_) => Some(0),
            Bound::Unbounded => None,
        };

        if let Some(to) = &mut to {
            *to = std::cmp::max(*to, from);
        }

        // Enable prefetch only for background sync when range end is known
        let prefetch_enabled = to.is_some();

        let mut downloader = ArchiveDownloader {
            ctx: Arc::new(DownloaderContext {
                engine: engine.clone(),
                writers_pool: ArchiveWritersPool::new(
                    engine.db.file_db_path(),
                    engine.sync_options.save_to_disk_threshold,
                ),
                new_archive_notification: Default::default(),
                cancellation_token: Default::default(),
                good_peers: Default::default(),
            }),
            pending_archives: Default::default(),
            prefetch_enabled,
            next_mc_seq_no: from,
            last_blocks,
            max_mc_seq_no: 0,
            to,
        };

        // Start with only the first archive
        downloader.start_downloading(downloader.next_mc_seq_no);

        downloader
    }

    /// Wait next archive
    pub async fn recv(&'_ mut self) -> Option<ReceivedBlockMaps<'_>> {
        const STEP: u32 = BlockMaps::MAX_MC_BLOCK_COUNT as u32;

        let next_index = self.next_mc_seq_no;
        let mut has_gap = false;

        let block_maps = loop {
            // Force fill gap
            if has_gap {
                self.start_downloading(next_index);
                has_gap = false;
                continue;
            }

            // Get pending archive with max priority
            let notified = match self.pending_archives.peek_mut() {
                // Process if this is an archive with required seq_no
                Some(item) if item.index < next_index + STEP => {
                    let data = {
                        let mut data = item.block_maps.lock();

                        // Check lowest id without taking inner data
                        if let Some(maps) = &mut *data {
                            match maps.preload(next_index, &self.last_blocks) {
                                Ok(maps) => {
                                    if matches!(
                                        maps.lowest_mc_id(),
                                        Some(id) if id.seq_no > next_index
                                    ) {
                                        has_gap = true;
                                        // Drop acquired lock and `PeekMut` object
                                        continue;
                                    }
                                }
                                Err(e) => {
                                    log::error!(
                                        "Failed to preload archive for mc block {next_index}: {e:?}"
                                    );
                                }
                            }
                        }

                        data.take()
                    };

                    // By this point when data is `Some`, `data.loaded` will be either `Some` if it
                    // was successfully loaded, or `None` if there was a preload error

                    if let Some(data) = data {
                        // Remove this item from the queue
                        PeekMut::pop(item);

                        match data.loaded {
                            Some(data) => {
                                // Result item was found
                                break data;
                            }
                            None => {
                                log::error!("Retrying invalid archive for mc block {next_index}");
                                continue;
                            }
                        }
                    }

                    // Create `Notified` future while lock is still acquired
                    self.ctx.new_archive_notification.notified()
                }
                // Queue is empty or there is a gap
                _ => {
                    has_gap = true;
                    // Drop `PeekMut` object
                    continue;
                }
            };

            // Wait until next archive is available
            notified.await;
        };

        // NOTE: when `to` is Some, then we need to prefetch until
        // `max_mc_seq_no` will be at least `STEP` greater than `to`.
        // That's because archives must overlap:
        //
        //                  to -.          / discarded \
        // |--------*-----|-----*---*----|---------*----|
        //       mS ^       mS+STEP ^    mS+2*STEP ^
        //
        // > where mS is `max_mc_seq_no`
        //
        while self.prefetch_enabled
            && self.pending_archives.len() < self.ctx.engine.sync_options.parallel_archive_downloads
            && !matches!(self.to, Some(to) if self.max_mc_seq_no + 2 * STEP > to)
        {
            self.start_downloading(self.max_mc_seq_no + STEP);
        }

        Some(ReceivedBlockMaps {
            downloader: self,
            index: next_index,
            block_maps,
            accepted: false,
        })
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
        let ctx = self.ctx.clone();

        // Spawn downloader
        tokio::spawn(async move {
            if let Some(writer) = download_archive(&ctx, mc_block_seq_no).await {
                *block_maps.lock() = Some(BlockMapsData {
                    writer: Some(writer),
                    loaded: None,
                });
                ctx.new_archive_notification.notify_waiters();
            }
        });
    }
}

impl Drop for ArchiveDownloader {
    fn drop(&mut self) {
        self.ctx.cancellation_token.cancel();
    }
}

struct DownloaderContext {
    engine: Arc<Engine>,
    writers_pool: ArchiveWritersPool,
    new_archive_notification: Notify,
    cancellation_token: CancellationToken,
    good_peers: GoodPeers,
}

#[derive(Default)]
struct GoodPeers {
    neighbour: parking_lot::RwLock<Option<Arc<Neighbour>>>,
}

impl GoodPeers {
    fn add(&self, neighbour: Arc<Neighbour>) {
        *self.neighbour.write() = Some(neighbour);
    }

    fn remove(&self, bad_neighbour: &Arc<Neighbour>) {
        let mut good_neigbour = self.neighbour.write();
        if matches!(&*good_neigbour, Some(n) if n.peer_id() == bad_neighbour.peer_id()) {
            *good_neigbour = None;
        }
    }

    fn get(&self) -> Option<Arc<Neighbour>> {
        self.neighbour.read().clone()
    }
}

struct PendingBlockMaps {
    index: u32,
    block_maps: Arc<Mutex<Option<BlockMapsData>>>,
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

struct BlockMapsData {
    loaded: Option<Arc<BlockMaps>>,
    writer: Option<ArchiveWriter>,
}

impl BlockMapsData {
    fn preload(&mut self, next_index: u32, edge: &Option<BlockMapsEdge>) -> Result<Arc<BlockMaps>> {
        if let Some(loaded) = &self.loaded {
            return Ok(loaded.clone());
        }

        if let Some(writer) = self.writer.take() {
            let block_maps = writer
                .parse_block_maps()
                .context("Failed to load block maps")?;
            block_maps.check(next_index, edge)?;
            return Ok(self.loaded.insert(block_maps).clone());
        }

        Err(ArchiveDownloaderError::EmptyBlockMapsData.into())
    }
}

pub struct ReceivedBlockMaps<'a> {
    downloader: &'a mut ArchiveDownloader,
    index: u32,
    block_maps: Arc<BlockMaps>,
    accepted: bool,
}

impl ReceivedBlockMaps<'_> {
    pub fn accept(mut self, edge: Option<BlockMapsEdge>) {
        self.accepted = true;
        if let Some(highest_mc_id) = self.block_maps.highest_mc_id() {
            self.downloader.last_blocks = edge;
            self.downloader.next_mc_seq_no = highest_mc_id.seq_no + 1;
        }
    }

    pub fn accept_with_time(self, time: u32, edge: Option<BlockMapsEdge>) {
        self.downloader.prefetch_enabled = time + ARCHIVE_EXISTENCE_THRESHOLD <= now() as u32;
        self.accept(edge);
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

async fn download_archive(ctx: &DownloaderContext, mc_seq_no: u32) -> Option<ArchiveWriter> {
    tokio::pin!(
        let signal = ctx.cancellation_token.cancelled();
    );

    log::info!("sync: Downloading archive for block {mc_seq_no}");

    loop {
        let mut writer = ctx.writers_pool.acquire();

        let good_peer = ctx.good_peers.get();

        let start = std::time::Instant::now();
        let result = tokio::select! {
            result = ctx.engine.download_archive(mc_seq_no, good_peer.as_ref(), &mut writer) => result,
            _ = (&mut signal) => return None,
        };

        match result {
            Ok(ArchiveDownloadStatus::Downloaded { neighbour, len }) => {
                ctx.good_peers.add(neighbour);
                log::info!(
                    "sync: Downloaded archive for block {mc_seq_no}, size {} bytes. Took: {} ms",
                    len,
                    start.elapsed().as_millis()
                );
                break Some(writer);
            }
            Ok(ArchiveDownloadStatus::NotFound) => {
                if let Some(neighbour) = &good_peer {
                    ctx.good_peers.remove(neighbour);
                }
                log::trace!("sync: No archive found for block {mc_seq_no}");
            }
            Err(e) => {
                if let Some(neighbour) = &good_peer {
                    ctx.good_peers.remove(neighbour);
                }
                log::warn!("sync: Failed to download archive for block {mc_seq_no}: {e:?}")
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ArchiveDownloaderError {
    #[error("Empty block maps data")]
    EmptyBlockMapsData,
}

const ARCHIVE_EXISTENCE_THRESHOLD: u32 = 1800;
