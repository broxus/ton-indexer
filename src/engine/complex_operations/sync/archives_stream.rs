use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use broxus_util::now;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::archive_writers_pool::*;
use super::block_maps::*;
use crate::engine::{ArchiveDownloadStatus, Engine};
use crate::network::Neighbour;

pub struct ArchivesStream {
    ctx: Arc<DownloaderContext>,
    pending_archives: BinaryHeap<PendingBlockMaps>,
    prefetch_enabled: bool,
    next_mc_seq_no: u32,
    last_blocks: Option<BlockMapsEdge>,
    /// The seq_no of the last archive that started downloading
    max_mc_seq_no: u32,
    to: Option<u32>,
}

impl ArchivesStream {
    /// `last_blocks` is a map of shard_id -> block_id of the last known shard block.
    /// Each of blocks of the first downloaded archive is expected to start with
    /// the next block of the last known shard block.
    ///
    /// NOTE: this check is disabled when `last_blocks` is `None`
    pub fn new(
        engine: &Arc<Engine>,
        range: impl RangeBounds<u32>,
        last_blocks: Option<BlockMapsEdge>,
    ) -> ArchivesStream {
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

        // Enable prefetch only for historical sync when range end is known
        let prefetch_enabled = to.is_some();

        let mut stream = ArchivesStream {
            ctx: Arc::new(DownloaderContext {
                engine: engine.clone(),
                writers_pool: ArchiveWritersPool::new(
                    engine.storage.file_db_path(),
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
        stream.start_downloading(stream.next_mc_seq_no);

        stream
    }

    /// Wait next archive
    #[tracing::instrument(skip(self))]
    pub async fn recv(&'_ mut self) -> ReceivedBlockMaps<'_> {
        const STEP: u32 = BlockMaps::MAX_MC_BLOCK_COUNT as u32;

        let next_index = self.next_mc_seq_no;
        let mut has_gap = false;

        let (block_maps, neighbour) = loop {
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
                                Ok(block_maps) => {
                                    if matches!(
                                        block_maps.lowest_mc_id(),
                                        Some(id) if id.seq_no > next_index
                                    ) {
                                        has_gap = true;
                                        // Drop acquired lock and `PeekMut` object
                                        continue;
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(target: "sync", next_index, "failed to preload archive: {e:?}");
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
                            Some(block_maps) => {
                                // Result item was found
                                break (block_maps, data.neighbour);
                            }
                            None => {
                                tracing::error!(target: "sync", next_index, "retrying invalid archive");
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

        ReceivedBlockMaps {
            stream: self,
            index: next_index,
            neighbour,
            block_maps,
            accepted: false,
        }
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
            if let Some((writer, neighbour)) = download_archive(&ctx, mc_block_seq_no).await {
                *block_maps.lock() = Some(BlockMapsData {
                    neighbour: Some(neighbour),
                    writer: Some(writer),
                    loaded: None,
                });
                ctx.new_archive_notification.notify_waiters();
            }
        });
    }
}

impl Drop for ArchivesStream {
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
    neighbours: parking_lot::RwLock<[GoodPeerSlot; GOOD_PEER_COUNT]>,
    index: AtomicUsize,
}

impl GoodPeers {
    fn add(&self, neighbour: &Arc<Neighbour>) {
        if !self.neighbours.read().iter().any(|item| item.is_none()) {
            // Do nothing if there are no empty slots
            // NOTE: neighbours don't change during read lock.
            return;
        }

        // Update first empty slot
        let mut neighbours = self.neighbours.write();
        for slot in neighbours.iter_mut() {
            if slot.is_none() {
                *slot = Some(neighbour.clone());
                break;
            }
        }
    }

    fn remove(&self, bad_neighbour: &Arc<Neighbour>) {
        let mut neighbours = self.neighbours.write();
        // Reset all slots with the specified peer id
        for slot in neighbours.iter_mut() {
            if matches!(slot, Some(n) if n.peer_id() == bad_neighbour.peer_id()) {
                *slot = None;
            }
        }
    }

    fn get(&self) -> Option<Arc<Neighbour>> {
        let neighbours = self.neighbours.read();
        // Move index each time good neighbour is requested
        let index = self.index.fetch_add(1, Ordering::Acquire) % neighbours.len();
        neighbours.get(index).cloned().flatten()
    }
}

type GoodPeerSlot = Option<Arc<Neighbour>>;

const GOOD_PEER_COUNT: usize = 4;

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
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingBlockMaps {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // NOTE: reverse comparison here because `BinaryHeap` is a max-heap
        other.index.cmp(&self.index)
    }
}

struct BlockMapsData {
    neighbour: Option<Arc<Neighbour>>,
    loaded: Option<Arc<BlockMaps>>,
    writer: Option<ArchiveWriter>,
}

impl BlockMapsData {
    fn preload(
        &'_ mut self,
        next_index: u32,
        edge: &Option<BlockMapsEdge>,
    ) -> Result<&'_ Arc<BlockMaps>> {
        if self.loaded.is_none() {
            if let Some(writer) = self.writer.take() {
                let block_maps = writer
                    .parse_block_maps()
                    .context("Failed to load block maps")?;
                block_maps.check(next_index, edge)?;

                self.loaded = Some(block_maps);
            }
        }

        if let Some(block_maps) = &self.loaded {
            Ok(block_maps)
        } else {
            Err(ArchivesStreamError::EmptyBlockMapsData.into())
        }
    }
}

pub struct ReceivedBlockMaps<'a> {
    stream: &'a mut ArchivesStream,
    index: u32,
    neighbour: Option<Arc<Neighbour>>,
    block_maps: Arc<BlockMaps>,
    accepted: bool,
}

impl ReceivedBlockMaps<'_> {
    pub fn accept(mut self, edge: Option<BlockMapsEdge>) {
        self.accepted = true;
        if let Some(highest_mc_id) = self.block_maps.highest_mc_id() {
            self.stream.last_blocks = edge;
            self.stream.next_mc_seq_no = highest_mc_id.seq_no + 1;
        }
    }

    pub fn accept_with_time(self, time: u32, edge: Option<BlockMapsEdge>) {
        self.stream.prefetch_enabled = time + ARCHIVE_EXISTENCE_THRESHOLD <= now();
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
            // Remove peer from good peers
            if let Some(neighbour) = &self.neighbour {
                self.stream.ctx.good_peers.remove(neighbour);
            }

            tracing::info!(target: "sync", index = self.index, "archive not accepted");
            self.stream.start_downloading(self.index);
        }
    }
}

async fn download_archive(
    ctx: &DownloaderContext,
    mc_seq_no: u32,
) -> Option<(ArchiveWriter, Arc<Neighbour>)> {
    tokio::pin!(
        let signal = ctx.cancellation_token.cancelled();
    );

    tracing::info!(target: "sync", mc_seq_no, "downloading archive");

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
                ctx.good_peers.add(&neighbour);
                tracing::info!(
                    target: "sync",
                    mc_seq_no,
                    bytes_len = len,
                    human_len = %bytesize::ByteSize(len as u64),
                    elapsed_ms = start.elapsed().as_millis(),
                    "downloaded archive",
                );
                break Some((writer, neighbour));
            }
            Ok(ArchiveDownloadStatus::NotFound) => {
                if let Some(neighbour) = &good_peer {
                    ctx.good_peers.remove(neighbour);
                }
                tracing::trace!(target: "sync", mc_seq_no, "no archive found");
            }
            Err(e) => {
                if let Some(neighbour) = &good_peer {
                    ctx.good_peers.remove(neighbour);
                }
                tracing::warn!(target: "sync", mc_seq_no, "failed to download archive: {e:?}")
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ArchivesStreamError {
    #[error("Empty block maps data")]
    EmptyBlockMapsData,
}

const ARCHIVE_EXISTENCE_THRESHOLD: u32 = 1800;
