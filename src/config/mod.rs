use bytesize::ByteSize;
use std::net::SocketAddrV4;
use std::num::{NonZeroU8, NonZeroUsize};
use std::path::PathBuf;

use everscale_network::{adnl, dht, overlay, rldp};
use rand::Rng;
use serde::{Deserialize, Serialize};

pub use self::node_keys::*;
use crate::network::NeighboursOptions;

mod node_keys;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    pub ip_address: SocketAddrV4,
    pub adnl_keys: NodeKeys,

    pub rocks_db_path: PathBuf,
    pub file_db_path: PathBuf,

    pub state_gc_options: Option<StateGcOptions>,
    pub blocks_gc_options: Option<BlocksGcOptions>,
    pub shard_state_cache_options: Option<ShardStateCacheOptions>,

    pub db_options: DbOptions,

    pub archive_options: Option<ArchiveOptions>,
    pub sync_options: SyncOptions,
    pub persistent_state_options: PersistentStateOptions,

    pub adnl_options: adnl::NodeOptions,
    pub rldp_options: rldp::NodeOptions,
    pub dht_options: dht::NodeOptions,
    pub overlay_shard_options: overlay::OverlayOptions,
    pub neighbours_options: NeighboursOptions,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            ip_address: SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 30303),
            adnl_keys: Default::default(),
            rocks_db_path: "db/rocksdb".into(),
            file_db_path: "db/file".into(),
            state_gc_options: None,
            blocks_gc_options: None,
            shard_state_cache_options: Some(Default::default()),
            archive_options: Some(Default::default()),
            db_options: Default::default(),
            sync_options: Default::default(),
            persistent_state_options: Default::default(),
            adnl_options: Default::default(),
            rldp_options: Default::default(),
            dht_options: Default::default(),
            overlay_shard_options: Default::default(),
            neighbours_options: Default::default(),
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DbOptions {
    pub metrics_update_interval_sec: Option<u64>,
    pub rocksdb_lru_capacity: ByteSize,
    pub cells_cache_size: ByteSize,
    #[serde(default = "default_thread_pool_size")]
    pub low_thread_pool_size: NonZeroUsize,
    #[serde(default = "default_thread_pool_size")]
    pub high_thread_pool_size: NonZeroUsize,
    #[serde(default = "default_thread_pool_size")]
    pub max_subcompactions: NonZeroUsize,
}

fn default_thread_pool_size() -> NonZeroUsize {
    let one = NonZeroUsize::new(1).unwrap();
    let tc = std::thread::available_parallelism().unwrap_or(one);
    NonZeroUsize::new(std::cmp::max(tc.get() / 2, 1)).unwrap()
}

impl Default for DbOptions {
    fn default() -> Self {
        // Fetch the currently available memory in bytes
        let available = {
            let mut sys = sysinfo::System::new();
            sys.refresh_memory();
            sys.available_memory()
        };

        // Estimated memory usage of components other than cache:
        // - 2 GiBs for write buffers(4 if we are out of luck and all memtables are being flushed at the same time)
        // - 2 GiBs for indexer logic
        // - 10 bits per cell for bloom filter. Realistic case is 100M cells, so 0.25 GiBs
        // - 1/3 of all available memory is reserved for kernel buffers
        const WRITE_BUFFERS: ByteSize = ByteSize::gib(2);
        const INDEXER_LOGIC: ByteSize = ByteSize::gib(2);
        const BLOOM_FILTER: ByteSize = ByteSize::mib(256);
        let estimated_memory_usage = WRITE_BUFFERS + INDEXER_LOGIC + BLOOM_FILTER + available / 3;

        // Reduce the available memory by the fixed offset
        let available = available
            .checked_sub(estimated_memory_usage.as_u64())
            .unwrap_or_else(|| {
                tracing::error!(
                    "Not enough memory for cache, using 1/4 of all available memory. \
                    Tweak `db_options` in config to improve performance."
                );
                available / 4
            });

        // We will use 3/4 of available memory for the cells cache (at most 4 GB).
        let cells_cache_size = std::cmp::min(ByteSize(available * 4 / 3), ByteSize::gib(4));

        // The reset of the memory is used for LRU cache (at least 128 MB)
        let rocksdb_lru_capacity = std::cmp::max(
            ByteSize(available.saturating_sub(cells_cache_size.as_u64())),
            ByteSize::mib(128),
        );

        Self {
            metrics_update_interval_sec: Some(10),
            rocksdb_lru_capacity,
            cells_cache_size,
            low_thread_pool_size: default_thread_pool_size(),
            high_thread_pool_size: default_thread_pool_size(),
            max_subcompactions: default_thread_pool_size(),
        }
    }
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveOptions {
    pub gc_interval: ArchivesGcInterval,
    #[cfg(feature = "archive-uploader")]
    pub uploader_options: Option<archive_uploader::ArchiveUploaderConfig>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "type", rename_all = "snake_case")]
pub enum ArchivesGcInterval {
    /// Do not perform archives GC
    Manual,
    /// Archives GC triggers on each persistent state
    PersistentStates {
        /// Remove archives after this interval after the new persistent state
        offset_sec: u64,
    },
}

impl Default for ArchivesGcInterval {
    fn default() -> Self {
        Self::PersistentStates { offset_sec: 300 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SyncOptions {
    /// Whether to sync very old blocks
    pub old_blocks_policy: OldBlocksPolicy,
    /// Default: 16
    pub parallel_archive_downloads: usize,
    /// Default: 1073741824 (1 GB)
    pub save_to_disk_threshold: usize,
    /// Default: 32
    pub max_block_applier_depth: u32,
    /// Ignore archives. Default: false.
    pub force_use_get_next_block: bool,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            old_blocks_policy: Default::default(),
            parallel_archive_downloads: 16,
            save_to_disk_threshold: 1024 * 1024 * 1024,
            max_block_applier_depth: 32,
            force_use_get_next_block: false,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum OldBlocksPolicy {
    Ignore,
    Sync { from_seqno: u32 },
}

impl Default for OldBlocksPolicy {
    fn default() -> Self {
        Self::Ignore
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StateGcOptions {
    /// Default: rand[0,900)
    pub offset_sec: u64,
    /// Default: 900
    pub interval_sec: u64,
}

impl Default for StateGcOptions {
    fn default() -> Self {
        Self {
            offset_sec: rand::thread_rng().gen_range(0..60),
            interval_sec: 60,
        }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlocksGcOptions {
    /// Blocks GC type
    /// - `before_previous_key_block` - on each new key block delete all blocks before the previous one
    /// - `before_previous_persistent_state` - on each new key block delete all blocks before the
    ///   previous key block with persistent state
    pub kind: BlocksGcKind,

    /// Whether to enable blocks GC during sync. Default: true
    pub enable_for_sync: bool,

    /// Max `WriteBatch` entries before apply
    pub max_blocks_per_batch: Option<usize>,
}

impl Default for BlocksGcOptions {
    fn default() -> Self {
        Self {
            kind: BlocksGcKind::BeforePreviousPersistentState,
            enable_for_sync: true,
            max_blocks_per_batch: Some(100_000),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlocksGcKind {
    BeforePreviousKeyBlock,
    BeforePreviousPersistentState,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ShardStateCacheOptions {
    /// LRU cache item duration. Default: `120`
    pub ttl_sec: u64,
}

impl Default for ShardStateCacheOptions {
    fn default() -> Self {
        Self { ttl_sec: 120 }
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PersistentStateOptions {
    pub prepare_persistent_states: bool,
    pub persistent_state_parallelism: NonZeroU8,
    pub remove_old_states: bool,
}

impl Default for PersistentStateOptions {
    fn default() -> Self {
        Self {
            prepare_persistent_states: false,
            persistent_state_parallelism: NonZeroU8::MIN,
            remove_old_states: true,
        }
    }
}
