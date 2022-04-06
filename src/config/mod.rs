use std::net::SocketAddrV4;
use std::path::PathBuf;

use rand::Rng;
use serde::{Deserialize, Serialize};
use sysinfo::SystemExt;
use tiny_adnl::*;

pub use self::node_keys::*;

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
    pub archives_enabled: bool,

    pub max_db_memory_usage: usize,

    pub sync_options: SyncOptions,

    pub adnl_options: AdnlNodeOptions,
    pub rldp_options: RldpNodeOptions,
    pub dht_options: DhtNodeOptions,
    pub neighbours_options: NeighboursOptions,
    pub overlay_shard_options: OverlayShardOptions,
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
            archives_enabled: false,
            max_db_memory_usage: default_max_db_memory_usage(),
            sync_options: Default::default(),
            adnl_options: Default::default(),
            rldp_options: Default::default(),
            dht_options: Default::default(),
            neighbours_options: Default::default(),
            overlay_shard_options: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SyncOptions {
    /// Whether to sync very old blocks
    pub old_blocks_policy: OldBlocksPolicy,
    /// Default: 16
    pub parallel_archive_downloads: usize,
    /// Default: 1073741824 (1 GB)
    pub save_to_disk_threshold: usize,
}

impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            old_blocks_policy: Default::default(),
            parallel_archive_downloads: 16,
            save_to_disk_threshold: 1024 * 1024 * 1024,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
            offset_sec: rand::thread_rng().gen_range(0..900),
            interval_sec: 900,
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

    /// Max element count. Default: `100000`
    pub capacity: usize,
}

impl Default for ShardStateCacheOptions {
    fn default() -> Self {
        Self {
            ttl_sec: 120,
            capacity: 100_000,
        }
    }
}

/// Third of all memory as suggested in docs
pub fn default_max_db_memory_usage() -> usize {
    let sys = sysinfo::System::new_all();
    let total = sys.total_memory() * 1024;
    (total / 3) as usize
}
