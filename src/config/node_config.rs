use std::net::SocketAddrV4;
use std::path::PathBuf;

use anyhow::Result;
use nekoton_utils::*;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sysinfo::SystemExt;
use tiny_adnl::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeConfig {
    pub ip_address: SocketAddrV4,
    pub adnl_keys: NodeKeys,

    pub rocks_db_path: PathBuf,
    pub file_db_path: PathBuf,

    pub state_gc_offset_sec: u64,
    pub state_gc_interval_sec: u64,

    pub old_blocks_policy: OldBlocksPolicy,
    pub shard_state_cache_enabled: bool,
    pub max_db_memory_usage: usize,

    pub parallel_archive_downloads: u32,

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
            state_gc_offset_sec: rand::thread_rng().gen_range(0..900),
            state_gc_interval_sec: 900,
            old_blocks_policy: Default::default(),
            shard_state_cache_enabled: false,
            max_db_memory_usage: default_max_db_memory_usage(),
            parallel_archive_downloads: 16,
            adnl_options: Default::default(),
            rldp_options: Default::default(),
            dht_options: Default::default(),
            neighbours_options: Default::default(),
            overlay_shard_options: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeKeys {
    #[serde(with = "serde_hex_array")]
    pub dht_key: [u8; 32],
    #[serde(with = "serde_hex_array")]
    pub overlay_key: [u8; 32],
}

impl Default for NodeKeys {
    fn default() -> Self {
        Self::generate()
    }
}

impl NodeKeys {
    pub fn generate() -> Self {
        let mut rng = rand::thread_rng();
        Self {
            overlay_key: rng.gen(),
            dht_key: rng.gen(),
        }
    }

    pub fn build_keystore(&self) -> Result<AdnlKeystore> {
        AdnlKeystore::from_tagged_keys(vec![
            (make_key(&self.dht_key), 1),
            (make_key(&self.overlay_key), 2),
        ])
    }
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum OldBlocksPolicy {
    Ignore,
    Sync { from_seqno: u32 },
}

impl Default for OldBlocksPolicy {
    fn default() -> Self {
        Self::Ignore
    }
}

/// Third of all memory as suggested in docs
pub fn default_max_db_memory_usage() -> usize {
    let sys = sysinfo::System::new_all();
    let total = sys.total_memory() * 1024;
    (total / 3) as usize
}

fn make_key(key: &[u8; 32]) -> ed25519_dalek::SecretKey {
    ed25519_dalek::SecretKey::from_bytes(key).trust_me()
}
