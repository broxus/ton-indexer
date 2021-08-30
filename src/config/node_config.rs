use std::net::SocketAddrV4;
use std::path::PathBuf;

use anyhow::Result;
use nekoton_utils::*;
use rand::Rng;
use serde::{Deserialize, Serialize};
use sysinfo::SystemExt;
use tiny_adnl::utils::AdnlAddressUdp;
use tiny_adnl::AdnlNodeConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub ip_address: SocketAddrV4,
    #[serde(default)]
    pub adnl_keys: NodeKeys,

    #[serde(default = "default_rocksdb_path")]
    pub rocks_db_path: PathBuf,
    #[serde(default = "default_file_path")]
    pub file_db_path: PathBuf,

    #[serde(default)]
    pub old_blocks_policy: OldBlocksPolicy,
    #[serde(default)]
    pub shard_state_cache_enabled: bool,
    #[serde(default = "default_max_db_memory_usage")]
    pub max_db_memory_usage: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            ip_address: SocketAddrV4::new(std::net::Ipv4Addr::LOCALHOST, 30303),
            adnl_keys: Default::default(),
            rocks_db_path: default_rocksdb_path(),
            file_db_path: default_file_path(),
            shard_state_cache_enabled: false,
            old_blocks_policy: Default::default(),
            max_db_memory_usage: default_max_db_memory_usage(),
        }
    }
}

impl NodeConfig {
    pub fn build_adnl_node_config(&self) -> Result<AdnlNodeConfig> {
        AdnlNodeConfig::from_ip_address_and_keys(
            AdnlAddressUdp::new(self.ip_address),
            self.adnl_keys.to_vec(),
        )
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

    pub fn to_vec(&self) -> Vec<(ed25519_dalek::SecretKey, usize)> {
        vec![
            (make_key(&self.dht_key), 1),
            (make_key(&self.overlay_key), 2),
        ]
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

fn default_rocksdb_path() -> PathBuf {
    "db/rocksdb".into()
}

fn default_file_path() -> PathBuf {
    "db/file".into()
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
