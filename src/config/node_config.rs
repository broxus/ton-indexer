use std::convert::TryFrom;
use std::net::SocketAddrV4;
use std::path::PathBuf;

use anyhow::Result;
use nekoton_utils::*;
use serde::{Deserialize, Serialize};
use sysinfo::SystemExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub ip_address: SocketAddrV4,
    pub keys: Vec<AdnlNodeKey>,
    pub rocks_db_path: PathBuf,
    pub file_db_path: PathBuf,

    #[serde(default)]
    pub shard_state_cache_enabled: bool,
    #[serde(default)]
    pub old_blocks_policy: OldBlocksPolicy,
    #[serde(default = "default_memtable_size")]
    pub max_db_memory_usage: usize,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "background_sync_before")]
pub enum OldBlocksPolicy {
    Ignore,
    WaitSyncBeforeSeqno(u32),
    ParallelSyncBeforeSeqno(u32),
}

impl Default for OldBlocksPolicy {
    fn default() -> Self {
        Self::Ignore
    }
}

//third of all memory as suggested in docs
fn default_memtable_size() -> usize {
    let sys = sysinfo::System::new_all();
    let total = sys.total_memory() * 1024;
    (total / 3) as usize
}

impl TryFrom<NodeConfig> for tiny_adnl::AdnlNodeConfig {
    type Error = anyhow::Error;

    fn try_from(value: NodeConfig) -> Result<Self, Self::Error> {
        tiny_adnl::AdnlNodeConfig::from_ip_address_and_keys(
            tiny_adnl::utils::AdnlAddressUdp::new(value.ip_address),
            value
                .keys
                .into_iter()
                .map(|item| Ok((ed25519_dalek::SecretKey::from_bytes(&item.key)?, item.tag)))
                .collect::<Result<Vec<_>>>()?,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdnlNodeKey {
    pub tag: usize,
    #[serde(with = "serde_hex_array")]
    pub key: [u8; 32],
}
