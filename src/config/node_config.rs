use std::convert::TryFrom;
use std::net::SocketAddrV4;
use std::path::PathBuf;

use anyhow::Result;
use nekoton_utils::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub ip_address: SocketAddrV4,
    pub keys: Vec<AdnlNodeKey>,
    pub rocks_db_path: PathBuf,
    pub file_db_path: PathBuf,

    #[serde(default)]
    pub shard_state_cache_enabled: bool,

    #[serde(default = "initial_sync_before")]
    pub initial_sync_before: i32,
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

fn initial_sync_before() -> i32 {
    300
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdnlNodeKey {
    tag: usize,
    #[serde(with = "serde_hex_array")]
    key: [u8; 32],
}
