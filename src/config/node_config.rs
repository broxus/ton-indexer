use std::convert::TryFrom;
use std::net::{IpAddr, SocketAddrV4};
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::utils::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    ip_address: SocketAddrV4,
    keys: Vec<AdnlNodeKey>,
    sled_db_path: PathBuf,
    file_db_path: PathBuf,
}

impl NodeConfig {
    pub async fn generate() -> Result<Self> {
        let ip = external_ip::ConsensusBuilder::new()
            .add_sources(external_ip::get_http_sources::<external_ip::Sources>())
            .build()
            .get_consensus()
            .await;

        let ip_address = match ip {
            Some(IpAddr::V4(ip)) => SocketAddrV4::new(ip, DEFAULT_PORT),
            Some(_) => return Err(NodeConfigError::NotSupported.into()),
            None => return Err(NodeConfigError::ExternalIpNotFound.into()),
        };

        Ok(Self {
            ip_address,
            keys: Vec::new(),
            sled_db_path: PathBuf::new().join("db/sled"),
            file_db_path: PathBuf::new().join("db/file"),
        })
    }

    pub fn sled_db_path(&self) -> &PathBuf {
        &self.sled_db_path
    }

    pub fn file_db_path(&self) -> &PathBuf {
        &self.file_db_path
    }
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
struct AdnlNodeKey {
    tag: usize,
    #[serde(with = "serde_hex_array")]
    key: [u8; 32],
}

const DEFAULT_PORT: u16 = 30303;

#[derive(thiserror::Error, Debug)]
enum NodeConfigError {
    #[error("IPv6 not yet supported")]
    NotSupported,
    #[error("Failed to determine external IP")]
    ExternalIpNotFound,
}
