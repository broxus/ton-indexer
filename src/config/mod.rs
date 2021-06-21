use std::convert::{TryFrom, TryInto};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use ton_api::{ton, IntoBoxed};

use crate::utils::*;
use tiny_adnl::utils::AdnlNodeIdFull;

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub adnl: Option<AdnlNodeConfig>,
    pub zero_state: ZeroState,
    pub global_config: DhtGlobalConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            adnl: Some(AdnlNodeConfig::default()),
            zero_state: Default::default(),
            global_config: Default::default(),
        }
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct ZeroState {
    pub workchain: i32,
    pub shard: i64,
    pub seqno: i32,
    #[serde(with = "serde_bytes_base64")]
    pub root_hash: Vec<u8>,
    #[serde(with = "serde_bytes_base64")]
    pub file_hash: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct AdnlNodeConfig {
    pub ip_address: SocketAddrV4,
    pub keys: Vec<AdnlNodeKey>,
}

impl Default for AdnlNodeConfig {
    fn default() -> Self {
        let runtime = tokio::runtime::Handle::current();

        let ip = std::thread::spawn(move || {
            runtime
                .block_on(
                    external_ip::ConsensusBuilder::new()
                        .add_sources(external_ip::get_http_sources::<external_ip::Sources>())
                        .build()
                        .get_consensus(),
                )
                .unwrap()
        })
        .join()
        .unwrap();

        let ip = match ip {
            IpAddr::V4(ip) => ip,
            IpAddr::V6(_) => Ipv4Addr::LOCALHOST,
        };

        Self {
            ip_address: SocketAddrV4::new(ip, DEFAULT_PORT),
            keys: vec![],
        }
    }
}

impl TryFrom<AdnlNodeConfig> for tiny_adnl::AdnlNodeConfig {
    type Error = anyhow::Error;

    fn try_from(value: AdnlNodeConfig) -> Result<Self, Self::Error> {
        tiny_adnl::AdnlNodeConfig::from_ip_address_and_keys(
            value.ip_address.into(),
            value
                .keys
                .into_iter()
                .map(|item| (item.key, item.tag))
                .collect::<Vec<_>>(),
        )
    }
}

#[derive(Serialize, Deserialize)]
pub struct AdnlNodeKey {
    pub tag: usize,
    #[serde(with = "serde_private_key")]
    pub key: ed25519_dalek::SecretKey,
}

pub mod serde_private_key {
    use serde::de::Error;
    use serde::Deserialize;

    pub fn serialize<S>(data: &ed25519_dalek::SecretKey, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&hex::encode(data.as_bytes()))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<ed25519_dalek::SecretKey, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = String::deserialize(deserializer)?;
        let bytes = hex::decode(&data).map_err(|_| D::Error::custom("Invalid SecretKey"))?;
        ed25519_dalek::SecretKey::from_bytes(&bytes)
            .map_err(|_| D::Error::custom("Invalid PublicKey"))
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DhtGlobalConfig {
    #[serde(alias = "@type")]
    type_dht: Option<String>,
    k: Option<i32>,
    a: Option<i32>,
    static_nodes: DhtNodes,
}

impl DhtGlobalConfig {
    pub fn get_dht_nodes_configs(&self) -> Result<Vec<ton::dht::node::Node>> {
        let mut result = Vec::new();
        for dht_node in self.static_nodes.nodes.iter() {
            let key = dht_node.id.convert_key()?;
            let mut addrs = Vec::new();
            for addr in dht_node.addr_list.addrs.iter() {
                let ip = if let Some(ip) = addr.ip {
                    ip
                } else {
                    continue;
                };
                let port = if let Some(port) = addr.port {
                    port
                } else {
                    continue;
                };
                let addr = ton::adnl::address::address::Udp {
                    ip: ip as i32,
                    port: port as i32,
                }
                .into_boxed();
                addrs.push(addr);
            }
            let version = if let Some(version) = dht_node.addr_list.version {
                version
            } else {
                continue;
            };
            let reinit_date = if let Some(reinit_date) = dht_node.addr_list.reinit_date {
                reinit_date
            } else {
                continue;
            };
            let priority = if let Some(priority) = dht_node.addr_list.priority {
                priority
            } else {
                continue;
            };
            let expire_at = if let Some(expire_at) = dht_node.addr_list.expire_at {
                expire_at
            } else {
                continue;
            };
            let addr_list = ton::adnl::addresslist::AddressList {
                addrs: addrs.into(),
                version,
                reinit_date,
                priority,
                expire_at,
            };
            let version = if let Some(version) = dht_node.version {
                version
            } else {
                continue;
            };
            let signature = if let Some(signature) = &dht_node.signature {
                signature
            } else {
                continue;
            };
            let node = ton::dht::node::Node {
                id: key.as_tl().into_boxed(),
                addr_list,
                version,
                signature: ton::bytes(base64::decode(signature)?),
            };
            result.push(node);
        }

        Ok(result)
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct DhtNodes {
    #[serde(alias = "@type")]
    type_dht: Option<String>,
    nodes: Vec<DhtNode>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct DhtNode {
    #[serde(alias = "@type")]
    type_node: Option<String>,
    id: IdDhtNode,
    addr_list: AddressList,
    version: Option<i32>,
    signature: Option<String>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct IdDhtNode {
    #[serde(alias = "@type")]
    type_node: Option<String>,
    key: Option<String>,
}

pub const PUB_ED25519: &str = "pub.ed25519";

impl IdDhtNode {
    pub fn convert_key(&self) -> Result<AdnlNodeIdFull> {
        let type_id = self
            .type_node
            .as_ref()
            .ok_or_else(|| anyhow!("Type_node is not set!"))?;

        if type_id != PUB_ED25519 {
            return Err(anyhow!("unknown type_node!"));
        };

        let key = if let Some(key) = &self.key {
            base64::decode(key)?
        } else {
            return Err(anyhow!("No public key!"));
        };
        Ok(ed25519_dalek::PublicKey::from_bytes(&key[..32])?.into())
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct AddressList {
    #[serde(alias = "@type")]
    type_node: Option<String>,
    addrs: Vec<Address>,
    version: Option<i32>,
    reinit_date: Option<i32>,
    priority: Option<i32>,
    expire_at: Option<i32>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Address {
    #[serde(alias = "@type")]
    type_node: Option<String>,
    ip: Option<i64>,
    port: Option<u16>,
}

const DEFAULT_PORT: u16 = 30303;
