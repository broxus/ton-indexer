use std::convert::{TryFrom, TryInto};
use std::net::SocketAddr;

use adnl::common::KeyOption;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use ton_api::{ton, IntoBoxed};

use crate::utils::*;

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    pub adnl: AdnlNodeConfig,
    pub zero_state: ZeroState,
    pub global_config: DhtGlobalConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            adnl: Default::default(),
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

#[derive(Clone, Serialize, Deserialize)]
pub struct AdnlNodeConfig {
    pub ip_address: SocketAddr,
    pub keys: Vec<AdnlNodeKey>,
    #[serde(default)]
    pub throughput: Option<u32>,
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

        Self {
            ip_address: SocketAddr::new(ip, DEFAULT_PORT),
            keys: vec![],
            throughput: None,
        }
    }
}

impl TryFrom<AdnlNodeConfig> for adnl::node::AdnlNodeConfig {
    type Error = anyhow::Error;

    fn try_from(value: AdnlNodeConfig) -> Result<Self, Self::Error> {
        let mut config = adnl::node::AdnlNodeConfig::from_ip_address_and_keys(
            value.ip_address,
            value
                .keys
                .into_iter()
                .map(|item| {
                    Ok((
                        adnl::common::KeyOption::from_private_key(&item.key)?,
                        item.tag,
                    ))
                })
                .collect::<ton_types::Result<Vec<_>>>()
                .map_err(|e| anyhow!("Failed to parse ADNL key: {:?}", e))?,
        )
        .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        config.set_throughput(value.throughput);
        Ok(config)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AdnlNodeKey {
    pub tag: usize,
    pub key: adnl::common::KeyOptionJson,
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
                id: ton::pub_::publickey::Ed25519 {
                    key: ton::int256(*key.pub_key().convert()?),
                }
                .into_boxed(),
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
    pub fn convert_key(&self) -> Result<KeyOption> {
        let type_id = self
            .type_node
            .as_ref()
            .ok_or_else(|| anyhow!("Type_node is not set!"))?;

        let type_id = if type_id.eq(PUB_ED25519) {
            KeyOption::KEY_ED25519
        } else {
            return Err(anyhow!("unknown type_node!"));
        };

        let key = if let Some(key) = &self.key {
            base64::decode(key)?
        } else {
            return Err(anyhow!("No public key!"));
        };

        let pub_key = key[..32].try_into()?;
        let ret = KeyOption::from_type_and_public_key(type_id, &pub_key);
        Ok(ret)
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
