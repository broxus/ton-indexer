use std::convert::{TryFrom, TryInto};
use std::path::Path;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Deserializer};
use ton_api::{ton, IntoBoxed};

#[derive(Debug, Clone)]
pub struct GlobalConfig {
    pub dht_nodes: Vec<ton::dht::node::Node>,
    pub zero_state: ton_block::BlockIdExt,
}

impl GlobalConfig {
    pub fn load<P>(path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
}

impl<'de> Deserialize<'de> for GlobalConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        GlobalConfigJson::deserialize(deserializer)?
            .try_into()
            .map_err(D::Error::custom)
    }
}

impl TryFrom<GlobalConfigJson> for GlobalConfig {
    type Error = anyhow::Error;

    fn try_from(value: GlobalConfigJson) -> Result<Self, Self::Error> {
        require_type(value.ty, "config.global")?;

        Ok(Self {
            dht_nodes: value.dht.try_into()?,
            zero_state: value.validator.try_into()?,
        })
    }
}

impl TryFrom<DhtJson> for Vec<ton::dht::node::Node> {
    type Error = anyhow::Error;

    fn try_from(value: DhtJson) -> Result<Self, Self::Error> {
        require_type(value.ty, "dht.config.global")?;
        require_type(value.static_nodes.ty, "dht.nodes")?;
        value
            .static_nodes
            .nodes
            .into_iter()
            .map(TryFrom::try_from)
            .collect()
    }
}

impl TryFrom<DhtNodeJson> for ton::dht::node::Node {
    type Error = anyhow::Error;

    fn try_from(value: DhtNodeJson) -> Result<Self, Self::Error> {
        require_type(value.ty, "dht.node")?;
        require_type(value.id.ty, "pub.ed25519")?;

        Ok(Self {
            id: ton::pub_::publickey::Ed25519 {
                key: ton::int256(value.id.key),
            }
            .into_boxed(),
            addr_list: value.addr_list.try_into()?,
            version: value.version,
            signature: ton::bytes(value.signature.to_vec()),
        })
    }
}

impl TryFrom<AddressListJson> for ton::adnl::addresslist::AddressList {
    type Error = anyhow::Error;

    fn try_from(value: AddressListJson) -> Result<Self, Self::Error> {
        require_type(value.ty, "adnl.addressList")?;

        Ok(Self {
            addrs: value
                .addrs
                .into_iter()
                .map(TryFrom::try_from)
                .collect::<Result<Vec<ton::adnl::Address>>>()?
                .into(),
            version: value.version,
            reinit_date: value.reinit_date,
            priority: value.priority,
            expire_at: value.expire_at,
        })
    }
}

impl TryFrom<AddressJson> for ton::adnl::Address {
    type Error = anyhow::Error;

    fn try_from(value: AddressJson) -> Result<Self, Self::Error> {
        require_type(value.ty, "adnl.address.udp")?;

        Ok(ton::adnl::address::address::Udp {
            ip: value.ip,
            port: value.port,
        }
        .into_boxed())
    }
}

impl TryFrom<ValidatorJson> for ton_block::BlockIdExt {
    type Error = anyhow::Error;

    fn try_from(value: ValidatorJson) -> Result<Self, Self::Error> {
        require_type(value.ty, "validator.config.global")?;
        Ok(ton_block::BlockIdExt {
            shard_id: ton_block::ShardIdent::with_tagged_prefix(
                value.zero_state.workchain,
                value.zero_state.shard as u64,
            )?,
            seq_no: value.zero_state.seqno as u32,
            root_hash: value.zero_state.root_hash.into(),
            file_hash: value.zero_state.file_hash.into(),
        })
    }
}

fn require_type(ty: String, required: &'static str) -> Result<()> {
    if ty == required {
        Ok(())
    } else {
        Err(anyhow!("Invalid type {}, expected {}", ty, required))
    }
}

#[derive(Deserialize)]
struct GlobalConfigJson {
    #[serde(rename = "@type")]
    ty: String,
    dht: DhtJson,
    validator: ValidatorJson,
}

#[derive(Deserialize)]
struct DhtJson {
    #[serde(rename = "@type")]
    ty: String,
    static_nodes: StaticNodesJson,
}

#[derive(Deserialize)]
struct StaticNodesJson {
    #[serde(rename = "@type")]
    ty: String,
    nodes: Vec<DhtNodeJson>,
}

#[derive(Deserialize)]
struct DhtNodeJson {
    #[serde(rename = "@type")]
    ty: String,
    id: IdJson,
    addr_list: AddressListJson,
    version: i32,
    #[serde(deserialize_with = "deserialize_base64_array")]
    signature: [u8; 64],
}

#[derive(Deserialize)]
struct IdJson {
    #[serde(rename = "@type")]
    ty: String,
    #[serde(deserialize_with = "deserialize_base64_array")]
    key: [u8; 32],
}

#[derive(Deserialize)]
struct AddressListJson {
    #[serde(rename = "@type")]
    ty: String,
    addrs: Vec<AddressJson>,
    version: i32,
    reinit_date: i32,
    priority: i32,
    expire_at: i32,
}

#[derive(Deserialize)]
struct AddressJson {
    #[serde(rename = "@type")]
    ty: String,
    ip: i32,
    port: i32,
}

#[derive(Deserialize)]
struct ValidatorJson {
    #[serde(rename = "@type")]
    ty: String,
    zero_state: ZeroStateJson,
}

#[derive(Deserialize)]
struct ZeroStateJson {
    workchain: i32,
    shard: i64,
    seqno: i32,
    #[serde(deserialize_with = "deserialize_base64_array")]
    root_hash: [u8; 32],
    #[serde(deserialize_with = "deserialize_base64_array")]
    file_hash: [u8; 32],
}

fn deserialize_base64_array<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;
    let data = String::deserialize(deserializer)?;
    let data = base64::decode(data).map_err(D::Error::custom)?;
    data.try_into()
        .map_err(|_| D::Error::custom(format!("Invalid array length, expected: {}", N)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialization() {
        const CONFIG: &str = r#"
{
    "@type": "config.global",
    "dht": {
        "@type": "dht.config.global",
        "k": 6,
        "a": 3,
        "static_nodes": {
            "@type": "dht.nodes",
            "nodes": [
                {
                    "@type": "dht.node",
                    "id": {
                        "@type": "pub.ed25519",
                        "key": "3fTNTotxKlHqgAHVYQkEItaClTBzcEbACHanxzqZyOg="
                    },
                    "addr_list": {
                        "@type": "adnl.addressList",
                        "addrs": [
                            {
                                "@type": "adnl.address.udp",
                                "ip": 1959450108,
                                "port": 30310
                            }
                        ],
                        "version": 1593767935,
                        "reinit_date": 1593767935,
                        "priority": 0,
                        "expire_at": 0
                    },
                    "version": 1596633674,
                    "signature": "199dp/+/u8BY+E7zkqAqeuMjbtEx/1hfS6jOg7zRoupIVHPLYvUFKqqlaeOySGwnbiBjzVl/+ANWVt5TjoikCg=="
                },
                {
                    "@type": "dht.node",
                    "id": {
                        "@type": "pub.ed25519",
                        "key": "dscXQ6eEPjh5hEFhnEtu0qmczeiBgmft1zAnlQlcGKc="
                    },
                    "addr_list": {
                        "@type": "adnl.addressList",
                        "addrs": [
                            {
                                "@type": "adnl.address.udp",
                                "ip": 65828309,
                                "port": 30310
                            }
                        ],
                        "version": 1596055838,
                        "reinit_date": 1596055838,
                        "priority": 0,
                        "expire_at": 0
                    },
                    "version": 1596633658,
                    "signature": "EvgprXZ6E5LElj2SxvdHUVm121ZB7+ZHM7ZU1DSpIB/u9TX92JlmqYVtMFpoE8o7ciA99JimKwW/CvPbfvp7DQ=="
                }
            ]
        }
    },
    "validator": {
        "@type": "validator.config.global",
        "zero_state": {
            "workchain": -1,
            "shard": -9223372036854775808,
            "seqno": 0,
            "root_hash": "WP/KGheNr/cF3lQhblQzyb0ufYUAcNM004mXhHq56EU=",
            "file_hash": "0nC4eylStbp9qnCq8KjDYb789NjS25L5ZA1UQwcIOOQ="
        }
    }
}"#;
        let config = serde_json::from_str::<GlobalConfig>(CONFIG).unwrap();
        println!("{:?}", config);
    }
}
