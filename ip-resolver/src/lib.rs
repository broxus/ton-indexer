use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

pub mod proto;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PublicIp {
    Explicit(Ipv4Addr),
    Public,
    BehindNat(ResolverOptions),
}

impl PublicIp {
    pub async fn resolve(&self, port: u16) -> Result<SocketAddrV4, ResolverError> {
        match self {
            Self::Explicit(ip) => Ok(SocketAddrV4::new(*ip, port)),
            Self::Public => {
                let ip = public_ip::addr_v4()
                    .await
                    .ok_or(ResolverError::PublicIpNotFound)?;
                Ok(SocketAddrV4::new(ip, port))
            }
            Self::BehindNat(options) => resolve(options, port).await,
        }
    }
}

impl Serialize for PublicIp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        match self {
            Self::Explicit(ip) => serializer.serialize_some(&ip.to_string()),
            Self::Public => serializer.serialize_none(),
            Self::BehindNat(options) => serializer.serialize_some(options),
        }
    }
}

impl<'de> Deserialize<'de> for PublicIp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Data {
            Explicit(String),
            BehindNat(ResolverOptions),
        }

        let data = Option::<Data>::deserialize(deserializer)?;
        match data {
            Some(Data::Explicit(ip)) => ip.parse().map(Self::Explicit).map_err(Error::custom),
            Some(Data::BehindNat(options)) => Ok(Self::BehindNat(options)),
            None => Ok(Self::Public),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResolverOptions {
    pub resolver_addr: SocketAddrV4,
    #[serde(default = "default_wave_len")]
    pub wave_len: usize,
    #[serde(default = "default_wave_interval_ms")]
    pub wave_interval_ms: u64,
    #[serde(default = "default_wave_count")]
    pub wave_count: usize,
}

fn default_wave_len() -> usize {
    10
}

fn default_wave_interval_ms() -> u64 {
    100
}

fn default_wave_count() -> usize {
    3
}

async fn resolve(options: &ResolverOptions, port: u16) -> Result<SocketAddrV4, ResolverError> {
    let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port))
        .await
        .map(Arc::new)
        .map_err(ResolverError::PortAlreadyInUse)?;

    socket
        .connect(options.resolver_addr)
        .await
        .map_err(ResolverError::InvalidResolverAddr)?;

    let interval = Duration::from_millis(options.wave_interval_ms);
    let req = proto::ResolutionRequest::new();

    for _ in 0..options.wave_count {
        let receiver = socket.clone();
        let response = tokio::spawn(async move {
            let mut response = [0u8; 128];

            loop {
                match receiver.recv(response.as_mut_slice()).await {
                    Ok(len) if len == proto::ResolutionResponse::SIZE => {
                        if let Some(res) = proto::ResolutionResponse::read_from(&response) {
                            if res.session_id == req.session_id
                                && res.reinit_date == req.reinit_date
                            {
                                return res;
                            }
                        }
                    }
                    _ => continue,
                }
            }
        });

        let buffer = req.as_bytes();
        for _ in 0..options.wave_len {
            socket
                .send(&buffer)
                .await
                .map_err(ResolverError::RequestError)?;
        }

        tokio::select! {
            res = response => if let Ok(res) = res {
                return Ok(SocketAddrV4::new(res.ip.into(), res.port));
            },
            _ = tokio::time::sleep(interval) => {},
        }
    }

    Err(ResolverError::ResolutionTimeout)
}

#[derive(thiserror::Error, Debug)]
pub enum ResolverError {
    #[error("Public IP not found")]
    PublicIpNotFound,
    #[error("Port already in use")]
    PortAlreadyInUse(#[source] std::io::Error),
    #[error("Invalid resolver socket address")]
    InvalidResolverAddr(#[source] std::io::Error),
    #[error("Failed to send request")]
    RequestError(#[source] std::io::Error),
    #[error("Resolution response timeout")]
    ResolutionTimeout,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correct_serialization() {
        let test = PublicIp::Explicit(Ipv4Addr::LOCALHOST);
        let test_str = serde_json::to_string(&test).unwrap();
        assert_eq!(test_str, "\"127.0.0.1\"");
        assert_eq!(test, serde_json::from_str(&test_str).unwrap());

        let test = PublicIp::Public;
        let test_str = serde_json::to_string(&test).unwrap();
        assert_eq!(test_str, "null");
        assert_eq!(test, serde_json::from_str(&test_str).unwrap());

        let test = PublicIp::BehindNat(ResolverOptions {
            resolver_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 53),
            wave_len: 10,
            wave_interval_ms: 100,
            wave_count: 10,
        });
        let test_str = serde_json::to_string(&test).unwrap();
        assert_eq!(
            test_str,
            r#"{"resolver_addr":"127.0.0.1:53","wave_len":10,"wave_interval_ms":100,"wave_count":10}"#
        );
        assert_eq!(test, serde_json::from_str(&test_str).unwrap());
    }

    #[tokio::test]
    #[ignore = "required local ip-resolver-server"]
    async fn resolve() {
        let my_ip = PublicIp::BehindNat(ResolverOptions {
            resolver_addr: SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7777),
            wave_len: 10,
            wave_interval_ms: 100,
            wave_count: 3,
        })
        .resolve(30000)
        .await
        .unwrap();

        println!("my ip: {my_ip:?}");
    }
}
