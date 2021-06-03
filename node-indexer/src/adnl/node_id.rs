use std::convert::{TryFrom, TryInto};

use anyhow::Result;
use sha2::Digest;
use ton_api::{ton, BoxedSerialize, IntoBoxed};

use crate::adnl::serialize;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct AdnlNodeIdFull([u8; 32]);

impl AdnlNodeIdFull {
    pub fn new(public_key: [u8; 32]) -> Self {
        Self(public_key)
    }

    pub fn public_key(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn as_tl(&self) -> ton::pub_::publickey::Ed25519 {
        ton::pub_::publickey::Ed25519 {
            key: ton::int256(self.0),
        }
    }

    pub fn compute_short_id(&self) -> Result<AdnlNodeIdShort> {
        let hash = hash(self.as_tl())?;
        Ok(AdnlNodeIdShort::new(hash))
    }
}

impl std::fmt::Display for AdnlNodeIdFull {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&hex::encode(&self.0))
    }
}

impl From<ed25519_dalek::PublicKey> for AdnlNodeIdFull {
    fn from(key: ed25519_dalek::PublicKey) -> Self {
        Self::new(key.to_bytes())
    }
}

impl TryFrom<&ton::PublicKey> for AdnlNodeIdFull {
    type Error = anyhow::Error;

    fn try_from(public_key: &ton::PublicKey) -> Result<Self> {
        match public_key {
            ton::PublicKey::Pub_Ed25519(public_key) => Ok(Self::new(public_key.key.0)),
            _ => Err(AdnlNodeIdError::UnsupportedPublicKey.into()),
        }
    }
}

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct AdnlNodeIdShort([u8; 32]);

impl AdnlNodeIdShort {
    pub fn new(hash: [u8; 32]) -> Self {
        Self(hash)
    }

    pub fn as_slice(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn is_zero(&self) -> bool {
        for b in &self.0 {
            if b != &0 {
                return false;
            }
        }
        true
    }

    pub fn as_tl(&self) -> ton::adnl::id::short::Short {
        ton::adnl::id::short::Short {
            id: ton::int256(self.0),
        }
    }
}

impl std::fmt::Display for AdnlNodeIdShort {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(&hex::encode(&self.0))
    }
}

impl PartialEq<[u8]> for AdnlNodeIdShort {
    fn eq(&self, other: &[u8]) -> bool {
        self.0.eq(other)
    }
}

impl PartialEq<[u8; 32]> for AdnlNodeIdShort {
    fn eq(&self, other: &[u8; 32]) -> bool {
        self.0.eq(other)
    }
}

pub trait ComputeNodeIds {
    fn compute_node_ids(&self) -> Result<(AdnlNodeIdFull, AdnlNodeIdShort)>;
}

impl ComputeNodeIds for ed25519_dalek::SecretKey {
    fn compute_node_ids(&self) -> Result<(AdnlNodeIdFull, AdnlNodeIdShort)> {
        let public_key = ed25519_dalek::PublicKey::from(self);
        let full_id = AdnlNodeIdFull::new(public_key.to_bytes());
        let short_id = full_id.compute_short_id()?;
        Ok((full_id, short_id))
    }
}

impl ComputeNodeIds for ed25519_dalek::PublicKey {
    fn compute_node_ids(&self) -> Result<(AdnlNodeIdFull, AdnlNodeIdShort)> {
        let full_id = AdnlNodeIdFull::new(self.to_bytes());
        let short_id = full_id.compute_short_id()?;
        Ok((full_id, short_id))
    }
}

#[derive(thiserror::Error, Debug)]
enum AdnlNodeIdError {
    #[error("Unsupported public key")]
    UnsupportedPublicKey,
}

pub fn hash<T: IntoBoxed>(object: T) -> Result<[u8; 32]> {
    hash_boxed(&object.into_boxed())
}

pub fn hash_boxed<T: BoxedSerialize>(object: &T) -> Result<[u8; 32]> {
    let buf = sha2::Sha256::digest(&serialize(object)?);
    Ok(buf.as_slice().try_into().unwrap())
}
