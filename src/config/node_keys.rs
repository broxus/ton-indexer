use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{Context, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use tiny_adnl::AdnlKeystore;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeKeys {
    #[serde(with = "serde_key")]
    pub dht_key: [u8; 32],
    #[serde(with = "serde_key")]
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

    /// Load from file
    ///
    /// NOTE: generates and saves new if it doesn't exist
    pub fn load<P>(path: P, force_regenerate: bool) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)
            .context("Failed to open ADNL keys")?;

        let keys = if force_regenerate {
            Self::generate()
        } else {
            match serde_json::from_reader(&file) {
                Ok(keys) => keys,
                Err(_) => {
                    log::warn!("Failed to read ADNL keys. Generating new");
                    Self::generate()
                }
            }
        };

        keys.save(file).context("Failed to save ADNL keys")?;

        Ok(keys)
    }

    pub fn save<W>(&self, mut file: W) -> Result<()>
    where
        W: Write + Seek,
    {
        file.seek(SeekFrom::Start(0))?;
        serde_json::to_writer_pretty(file, self)?;
        Ok(())
    }

    pub fn build_keystore(&self) -> Result<AdnlKeystore> {
        AdnlKeystore::from_tagged_keys(vec![
            (make_key(&self.dht_key), 1),
            (make_key(&self.overlay_key), 2),
        ])
    }
}

fn make_key(key: &[u8; 32]) -> ed25519_dalek::SecretKey {
    ed25519_dalek::SecretKey::from_bytes(key).expect("Shouldn't fail")
}

mod serde_key {
    use super::*;
    use serde::de::Error;

    pub fn serialize<S, T>(data: T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: AsRef<[u8]> + Sized,
        S: serde::Serializer,
    {
        hex::encode(&data.as_ref()).serialize(serializer)
    }

    pub fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = String::deserialize(deserializer)?;
        let data = hex::decode(data).map_err(D::Error::custom)?;
        data.try_into().map_err(|_| D::Error::custom("Invalid key"))
    }
}
