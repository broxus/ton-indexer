use std::convert::TryInto;

use serde::Deserialize;

pub use block::*;
pub use block_proof::*;
pub use shard_state::*;
pub use shard_state_cache::*;

mod block;
mod block_proof;
mod shard_state;
mod shard_state_cache;

pub trait NoFailure {
    type Output;
    fn convert(self) -> anyhow::Result<Self::Output>;
}

impl<T> NoFailure for ton_types::Result<T> {
    type Output = T;
    fn convert(self) -> anyhow::Result<Self::Output> {
        self.map_err(|e| anyhow::Error::msg(e.to_string()))
    }
}

pub mod serde_hex_array {
    use super::*;

    pub fn serialize<S, T>(data: T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: AsRef<[u8]> + Sized,
        S: serde::Serializer,
    {
        serializer.serialize_str(&hex::encode(&data.as_ref()))
    }

    pub fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        let data = String::deserialize(deserializer)?;
        let data = hex::decode(data).map_err(D::Error::custom)?;
        data.try_into()
            .map_err(|_| D::Error::custom(format!("Invalid array length, expected: {}", N)))
    }
}

pub fn serde_deserialize_base64_array<'de, D, const N: usize>(
    deserializer: D,
) -> Result<[u8; N], D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    let data = String::deserialize(deserializer)?;
    let data = base64::decode(data).map_err(D::Error::custom)?;
    data.try_into()
        .map_err(|_| D::Error::custom(format!("Invalid array length, expected: {}", N)))
}
