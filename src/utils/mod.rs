pub use block::*;
pub use block_proof::*;
pub use shard_state::*;

mod block;
mod block_proof;
mod shard_state;

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

pub mod serde_bytes_base64 {
    pub fn serialize<S, T>(data: T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: AsRef<[u8]> + Sized,
        S: serde::Serializer,
    {
        serializer.serialize_str(&base64::encode(&data.as_ref()))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        <String as serde::Deserialize>::deserialize(deserializer)
            .and_then(|string| base64::decode(string).map_err(|e| D::Error::custom(e.to_string())))
    }
}
