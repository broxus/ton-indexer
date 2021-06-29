use std::io::{Read, Write};

use anyhow::Result;

pub use self::archive_manager::*;
pub use self::block_handle::*;
pub use self::block_handle_storage::*;
pub use self::block_meta::*;
pub use self::node_state_storage::*;
pub use self::package_entry_id::*;

mod archive_manager;
mod block_handle;
mod block_handle_storage;
mod block_meta;
mod node_state_storage;
mod package_entry_id;

pub trait StoredValue {
    fn size_hint(&self) -> Option<usize> {
        None
    }

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()>;

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized;

    fn from_slice(data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        Self::deserialize(&mut std::io::Cursor::new(data))
    }

    fn to_vec(&self) -> Result<Vec<u8>> {
        let mut result = self.size_hint().map(Vec::with_capacity).unwrap_or_default();
        self.serialize(&mut result)?;
        Ok(result)
    }
}
