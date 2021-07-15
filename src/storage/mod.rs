use std::io::{Read, Write};

use anyhow::Result;
use ton_types::ByteOrderRead;

pub use self::archive_manager::*;
pub use self::block_handle::*;
pub use self::block_handle_storage::*;
pub use self::block_index_db::*;
pub use self::block_meta::*;
pub use self::node_state_storage::*;
pub use self::package_entry_id::*;
pub use self::shard_state_storage::*;
use crate::utils::*;

mod archive_manager;
mod archive_package;
mod block_handle;
mod block_handle_storage;
mod block_index_db;
mod block_meta;
mod node_state_storage;
mod package_entry_id;
mod shard_state_storage;
mod storage_cell;

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

impl StoredValue for ton_block::BlockIdExt {
    fn size_hint(&self) -> Option<usize> {
        Some(4 + 8 + 4 + 32 + 32)
    }

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.shard_id.serialize(writer)?;
        writer.write_all(&self.seq_no.to_le_bytes())?;
        writer.write_all(self.root_hash.as_slice())?;
        writer.write_all(self.file_hash.as_slice())?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let shard_id = ton_block::ShardIdent::deserialize(reader)?;
        let seq_no = reader.read_le_u32()?;
        let root_hash = ton_types::UInt256::from(reader.read_u256()?);
        let file_hash = ton_types::UInt256::from(reader.read_u256()?);
        Ok(Self::with_params(shard_id, seq_no, root_hash, file_hash))
    }
}

impl StoredValue for ton_block::ShardIdent {
    fn size_hint(&self) -> Option<usize> {
        Some(4 + 8)
    }

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.workchain_id().to_le_bytes())?;
        writer.write_all(&self.shard_prefix_with_tag().to_le_bytes())?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let workchain_id = reader.read_le_u32()? as i32;
        let shard_prefix_tagged = reader.read_le_u64()?;
        Self::with_tagged_prefix(workchain_id, shard_prefix_tagged).convert()
    }
}
