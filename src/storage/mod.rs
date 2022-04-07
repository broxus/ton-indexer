use std::io::Write;

use anyhow::Result;
use rocksdb::MergeOperands;
use smallvec::SmallVec;
use ton_types::ByteOrderRead;

pub use self::archive_manager::*;
pub use self::archive_package::*;
pub use self::background_sync_meta::*;
pub use self::block_handle::*;
pub use self::block_handle_storage::*;
pub use self::block_meta::*;
pub use self::cell_storage::*;
pub use self::node_state_storage::*;
pub use self::package_entry_id::*;
pub use self::shard_state_storage::*;
pub use self::top_blocks::*;
pub use self::tree::*;

mod archive_manager;
mod archive_package;
mod background_sync_meta;
mod block_handle;
mod block_handle_storage;
mod block_meta;
mod cell_storage;
mod node_state_storage;
mod package_entry_id;
mod shard_state_storage;
mod top_blocks;
mod tree;

pub mod columns {
    use rocksdb::Options;

    use super::{archive_data_merge, Column};

    /// Stores prepared archives
    /// - Key: `u32 (BE)` (archive id)
    /// - Value: `Vec<u8>` (archive data)
    pub struct Archives;
    impl Column for Archives {
        const NAME: &'static str = "archives";

        fn options(opts: &mut Options) {
            opts.set_merge_operator_associative("archive_data_merge", archive_data_merge);
        }
    }

    /// Maps block root hash to block meta
    /// - Key: `ton_types::UInt256`
    /// - Value: `BlockMeta`
    pub struct BlockHandles;
    impl Column for BlockHandles {
        const NAME: &'static str = "block_handles";
    }

    /// Maps seqno to key block id
    /// - Key: `u32 (BE)`
    /// - Value: `ton_block::BlockIdExt`
    pub struct KeyBlocks;
    impl Column for KeyBlocks {
        const NAME: &'static str = "key_blocks";
    }

    /// Maps package entry id to entry data
    /// - Key: `PackageEntryId<I>, where I: Borrow<ton_block::BlockIdExt>`
    /// - Value: `Vec<u8>`
    pub struct PackageEntries;
    impl Column for PackageEntries {
        const NAME: &'static str = "package_entries";

        fn options(opts: &mut Options) {
            opts.set_optimize_filters_for_hits(true);
        }
    }

    /// Maps BlockId to root cell hash
    /// - Key: `ton_block::BlockIdExt`
    /// - Value: `ton_types::UInt256`
    pub struct ShardStates;
    impl Column for ShardStates {
        const NAME: &'static str = "shard_states";
    }

    /// Stores cells data
    /// - Key: `ton_types::UInt256` (cell repr hash)
    /// - Value: `StorageCell`
    pub struct Cells;
    impl Column for Cells {
        const NAME: &'static str = "cells";

        fn options(opts: &mut Options) {
            opts.set_optimize_filters_for_hits(true);
        }
    }

    /// Stores generic node parameters
    /// - Key: `...`
    /// - Value: `...`
    pub struct NodeStates;
    impl Column for NodeStates {
        const NAME: &'static str = "node_states";

        fn options(opts: &mut Options) {
            opts.set_optimize_filters_for_hits(true);
        }
    }

    pub struct Prev1;
    impl Column for Prev1 {
        const NAME: &'static str = "prev1";
    }

    pub struct Prev2;
    impl Column for Prev2 {
        const NAME: &'static str = "prev2";
    }

    pub struct Next1;
    impl Column for Next1 {
        const NAME: &'static str = "next1";
    }

    pub struct Next2;
    impl Column for Next2 {
        const NAME: &'static str = "next2";
    }
}

fn archive_data_merge(
    _: &[u8],
    current_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let total_len: usize = operands.iter().map(|data| data.len()).sum();
    let mut result = Vec::with_capacity(ARCHIVE_PREFIX.len() + total_len);

    result.extend_from_slice(current_value.unwrap_or(&ARCHIVE_PREFIX));

    for data in operands {
        let data = data.strip_prefix(&ARCHIVE_PREFIX).unwrap_or(data);
        result.extend_from_slice(data);
    }

    Some(result)
}

pub trait StoredValue {
    const SIZE_HINT: usize;

    type OnStackSlice: smallvec::Array<Item = u8>;

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()>;

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized;

    #[inline(always)]
    fn from_slice(mut data: &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        Self::deserialize(&mut data)
    }

    fn to_vec(&self) -> Result<SmallVec<Self::OnStackSlice>> {
        let mut result = SmallVec::with_capacity(Self::SIZE_HINT);
        self.serialize(&mut result)?;
        Ok(result)
    }
}

impl StoredValue for ton_block::BlockIdExt {
    /// 4 bytes workchain id,
    /// 8 bytes shard id,
    /// 4 bytes seqno,
    /// 32 bytes root hash,
    /// 32 bytes file hash
    const SIZE_HINT: usize = ton_block::ShardIdent::SIZE_HINT + 4 + 32 + 32;

    /// 96 is minimal suitable for `smallvec::Array` and `SIZE_HINT`
    type OnStackSlice = [u8; 96];

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        self.shard_id.serialize(writer)?;
        writer.write_all(&self.seq_no.to_be_bytes())?;
        writer.write_all(self.root_hash.as_slice())?;
        writer.write_all(self.file_hash.as_slice())?;
        Ok(())
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let shard_id = ton_block::ShardIdent::deserialize(reader)?;
        let seq_no = reader.read_be_u32()?;
        let root_hash = ton_types::UInt256::from(reader.read_u256()?);
        let file_hash = ton_types::UInt256::from(reader.read_u256()?);
        Ok(Self::with_params(shard_id, seq_no, root_hash, file_hash))
    }
}

impl StoredValue for ton_block::ShardIdent {
    /// 4 bytes workchain id
    /// 8 bytes shard id
    const SIZE_HINT: usize = 4 + 8;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&self.workchain_id().to_be_bytes())?;
        writer.write_all(&self.shard_prefix_with_tag().to_be_bytes())?;
        Ok(())
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let workchain_id = reader.read_be_u32()? as i32;
        let shard_prefix_tagged = reader.read_be_u64()?;
        Self::with_tagged_prefix(workchain_id, shard_prefix_tagged)
    }
}
