use rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, DataBlockIndexType, MergeOperands, Options,
    SliceTransform,
};

use super::{Column, DbCaches, StoredValue, ARCHIVE_PREFIX};

/// Stores prepared archives
/// - Key: `u32 (BE)` (archive id)
/// - Value: `Vec<u8>` (archive data)
pub struct Archives;
impl Column for Archives {
    const NAME: &'static str = "archives";

    fn options(opts: &mut Options, caches: &DbCaches) {
        default_block_based_table_factory(opts, caches);

        opts.set_merge_operator_associative("archive_data_merge", archive_data_merge);
    }
}

/// Maps block root hash to block meta
/// - Key: `ton_types::UInt256`
/// - Value: `BlockMeta`
pub struct BlockHandles;
impl Column for BlockHandles {
    const NAME: &'static str = "block_handles";

    fn options(opts: &mut Options, caches: &DbCaches) {
        opts.set_write_buffer_size(128 * 1024 * 1024);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));

        let mut block_factory = BlockBasedOptions::default();
        block_factory.set_block_cache(&caches.block_cache);
        block_factory.set_block_cache_compressed(&caches.compressed_block_cache);

        block_factory.set_index_type(BlockBasedIndexType::HashSearch);
        block_factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);

        opts.set_block_based_table_factory(&block_factory);

        opts.optimize_for_point_lookup(10);
    }
}

/// Maps seqno to key block id
/// - Key: `u32 (BE)`
/// - Value: `ton_block::BlockIdExt`
pub struct KeyBlocks;
impl Column for KeyBlocks {
    const NAME: &'static str = "key_blocks";
}

/// Maps package entry id to entry data
/// - Key: `ton_block::BlockIdExt, package type (1 byte)`
/// - Value: `Vec<u8>`
pub struct PackageEntries;
impl Column for PackageEntries {
    const NAME: &'static str = "package_entries";

    fn options(opts: &mut Options, caches: &DbCaches) {
        default_block_based_table_factory(opts, caches);

        // Root hash, file hash and type are not needed
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(
            ton_block::ShardIdent::SIZE_HINT + std::mem::size_of::<u32>(),
        ));

        opts.set_optimize_filters_for_hits(true);
    }
}

/// Maps BlockId to root cell hash
/// - Key: `ton_block::BlockIdExt`
/// - Value: `ton_types::UInt256`
pub struct ShardStates;
impl Column for ShardStates {
    const NAME: &'static str = "shard_states";

    fn options(opts: &mut Options, caches: &DbCaches) {
        default_block_based_table_factory(opts, caches);

        // Root hash and file hash are not needed
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(
            ton_block::ShardIdent::SIZE_HINT + std::mem::size_of::<u32>(),
        ))
    }
}

/// Stores cells data
/// - Key: `ton_types::UInt256` (cell repr hash)
/// - Value: `StorageCell`
pub struct Cells;
impl Column for Cells {
    const NAME: &'static str = "cells";

    fn options(opts: &mut Options, caches: &DbCaches) {
        opts.set_write_buffer_size(128 * 1024 * 1024);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));

        let mut block_factory = BlockBasedOptions::default();
        block_factory.set_block_cache(&caches.block_cache);
        block_factory.set_block_cache_compressed(&caches.compressed_block_cache);

        block_factory.set_index_type(BlockBasedIndexType::HashSearch);
        block_factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);

        opts.set_block_based_table_factory(&block_factory);

        opts.set_optimize_filters_for_hits(true);
    }
}

/// Stores generic node parameters
/// - Key: `...`
/// - Value: `...`
pub struct NodeStates;
impl Column for NodeStates {
    const NAME: &'static str = "node_states";

    fn options(opts: &mut Options, caches: &DbCaches) {
        default_block_based_table_factory(opts, caches);

        opts.set_optimize_filters_for_hits(true);
        opts.optimize_for_point_lookup(1);
    }
}

/// Stores connections data
/// - Key: `ton_types::UInt256` (block root hash)
/// - Value: `ton_block::BlockIdExt (LE)`
pub struct Prev1;
impl Column for Prev1 {
    const NAME: &'static str = "prev1";

    fn options(opts: &mut Options, caches: &DbCaches) {
        default_block_based_table_factory(opts, caches);

        opts.optimize_for_point_lookup(10);
    }
}

/// Stores connections data
/// - Key: `ton_types::UInt256` (block root hash)
/// - Value: `ton_block::BlockIdExt (LE)`
pub struct Prev2;
impl Column for Prev2 {
    const NAME: &'static str = "prev2";

    fn options(opts: &mut Options, caches: &DbCaches) {
        default_block_based_table_factory(opts, caches);

        opts.optimize_for_point_lookup(10);
    }
}

/// Stores connections data
/// - Key: `ton_types::UInt256` (block root hash)
/// - Value: `ton_block::BlockIdExt (LE)`
pub struct Next1;
impl Column for Next1 {
    const NAME: &'static str = "next1";

    fn options(opts: &mut Options, caches: &DbCaches) {
        default_block_based_table_factory(opts, caches);

        opts.optimize_for_point_lookup(10);
    }
}

/// Stores connections data
/// - Key: `ton_types::UInt256` (block root hash)
/// - Value: `ton_block::BlockIdExt (LE)`
pub struct Next2;
impl Column for Next2 {
    const NAME: &'static str = "next2";

    fn options(opts: &mut Options, caches: &DbCaches) {
        default_block_based_table_factory(opts, caches);

        opts.optimize_for_point_lookup(10);
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

fn default_block_based_table_factory(opts: &mut Options, caches: &DbCaches) {
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&caches.block_cache);
    block_factory.set_block_cache_compressed(&caches.compressed_block_cache);
    opts.set_block_based_table_factory(&block_factory);
}
