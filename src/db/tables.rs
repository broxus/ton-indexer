use crate::db::rocksdb::{DBCompressionType, MemtableFactory, SliceTransform};
use bytesize::ByteSize;
use weedb::rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, DataBlockIndexType, MergeOperands, Options, ReadOptions,
};
use weedb::{rocksdb, Caches, ColumnFamily};

use super::refcount;

/// Stores prepared archives
/// - Key: `u32 (BE)` (archive id)
/// - Value: `Vec<u8>` (archive data)
pub struct Archives;
impl ColumnFamily for Archives {
    const NAME: &'static str = "archives";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
        optimize_for_level_compaction(opts, ByteSize::mib(512u64));

        opts.set_merge_operator_associative("archive_data_merge", archive_data_merge);
        opts.set_compression_type(DBCompressionType::Zstd);
    }
}

/// Maps block root hash to block meta
/// - Key: `ton_types::UInt256`
/// - Value: `BlockMeta`
pub struct BlockHandles;
impl ColumnFamily for BlockHandles {
    const NAME: &'static str = "block_handles";

    fn options(opts: &mut Options, caches: &Caches) {
        optimize_for_level_compaction(opts, ByteSize::mib(512u64));

        let mut block_factory = BlockBasedOptions::default();
        block_factory.set_block_cache(&caches.block_cache);

        block_factory.set_index_type(BlockBasedIndexType::HashSearch);
        block_factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
        block_factory.set_format_version(5);

        opts.set_block_based_table_factory(&block_factory);
        optimize_for_point_lookup(opts, caches);
    }

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

/// Maps seqno to key block id
/// - Key: `u32 (BE)`
/// - Value: `ton_block::BlockIdExt`
pub struct KeyBlocks;
impl ColumnFamily for KeyBlocks {
    const NAME: &'static str = "key_blocks";

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

/// Maps package entry id to entry data
/// - Key: `BlockIdShort (16 bytes), ton_types::Uint256, package type (1 byte)`
/// - Value: `Vec<u8>`
pub struct PackageEntries;
impl ColumnFamily for PackageEntries {
    const NAME: &'static str = "package_entries";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
        opts.set_compression_type(DBCompressionType::Zstd);

        // This flag specifies that the implementation should optimize the filters
        // mainly for cases where keys are found rather than also optimize for keys
        // missed. This would be used in cases where the application knows that
        // there are very few misses or the performance in the case of misses is not
        // important.
        //
        // For now, this flag allows us to not store filters for the last level i.e
        // the largest level which contains data of the LSM store. For keys which
        // are hits, the filters in this level are not useful because we will search
        // for the data anyway. NOTE: the filters in other levels are still useful
        // even for key hit because they tell us whether to look in that level or go
        // to the higher level.
        // https://github.com/facebook/rocksdb/blob/81aeb15988e43c49952c795e32e5c8b224793589/include/rocksdb/advanced_options.h#L846
        opts.set_optimize_filters_for_hits(true);
    }
}

/// Maps BlockId to root cell hash
/// - Key: `ton_block::BlockIdExt`
/// - Value: `ton_types::UInt256`
pub struct ShardStates;
impl ColumnFamily for ShardStates {
    const NAME: &'static str = "shard_states";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
        opts.set_compression_type(DBCompressionType::Zstd);
    }
}

/// Stores cells data
/// - Key: `ton_types::UInt256` (cell repr hash)
/// - Value: `StorageCell`
pub struct Cells;
impl ColumnFamily for Cells {
    const NAME: &'static str = "cells";

    fn options(opts: &mut Options, caches: &Caches) {
        cells_opts(opts, caches);
    }
}

/// Stores temp cells data
/// - Key: `ton_types::UInt256` (cell repr hash)
/// - Value: `StorageCell`
pub struct TempCells;

impl ColumnFamily for TempCells {
    const NAME: &'static str = "temp_cells";

    fn options(opts: &mut rocksdb::Options, caches: &Caches) {
        cells_opts(opts, caches);
    }
}

fn cells_opts(opts: &mut Options, caches: &Caches) {
    opts.set_level_compaction_dynamic_level_bytes(true);

    opts.set_merge_operator_associative("cell_merge", refcount::merge_operator);
    opts.set_compaction_filter("cell_compaction", refcount::compaction_filter);

    // optimize for bulk inserts and single writer

    // Uses 8 * 512MB = 4GB
    let buffer_size = ByteSize::mib(512);
    let buffers_to_merge = 2;
    let buffer_count = 8;
    opts.set_write_buffer_size(buffer_size.as_u64() as _);
    opts.set_max_write_buffer_number(buffer_count);
    opts.set_min_write_buffer_number_to_merge(buffers_to_merge); // allow early flush

    opts.set_max_successive_merges(0); // it will eat cpu, we are doing first merge in hashmap anyway.

    // - Write batch size: 500K entries
    // - Entry size: ~244 bytes (32 SHA + 8 seq + 192 value + 12 overhead)
    // - Memtable size: 512MB

    // 1. Entries per memtable = 512MB / 244B ≈ 2.2M entries
    // 2. Target bucket load factor = 10-12 entries per bucket (RocksDB recommendation)
    // 3. Bucket count = entries / target_load = 2.2M / 11 ≈ 200K
    opts.set_memtable_factory(MemtableFactory::HashLinkList {
        bucket_count: 200_000,
    });

    opts.set_memtable_prefix_bloom_ratio(0.1); // we use hash-based memtable so bloom filter is not that useful
    opts.set_bloom_locality(1); // Optimize bloom filter locality

    let mut block_factory = BlockBasedOptions::default();

    // todo: some how make block cache separate for cells,
    // using 3/4 of all available cache space
    block_factory.set_block_cache(&caches.block_cache);

    // 10 bits per key, stored at the end of the sst
    block_factory.set_bloom_filter(10.0, false);
    block_factory.set_optimize_filters_for_memory(true);
    block_factory.set_whole_key_filtering(true);

    // to match fs block size
    block_factory.set_block_size(4096);
    block_factory.set_format_version(6);

    // we have 4096 / 256 = 16 keys per block, so binary search is enough
    block_factory.set_data_block_index_type(DataBlockIndexType::BinarySearch);

    block_factory.set_index_type(BlockBasedIndexType::HashSearch);
    block_factory.set_pin_l0_filter_and_index_blocks_in_cache(true);

    opts.set_block_based_table_factory(&block_factory);
    opts.set_prefix_extractor(SliceTransform::create_noop());

    opts.set_memtable_whole_key_filtering(true);
    opts.set_memtable_prefix_bloom_ratio(0.25);

    opts.set_compression_type(DBCompressionType::None);

    opts.set_level_zero_file_num_compaction_trigger(8);

    opts.set_target_file_size_base(512 * 1024 * 1024); // smaller files for more efficient GC

    opts.set_max_bytes_for_level_base(4 * 1024 * 1024 * 1024); // 4GB per level
    opts.set_max_bytes_for_level_multiplier(8.0);

    // 512MB per file; less files - less compactions
    opts.set_target_file_size_base(512 * 1024 * 1024);
    // L1: 4GB
    // L2: ~32GB
    // L3: ~256GB
    // L4: ~2TB
    opts.set_num_levels(5);

    opts.set_optimize_filters_for_hits(true);

    // we have our own cache and don't want `kcompactd` goes brrr scenario
    opts.set_use_direct_reads(true);
    opts.set_use_direct_io_for_flush_and_compaction(true);

    opts.add_compact_on_deletion_collector_factory(
        100, // N: examine 100 consecutive entries
        // Small enough window to detect local delete patterns
        // Large enough to avoid spurious compactions
        45, // D: trigger on 45 deletions in window
        // Balance between the space reclaim and compaction frequency
        // ~45% deletion density trigger
        0.5, /* deletion_ratio: trigger if 50% of a total file is deleted
              * Backup trigger for overall file health
              * Higher than window trigger to prefer local optimization */
    );

    // single writer optimizations
    opts.set_enable_write_thread_adaptive_yield(false);
    opts.set_allow_concurrent_memtable_write(false);
    opts.set_enable_pipelined_write(true);
    opts.set_inplace_update_support(false);
    opts.set_unordered_write(true); // we don't use snapshots

    opts.set_periodic_compaction_seconds(3600 * 24); // force compaction once a day
}

/// Stores generic node parameters
/// - Key: `...`
/// - Value: `...`
pub struct NodeStates;
impl ColumnFamily for NodeStates {
    const NAME: &'static str = "node_states";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);

        opts.set_optimize_filters_for_hits(true);
        optimize_for_point_lookup(opts, caches);
    }
}

/// Stores connections data
/// - Key: `ton_types::UInt256` (block root hash)
/// - Value: `ton_block::BlockIdExt (LE)`
pub struct Prev1;
impl ColumnFamily for Prev1 {
    const NAME: &'static str = "prev1";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);

        optimize_for_point_lookup(opts, caches);
    }

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

/// Stores connections data
/// - Key: `ton_types::UInt256` (block root hash)
/// - Value: `ton_block::BlockIdExt (LE)`
pub struct Prev2;
impl ColumnFamily for Prev2 {
    const NAME: &'static str = "prev2";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);

        optimize_for_point_lookup(opts, caches);
    }

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

/// Stores connections data
/// - Key: `ton_types::UInt256` (block root hash)
/// - Value: `ton_block::BlockIdExt (LE)`
pub struct Next1;
impl ColumnFamily for Next1 {
    const NAME: &'static str = "next1";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);

        optimize_for_point_lookup(opts, caches);
    }

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

/// Stores connections data
/// - Key: `ton_types::UInt256` (block root hash)
/// - Value: `ton_block::BlockIdExt (LE)`
pub struct Next2;
impl ColumnFamily for Next2 {
    const NAME: &'static str = "next2";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);

        optimize_for_point_lookup(opts, caches);
    }

    fn read_options(opts: &mut ReadOptions) {
        opts.set_verify_checksums(false);
    }
}

fn archive_data_merge(
    _: &[u8],
    current_value: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    use crate::utils::ARCHIVE_PREFIX;

    let total_len: usize = operands.iter().map(|data| data.len()).sum();
    let mut result = Vec::with_capacity(ARCHIVE_PREFIX.len() + total_len);

    result.extend_from_slice(current_value.unwrap_or(&ARCHIVE_PREFIX));

    for data in operands {
        let data = data.strip_prefix(&ARCHIVE_PREFIX).unwrap_or(data);
        result.extend_from_slice(data);
    }

    Some(result)
}

fn default_block_based_table_factory(opts: &mut Options, caches: &Caches) {
    opts.set_level_compaction_dynamic_level_bytes(true);
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&caches.block_cache);
    block_factory.set_format_version(5);
    opts.set_block_based_table_factory(&block_factory);
}

// setting our shared cache instead of individual caches for each cf
fn optimize_for_point_lookup(opts: &mut Options, caches: &Caches) {
    //     https://github.com/facebook/rocksdb/blob/81aeb15988e43c49952c795e32e5c8b224793589/options/options.cc
    //     BlockBasedTableOptions block_based_options;
    //     block_based_options.data_block_index_type =
    //         BlockBasedTableOptions::kDataBlockBinaryAndHash;
    //     block_based_options.data_block_hash_table_util_ratio = 0.75;
    //     block_based_options.filter_policy.reset(NewBloomFilterPolicy(10));
    //     block_based_options.block_cache =
    //         NewLRUCache(static_cast<size_t>(block_cache_size_mb * 1024 * 1024));
    //     table_factory.reset(new BlockBasedTableFactory(block_based_options));
    //     memtable_prefix_bloom_size_ratio = 0.02;
    //     memtable_whole_key_filtering = true;
    //
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_data_block_index_type(DataBlockIndexType::BinaryAndHash);
    block_factory.set_data_block_hash_ratio(0.75);
    block_factory.set_bloom_filter(10.0, false);
    block_factory.set_block_cache(&caches.block_cache);
    opts.set_block_based_table_factory(&block_factory);

    opts.set_memtable_prefix_bloom_ratio(0.02);
    opts.set_memtable_whole_key_filtering(true);
}

fn optimize_for_level_compaction(opts: &mut Options, budget: ByteSize) {
    opts.set_write_buffer_size(budget.as_u64() as usize / 4);
    // this means we'll use 50% extra memory in the worst case, but will reduce
    //  write stalls.
    opts.set_min_write_buffer_number_to_merge(2);
    // this means we'll use 50% extra memory in the worst case, but will reduce
    // write stalls.
    opts.set_max_write_buffer_number(6);
    // start flushing L0->L1 as soon as possible. each file on level0 is
    // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
    // memtable_memory_budget.
    opts.set_level_zero_file_num_compaction_trigger(2);
    // doesn't really matter much, but we don't want to create too many files
    opts.set_target_file_size_base(budget.as_u64() / 8);
    // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
    opts.set_max_bytes_for_level_base(budget.as_u64());
}
