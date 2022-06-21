use std::sync::Arc;

use anyhow::{Context, Result};
use smallvec::SmallVec;

use super::Migrations;
use crate::db::columns;
use crate::db::tree::Tree;
use crate::utils::*;

// 2.0.6 to 2.0.7
// - Change key for `package_entries`:
//    * `ton_block::BlockIdExt, package type (1 byte)` -> `ton_block::BlockId, package type (1 byte)`
// - Change key for `ShardStates`:
//    * `ton_block::BlockIdExt` -> `ton_block::BlockId`
// - Change value for `ShardStates`:
//    * Add `ton_types::UInt256` (block root hash), `ton_types::UInt256` (block file hash)
// - Reduce possible cell marker values
pub(super) fn register(migrations: &mut Migrations) -> Result<()> {
    migrations.register([2, 0, 6], [2, 0, 7], |db| async move {
        update_package_entries(&db)?;
        update_shard_states(&db)?;
        // update_cell_markers(&db)?;
        Ok(())
    })
}

fn update_package_entries(db: &Arc<rocksdb::DB>) -> Result<()> {
    let package_entries = Tree::<columns::PackageEntries>::new(db)?;
    let package_entries_cf = package_entries.get_cf();
    let write_options = package_entries.write_config();

    let mut iter = package_entries.raw_iterator();
    iter.seek_to_first();

    const ENTRIES_PER_BATCH: usize = 10000;

    let mut total_entries = 0;
    let mut batch = rocksdb::WriteBatch::default();
    let mut entries_in_batch = 0;
    while let (Some(old_key), Some(value)) = (iter.key(), iter.value()) {
        // Full block id
        const OLD_KEY_PREFIX_LEN: usize = ton_block::BlockIdExt::SIZE_HINT;
        // Workchain, shard, seqno
        const NEW_KEY_PREFIX_LEN: usize = ton_block::ShardIdent::SIZE_HINT + 4;

        // Prefix + 1 byte of package type
        const NEW_KEY_LEN: usize = NEW_KEY_PREFIX_LEN + 1;

        // Apply only for old keys
        if old_key.len() > NEW_KEY_LEN {
            let mut new_key = [0u8; NEW_KEY_LEN];
            new_key[..NEW_KEY_PREFIX_LEN].copy_from_slice(&old_key[..NEW_KEY_PREFIX_LEN]);
            new_key[NEW_KEY_PREFIX_LEN] = old_key[OLD_KEY_PREFIX_LEN];

            batch.delete_cf(&package_entries_cf, old_key);
            batch.put_cf(&package_entries_cf, new_key, value);

            total_entries += 1;
            entries_in_batch += 1;
            if entries_in_batch >= ENTRIES_PER_BATCH {
                db.write_opt(std::mem::take(&mut batch), write_options)
                    .context("Failed to apply write batch")?;
                entries_in_batch = 0;
            }
        }

        iter.next();
    }

    if entries_in_batch > 0 {
        db.write_opt(batch, write_options)
            .context("Failed to apply write batch")?;
    }

    log::info!("Migrated {total_entries} package entries");
    Ok(())
}

fn update_shard_states(db: &Arc<rocksdb::DB>) -> Result<()> {
    let shard_states = Tree::<columns::ShardStates>::new(db)?;
    let shard_states_cf = shard_states.get_cf();
    let write_options = shard_states.write_config();

    let mut iter = shard_states.raw_iterator();
    iter.seek_to_first();

    const STATES_PER_BATCH: usize = 10000;

    // Workchain, shard, seqno
    const NEW_KEY_LEN: usize = ton_block::ShardIdent::SIZE_HINT + 4;
    // state root hash, block root hash, block file hash
    const NEW_VALUE_LEN: usize = 32 * 3;

    let mut total_states = 0;
    let mut batch = rocksdb::WriteBatch::default();
    let mut entries_in_batch = 0;

    let mut value_buffer = SmallVec::<[u8; NEW_VALUE_LEN]>::with_capacity(NEW_VALUE_LEN);

    while let (Some(old_key), Some(value)) = (iter.key(), iter.value()) {
        // Apply only for old keys
        if old_key.len() > NEW_KEY_LEN {
            let block_id = ton_block::BlockIdExt::deserialize(&mut std::convert::identity(old_key))
                .context("Invalid state key")?;

            if value.len() != 32 {
                return Err(anyhow::anyhow!(
                    "Invalid state value {block_id}: {}",
                    hex::encode(value)
                ));
            }

            value_buffer.clear();
            value_buffer.extend_from_slice(value);
            value_buffer.extend_from_slice(block_id.root_hash.as_slice());
            value_buffer.extend_from_slice(block_id.file_hash.as_slice());

            batch.delete_cf(&shard_states_cf, old_key);
            batch.put_cf(&shard_states_cf, &old_key[..NEW_KEY_LEN], &value_buffer);

            total_states += 1;
            entries_in_batch += 1;
            if entries_in_batch >= STATES_PER_BATCH {
                db.write_opt(std::mem::take(&mut batch), write_options)
                    .context("Failed to apply write batch")?;
                entries_in_batch = 0;
            }
        }

        iter.next();
    }

    if entries_in_batch > 0 {
        db.write_opt(batch, write_options)
            .context("Failed to apply write batch")?;
    }

    log::info!("Migrated {total_states} shard states");
    Ok(())
}

// fn update_cell_markers(db: &Arc<rocksdb::DB>) -> Result<()> {}
