use std::sync::Arc;

use anyhow::{Context, Result};
use ton_block::Deserializable;

use super::Migrations;
use crate::db::columns;
use crate::db::tree::{Column, Tree};
use crate::utils::*;

// 2.0.7 to 2.0.8
// - Change key for `package_entries`:
//    * `BlockIdShort, package type (1 byte)` -> `BlockIdShort, ton_types::Uint256, package type (1 byte)`
pub(super) fn register(migrations: &mut Migrations) -> Result<()> {
    migrations.register([2, 0, 7], [2, 0, 8], |db| async move {
        update_package_entries(&db)?;
        Ok(())
    })
}

fn update_package_entries(db: &Arc<rocksdb::DB>) -> Result<()> {
    let package_entries = Tree::<columns::PackageEntries>::new(db)?;
    let package_entries_cf = package_entries.get_cf();

    let mut read_options = Default::default();
    columns::PackageEntries::read_options(&mut read_options);
    let write_options = package_entries.write_config();

    let snapshot = db.snapshot();

    let mut iter = snapshot.raw_iterator_cf_opt(&package_entries_cf, read_options);
    iter.seek_to_first();

    const ENTRIES_PER_BATCH: usize = 10000;

    // Iterate through the snapshot
    let mut total_entries = 0;
    let mut batch = rocksdb::WriteBatch::default();
    let mut entries_in_batch = 0;
    while let (Some(old_key), Some(value)) = (iter.key(), iter.value()) {
        // Workchain, shard, seqno
        const OLD_KEY_PREFIX_LEN: usize = BlockIdShort::SIZE_HINT;
        // Workchain, shard, seqno, root hash
        const NEW_KEY_PREFIX_LEN: usize = BlockIdShort::SIZE_HINT + 32;

        // Prefix + 1 byte of package type
        const NEW_KEY_LEN: usize = NEW_KEY_PREFIX_LEN + 1;

        if old_key.len() < NEW_KEY_LEN {
            let cell = ton_types::deserialize_tree_of_cells(&mut std::convert::identity(value))
                .context("Invalid package entry value")?;

            let root_hash = match old_key.get(OLD_KEY_PREFIX_LEN) {
                Some(0) => cell.repr_hash(),
                Some(1) | Some(2) => {
                    let (shard_ident, seqno) =
                        BlockIdShort::deserialize(&mut std::convert::identity(old_key))
                            .context("Invalid package entry key")?;

                    let proof = ton_block::BlockProof::construct_from(&mut cell.into())?;
                    let block_id = proof.proof_for;
                    anyhow::ensure!(
                        block_id.shard_id == shard_ident && block_id.seq_no == seqno,
                        "Proof block id mismatch"
                    );

                    block_id.root_hash
                }
                ty => anyhow::bail!("Invalid package entry type: {ty:?}"),
            };

            let mut new_key = [0u8; NEW_KEY_LEN];
            new_key[..OLD_KEY_PREFIX_LEN].copy_from_slice(&old_key[..OLD_KEY_PREFIX_LEN]);
            new_key[OLD_KEY_PREFIX_LEN..NEW_KEY_PREFIX_LEN].copy_from_slice(root_hash.as_slice());
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
