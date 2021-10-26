/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - replaced file storage with direct rocksdb storage
/// - removed all temporary unused code
///
use std::borrow::Borrow;
use std::convert::TryInto;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Context, Result};
use tiny_adnl::utils::*;

use super::archive_package::*;
use super::block_handle::*;
use super::package_entry_id::*;
use crate::storage::{columns, StoredValue, Tree};

pub struct ArchiveManager {
    db: Arc<rocksdb::DB>,
    blocks: Tree<columns::ArchiveManagerDb>,
    archive_storage: Tree<columns::ArchiveStorage>,
    block_handles: Tree<columns::BlockHandles>,
    key_blocks: Tree<columns::KeyBlocks>,
}

impl ArchiveManager {
    pub fn with_db(db: &Arc<rocksdb::DB>) -> Result<Self> {
        let manager = Self {
            db: db.clone(),
            blocks: Tree::new(db)?,
            archive_storage: Tree::new(db)?,
            block_handles: Tree::new(db)?,
            key_blocks: Tree::new(db)?,
        };

        manager.self_check()?;

        Ok(manager)
    }

    fn self_check(&self) -> Result<()> {
        let storage_cf = self.archive_storage.get_cf()?;
        let mut iter = self.db.raw_iterator_cf(&storage_cf);
        iter.seek_to_first();

        while let (Some(key), value) = (iter.key(), iter.value()) {
            let archive_id = u64::from_be_bytes(
                key.try_into()
                    .with_context(|| format!("Invalid archive key: {}", hex::encode(key)))?,
            );

            let read = |value: &[u8]| -> Result<()> {
                let mut reader = ArchivePackageViewReader::new(value)?;

                let mut index = 0;
                while reader
                    .read_next()
                    .with_context(|| {
                        format!(
                            "Failed to read archive entry {}. Index: {}",
                            archive_id, index
                        )
                    })?
                    .is_some()
                {
                    index += 1;
                }

                Ok(())
            };

            if let Some(Err(e)) = value.map(read) {
                log::error!("Failed to read archive {}: {:?}", archive_id, e)
            }

            iter.next();
        }

        log::info!("Selfcheck complete");
        Ok(())
    }

    pub fn add_data<I>(&self, id: &PackageEntryId<I>, data: &[u8]) -> Result<()>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.blocks.insert(id.to_vec()?, data)?;
        Ok(())
    }

    pub fn has_data<I>(&self, id: &PackageEntryId<I>) -> Result<bool>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.blocks.contains_key(id.to_vec()?)
    }

    pub async fn get_data<I>(&self, handle: &BlockHandle, id: &PackageEntryId<I>) -> Result<Vec<u8>>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        let _lock = match &id {
            PackageEntryId::Block(_) => handle.block_data_lock().read().await,
            PackageEntryId::Proof(_) | PackageEntryId::ProofLink(_) => {
                handle.proof_data_lock().read().await
            }
        };

        match self.blocks.get(id.to_vec()?)? {
            Some(a) => Ok(a.to_vec()),
            None => Err(ArchiveManagerError::InvalidBlockData.into()),
        }
    }

    pub async fn gc(&self, top_blocks: &FxHashMap<ton_block::ShardIdent, u32>) -> Result<()> {
        let (raw_db, batch) = {
            // Cache cfs before loop
            let blocks_cf = self.blocks.get_cf()?;
            let key_blocks_cf = self.key_blocks.get_cf()?;
            let raw_db = self.blocks.raw_db_handle().clone();

            // Create batch
            let mut batch = rocksdb::WriteBatch::default();

            // Iterate all entries and find expired items
            let blocks_iter = self.blocks.iterator(rocksdb::IteratorMode::Start)?;
            for (key, _) in blocks_iter {
                // Read only prefix with shard ident and seqno
                let prefix = PackageEntryIdPrefix::from_slice(key.as_ref())?;
                match top_blocks.get(&prefix.shard_ident) {
                    Some(top_seq_no) if &prefix.seq_no < top_seq_no => { /* gc block */ }
                    // Skip blocks with seq.no. >= top seq.no.
                    _ => continue,
                };

                // Additionally check whether this item is a key block
                if prefix.shard_ident.is_masterchain()
                    && raw_db
                        .get_pinned_cf_opt(
                            &key_blocks_cf,
                            prefix.seq_no.to_be_bytes(),
                            self.key_blocks.read_config(),
                        )?
                        .is_some()
                {
                    // Don't remove key blocks
                    continue;
                }

                // Add item to the batch
                batch.delete_cf(&blocks_cf, key);
            }

            (raw_db, batch)
        };

        // Apply batch
        tokio::task::spawn_blocking(move || raw_db.write(batch)).await??;

        // Done
        Ok(())
    }

    pub async fn move_into_archive(&self, handle: &BlockHandle) -> Result<()> {
        if handle.meta().is_archived() {
            return Ok(());
        }
        if !handle.meta().set_is_moving_to_archive() {
            return Ok(());
        }

        // Prepare data
        let block_id = handle.id();

        let has_data = handle.meta().has_data();
        let mut is_link = false;
        let has_proof = handle.has_proof_or_link(&mut is_link);

        let block_data = if has_data {
            let lock = handle.block_data_lock().write().await;

            let entry_id = PackageEntryId::Block(block_id);
            let data = self.make_archive_segment(&entry_id)?;

            Some((lock, data))
        } else {
            None
        };

        let block_proof_data = if has_proof {
            let lock = handle.proof_data_lock().write().await;

            let entry_id = if is_link {
                PackageEntryId::ProofLink(block_id)
            } else {
                PackageEntryId::Proof(block_id)
            };
            let data = self.make_archive_segment(&entry_id)?;

            Some((lock, data))
        } else {
            None
        };

        // Prepare cf
        let storage_cf = self.archive_storage.get_cf()?;
        let handle_cf = self.block_handles.get_cf()?;

        // Prepare archive
        let ref_seqno = handle.masterchain_ref_seqno();

        let mut archive_id =
            self.compute_archive_id(&storage_cf, ref_seqno, handle.meta().is_key_block())?;
        if (ref_seqno as u64).saturating_sub(archive_id) >= (ARCHIVE_PACKAGE_SIZE as u64) {
            archive_id = ref_seqno as u64;
        }

        let archive_id_bytes = archive_id.to_be_bytes();

        // 0. Create transaction
        let mut batch = rocksdb::WriteBatch::default();
        // 1. Append archive segment with block data
        if let Some((_, data)) = &block_data {
            batch.merge_cf(&storage_cf, &archive_id_bytes, data);
        }
        // 2. Append archive segment with block proof data
        if let Some((_, data)) = &block_proof_data {
            batch.merge_cf(&storage_cf, &archive_id_bytes, data);
        }
        // 3. Update block handle meta
        if handle.meta().set_is_archived() {
            batch.put_cf(
                &handle_cf,
                handle.id().root_hash.as_slice(),
                handle.meta().to_vec()?,
            );
        }
        // 5. Execute transaction
        self.db.write(batch)?;

        // TODO: remove block

        // Done
        Ok(())
    }

    pub fn get_archive_id(&self, mc_seq_no: u32) -> Result<Option<u64>> {
        let storage_cf = self.archive_storage.get_cf()?;

        let mut iterator = self.db.raw_iterator_cf(&storage_cf);
        iterator.seek_for_prev(&(mc_seq_no as u64).to_be_bytes());
        Ok(if let Some(prev_id) = iterator.key() {
            Some(u64::from_be_bytes(prev_id.try_into()?))
        } else {
            None
        })
    }

    pub fn get_archive_slice(
        &self,
        id: u64,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        match self.archive_storage.get(id.to_be_bytes())? {
            Some(slice) if offset < slice.len() => {
                let end = std::cmp::min(offset + limit, slice.len());
                Ok(Some(slice[offset..end].to_vec()))
            }
            Some(_) => Err(ArchiveManagerError::InvalidOffset.into()),
            None => Ok(None),
        }
    }

    fn compute_archive_id(
        &self,
        storage_cf: &Arc<rocksdb::BoundColumnFamily<'_>>,
        mc_seq_no: u32,
        is_key_block: bool,
    ) -> Result<u64> {
        if is_key_block {
            return Ok(mc_seq_no as u64);
        }

        let mut archive_id = (mc_seq_no - mc_seq_no % ARCHIVE_SLICE_SIZE) as u64;

        let mut iterator = self.db.raw_iterator_cf(storage_cf);
        iterator.seek_for_prev(&(mc_seq_no as u64).to_be_bytes());
        if let Some(prev_id) = iterator.key() {
            let prev_id = u64::from_be_bytes(prev_id.try_into()?);
            if archive_id < prev_id {
                archive_id = prev_id;
            }
        }

        Ok(archive_id)
    }

    fn make_archive_segment<I>(&self, entry_id: &PackageEntryId<I>) -> Result<Vec<u8>>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        match self.blocks.get(entry_id.to_vec()?)? {
            Some(data) => make_archive_segment(&entry_id.filename(), &data).map_err(From::from),
            None => Err(ArchiveManagerError::InvalidBlockData.into()),
        }
    }
}

pub const ARCHIVE_PACKAGE_SIZE: u32 = 100;
pub const ARCHIVE_SLICE_SIZE: u32 = 20_000;

#[derive(thiserror::Error, Debug)]
enum ArchiveManagerError {
    #[error("Invalid block data")]
    InvalidBlockData,
    #[error("Offset is outside of the archive slice")]
    InvalidOffset,
}
