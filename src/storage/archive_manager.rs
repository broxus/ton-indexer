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

use super::archive_package::*;
use super::block_handle::*;
use super::package_entry_id::*;
use crate::storage::{columns, StoredValue, TopBlocks, Tree};

pub struct ArchiveManager {
    db: Arc<rocksdb::DB>,
    archives: Tree<columns::Archives>,
    package_entries: Tree<columns::PackageEntries>,
    block_handles: Tree<columns::BlockHandles>,
    key_blocks: Tree<columns::KeyBlocks>,
}

impl ArchiveManager {
    pub fn with_db(db: &Arc<rocksdb::DB>) -> Result<Self> {
        let manager = Self {
            db: db.clone(),
            archives: Tree::new(db)?,
            package_entries: Tree::new(db)?,
            block_handles: Tree::new(db)?,
            key_blocks: Tree::new(db)?,
        };

        manager.self_check()?;

        Ok(manager)
    }

    fn self_check(&self) -> Result<()> {
        let storage_cf = self.archives.get_cf()?;
        let mut iter = self.db.raw_iterator_cf(&storage_cf);
        iter.seek_to_first();

        while let (Some(key), value) = (iter.key(), iter.value()) {
            let archive_id = u64::from_be_bytes(
                key.try_into()
                    .with_context(|| format!("Invalid archive key: {}", hex::encode(key)))?,
            );

            let read = |value: &[u8]| {
                let mut verifier = ArchivePackageVerifier::default();
                verifier.verify(value)?;
                verifier.final_check()
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
        self.package_entries.insert(id.to_vec()?, data)?;
        Ok(())
    }

    pub fn has_data<I>(&self, id: &PackageEntryId<I>) -> Result<bool>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.package_entries.contains_key(id.to_vec()?)
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

        match self.package_entries.get(id.to_vec()?)? {
            Some(a) => Ok(a.to_vec()),
            None => Err(ArchiveManagerError::InvalidBlockData.into()),
        }
    }

    pub async fn get_data_ref<'a, I>(
        &'a self,
        handle: &'a BlockHandle,
        id: &PackageEntryId<I>,
    ) -> Result<impl AsRef<[u8]> + 'a>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        let lock = match id {
            PackageEntryId::Block(_) => handle.block_data_lock().read().await,
            PackageEntryId::Proof(_) | PackageEntryId::ProofLink(_) => {
                handle.proof_data_lock().read().await
            }
        };

        match self.package_entries.get(id.to_vec()?)? {
            Some(data) => Ok(BlockContentsLock { _lock: lock, data }),
            None => Err(ArchiveManagerError::InvalidBlockData.into()),
        }
    }

    pub fn gc(
        &self,
        max_blocks_per_batch: Option<usize>,
        top_blocks: &TopBlocks,
    ) -> Result<BlockGcStats> {
        let mut stats = BlockGcStats::default();

        // Cache cfs before loop
        let blocks_cf = self.package_entries.get_cf()?;
        let block_handles_cf = self.block_handles.get_cf()?;
        let key_blocks_cf = self.key_blocks.get_cf()?;
        let raw_db = self.package_entries.raw_db_handle().clone();

        // Create batch
        let mut batch = rocksdb::WriteBatch::default();
        let mut batch_len = 0;

        // Iterate all entries and find expired items
        let blocks_iter = self
            .package_entries
            .iterator(rocksdb::IteratorMode::Start)?;
        for (key, _) in blocks_iter {
            // Read only prefix with shard ident and seqno
            let prefix = PackageEntryIdPrefix::from_slice(key.as_ref())?;

            // Don't gc latest blocks
            if top_blocks.contains_shard_seq_no(&prefix.shard_ident, prefix.seq_no) {
                continue;
            }

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
            batch.delete_cf(&blocks_cf, &key);
            stats.total_package_entries_removed += 1;
            if prefix.shard_ident.is_masterchain() {
                stats.mc_package_entries_removed += 1;
            }

            // Key structure:
            // [workchain id, 4 bytes]
            // [shard id, 8 bytes]
            // [seqno, 4 bytes]
            // [root hash, 32 bytes] <-
            // ..
            if key.len() >= 48 {
                batch.delete_cf(&block_handles_cf, &key[16..48]);
                stats.total_handles_removed += 1;
            }

            batch_len += 1;
            if matches!(
                max_blocks_per_batch,
                Some(max_blocks_per_batch) if batch_len >= max_blocks_per_batch
            ) {
                log::info!(
                    "Applying intermediate batch {}...",
                    stats.total_package_entries_removed
                );
                let batch = std::mem::take(&mut batch);
                raw_db.write(batch)?;
                batch_len = 0;
            }
        }

        if batch_len > 0 {
            log::info!("Applying final batch...");
            raw_db.write(batch)?;
        }

        // Done
        Ok(stats)
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
        let storage_cf = self.archives.get_cf()?;
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
        let storage_cf = self.archives.get_cf()?;

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
        match self.archives.get(id.to_be_bytes())? {
            Some(slice) if offset < slice.len() => {
                let end = std::cmp::min(offset.saturating_add(limit), slice.len());
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
        match self.package_entries.get(entry_id.to_vec()?)? {
            Some(data) => make_archive_segment(&entry_id.filename(), &data).map_err(From::from),
            None => Err(ArchiveManagerError::InvalidBlockData.into()),
        }
    }
}

#[derive(Debug, Copy, Clone, Default)]
pub struct BlockGcStats {
    pub mc_package_entries_removed: usize,
    pub total_package_entries_removed: usize,
    pub total_handles_removed: usize,
}

struct BlockContentsLock<'a> {
    _lock: tokio::sync::RwLockReadGuard<'a, ()>,
    data: rocksdb::DBPinnableSlice<'a>,
}

impl<'a> AsRef<[u8]> for BlockContentsLock<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
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
