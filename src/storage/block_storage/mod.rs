/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - replaced file storage with direct rocksdb storage
/// - removed all temporary unused code
///
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::convert::TryInto;
use std::hash::Hash;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::models::*;
use parking_lot::RwLock;

use super::block_handle_storage::{BlockHandleStorage, HandleCreationStatus};
use super::models::*;
use crate::config::BlocksGcKind;
use crate::db::*;
use crate::utils::*;

pub struct BlockStorage {
    db: Arc<Db>,
    block_handle_storage: Arc<BlockHandleStorage>,
    archive_ids: RwLock<BTreeSet<u32>>,
}

impl BlockStorage {
    pub fn new(db: Arc<Db>, block_handle_storage: Arc<BlockHandleStorage>) -> Result<Self> {
        let manager = Self {
            db,
            block_handle_storage,
            archive_ids: Default::default(),
        };

        manager.preload()?;

        Ok(manager)
    }

    fn preload(&self) -> Result<()> {
        fn check_archive(value: &[u8]) -> Result<(), ArchivePackageError> {
            let mut verifier = ArchivePackageVerifier::default();
            verifier.verify(value)?;
            verifier.final_check()
        }

        let mut iter = self.db.archives.raw_iterator();
        iter.seek_to_first();

        let mut archive_ids = self.archive_ids.write();

        while let (Some(key), value) = (iter.key(), iter.value()) {
            let archive_id = u32::from_be_bytes(
                key.try_into()
                    .with_context(|| format!("Invalid archive key: {}", hex::encode(key)))?,
            );

            if let Some(Err(e)) = value.map(check_archive) {
                tracing::error!(archive_id, "failed to read archive: {e:?}")
            }

            archive_ids.insert(archive_id);
            iter.next();
        }

        tracing::info!("selfcheck complete");
        Ok(())
    }

    pub async fn store_block_data(
        &self,
        block: &BlockStuffAug,
        meta_data: BlockMetaData,
    ) -> Result<StoreBlockResult> {
        let block_id = block.id();
        let (handle, status) = self
            .block_handle_storage
            .create_or_load_handle(block_id, meta_data)?;

        let archive_id = PackageEntryId::Block(block_id);
        let mut updated = false;
        if !handle.meta().has_data() {
            let data = block.new_archive_data()?;

            let _lock = handle.block_data_lock().write().await;
            if !handle.meta().has_data() {
                self.add_data(&archive_id, data)?;
                if handle.meta().set_has_data() {
                    self.block_handle_storage.store_handle(&handle)?;
                    updated = true;
                }
            }
        }

        Ok(StoreBlockResult {
            handle,
            updated,
            new: status == HandleCreationStatus::Created,
        })
    }

    pub async fn load_block_data(&self, handle: &BlockHandle) -> Result<BlockStuff> {
        let raw_block = self.load_block_data_raw_ref(handle).await?;
        BlockStuff::deserialize(handle.id().clone(), raw_block.as_ref())
    }

    pub async fn load_block_data_raw(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
        if !handle.meta().has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }
        self.get_data(handle, &PackageEntryId::Block(handle.id()))
            .await
    }

    pub async fn load_block_data_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        if !handle.meta().has_data() {
            return Err(BlockStorageError::BlockDataNotFound.into());
        }
        self.get_data_ref(handle, &PackageEntryId::Block(handle.id()))
            .await
    }

    pub async fn store_block_proof(
        &self,
        proof: &BlockProofStuffAug,
        handle: BlockProofHandle,
    ) -> Result<StoreBlockResult> {
        let block_id = proof.id();
        if matches!(&handle, BlockProofHandle::Existing(handle) if handle.id() != block_id) {
            return Err(BlockStorageError::BlockHandleIdMismatch.into());
        }

        let (handle, status) = match handle {
            BlockProofHandle::Existing(handle) => (handle, HandleCreationStatus::Fetched),
            BlockProofHandle::New(meta_data) => self
                .block_handle_storage
                .create_or_load_handle(block_id, meta_data)?,
        };

        let mut updated = false;
        if proof.is_link() {
            let archive_id = PackageEntryId::ProofLink(block_id);
            if !handle.meta().has_proof_link() {
                let data = proof.new_archive_data()?;

                let _lock = handle.proof_data_lock().write().await;
                if !handle.meta().has_proof_link() {
                    self.add_data(&archive_id, data)?;
                    if handle.meta().set_has_proof_link() {
                        self.block_handle_storage.store_handle(&handle)?;
                        updated = true;
                    }
                }
            }
        } else {
            let archive_id = PackageEntryId::Proof(block_id);
            if !handle.meta().has_proof() {
                let data = proof.new_archive_data()?;

                let _lock = handle.proof_data_lock().write().await;
                if !handle.meta().has_proof() {
                    self.add_data(&archive_id, data)?;
                    if handle.meta().set_has_proof() {
                        self.block_handle_storage.store_handle(&handle)?;
                        updated = true;
                    }
                }
            }
        }

        Ok(StoreBlockResult {
            handle,
            updated,
            new: status == HandleCreationStatus::Created,
        })
    }

    pub async fn load_block_proof(
        &self,
        handle: &BlockHandle,
        is_link: bool,
    ) -> Result<BlockProofStuff> {
        let raw_proof = self.load_block_proof_raw_ref(handle, is_link).await?;
        BlockProofStuff::deserialize(handle.id().clone(), raw_proof.as_ref(), is_link)
    }

    pub async fn load_block_proof_raw(
        &self,
        handle: &BlockHandle,
        is_link: bool,
    ) -> Result<Vec<u8>> {
        let (archive_id, exists) = if is_link {
            (
                PackageEntryId::ProofLink(handle.id()),
                handle.meta().has_proof_link(),
            )
        } else {
            (
                PackageEntryId::Proof(handle.id()),
                handle.meta().has_proof(),
            )
        };

        if !exists {
            return Err(BlockStorageError::BlockProofNotFound.into());
        }

        self.get_data(handle, &archive_id).await
    }

    pub async fn load_block_proof_raw_ref<'a>(
        &'a self,
        handle: &'a BlockHandle,
        is_link: bool,
    ) -> Result<impl AsRef<[u8]> + 'a> {
        let (archive_id, exists) = if is_link {
            (
                PackageEntryId::ProofLink(handle.id()),
                handle.meta().has_proof_link(),
            )
        } else {
            (
                PackageEntryId::Proof(handle.id()),
                handle.meta().has_proof(),
            )
        };

        if !exists {
            return Err(BlockStorageError::BlockProofNotFound.into());
        }

        self.get_data_ref(handle, &archive_id).await
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
        let storage_cf = self.db.archives.cf();
        let handle_cf = self.db.block_handles.cf();

        // Prepare archive
        let archive_id = self.compute_archive_id(handle);
        let archive_id_bytes = archive_id.to_be_bytes();

        // 0. Create transaction
        let mut batch = rocksdb::WriteBatch::default();
        // 1. Append archive segment with block data
        if let Some((_, data)) = &block_data {
            batch.merge_cf(&storage_cf, archive_id_bytes, data);
        }
        // 2. Append archive segment with block proof data
        if let Some((_, data)) = &block_proof_data {
            batch.merge_cf(&storage_cf, archive_id_bytes, data);
        }
        // 3. Update block handle meta
        if handle.meta().set_is_archived() {
            batch.put_cf(
                &handle_cf,
                block_id.root_hash.as_slice(),
                handle.meta().to_vec(),
            );
        }
        // 5. Execute transaction
        self.db.raw().write(batch)?;

        // Block will be removed after blocks gc

        // Done
        Ok(())
    }

    pub fn move_into_archive_with_data(
        &self,
        handle: &BlockHandle,
        is_link: bool,
        block_data: &[u8],
        block_proof_data: &[u8],
    ) -> Result<()> {
        if handle.meta().is_archived() {
            return Ok(());
        }
        if !handle.meta().set_is_moving_to_archive() {
            return Ok(());
        }

        let block_id = handle.id();

        // Prepare cf
        let archives_cf = self.db.archives.cf();
        let block_handles_cf = self.db.block_handles.cf();

        // Prepare archive
        let archive_id = self.compute_archive_id(handle);
        let archive_id_bytes = archive_id.to_be_bytes();

        let mut batch = rocksdb::WriteBatch::default();

        batch.merge_cf(
            &archives_cf,
            archive_id_bytes,
            make_archive_segment(&PackageEntryId::Block(handle.id()).filename(), block_data),
        );

        batch.merge_cf(
            &archives_cf,
            archive_id_bytes,
            make_archive_segment(
                &if is_link {
                    PackageEntryId::ProofLink(block_id)
                } else {
                    PackageEntryId::Proof(block_id)
                }
                .filename(),
                block_proof_data,
            ),
        );

        if handle.meta().set_is_archived() {
            batch.put_cf(
                &block_handles_cf,
                block_id.root_hash.as_slice(),
                handle.meta().to_vec(),
            );
        }

        self.db.raw().write(batch)?;

        Ok(())
    }

    pub fn get_archive_id(&self, mc_seq_no: u32) -> Option<u32> {
        match self.archive_ids.read().range(..=mc_seq_no).next_back() {
            // NOTE: handles case when mc_seq_no is far in the future.
            // However if there is a key block between `id` and `mc_seq_no`,
            // this will return an archive without that specified block.
            Some(id) if mc_seq_no < id + ARCHIVE_PACKAGE_SIZE => Some(*id),
            _ => None,
        }
    }

    #[allow(unused)]
    pub fn get_archives(
        &self,
        range: impl RangeBounds<u32> + 'static,
    ) -> impl Iterator<Item = (u32, Vec<u8>)> + '_ {
        struct ArchivesIterator<'a> {
            first: bool,
            ids: (Bound<u32>, Bound<u32>),
            iter: rocksdb::DBRawIterator<'a>,
        }

        impl<'a> Iterator for ArchivesIterator<'a> {
            type Item = (u32, Vec<u8>);

            fn next(&mut self) -> Option<Self::Item> {
                if self.first {
                    match self.ids.0 {
                        Bound::Included(id) => {
                            self.iter.seek(id.to_be_bytes());
                        }
                        Bound::Excluded(id) => {
                            self.iter.seek((id + 1).to_be_bytes());
                        }
                        Bound::Unbounded => {
                            self.iter.seek_to_first();
                        }
                    }
                    self.first = false;
                } else {
                    self.iter.next();
                }

                match (self.iter.key(), self.iter.value()) {
                    (Some(key), Some(value)) => {
                        let id = u32::from_be_bytes(key.try_into().unwrap_or_default());
                        match self.ids.1 {
                            Bound::Included(bound_id) if id > bound_id => None,
                            Bound::Excluded(bound_id) if id >= bound_id => None,
                            _ => Some((id, value.to_vec())),
                        }
                    }
                    _ => None,
                }
            }
        }

        ArchivesIterator {
            first: true,
            ids: (range.start_bound().cloned(), range.end_bound().cloned()),
            iter: self.db.archives.raw_iterator(),
        }
    }

    pub fn get_archive_slice(
        &self,
        id: u32,
        offset: usize,
        limit: usize,
    ) -> Result<Option<Vec<u8>>> {
        match self.db.archives.get(id.to_be_bytes())? {
            Some(slice) if offset < slice.len() => {
                let end = std::cmp::min(offset.saturating_add(limit), slice.len());
                Ok(Some(slice[offset..end].to_vec()))
            }
            Some(_) => Err(BlockStorageError::InvalidOffset.into()),
            None => Ok(None),
        }
    }

    pub async fn remove_outdated_blocks(
        &self,
        key_block_id: &BlockId,
        max_blocks_per_batch: Option<usize>,
        gc_type: BlocksGcKind,
    ) -> Result<()> {
        let _compaction_guard = self.db.delay_compaction().await;

        // Find target block
        let target_block = match gc_type {
            BlocksGcKind::BeforePreviousKeyBlock => self
                .block_handle_storage
                .find_prev_key_block(key_block_id.seq_no)?,
            BlocksGcKind::BeforePreviousPersistentState => self
                .block_handle_storage
                .find_prev_persistent_key_block(key_block_id.seq_no)?,
        };

        // Load target block data
        let top_blocks = match target_block {
            Some(handle) if handle.meta().has_data() => {
                tracing::info!(
                    key_block_id = %key_block_id.display(),
                    target_block_id = %handle.id().display(),
                    "starting blocks GC",
                );
                self.load_block_data(&handle)
                    .await
                    .context("Failed to load target key block data")
                    .and_then(|block_data| TopBlocks::from_mc_block(&block_data))
                    .context("Failed to compute top blocks for target block")?
            }
            _ => {
                tracing::info!(
                    key_block_id = %key_block_id.display(),
                    "blocks GC skipped"
                );
                return Ok(());
            }
        };

        // Remove all expired entries
        let total_cached_handles_removed = self.block_handle_storage.gc_handles_cache(&top_blocks);

        let db = self.db.clone();
        let BlockGcStats {
            mc_package_entries_removed,
            total_package_entries_removed,
            total_handles_removed,
        } = tokio::task::spawn_blocking(move || {
            remove_blocks(db, max_blocks_per_batch, &top_blocks)
        })
        .await??;

        tracing::info!(
            key_block_id = %key_block_id.display(),
            total_cached_handles_removed,
            mc_package_entries_removed,
            total_package_entries_removed,
            total_handles_removed,
            "finished blocks GC"
        );

        // Done
        Ok(())
    }

    pub async fn remove_outdated_archives(&self, until_id: u32) -> Result<()> {
        let _compaction_guard = self.db.delay_compaction().await;

        let mut archive_ids = self.archive_ids.write();

        let retained_ids = match archive_ids.iter().rev().find(|&id| *id < until_id).cloned() {
            // Splits `archive_ids` into two parts - [..until_id] and [until_id..]
            // `archive_ids` will now contain [..until_id]
            Some(until_id) => archive_ids.split_off(&until_id),
            None => {
                tracing::info!("archives GC: nothing to remove");
                return Ok(());
            }
        };
        // so we must swap maps to retain [until_id..] and get ids to remove
        let removed_ids = std::mem::replace(&mut *archive_ids, retained_ids);

        // Print removed range bounds and compute real `until_id`
        let until_id = match (removed_ids.first(), removed_ids.last()) {
            (Some(first), Some(last)) => {
                let len = removed_ids.len();
                tracing::info!(
                    archive_count = len,
                    first,
                    last,
                    "archives GC: removing archives"
                );

                match archive_ids.first() {
                    Some(until_id) => *until_id,
                    None => *last + 1,
                }
            }
            _ => {
                tracing::info!("archives GC: nothing to remove");
                return Ok(());
            }
        };

        // Remove archives
        let archives_cf = self.db.archives.cf();
        let write_options = self.db.archives.write_config();

        self.db.raw().delete_range_cf_opt(
            &archives_cf,
            [0; 4],
            until_id.to_be_bytes(),
            write_options,
        )?;

        tracing::info!("archives GC: done");
        Ok(())
    }

    fn add_data<I>(&self, id: &PackageEntryId<I>, data: &[u8]) -> Result<(), rocksdb::Error>
    where
        I: Borrow<BlockId> + Hash,
    {
        self.db.package_entries.insert(id.to_vec(), data)
    }

    #[allow(dead_code)]
    fn has_data<I>(&self, id: &PackageEntryId<I>) -> Result<bool, rocksdb::Error>
    where
        I: Borrow<BlockId> + Hash,
    {
        self.db.package_entries.contains_key(id.to_vec())
    }

    async fn get_data<I>(&self, handle: &BlockHandle, id: &PackageEntryId<I>) -> Result<Vec<u8>>
    where
        I: Borrow<BlockId> + Hash,
    {
        let _lock = match &id {
            PackageEntryId::Block(_) => handle.block_data_lock().read().await,
            PackageEntryId::Proof(_) | PackageEntryId::ProofLink(_) => {
                handle.proof_data_lock().read().await
            }
        };

        match self.db.package_entries.get(id.to_vec())? {
            Some(a) => Ok(a.to_vec()),
            None => Err(BlockStorageError::InvalidBlockData.into()),
        }
    }

    async fn get_data_ref<'a, I>(
        &'a self,
        handle: &'a BlockHandle,
        id: &PackageEntryId<I>,
    ) -> Result<impl AsRef<[u8]> + 'a>
    where
        I: Borrow<BlockId> + Hash,
    {
        let lock = match id {
            PackageEntryId::Block(_) => handle.block_data_lock().read().await,
            PackageEntryId::Proof(_) | PackageEntryId::ProofLink(_) => {
                handle.proof_data_lock().read().await
            }
        };

        match self.db.package_entries.get(id.to_vec())? {
            Some(data) => Ok(BlockContentsLock { _lock: lock, data }),
            None => Err(BlockStorageError::InvalidBlockData.into()),
        }
    }

    fn compute_archive_id(&self, handle: &BlockHandle) -> u32 {
        let mc_seq_no = handle.masterchain_ref_seqno();

        if handle.meta().is_key_block() {
            self.archive_ids.write().insert(mc_seq_no);
            return mc_seq_no;
        }

        let mut archive_id = mc_seq_no - mc_seq_no % ARCHIVE_SLICE_SIZE;

        let prev_id = {
            let latest_archives = self.archive_ids.read();
            latest_archives.range(..=mc_seq_no).next_back().cloned()
        };

        if let Some(prev_id) = prev_id {
            if archive_id < prev_id {
                archive_id = prev_id;
            }
        }

        if mc_seq_no.saturating_sub(archive_id) >= ARCHIVE_PACKAGE_SIZE {
            self.archive_ids.write().insert(mc_seq_no);
            archive_id = mc_seq_no;
        }

        archive_id
    }

    fn make_archive_segment<I>(&self, entry_id: &PackageEntryId<I>) -> Result<Vec<u8>>
    where
        I: Borrow<BlockId> + Hash,
    {
        match self.db.package_entries.get(entry_id.to_vec())? {
            Some(data) => Ok(make_archive_segment(&entry_id.filename(), &data)),
            None => Err(BlockStorageError::InvalidBlockData.into()),
        }
    }
}

#[derive(Clone)]
pub enum BlockProofHandle {
    Existing(Arc<BlockHandle>),
    New(BlockMetaData),
}

impl From<Arc<BlockHandle>> for BlockProofHandle {
    fn from(handle: Arc<BlockHandle>) -> Self {
        Self::Existing(handle)
    }
}

impl From<BlockMetaData> for BlockProofHandle {
    fn from(meta_data: BlockMetaData) -> Self {
        Self::New(meta_data)
    }
}

pub struct StoreBlockResult {
    pub handle: Arc<BlockHandle>,
    pub updated: bool,
    pub new: bool,
}

fn remove_blocks(
    db: Arc<Db>,
    max_blocks_per_batch: Option<usize>,
    top_blocks: &TopBlocks,
) -> Result<BlockGcStats> {
    let mut stats = BlockGcStats::default();

    let raw = db.raw().as_ref();
    let package_entries_cf = db.package_entries.cf();
    let block_handles_cf = db.block_handles.cf();
    let key_blocks_cf = db.key_blocks.cf();

    // Create batch
    let mut batch = rocksdb::WriteBatch::default();
    let mut batch_len = 0;

    let package_entries_readopts = db.package_entries.new_read_config();
    let key_blocks_readopts = db.key_blocks.new_read_config();

    // Iterate all entries and find expired items
    let mut blocks_iter = raw.raw_iterator_cf_opt(&package_entries_cf, package_entries_readopts);
    blocks_iter.seek_to_first();

    loop {
        let key = match blocks_iter.key() {
            Some(key) => key,
            None => break blocks_iter.status()?,
        };

        // Read only prefix with shard ident and seqno
        let (shard_ident, seq_no) = BlockIdShort::deserialize(&mut std::convert::identity(key))?;

        // Don't gc latest blocks
        if top_blocks.contains_shard_seq_no(&shard_ident, seq_no) {
            blocks_iter.next();
            continue;
        }

        // Additionally check whether this item is a key block
        if seq_no == 0
            || shard_ident.is_masterchain()
                && raw
                    .get_pinned_cf_opt(&key_blocks_cf, seq_no.to_be_bytes(), &key_blocks_readopts)?
                    .is_some()
        {
            // Don't remove key blocks
            blocks_iter.next();
            continue;
        }

        // Add item to the batch
        batch.delete_cf(&package_entries_cf, key);
        stats.total_package_entries_removed += 1;
        if shard_ident.is_masterchain() {
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
            tracing::info!(
                total_package_entries_removed = stats.total_package_entries_removed,
                "applying intermediate batch",
            );
            let batch = std::mem::take(&mut batch);
            raw.write(batch)?;
            batch_len = 0;
        }

        blocks_iter.next();
    }

    if batch_len > 0 {
        tracing::info!("applying final batch");
        raw.write(batch)?;
    }

    // Done
    Ok(stats)
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
enum BlockStorageError {
    #[error("Block data not found")]
    BlockDataNotFound,
    #[error("Block proof not found")]
    BlockProofNotFound,
    #[error("Block handle id mismatch")]
    BlockHandleIdMismatch,
    #[error("Invalid block data")]
    InvalidBlockData,
    #[error("Offset is outside of the archive slice")]
    InvalidOffset,
}
