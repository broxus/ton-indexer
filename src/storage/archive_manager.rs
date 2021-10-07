/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - replaced file storage with direct rocksdb storage
/// - removed all temporary unused code
///
use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::Result;
use nekoton_utils::*;
use serde::{Deserialize, Serialize};

use super::archive_package::*;
use super::block_handle::*;
use super::package_entry_id::*;
use crate::storage::{columns, StoredValue, Tree};
use crate::Engine;

pub struct ArchiveManager {
    db: Tree<columns::ArchiveManagerDb>,
    archive_storage: Tree<columns::ArchiveStorage>,
    block_handles: Tree<columns::BlockHandles>,
    archive_meta: Tree<columns::ArchiveMeta>,
}

impl ArchiveManager {
    pub fn with_db(db: &Arc<rocksdb::DB>) -> Result<Self> {
        Ok(Self {
            db: Tree::new(db)?,
            archive_storage: Tree::new(db)?,
            block_handles: Tree::new(db)?,
            archive_meta: Tree::new(db)?,
        })
    }

    pub fn add_data<I>(&self, id: &PackageEntryId<I>, data: &[u8]) -> Result<()>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.db.insert(id.to_vec()?, data)?;
        Ok(())
    }

    pub fn has_data<I>(&self, id: &PackageEntryId<I>) -> Result<bool>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.db.contains_key(id.to_vec()?)
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

        match self.db.get(id.to_vec()?)? {
            Some(a) => Ok(a.to_vec()),
            None => Err(ArchiveManagerError::InvalidBlockData.into()),
        }
    }

    pub async fn gc<'a>(
        &'a self,
        ids: impl Iterator<Item = &'a ton_block::BlockIdExt>,
    ) -> Result<()> {
        let cf = self.db.get_cf()?;
        let raw_db = self.db.raw_db_handle().clone();

        let mut tx = rocksdb::WriteBatch::default();
        for id in ids {
            let id1 = PackageEntryId::Block(id).to_vec()?;
            let id2 = PackageEntryId::Proof(id).to_vec()?;
            let id3 = PackageEntryId::ProofLink(id).to_vec()?;
            tx.delete_cf(&cf, id1);
            tx.delete_cf(&cf, id2);
            tx.delete_cf(&cf, id3);
        }

        tokio::task::spawn_blocking(move || raw_db.write(tx)).await??;
        Ok(())
    }

    pub async fn get_total_size(&self) -> Result<usize> {
        self.db.size()
    }

    pub fn add_entry(&self, handle: &BlockHandle) -> Result<()> {
        let data = &[]; // temp

        // Prepare cf
        let archive_cf = self.archive_storage.get_cf()?;
        let handle_cf = self.block_handles.get_cf()?;
        let meta_cf = self.archive_meta.get_cf()?;

        // Prepare archive
        let archive_id =
            calculate_archive_id(handle.masterchain_ref_seqno(), handle.meta().is_key_block());
        let archive_id_bytes = archive_id.to_be_bytes();

        let archive_data = self.prepare_archive_data(&archive_id_bytes, handle, data)?;

        let mut batch = rocksdb::WriteBatch::default();
        batch.merge_cf(&archive_cf, &archive_id_bytes, archive_data.as_bytes());
        batch.merge_cf(&meta_cf, &archive_id_bytes, &[]);
        if handle.meta().set_is_archived() {
            batch.put_cf(
                &handle_cf,
                handle.id().root_hash.as_slice(),
                handle.meta().to_vec()?,
            );
        }
        self.archive_storage.raw_db_handle().write(batch)?;

        Ok(())
    }

    /// Calculates archive id for `FullNodeOverlayService`
    pub fn get_archive_id(&self, engine: &Engine, mc_seq_no: u32) -> Result<u64> {
        let is_key_block = engine.find_key_block_id(mc_seq_no)?.is_some();
        let archive_id = calculate_archive_id(mc_seq_no, is_key_block);

        let meta: ArchiveMetaEntry = match self.archive_meta.get(archive_id.to_be_bytes())? {
            Some(data) => bincode::deserialize(&data)?,
            None => return Err(ArchiveManagerError::ArchiveMetaNotFound.into()),
        };

        if meta.finalized {
            Ok(archive_id)
        } else {
            Err(ArchiveManagerError::ArchiveNotFinalized.into())
        }
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

    fn prepare_archive_data(
        &self,
        id: &[u8],
        block: &BlockHandle,
        data: &[u8],
    ) -> Result<PackageWriter> {
        Ok(match self.archive_storage.get(id)? {
            Some(archive) => {
                let mut archive = PackageWriter::from_bytes(&archive);
                archive.add_package_segment(&block.id().filename(), data);
                archive
            }
            None => PackageWriter::with_segment(&block.id().filename(), data),
        })
    }
}

#[derive(Default, Copy, Clone, Serialize, Deserialize)]
pub struct ArchiveMetaEntry {
    pub deleted: bool,
    pub finalized: bool,
    pub num_blobs: u32,
}

impl ArchiveMetaEntry {
    pub fn with_data(deleted: bool, finalized: bool, num_blobs: u32) -> Self {
        Self {
            deleted,
            finalized,
            num_blobs,
        }
    }

    pub fn add_blobs(&mut self, count: usize) {
        self.num_blobs += count as u32;
        if self.num_blobs >= ARCHIVE_PACKAGE_SIZE {}
    }

    pub fn to_vec(&self) -> Vec<u8> {
        bincode::serialize(&self).trust_me()
    }
}

fn calculate_archive_id(mc_seq_no: u32, key_block: bool) -> u64 {
    let seq_no = if key_block {
        mc_seq_no
    } else {
        mc_seq_no - (mc_seq_no % ARCHIVE_SLICE_SIZE as u32)
    };
    seq_no as u64
}

pub const ARCHIVE_PACKAGE_SIZE: u32 = 100;
pub const ARCHIVE_SLICE_SIZE: u32 = 20_000;

#[derive(thiserror::Error, Debug)]
enum ArchiveManagerError {
    #[error("Invalid block data")]
    InvalidBlockData,
    #[error("Offset is outside of the archive slice")]
    InvalidOffset,
    #[error("Archive meta not found")]
    ArchiveMetaNotFound,
    #[error("Archive not finalized")]
    ArchiveNotFinalized,
}
