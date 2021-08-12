use std::borrow::Borrow;
use std::hash::Hash;

use anyhow::{Context, Result};
use rocksdb::DBPinnableSlice;
use tokio::io::AsyncWriteExt;
use ton_block::Serializable;

use crate::storage::columns::ArchiveManagerDb;
use crate::storage::Tree;

use super::block_handle::*;
use super::package_entry_id::*;

pub struct ArchiveManager {
    db: Tree<ArchiveManagerDb>,
}

impl ArchiveManager {
    pub fn with_db(db: Tree<ArchiveManagerDb>) -> Self {
        Self { db }
    }
    pub async fn add_file<I>(&self, id: &PackageEntryId<I>, data: &[u8]) -> Result<()>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.db
            .insert(id.block_id().write_to_bytes().unwrap(), data)?;
        Ok(())
    }

    pub fn has_file<I>(&self, id: &PackageEntryId<I>) -> bool
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.read_temp_file(id).is_ok()
    }

    pub async fn get_file<I>(
        &self,
        handle: &BlockHandle,
        id: &PackageEntryId<I>,
    ) -> Result<DBPinnableSlice<'_>>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        let _lock = match &id {
            PackageEntryId::Block(_) => handle.block_file_lock().read().await,
            PackageEntryId::Proof(_) | PackageEntryId::ProofLink(_) => {
                handle.proof_file_lock().read().await
            }
        };

        self.read_temp_file(id)
    }

    fn read_temp_file<I>(&self, id: &PackageEntryId<I>) -> Result<DBPinnableSlice<'_>>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        match self.db.get(id.block_id().write_to_bytes().unwrap())? {
            Some(a) => Ok(a),
            None => Err(ArchiveManagerError::InvalidFileData.into()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum ArchiveManagerError {
    #[error("Trying to write empty data")]
    EmptyData,
    #[error("File not found")]
    FileNotFound,
    #[error("Failed to read file")]
    FailedToReadFile,
    #[error("Invalid file data")]
    InvalidFileData,
}
