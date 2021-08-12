use std::borrow::Borrow;
use std::hash::Hash;

use anyhow::Result;
use rocksdb::DBPinnableSlice;

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
        self.db.insert(id.to_vec()?, data)?;
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
        match self.db.get(id.to_vec()?)? {
            Some(a) => Ok(a),
            None => Err(ArchiveManagerError::InvalidFileData.into()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum ArchiveManagerError {
    #[error("Invalid file data")]
    InvalidFileData,
}
