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
        self.read_block_data(id).is_ok()
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

        self.read_block_data(id)
    }

    fn read_block_data<I>(&self, id: &PackageEntryId<I>) -> Result<DBPinnableSlice<'_>>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        match self.db.get(id.to_vec()?)? {
            Some(a) => Ok(a),
            None => Err(ArchiveManagerError::InvalidFileData.into()),
        }
    }

    pub fn gc<'a>(&'a self, ids: impl Iterator<Item = &'a ton_block::BlockIdExt>) -> Result<()> {
        let cf = self.db.get_cf()?;
        let mut tx = rocksdb::WriteBatch::default();
        for id in ids {
            let id1 = PackageEntryId::Block(id).to_vec()?;
            let id2 = PackageEntryId::Proof(id).to_vec()?;
            let id3 = PackageEntryId::ProofLink(id).to_vec()?;
            tx.delete_cf(&cf, id1);
            tx.delete_cf(&cf, id2);
            tx.delete_cf(&cf, id3);
        }
        self.db.raw_db_handle().write(tx)?;
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
enum ArchiveManagerError {
    #[error("Invalid file data")]
    InvalidFileData,
}
