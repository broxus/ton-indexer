/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - replaced file storage with direct rocksdb storage
/// - removed all temporary unused code
///
use std::borrow::Borrow;
use std::hash::Hash;

use anyhow::Result;
use tokio::sync::RwLock;

use crate::storage::columns::ArchiveManagerDb;
use crate::storage::Tree;

use super::block_handle::*;
use super::package_entry_id::*;

pub struct ArchiveManager {
    db: RwLock<Tree<ArchiveManagerDb>>,
}

impl ArchiveManager {
    pub fn with_db(db: Tree<ArchiveManagerDb>) -> Self {
        Self {
            db: RwLock::new(db),
        }
    }

    pub async fn add_file<I>(&self, id: &PackageEntryId<I>, data: &[u8]) -> Result<()>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.db.read().await.insert(id.to_vec()?, data)?;
        Ok(())
    }

    pub async fn has_data<I>(&self, id: &PackageEntryId<I>) -> bool
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.read_block_data(id).await.is_ok()
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

        self.read_block_data(id).await
    }

    async fn read_block_data<I>(&self, id: &PackageEntryId<I>) -> Result<Vec<u8>>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        let lock = self.db.read().await;

        let res = match lock.get(id.to_vec()?)? {
            Some(a) => Ok(a.to_vec()),
            None => Err(ArchiveManagerError::InvalidFileData.into()),
        };
        res
    }

    pub async fn gc<'a>(
        &'a self,
        ids: impl Iterator<Item = &'a ton_block::BlockIdExt>,
    ) -> Result<()> {
        let lock = self.db.write().await;
        let cf = lock.get_cf()?;
        let mut tx = rocksdb::WriteBatch::default();
        for id in ids {
            let id1 = PackageEntryId::Block(id).to_vec()?;
            let id2 = PackageEntryId::Proof(id).to_vec()?;
            let id3 = PackageEntryId::ProofLink(id).to_vec()?;
            tx.delete_cf(&cf, id1);
            tx.delete_cf(&cf, id2);
            tx.delete_cf(&cf, id3);
        }
        let db = lock.raw_db_handle().clone();
        tokio::task::spawn_blocking(move || db.write(tx)).await??;
        Ok(())
    }

    pub async fn get_tot_size(&self) -> Result<usize> {
        self.db.read().await.size()
    }
}

#[derive(thiserror::Error, Debug)]
enum ArchiveManagerError {
    #[error("Invalid file data")]
    InvalidFileData,
}
