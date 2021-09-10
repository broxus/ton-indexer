use std::borrow::Borrow;
use std::hash::Hash;

use anyhow::Result;

use crate::storage::columns::ArchiveManagerDb;
use crate::storage::Tree;

use super::block_handle::*;
use super::package_entry_id::*;
use tokio::sync::RwLock;

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

    pub async fn has_file<I>(&self, id: &PackageEntryId<I>) -> bool
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.read_block_data(id).await.is_ok()
    }

    pub async fn get_file<I>(&self, handle: &BlockHandle, id: &PackageEntryId<I>) -> Result<Vec<u8>>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        let _lock = match &id {
            PackageEntryId::Block(_) => handle.block_file_lock().read().await,
            PackageEntryId::Proof(_) | PackageEntryId::ProofLink(_) => {
                handle.proof_file_lock().read().await
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
        let mut tot = 0;
        for id in ids {
            let id1 = PackageEntryId::Block(id).to_vec()?;
            let id2 = PackageEntryId::Proof(id).to_vec()?;
            let id3 = PackageEntryId::ProofLink(id).to_vec()?;
            if lock.contains_key(&id1)? {
                tot += 1;
            }
            if lock.contains_key(&id2)? {
                tot += 1;
            }
            if lock.contains_key(&id3)? {
                tot += 1;
            }
            tx.delete_cf(&cf, id1);
            tx.delete_cf(&cf, id2);
            tx.delete_cf(&cf, id3);
        }
        log::warn!("Tot: {}", tot);
        let db = lock.raw_db_handle().clone();
        tokio::task::spawn_blocking(move || db.write(tx)).await??;
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
enum ArchiveManagerError {
    #[error("Invalid file data")]
    InvalidFileData,
}
