use std::borrow::Borrow;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use tokio::io::AsyncWriteExt;

use super::block_handle::*;
use super::package_entry_id::*;

pub struct ArchiveManager {
    temp_dir: Arc<PathBuf>,
}

impl ArchiveManager {
    pub async fn with_root_dir<P>(root_dir: &P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let temp_dir = Arc::new(root_dir.as_ref().join("temp"));
        tokio::fs::create_dir_all(temp_dir.as_ref()).await?;

        Ok(Self { temp_dir })
    }

    pub async fn add_file<I>(&self, id: &PackageEntryId<I>, data: &[u8]) -> Result<()>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        if data.is_empty() {
            return Err(ArchiveManagerError::EmptyData.into());
        }

        let filename = self.temp_dir.join(id.filename());
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(filename)
            .await?;

        file.write_all(data).await?;
        file.flush().await?;

        Ok(())
    }

    pub fn has_file<I>(&self, id: &PackageEntryId<I>) -> bool
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        self.temp_dir.join(id.filename()).exists()
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
        self.read_temp_file(id).await
    }

    async fn read_temp_file<I>(&self, id: &PackageEntryId<I>) -> Result<Vec<u8>>
    where
        I: Borrow<ton_block::BlockIdExt> + Hash,
    {
        let filename = self.temp_dir.join(id.filename());
        let data = match tokio::fs::read(&filename).await {
            Ok(data) => data,
            Err(e) => {
                return Err(match e.kind() {
                    std::io::ErrorKind::NotFound => ArchiveManagerError::FileNotFound.into(),
                    _ => ArchiveManagerError::FailedToReadFile.into(),
                })
            }
        };

        if data.is_empty() {
            return Err(ArchiveManagerError::InvalidFileData.into());
        }

        Ok(data)
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
