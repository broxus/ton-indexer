use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::utils::MappedFile;

pub struct FilesContext {
    cells_path: PathBuf,
    cells_file: Option<BufWriter<File>>,
    hashes_path: PathBuf,
}

impl FilesContext {
    pub async fn new<P>(downloads_dir: P, block_id: &ton_block::BlockIdExt) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let block_id = format!(
            "({},{:016x},{})",
            block_id.shard_id.workchain_id(),
            block_id.shard_id.shard_prefix_with_tag(),
            block_id.seq_no
        );

        let cells_path = downloads_dir
            .as_ref()
            .join(format!("state_cells_{block_id}"));
        let hashes_path = downloads_dir
            .as_ref()
            .join(format!("state_hashes_{block_id}"));

        let cells_file = Some(BufWriter::new(
            std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .read(true)
                .open(&cells_path)
                .context("Failed to create cells file")?,
        ));

        Ok(Self {
            cells_path,
            cells_file,
            hashes_path,
        })
    }

    pub fn cells_file(&mut self) -> Result<&mut BufWriter<File>> {
        match &mut self.cells_file {
            Some(file) => Ok(file),
            None => Err(FilesContextError::AlreadyFinalized.into()),
        }
    }

    pub fn create_mapped_hashes_file(&self, length: usize) -> Result<MappedFile> {
        let mapped_file = MappedFile::new(&self.hashes_path, length)?;
        Ok(mapped_file)
    }

    pub fn create_mapped_cells_file(&mut self) -> Result<MappedFile> {
        let mut file = match self.cells_file.take() {
            Some(file) => file.into_inner()?,
            None => return Err(FilesContextError::AlreadyFinalized.into()),
        };
        file.flush()?;

        let mapped_file = MappedFile::from_existing_file(file)?;
        Ok(mapped_file)
    }
}

impl Drop for FilesContext {
    fn drop(&mut self) {
        std::fs::remove_file(&self.cells_path).ok();
        std::fs::remove_file(&self.hashes_path).ok();
    }
}

#[derive(thiserror::Error, Debug)]
enum FilesContextError {
    #[error("Already finalized")]
    AlreadyFinalized,
}
