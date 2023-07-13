use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;

use crate::db::Db;
use crate::storage::persistent_state_storage::cell_writer::clear_temp;

mod cell_writer;

pub struct PersistentStateStorage {
    storage_path: PathBuf,
    db: Arc<Db>,
    is_cancelled: Arc<AtomicBool>,
}

impl PersistentStateStorage {
    pub async fn new(file_db_path: PathBuf, db: Arc<Db>) -> Result<Self> {
        let dir = file_db_path.join("states");
        tokio::fs::create_dir_all(&dir).await?;
        let is_cancelled = Arc::new(Default::default());

        Ok(Self {
            storage_path: dir,
            db,
            is_cancelled,
        })
    }

    pub async fn save_state(
        &self,
        state_root_hash: [u8; 32],
        block_root_hash: [u8; 32],
    ) -> Result<()> {
        let cell_hex = hex::encode(block_root_hash);

        let db = self.db.clone();
        let path = self.storage_path.clone();
        let clone = path.clone();
        let is_cancelled = self.is_cancelled.clone();

        let fut = tokio::task::spawn_blocking(move || {
            let cell_writer = cell_writer::CellWriter::new(&db, &clone);
            let path_opt = cell_writer.write(&state_root_hash, &block_root_hash, is_cancelled);
            match path_opt {
                Ok(path) => {
                    tracing::info!(
                        "Successfully wrote persistent {} state to a file: {} ",
                        cell_hex,
                        path.display()
                    );
                }
                Err(e) => {
                    tracing::error!("Writing persistent state {} failed. Err: {e:?}", cell_hex);
                    clear_temp(&path, &block_root_hash);
                }
            }
        });

        fut.await?;
        Ok(())
    }

    pub async fn read_state_part(
        &self,
        block_root_hash: &[u8; 32],
        offset: usize,
        size: usize,
    ) -> Option<Vec<u8>> {
        let hex = hex::encode(block_root_hash);
        let prefix = &hex[0..8];
        let postfix = &hex[8..];

        let dir = PathBuf::from(prefix);
        let file = PathBuf::from(postfix);

        let file_path = self.get_state_file_path(dir, file);
        let file = match File::open(file_path) {
            Ok(file) => file,
            Err(e) => {
                tracing::error!("Failed to find file to read part. Err: {e:?} ");
                return None;
            }
        };
        let mmap = match unsafe { memmap::Mmap::map(&file) } {
            Ok(nmap) => nmap,
            Err(e) => {
                tracing::error!("Failed to create file-memory mapping. Err: {e:?}");
                return None;
            }
        };

        let state_size = mmap.len();
        if offset > state_size {
            tracing::error!("Trying to read non-existent state slice");
            return None;
        }

        let length = core::cmp::min(size, state_size - offset);

        Some(Vec::from(&mmap[offset..offset + length]))
    }

    pub async fn state_exists(&self, block_root_hash: &[u8; 32]) -> bool {
        let hex = hex::encode(block_root_hash);
        let prefix = &hex[0..8];

        let dir = PathBuf::from(prefix);
        if let Err(e) = self.check_directory(dir.clone()).await {
            tracing::error!("Failed to check directory: {:?}. Err: {e:?}", prefix);
            return false;
        }

        let file_name = self.get_state_file_path(dir, PathBuf::from(hex));
        if let Ok(meta) = tokio::fs::metadata(file_name).await {
            meta.is_file()
        } else {
            false
        }
    }

    fn get_state_file_path(&self, dir: PathBuf, file: PathBuf) -> PathBuf {
        self.storage_path.join(dir).join(file)
    }

    async fn check_directory(&self, dir: PathBuf) -> Result<()> {
        let path = self.storage_path.join(dir);
        if tokio::fs::metadata(&path).await.is_err() {
            tokio::fs::create_dir(path).await?;
        }

        Ok(())
    }

    pub fn cancel(&self) {
        self.is_cancelled.store(true, Ordering::Release);
    }
}
