use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use tokio::time::Instant;

use self::cell_writer::*;
use crate::db::Db;
use crate::utils::*;

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
        block_id: &ton_block::BlockIdExt,
        state_root_hash: &ton_types::UInt256,
    ) -> Result<()> {
        let block_id = block_id.clone();
        let state_root_hash = *state_root_hash;
        let db = self.db.clone();
        let base_path = self.storage_path.clone();
        let is_cancelled = self.is_cancelled.clone();

        tokio::task::spawn_blocking(move || {
            let cell_writer = CellWriter::new(&db, &base_path);
            match cell_writer.write(&block_id, &state_root_hash, is_cancelled) {
                Ok(path) => {
                    tracing::info!(
                        block_id = %block_id.display(),
                        path = %path.display(),
                        "Successfully wrote persistent state to a file",
                    );
                }
                Err(e) => {
                    tracing::error!(
                        block_id = %block_id.display(),
                        "Writing persistent state failed. Err: {e:?}"
                    );

                    CellWriter::clear_temp(&base_path, &block_id);
                }
            }
        })
        .await
        .map_err(From::from)
    }

    pub async fn read_state_part(
        &self,
        block_id: &ton_block::BlockIdExt,
        offset: u64,
        size: u64,
    ) -> Option<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

        // TODO: cache file handles
        let mut file = tokio::fs::File::open(self.get_state_file_path(block_id))
            .await
            .ok()?;

        tracing::info!("Opened state file");

        if let Err(e) = file.seek(SeekFrom::Start(offset)).await {
            tracing::error!("Failed to seek state file offset. Err: {e:?}");
            return None;
        }

        // SAFETY: size must be checked
        let mut result = BytesMut::with_capacity(size as usize);
        let now = Instant::now();
        loop {
            match file.read_buf(&mut result).await {
                Ok(bytes_read) => {
                    tracing::info!("Reading state file. Bytes read: {}", bytes_read);
                    if bytes_read == 0 || bytes_read == size as usize {
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to read state file. Err: {e:?}");
                    return None;
                }
            }
        }
        tracing::info!(
            "Finished reading buffer after: {} ms",
            now.elapsed().as_millis()
        );

        // TODO: use `Bytes`
        Some(result.to_vec())
    }

    pub fn state_exists(&self, block_id: &ton_block::BlockIdExt) -> bool {
        // TODO: cache file handles
        self.get_state_file_path(block_id).is_file()
    }

    fn get_state_file_path(&self, block_id: &ton_block::BlockIdExt) -> PathBuf {
        CellWriter::make_pss_path(&self.storage_path, block_id)
    }

    pub fn cancel(&self) {
        self.is_cancelled.store(true, Ordering::Release);
    }
}
