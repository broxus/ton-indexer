use std::fs;
use std::fs::DirEntry;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

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
        master_block_id: &ton_block::BlockIdExt,
        state_root_hash: &ton_types::UInt256,
    ) -> Result<()> {
        let block_id = block_id.clone();
        let master_block_id = master_block_id.clone();
        let state_root_hash = *state_root_hash;
        let db = self.db.clone();
        let base_path = self.storage_path.clone();
        let is_cancelled = self.is_cancelled.clone();

        tokio::task::spawn_blocking(move || {
            let cell_writer = CellWriter::new(&db, &base_path);
            match cell_writer.write(&master_block_id, &block_id, &state_root_hash, is_cancelled) {
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

                    CellWriter::clear_temp(&base_path, &master_block_id, &block_id);
                }
            }
        })
        .await
        .map_err(From::from)
    }

    pub async fn read_state_part(
        &self,
        mc_block_id: &ton_block::BlockIdExt,
        block_id: &ton_block::BlockIdExt,
        offset: u64,
        size: u64,
    ) -> Option<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

        // TODO: cache file handles
        let mut file = tokio::fs::File::open(self.get_state_file_path(mc_block_id, block_id))
            .await
            .ok()?;

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
                    tracing::debug!("Reading state file. Bytes read: {}", bytes_read);
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

    pub fn state_exists(
        &self,
        mc_block_id: &ton_block::BlockIdExt,
        block_id: &ton_block::BlockIdExt,
    ) -> bool {
        // TODO: cache file handles
        self.get_state_file_path(mc_block_id, block_id).is_file()
    }

    pub fn prepare_persistent_states_dir(&self, mc_block: &ton_block::BlockIdExt) -> Result<()> {
        let dir_path = mc_block.seq_no.to_string();
        let path = self.storage_path.join(dir_path);
        if !path.exists() {
            tracing::info!(mc_block = %mc_block.display(), "Creating persistent state directory");
            fs::create_dir(path)?;
        }
        Ok(())
    }

    fn get_state_file_path(
        &self,
        mc_block_id: &ton_block::BlockIdExt,
        block_id: &ton_block::BlockIdExt,
    ) -> PathBuf {
        CellWriter::make_pss_path(&self.storage_path, mc_block_id, block_id)
    }

    pub fn cancel(&self) {
        self.is_cancelled.store(true, Ordering::Release);
    }

    pub fn start_persistent_state_gc(&self) {
        let base_path = self.storage_path.clone();
        let interval = Duration::from_secs(360);
        let fallback_interval = Duration::from_secs(60);
        tokio::spawn(async move {
            loop {
                let now = Instant::now();
                tracing::info!("Running persistent state storage cleanup");
                let paths = match fs::read_dir(&base_path) {
                    Ok(paths) => paths,
                    Err(e) => {
                        tracing::error!(path = %base_path.display(), "Failed to read base_path directory. Err: {e:?}");
                        tokio::time::sleep(fallback_interval).await;
                        continue;
                    }
                };

                for path in paths {
                    let entry = match path {
                        Ok(entry) => entry,
                        Err(e) => {
                            tracing::error!("Failed to get entry. Err: {e:?}");
                            tokio::time::sleep(fallback_interval).await;
                            continue;
                        }
                    };

                    if let Err(e) = Self::process_entry(&entry) {
                        tracing::error!(entry = %entry.path().display(), "Failed to process entry. Err: {e:?}");
                        tokio::time::sleep(fallback_interval).await;
                        continue;
                    }
                }

                tracing::info!(
                    elapsed_ms = now.elapsed().as_millis(),
                    "Persistent state storage cleanup complete"
                );

                tokio::time::sleep(interval).await;
            }
        });
    }

    fn process_entry(entry: &DirEntry) -> Result<()> {
        let metadata = entry.metadata()?;
        let created = metadata.created()?;
        let now = SystemTime::now();
        let offset = now.duration_since(created)?;

        if offset > Duration::from_secs(86400 * 2) && metadata.is_dir() {
            fs::remove_dir(entry.path())?
        }

        Ok(())
    }
}
