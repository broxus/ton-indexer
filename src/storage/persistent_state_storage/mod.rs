use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::BytesMut;
use tokio::time::Instant;

use self::cell_writer::*;
use crate::db::Db;
use crate::storage::BlockHandleStorage;
use crate::utils::*;

mod cell_writer;

pub struct PersistentStateStorage {
    block_handle_storage: Arc<BlockHandleStorage>,
    storage_path: PathBuf,
    db: Arc<Db>,
    is_cancelled: Arc<AtomicBool>,
}

impl PersistentStateStorage {
    pub async fn new(
        file_db_path: PathBuf,
        db: Arc<Db>,
        block_handle_storage: Arc<BlockHandleStorage>,
    ) -> Result<Self> {
        let dir = file_db_path.join("states");
        tokio::fs::create_dir_all(&dir).await?;
        let is_cancelled = Arc::new(Default::default());

        Ok(Self {
            block_handle_storage,
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

    pub async fn clear_old_persistent_states(&self) -> Result<()> {
        tracing::info!("Started clearing old persistent state directories");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        let mut current_block = self.block_handle_storage.find_last_key_block()?;

        let block = loop {
            match self
                .block_handle_storage
                .find_prev_persistent_key_block(current_block.id().seq_no)?
            {
                Some(prev_state_block) => {
                    let block_get_utime = prev_state_block.meta().gen_utime();
                    if block_get_utime < now - 86000 {
                        break prev_state_block;
                    }
                    current_block = prev_state_block
                }
                None => return Ok(()),
            }
        };

        self.clear_outdated_state_directories(block.id().seq_no())?;

        let end = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;

        tracing::info!(
            elapsed_secs = end - now,
            "Clearing old persistent state directories completed"
        );

        Ok(())
    }

    fn clear_outdated_state_directories(&self, actual_seqno: u32) -> Result<()> {
        let paths = fs::read_dir(&self.storage_path)?;
        let mut entries_to_remove: Vec<PathBuf> = Vec::new();

        for path in paths.flatten() {
            let meta = path.metadata();
            if let Ok(meta) = meta {
                if meta.is_file() {
                    entries_to_remove.push(path.path());
                    continue;
                }
            }

            let filename_os = path.file_name();
            let filename_str = filename_os.to_str();
            let name = match filename_str {
                Some(name) => name,
                None => {
                    entries_to_remove.push(path.path());
                    continue;
                }
            };

            let seqno_res = name.parse::<u32>();
            match seqno_res {
                Ok(seqno) => {
                    if seqno < actual_seqno {
                        entries_to_remove.push(path.path());
                    }
                }
                Err(_) => {
                    entries_to_remove.push(path.path());
                }
            }
        }

        for dir in entries_to_remove {
            tracing::info!(dir = %dir.display(), "Removing old persistent state directory");
            if let Err(e) = fs::remove_dir_all(&dir) {
                tracing::error!(dir = %dir.display(), "Removing old persistent state directory failed. Err: {e:?}");
            }
        }

        Ok(())
    }
}
