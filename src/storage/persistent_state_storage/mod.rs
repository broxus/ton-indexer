use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use tokio_util::sync::CancellationToken;

use crate::db::Db;

mod cell_writer;

pub struct PersistentStateStorage {
    storage_path: PathBuf,
    db: Arc<Db>,
    //drop_guard: DropGuard,
    cancellation_token: CancellationToken,
}

impl PersistentStateStorage {
    #[allow(unused)]
    pub async fn new(file_db_path: PathBuf, db: Arc<Db>) -> Result<Self> {
        let dir = file_db_path.join("states");
        tokio::fs::create_dir_all(&dir).await?;
        let cancellation_token = CancellationToken::new();
        //let drop_guard = cancellation_token.drop_guard();
        Ok(Self {
            storage_path: dir,
            db,
            //drop_guard,
            cancellation_token,
        })
    }

    #[allow(unused)]
    pub async fn save_state(
        &self,
        state_root_hash: [u8; 32],
        block_root_hash: [u8; 32],
    ) -> Result<()> {
        tokio::pin!(
            let signal = self.cancellation_token.cancelled();
        );

        let cell_hex = hex::encode(block_root_hash);

        let db = self.db.clone();
        let path = self.storage_path.clone();
        let cell_writer = cell_writer::CellWriter::new(&db, &path);

        let mut future = async move { cell_writer.write(&state_root_hash, &block_root_hash) };

        let path = tokio::select! {
            result = future => result?,
            _ = (&mut signal) => {
                cell_writer::clear_temp(&path, &block_root_hash);
                return Ok(())
            },
        };

        tracing::info!(
            "Successfully wrote persistent {} state to a file: {} ",
            cell_hex,
            path.display()
        );

        Ok(())
    }

    #[allow(unused)]
    pub async fn read_state_part(
        &self,
        block_root_hash: &[u8; 32],
        offset: usize,
        size: usize,
    ) -> Option<Vec<u8>> {
        let file_path = self.prepare_block_root_hash(block_root_hash);
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

    #[allow(unused)]
    pub async fn state_exists(&self, block_root_hash: &[u8; 32]) -> bool {
        let file_name = self.prepare_block_root_hash(block_root_hash);
        if let Ok(meta) = tokio::fs::metadata(file_name).await {
            meta.is_file()
        } else {
            false
        }
    }

    #[allow(unused)]
    pub async fn get_state_hash(
        &self,
        mc_root_hash: &[u8; 32],
        sc_root_hash: &[u8; 32],
    ) -> Option<[u8; 32]> {
        let _unused = mc_root_hash;
        let _unused2 = sc_root_hash;
        todo!()
    }

    #[allow(unused)]
    fn prepare_block_root_hash(&self, block_root_hash: &[u8; 32]) -> PathBuf {
        let file_name = format!("{:x?}", block_root_hash);
        self.storage_path.join(file_name)
    }
}

impl Drop for PersistentStateStorage {
    fn drop(&mut self) {
        tracing::error!("Call drop");
        self.cancellation_token.cancel()
    }
}
