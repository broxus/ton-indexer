/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - rewritten initial state processing logic using files and stream processing
/// - replaced recursions with dfs to prevent stack overflow
///
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tiny_adnl::utils::*;
use ton_types::UInt256;

use self::files_context::*;
use self::replace_transaction::*;
use super::tree::*;
use crate::storage::cell_storage::*;
use crate::storage::{columns, StoredValue};

mod entries_buffer;
mod files_context;
mod parser;
mod replace_transaction;

pub struct ShardStateStorage {
    downloads_dir: Arc<PathBuf>,
    state: Arc<ShardStateStorageState>,
}

impl ShardStateStorage {
    pub async fn with_db<P>(db: &Arc<rocksdb::DB>, file_db_path: &P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let downloads_dir = Arc::new(file_db_path.as_ref().join("downloads"));
        tokio::fs::create_dir_all(downloads_dir.as_ref()).await?;

        Ok(Self {
            downloads_dir,
            state: Arc::new(ShardStateStorageState::new(db)?),
        })
    }

    pub async fn store_state(
        &self,
        block_id: &ton_block::BlockIdExt,
        root: ton_types::Cell,
    ) -> Result<()> {
        let cell_id = root.repr_hash();

        self.state.cell_storage.store_dynamic_boc(0, root)?;
        self.state
            .shard_state_db
            .insert(block_id.to_vec()?, cell_id.as_slice())
    }

    pub async fn load_state(&self, block_id: &ton_block::BlockIdExt) -> Result<ton_types::Cell> {
        let shard_state = self.state.shard_state_db.get(block_id.to_vec()?)?;
        match shard_state {
            Some(root) => {
                let cell_id = UInt256::from_be_bytes(&root);
                let cell = self.state.cell_storage.load_cell(cell_id)?;
                Ok(ton_types::Cell::with_cell_impl_arc(cell))
            }
            None => Err(ShardStateStorageError::NotFound.into()),
        }
    }

    pub async fn begin_replace(
        &'_ self,
        block_id: &ton_block::BlockIdExt,
        clear_db: bool,
    ) -> Result<(ShardStateReplaceTransaction<'_>, FilesContext)> {
        if clear_db {
            self.state.shard_state_db.clear()?;
            self.state.cell_storage.clear()?;
        }

        let ctx = FilesContext::new(self.downloads_dir.as_ref(), block_id).await?;
        Ok((
            ShardStateReplaceTransaction::new(
                &self.state.shard_state_db,
                &self.state.cell_storage,
                0,
            ),
            ctx,
        ))
    }

    pub fn start_gc(
        &self,
        resolver: Arc<dyn StatesGcResolver>,
        offset: Duration,
        interval: Duration,
    ) {
        let state = Arc::downgrade(&self.state);

        tokio::spawn(async move {
            tokio::time::sleep(offset).await;
            loop {
                tokio::time::sleep(interval).await;

                let state = match state.upgrade() {
                    Some(state) => state,
                    None => return,
                };

                if let Err(e) = state.gc(&resolver).await {
                    log::error!("Failed to GC state: {:?}", e);
                }
            }
        });
    }
}

pub trait StatesGcResolver: Send + Sync {
    fn state_expired(&self, block_id: &ton_block::BlockIdExt) -> Result<bool>;
}

struct ShardStateStorageState {
    node_state: Tree<columns::NodeStates>,
    shard_state_db: Tree<columns::ShardStates>,
    cell_storage: Arc<CellStorage>,
}

impl ShardStateStorageState {
    fn new(db: &Arc<rocksdb::DB>) -> Result<Self> {
        Ok(Self {
            node_state: Tree::new(db)?,
            shard_state_db: Tree::new(db)?,
            cell_storage: Arc::new(CellStorage::new(db)?),
        })
    }

    async fn gc(self: &Arc<Self>, _resolver: &Arc<dyn StatesGcResolver>) -> Result<()> {
        // let active_boc_db = self.active_boc_db.load(Ordering::Acquire);
        // let inactive_boc_db = match active_boc_db {
        //     0 => self.dynamic_boc_db_1.clone(),
        //     1 => self.dynamic_boc_db_0.clone(),
        //     _ => return Err(ShardStateStorageError::UnknownBocDb.into()),
        // };
        //
        // log::info!("shard GC: Starting GC for boc_db{}", inactive_boc_db.id);
        //
        // let start = Instant::now();
        // while inactive_boc_db.writer_count.load(Ordering::Acquire) > 0 {
        //     tokio::time::sleep(Duration::from_secs(1)).await;
        //     let time = start.elapsed().as_millis();
        //     if time > 2000 {
        //         log::warn!("Waiting writers for {} ms", time);
        //     }
        // }
        // log::info!(
        //     "shard GC: Waiting writers in boc_db{} took {} ms",
        //     inactive_boc_db.id,
        //     start.elapsed().as_millis()
        // );
        //
        // let start = Instant::now();
        // let StateMarkup { marked, to_sweep } = {
        //     let state = self.clone();
        //     let inactive_boc_db = inactive_boc_db.clone();
        //     let resolver = resolver.clone();
        //     tokio::task::spawn_blocking(move || state.mark(inactive_boc_db, resolver))
        //         .await?
        //         .context("Failed to mark roots")?
        // };
        //
        // log::info!(
        //     "shard GC: Marking roots in boc_db{} took {} ms. Marked: {}. To sweep: {}",
        //     inactive_boc_db.id,
        //     start.elapsed().as_millis(),
        //     marked.len(),
        //     to_sweep.len()
        // );
        //
        // let start = Instant::now();
        // if !to_sweep.is_empty() {
        //     let state = self.clone();
        //     let inactive_boc_db = inactive_boc_db.clone();
        //     tokio::task::spawn_blocking(move || state.sweep(inactive_boc_db, marked, to_sweep))
        //         .await?
        //         .context("Failed to sweep cells")?;
        // }
        // log::info!(
        //     "shard GC: Sweeping roots in boc_db{} took {} ms",
        //     inactive_boc_db.id,
        //     start.elapsed().as_millis()
        // );
        //
        // self.active_boc_db
        //     .store(inactive_boc_db.id, Ordering::Release);

        Ok(())
    }

    fn mark(
        &self,
        cell_storage: Arc<CellStorage>,
        resolver: Arc<dyn StatesGcResolver>,
    ) -> Result<StateMarkup> {
        let mut to_sweep = Vec::new();
        let mut to_mark = Vec::new();

        let start = Instant::now();
        for (key, value) in self.shard_state_db.iterator(rocksdb::IteratorMode::Start)? {
            let block_id = ton_block::BlockIdExt::from_slice(&key)?;
            let cell_id = UInt256::from_be_bytes(&value);

            if resolver.state_expired(&block_id)? {
                to_sweep.push((block_id, cell_id));
            } else {
                to_mark.push(cell_id);
            }
        }
        log::info!(
            "shard GC: Blocks iteration took {} ms",
            start.elapsed().as_millis()
        );

        let mut marked = FxHashSet::default();
        let mut mark_subtree = |cell_id: UInt256| -> Result<()> {
            marked.insert(cell_id);
            let mut stack = VecDeque::with_capacity(32);
            stack.push_back(cell_id);

            while let Some(cell_id) = stack.pop_back() {
                for reference in cell_storage.load_cell_references(&cell_id)? {
                    let cell_id = reference.hash();
                    if marked.contains(&cell_id) {
                        continue;
                    }

                    marked.insert(cell_id);
                    stack.push_back(cell_id);
                }
            }

            Ok(())
        };

        if !to_sweep.is_empty() {
            for cell_id in to_mark {
                mark_subtree(cell_id)?;
            }
        }

        Ok(StateMarkup { marked, to_sweep })
    }

    fn sweep(
        &self,
        marked: FxHashSet<UInt256>,
        to_sweep: Vec<(ton_block::BlockIdExt, UInt256)>,
    ) -> Result<()> {
        let states_cf = self.shard_state_db.get_cf()?;
        let cell_cf = self.cell_storage.get_cf()?;
        let db = self.shard_state_db.raw_db_handle();

        let mut sweeped = FxHashSet::default();
        let mut sweep_subtree = |block_id: ton_block::BlockIdExt, cell_id: UInt256| -> Result<()> {
            let mut batch = rocksdb::WriteBatch::default();
            batch.delete_cf(&states_cf, block_id.to_vec()?);
            batch.delete_cf(&cell_cf, cell_id.as_slice());

            sweeped.insert(cell_id);
            let mut stack = VecDeque::with_capacity(32);
            stack.push_back(cell_id);

            while let Some(cell_id) = stack.pop_back() {
                let references = self.cell_storage.load_cell_references(&cell_id)?;

                for reference in references {
                    let cell_id = reference.hash();
                    if marked.contains(&cell_id) || sweeped.contains(&cell_id) {
                        continue;
                    }

                    batch.delete_cf(&cell_cf, cell_id.as_slice());
                    sweeped.insert(cell_id);
                    stack.push_back(cell_id);
                }
            }

            db.write(batch)?;
            Ok(())
        };

        for (block_id, cell_id) in to_sweep {
            sweep_subtree(block_id, cell_id)?;
        }

        Ok(())
    }
}

struct StateMarkup {
    marked: FxHashSet<UInt256>,
    to_sweep: Vec<(ton_block::BlockIdExt, UInt256)>,
}

struct GcState {
    current_marker: u8,
    active: bool,
    since_block_id: Option<ton_block::BlockIdExt>,
}

impl StoredValue for GcState {
    const SIZE_HINT: usize = 1 + 1 + 1 + ton_block::BlockIdExt::SIZE_HINT;

    type OnStackSlice = [u8; 96];

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(&[
            self.current_marker,
            self.active as u8,
            self.since_block_id.is_some() as u8,
        ])?;
        if let Some(last_block) = &self.since_block_id {
            last_block.serialize(writer)?;
        }
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let mut data = [0; 3];
        reader.read_exact(&mut data)?;
        let last_block_id = match data[2] {
            0 => None,
            _ => Some(ton_block::BlockIdExt::deserialize(reader)?),
        };

        Ok(Self {
            current_marker: data[0],
            active: data[1] != 0,
            since_block_id: last_block_id,
        })
    }
}

#[derive(thiserror::Error, Debug)]
enum ShardStateStorageError {
    #[error("Not found")]
    NotFound,
}
