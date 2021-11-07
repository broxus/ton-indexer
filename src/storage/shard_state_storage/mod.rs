/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - rewritten initial state processing logic using files and stream processing
/// - replaced recursions with dfs to prevent stack overflow
///
use std::collections::hash_map;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use tiny_adnl::utils::*;
use ton_types::{ByteOrderRead, UInt256};

use self::files_context::*;
use self::replace_transaction::*;
use super::tree::*;
use crate::storage::cell_storage::*;
use crate::storage::{columns, StoredValue, TopBlocks};

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

        let (marker, _handle) = self.state.begin_store();

        self.state.cell_storage.store_dynamic_boc(marker, root)?;
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
                0, // NOTE: zero marker is used for 'persistent' state
            ),
            ctx,
        ))
    }

    pub async fn gc(&self, top_blocks: &TopBlocks) -> Result<()> {
        log::info!(
            "Starting shard states GC for target block: {}",
            top_blocks.target_mc_block
        );
        let instant = Instant::now();

        self.state.gc(top_blocks).await?;

        log::info!(
            "Finished shard states GC for target block: {}. Took: {} ms",
            top_blocks.target_mc_block,
            instant.elapsed().as_millis()
        );
        Ok(())
    }
}

struct ShardStateStorageState {
    writer_state: Mutex<WriterState>,
    gc_state: GcStateStorage,
    shard_state_db: Tree<columns::ShardStates>,
    cell_storage: Arc<CellStorage>,
}

#[derive(Default)]
struct WriterState {
    current_marker: u8,
    gc_started: bool,
    writer_count: u32,
}

impl ShardStateStorageState {
    fn new(db: &Arc<rocksdb::DB>) -> Result<Self> {
        let state = Self {
            writer_state: Default::default(),
            gc_state: GcStateStorage::new(db)?,
            shard_state_db: Tree::new(db)?,
            cell_storage: Arc::new(CellStorage::new(db)?),
        };

        let gc_state = state.gc_state.load()?;
        let target_marker = gc_state.next_marker();

        let current_marker = match &gc_state.step {
            Step::Wait => {
                log::info!("Shard state GC is pending");
                gc_state.current_marker
            }
            Step::Mark(top_blocks) => {
                state.mark(gc_state.current_marker, target_marker, top_blocks, true)?;
                state.sweep_cells(gc_state.current_marker, target_marker, top_blocks)?;
                state.sweep_blocks(target_marker, top_blocks)?;
                target_marker
            }
            Step::SweepCells(top_blocks) => {
                state.sweep_cells(gc_state.current_marker, target_marker, top_blocks)?;
                state.sweep_blocks(target_marker, top_blocks)?;
                target_marker
            }
            Step::SweepBlocks(top_blocks) => {
                state.sweep_blocks(target_marker, top_blocks)?;
                target_marker
            }
        };

        state.writer_state.lock().current_marker = current_marker;

        Ok(state)
    }

    fn begin_store(&'_ self) -> (u8, Option<impl Drop + '_>) {
        struct WriterStateHandle<'a>(&'a Mutex<WriterState>);

        impl Drop for WriterStateHandle<'_> {
            fn drop(&mut self) {
                let mut state = self.0.lock();
                state.writer_count = state.writer_count.saturating_sub(1);
            }
        }

        let mut writer_state = self.writer_state.lock();
        if writer_state.gc_started {
            (writer_state.current_marker, None)
        } else {
            writer_state.writer_count += 1;
            (
                writer_state.current_marker,
                Some(WriterStateHandle(&self.writer_state)),
            )
        }
    }

    async fn gc(self: &Arc<Self>, top_blocks: &TopBlocks) -> Result<()> {
        let gc_state = self.gc_state.load()?;
        if !matches!(&gc_state.step, Step::Wait) {
            log::info!("Invalid GC state: {:?}", gc_state);
        };

        self.gc_state
            .update(&GcState {
                current_marker: gc_state.current_marker,
                step: Step::Mark(top_blocks.clone()),
            })
            .context("Failed to update gc state to 'Mark'")?;

        let target_marker = gc_state.next_marker();

        let mut has_writers = {
            let mut writer_state = self.writer_state.lock();
            writer_state.gc_started = true;
            writer_state.current_marker = target_marker;
            writer_state.writer_count > 0
        };
        while has_writers {
            log::info!("Waiting writers...");
            tokio::time::sleep(Duration::from_secs(1)).await;
            has_writers = self.writer_state.lock().writer_count > 0;
        }

        let result = tokio::task::spawn_blocking({
            let state = self.clone();
            let top_blocks = top_blocks.clone();
            move || {
                state.mark(gc_state.current_marker, target_marker, &top_blocks, false)?;
                state.sweep_cells(gc_state.current_marker, target_marker, &top_blocks)?;
                state.sweep_blocks(target_marker, &top_blocks)
            }
        })
        .await?;

        self.writer_state.lock().gc_started = false;

        result
    }

    fn mark(
        &self,
        current_marker: u8,
        target_marker: u8,
        top_blocks: &TopBlocks,
        force: bool,
    ) -> Result<()> {
        let mut total = 0;

        let mut last_blocks = self
            .gc_state
            .load_last_blocks()
            .context("Failed to load last shard blocks")?;

        log::info!("Last blocks for states GC: {:?}", last_blocks);

        // Mark all cells for new blocks recursively
        for (key, value) in self.shard_state_db.iterator(rocksdb::IteratorMode::Start)? {
            let block_id = ton_block::BlockIdExt::from_slice(&key)?;
            if !top_blocks.contains(&block_id) {
                continue;
            }

            let edge_block = match last_blocks.entry(block_id.shard_id) {
                // Skip blocks which were definitely processed
                hash_map::Entry::Occupied(entry) if block_id.seq_no < *entry.get() => continue,
                // Block may have been processed
                hash_map::Entry::Occupied(entry) => {
                    entry.remove();
                    true
                }
                // Block is definitely processed first time
                hash_map::Entry::Vacant(_) => false,
            };

            self.gc_state
                .update_last_block(block_id.shard_id, block_id.seq_no)
                .context("Failed to update last block")?;

            let count = self.cell_storage.mark_cells_tree(
                UInt256::from_be_bytes(&value),
                target_marker,
                force && edge_block,
            )?;
            total += count;
        }

        self.gc_state
            .clear_last_blocks()
            .context("Failed to reset last block")?;

        log::info!("Marked {} cells", total);

        // Update gc state
        self.gc_state
            .update(&GcState {
                current_marker,
                step: Step::SweepCells(top_blocks.clone()),
            })
            .context("Failed to update gc state to 'Sweep'")
    }

    fn sweep_cells(
        &self,
        current_marker: u8,
        target_marker: u8,
        top_blocks: &TopBlocks,
    ) -> Result<()> {
        log::info!("Sweeping cells other than {}", target_marker);

        let time = Instant::now();

        // Remove all unmarked cells
        let total = self
            .cell_storage
            .sweep_cells(target_marker)
            .context("Failed to sweep cells")?;

        log::info!(
            "Swept {} cells. Took: {} ms",
            total,
            time.elapsed().as_millis()
        );

        // Update gc state
        self.gc_state
            .update(&GcState {
                current_marker,
                step: Step::SweepBlocks(top_blocks.clone()),
            })
            .context("Failed to update gc state to 'SweepBlocks'")
    }

    fn sweep_blocks(&self, target_marker: u8, top_blocks: &TopBlocks) -> Result<()> {
        log::info!("Sweeping block states");

        let time = Instant::now();

        let mut total = 0;

        // Remove all unmarked cells
        for (key, _) in self.shard_state_db.iterator(rocksdb::IteratorMode::Start)? {
            let block_id = ton_block::BlockIdExt::from_slice(&key)?;
            if top_blocks.contains(&block_id) {
                continue;
            }

            self.shard_state_db
                .remove(key)
                .context("Failed to remove swept block")?;
            total += 1;
        }

        log::info!(
            "Swept {} block states. Took: {} ms",
            total,
            time.elapsed().as_millis()
        );

        // Update gc state
        self.gc_state
            .update(&GcState {
                current_marker: target_marker,
                step: Step::Wait,
            })
            .context("Failed to update gc state to 'Wait'")
    }
}

struct GcStateStorage {
    node_states: Tree<columns::NodeStates>,
}

impl GcStateStorage {
    fn new(db: &Arc<rocksdb::DB>) -> Result<Self> {
        let storage = Self {
            node_states: Tree::new(db)?,
        };
        let _ = storage.load()?;
        Ok(storage)
    }

    fn load(&self) -> Result<GcState> {
        Ok(match self.node_states.get(STATES_GC_STATE_KEY)? {
            Some(value) => {
                GcState::from_slice(&value).context("Failed to decode states GC state")?
            }
            None => {
                let state = GcState {
                    current_marker: 1, // NOTE: zero marker is reserved for persistent state
                    step: Step::Wait,
                };
                self.update(&state)?;
                state
            }
        })
    }

    fn update(&self, state: &GcState) -> Result<()> {
        self.node_states
            .insert(STATES_GC_STATE_KEY, state.to_vec()?)
            .context("Failed to update shards GC state")
    }

    fn clear_last_blocks(&self) -> Result<()> {
        let iter = self.node_states.prefix_iterator(GC_LAST_BLOCK_KEY)?;
        for (key, _) in iter.filter(|(key, _)| key.starts_with(GC_LAST_BLOCK_KEY)) {
            self.node_states.remove(key)?
        }
        Ok(())
    }

    fn load_last_blocks(&self) -> Result<FxHashMap<ton_block::ShardIdent, u32>> {
        let mut result = FxHashMap::default();

        let iter = self.node_states.prefix_iterator(GC_LAST_BLOCK_KEY)?;
        for (key, value) in iter.filter(|(key, _)| key.starts_with(GC_LAST_BLOCK_KEY)) {
            let shard_ident = LastShardBlockKey::from_slice(&key)
                .context("Failed to load last shard id")?
                .0;
            let top_block = std::io::Cursor::new(&value)
                .read_le_u32()
                .context("Failed to load top block")?;
            result.insert(shard_ident, top_block);
        }

        Ok(result)
    }

    fn update_last_block(&self, shard_ident: ton_block::ShardIdent, seq_no: u32) -> Result<()> {
        self.node_states.insert(
            LastShardBlockKey(shard_ident).to_vec()?,
            seq_no.to_le_bytes(),
        )
    }
}

#[derive(Debug)]
struct GcState {
    current_marker: u8,
    step: Step,
}

impl StoredValue for GcState {
    const SIZE_HINT: usize = 512;

    type OnStackSlice = [u8; 512];

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        let (step, blocks) = match &self.step {
            Step::Wait => (0, None),
            Step::Mark(blocks) => (1, Some(blocks)),
            Step::SweepCells(blocks) => (2, Some(blocks)),
            Step::SweepBlocks(blocks) => (3, Some(blocks)),
        };
        writer.write_all(&[self.current_marker, step])?;

        if let Some(blocks) = blocks {
            blocks.serialize(writer)?;
        }

        Ok(())
    }

    fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let mut data = [0u8; 2];
        reader.read_exact(&mut data)?;
        let step = match data[1] {
            0 => Step::Wait,
            1 => Step::Mark(TopBlocks::deserialize(reader)?),
            2 => Step::SweepCells(TopBlocks::deserialize(reader)?),
            3 => Step::SweepBlocks(TopBlocks::deserialize(reader)?),
            _ => return Err(ShardStateStorageError::InvalidStatesGcStep.into()),
        };

        Ok(Self {
            current_marker: data[0],
            step,
        })
    }
}

impl GcState {
    fn next_marker(&self) -> u8 {
        match self.current_marker {
            u8::MAX => 1,
            marker => marker + 1,
        }
    }
}

#[derive(Debug)]
enum Step {
    Wait,
    Mark(TopBlocks),
    SweepCells(TopBlocks),
    SweepBlocks(TopBlocks),
}

#[derive(Debug)]
struct LastShardBlockKey(ton_block::ShardIdent);

impl StoredValue for LastShardBlockKey {
    const SIZE_HINT: usize = GC_LAST_BLOCK_KEY.len() + ton_block::ShardIdent::SIZE_HINT;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write_all(GC_LAST_BLOCK_KEY)?;
        self.0.serialize(writer)
    }

    fn deserialize<R: Read + Seek>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        reader.seek(std::io::SeekFrom::Start(GC_LAST_BLOCK_KEY.len() as u64))?;
        ton_block::ShardIdent::deserialize(reader).map(Self)
    }
}

const STATES_GC_STATE_KEY: &[u8] = b"states_gc_state";
const GC_LAST_BLOCK_KEY: &[u8] = b"gc_last_block";

#[derive(thiserror::Error, Debug)]
enum ShardStateStorageError {
    #[error("Not found")]
    NotFound,
    #[error("Invalid states GC step")]
    InvalidStatesGcStep,
}
