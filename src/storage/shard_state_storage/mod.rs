/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - rewritten initial state processing logic using files and stream processing
/// - replaced recursions with dfs to prevent stack overflow
///
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use ton_types::UInt256;

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

    pub fn gc(&self, top_blocks: &TopBlocks) -> Result<()> {
        log::info!(
            "Starting shard states GC for target block: {}",
            top_blocks.target_mc_block
        );
        let instant = Instant::now();

        self.state.gc(top_blocks)?;

        log::info!(
            "Finished shard states GC for target block: {}. Took: {} ms",
            top_blocks.target_mc_block,
            instant.elapsed().as_millis()
        );
        Ok(())
    }
}

struct ShardStateStorageState {
    gc_state: GcStateStorage,
    shard_state_db: Tree<columns::ShardStates>,
    cell_storage: Arc<CellStorage>,
}

impl ShardStateStorageState {
    fn new(db: &Arc<rocksdb::DB>) -> Result<Self> {
        let state = Self {
            gc_state: GcStateStorage::new(db)?,
            shard_state_db: Tree::new(db)?,
            cell_storage: Arc::new(CellStorage::new(db)?),
        };

        let gc_state = state.gc_state.load()?;
        match &gc_state.step {
            Step::Wait => {
                log::info!("Shard state GC is pending");
            }
            Step::Mark(top_blocks) => {
                state.mark(
                    gc_state.current_marker,
                    gc_state.next_marker(),
                    top_blocks,
                    true,
                )?;
                state.sweep(gc_state.next_marker())?;
            }
            Step::Sweep => state.sweep(gc_state.current_marker)?,
        }

        Ok(state)
    }

    fn gc(&self, top_blocks: &TopBlocks) -> Result<()> {
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

        self.mark(
            gc_state.current_marker,
            gc_state.next_marker(),
            top_blocks,
            false,
        )?;

        self.sweep(gc_state.next_marker())
    }

    fn mark(
        &self,
        current_marker: u8,
        target_marker: u8,
        top_blocks: &TopBlocks,
        force: bool,
    ) -> Result<()> {
        let mut total = 0;

        // Mark all cells for new blocks recursively
        for (key, value) in self.shard_state_db.iterator(rocksdb::IteratorMode::Start)? {
            let block_id = ton_block::BlockIdExt::from_slice(&key)?;
            if !top_blocks.contains(&block_id)? {
                continue;
            }

            total += self.cell_storage.mark_cells_tree(
                UInt256::from_be_bytes(&value),
                target_marker,
                force,
            )?;
        }

        log::info!("Marked {} cells", total);

        // Update gc state
        self.gc_state
            .update(&GcState {
                current_marker,
                step: Step::Sweep,
            })
            .context("Failed to update gc state to 'Sweep'")
    }

    fn sweep(&self, target_marker: u8) -> Result<()> {
        // Remove all unmarked cells
        let total = self
            .cell_storage
            .sweep_cells(target_marker)
            .context("Failed to sweep cells")?;

        log::info!("Swept {} cells", total);

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
                    current_marker: 0,
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
            Step::Sweep => (1, None),
        };
        writer.write_all(&[self.current_marker, step])?;

        if let Some(blocks) = blocks {
            blocks.serialize(writer)?;
        }

        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self>
    where
        Self: Sized,
    {
        let mut data = [0u8; 2];
        reader.read_exact(&mut data)?;
        let step = match data[1] {
            0 => Step::Wait,
            1 => Step::Mark(TopBlocks::deserialize(reader)?),
            2 => Step::Sweep,
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
        self.current_marker.wrapping_add(1)
    }
}

#[derive(Debug)]
enum Step {
    Wait,
    Mark(TopBlocks),
    Sweep,
}

const STATES_GC_STATE_KEY: &str = "states_gc_state";

#[derive(thiserror::Error, Debug)]
enum ShardStateStorageError {
    #[error("Not found")]
    NotFound,
    #[error("Invalid states GC step")]
    InvalidStatesGcStep,
}
