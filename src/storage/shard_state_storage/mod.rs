use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{Read, Write};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use crc::crc32::{self, Hasher32};
use dashmap::DashMap;
use tokio::sync::{RwLock, RwLockWriteGuard};
use ton_types::{ByteOrderRead, CellImpl, UInt256};

use self::parser::*;
use super::storage_cell::StorageCell;
use crate::storage::StoredValue;
use crate::utils::*;

mod parser;

pub struct ShardStateStorage {
    state: RwLock<ShardStateStorageState>,
}

impl ShardStateStorage {
    pub fn with_db(shard_state_db: sled::Tree, cell_db_path: sled::Tree) -> Self {
        Self {
            state: RwLock::new(ShardStateStorageState {
                shard_state_db,
                dynamic_boc_db: DynamicBocDb::with_db(cell_db_path),
            }),
        }
    }

    pub async fn store_state(
        &self,
        block_id: &ton_block::BlockIdExt,
        root: ton_types::Cell,
    ) -> Result<()> {
        let state = self.state.read().await;

        let cell_id = root.repr_hash();
        state.dynamic_boc_db.store_dynamic_boc(root)?;

        let key = block_id.to_vec()?;
        state.shard_state_db.insert(key, cell_id.as_slice())?;

        Ok(())
    }

    pub async fn load_state(&self, block_id: &ton_block::BlockIdExt) -> Result<ton_types::Cell> {
        let state = self.state.read().await;

        match state.shard_state_db.get(block_id.to_vec()?)? {
            Some(root) => {
                let cell_id = ton_types::UInt256::from_be_bytes(root.as_ref());
                let cell = state.dynamic_boc_db.load_cell(cell_id)?;
                Ok(ton_types::Cell::with_cell_impl_arc(cell))
            }
            None => Err(ShardStateStorageError::NotFound.into()),
        }
    }

    pub async fn begin_replace(&'_ self) -> Result<ShardStateReplaceTransaction<'_>> {
        let state = self.state.write().await;
        state.shard_state_db.clear()?;
        state.dynamic_boc_db.clear()?;
        Ok(ShardStateReplaceTransaction::new(state))
    }
}

struct ShardStateStorageState {
    shard_state_db: sled::Tree,
    dynamic_boc_db: DynamicBocDb,
}

pub struct ShardStateReplaceTransaction<'a> {
    state: RwLockWriteGuard<'a, ShardStateStorageState>,
    executed: bool,
    reader: ShardStatePacketReader,
    boc_header: Option<BocHeader>,
    cells_read: usize,
}

impl<'a> ShardStateReplaceTransaction<'a> {
    fn new(state: RwLockWriteGuard<'a, ShardStateStorageState>) -> Self {
        Self {
            state,
            executed: false,
            reader: ShardStatePacketReader::new(),
            boc_header: None,
            cells_read: 0,
        }
    }

    pub fn finalize(self, block_id: &ton_block::BlockIdExt, root_cell_hash: &[u8]) -> Result<()> {
        let key = block_id.to_vec()?;
        self.state.shard_state_db.insert(key, root_cell_hash)?;
        Ok(())
    }

    pub fn process(&mut self, data: Vec<u8>, last: bool) -> Result<Option<UInt256>> {
        self.reader.set_next_packet(data);

        loop {
            let header = match &self.boc_header {
                Some(header) => header,
                None => {
                    let mut reader = self.reader.begin();
                    let header = BocHeader::from_reader(&mut reader)?;
                    reader.end();

                    log::info!("HEADER: {:?}", header);

                    if header.index_included {
                        self.reader.set_skip(header.cell_count * header.offset_size);
                    }
                    self.boc_header = Some(header);
                    continue;
                }
            };

            match self.reader.process_skip() {
                ReaderAction::Incomplete if last => {
                    return Err(ShardStateStorageError::InvalidShardStateHeader)
                        .context("Cells index underflow")
                }
                ReaderAction::Incomplete => return Ok(None),
                ReaderAction::Complete => { /* go further */ }
            }

            log::info!("CELLS READ: {} of {}", self.cells_read, header.cell_count);

            while self.cells_read < header.cell_count {
                let mut reader = self.reader.begin();

                match RawCell::from_reader(
                    &mut reader,
                    header.ref_size,
                    header.cell_count,
                    self.cells_read,
                )? {
                    Some(_cell) => {
                        reader.end();
                        self.cells_read += 1;
                    }
                    None => return Ok(None),
                };
            }

            log::info!("CELLS READ: {} of {}", self.cells_read, header.cell_count);

            if header.has_crc {
                let mut reader = self.reader.begin();
                let crc = match PacketCrc::from_reader(&mut reader)? {
                    Some(crc) => {
                        reader.end();
                        crc
                    }
                    None => return Ok(None),
                };

                if crc.value != self.reader.crc32() {
                    return Err(ShardStateStorageError::InvalidShardStateHeader)
                        .context("Invalid crc");
                }
            }

            return Ok(Some(UInt256::default()));
        }
    }

    fn add_cell(
        &self,
        hash: &[u8; 32],
        cell: ton_types::DataCell,
        buffer: &mut Vec<u8>,
    ) -> Result<()> {
        buffer.clear();

        let references_count = cell.references_count() as u8;

        cell.cell_data().serialize(buffer).convert()?;
        buffer.write_all(&[references_count])?;

        for i in 0..references_count {
            buffer.write_all(cell.reference(i as usize).convert()?.repr_hash().as_slice())?;
        }

        buffer.write_all(&cell.tree_bits_count().to_be_bytes())?;
        buffer.write_all(&cell.tree_cell_count().to_be_bytes())?;

        self.state
            .dynamic_boc_db
            .cell_db
            .db
            .insert(hash, buffer.as_slice())?;

        Ok(())
    }
}

impl<'a> Drop for ShardStateReplaceTransaction<'a> {
    fn drop(&mut self) {
        if !self.executed {
            let _ = self.state.shard_state_db.clear();
            let _ = self.state.dynamic_boc_db.clear();
        }
    }
}

#[derive(Clone)]
pub struct DynamicBocDb {
    cell_db: CellDb,
    cells: Arc<DashMap<ton_types::UInt256, Weak<StorageCell>>>,
}

impl DynamicBocDb {
    fn with_db(db: sled::Tree) -> Self {
        Self {
            cell_db: CellDb { db },
            cells: Arc::new(DashMap::new()),
        }
    }

    pub fn store_dynamic_boc(&self, root: ton_types::Cell) -> Result<usize> {
        let mut transaction = HashMap::new();

        let written_count = self.prepare_tree_of_cells(root, &mut transaction)?;

        self.cell_db
            .db
            .transaction::<_, _, ()>(move |diff| {
                for (cell_id, data) in &transaction {
                    diff.insert(cell_id.as_slice(), data.as_slice())?;
                }
                Ok(())
            })
            .map_err(|_| ShardStateStorageError::TransactionConflict)?;

        Ok(written_count)
    }

    pub fn load_cell(&self, hash: ton_types::UInt256) -> Result<Arc<StorageCell>> {
        if let Some(cell) = self.cells.get(&hash) {
            if let Some(cell) = cell.upgrade() {
                return Ok(cell);
            }
        }

        let cell = Arc::new(self.cell_db.load(self, &hash)?);
        self.cells.insert(hash, Arc::downgrade(&cell));

        Ok(cell)
    }

    pub fn drop_cell(&self, hash: &ton_types::UInt256) {
        self.cells.remove(hash);
    }

    pub fn clear(&self) -> Result<()> {
        self.cell_db.db.clear()?;
        self.cells.clear();
        Ok(())
    }

    fn prepare_tree_of_cells(
        &self,
        cell: ton_types::Cell,
        transaction: &mut HashMap<ton_types::UInt256, Vec<u8>>,
    ) -> Result<usize> {
        // TODO: rewrite using DFS

        let cell_id = cell.hash(ton_types::MAX_LEVEL);
        if self.cell_db.contains(&cell_id)? || transaction.contains_key(&cell_id) {
            return Ok(0);
        }

        transaction.insert(cell_id, StorageCell::serialize(&*cell)?);

        let mut count = 1;
        for i in 0..cell.references_count() {
            count += self.prepare_tree_of_cells(cell.reference(i).convert()?, transaction)?;
        }

        Ok(count)
    }
}

#[derive(Clone)]
pub struct CellDb {
    db: sled::Tree,
}

impl CellDb {
    pub fn contains(&self, hash: &ton_types::UInt256) -> Result<bool> {
        let has_key = self.db.contains_key(hash.as_ref())?;
        Ok(has_key)
    }

    pub fn load(&self, boc_db: &DynamicBocDb, hash: &ton_types::UInt256) -> Result<StorageCell> {
        match self.db.get(hash.as_slice())? {
            Some(value) => StorageCell::deserialize(boc_db.clone(), value.as_ref()),
            None => Err(ShardStateStorageError::CellNotFound.into()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum ShardStateStorageError {
    #[error("Cell not found in cell db")]
    CellNotFound,
    #[error("Not found")]
    NotFound,
    #[error("Cell db transaction conflict")]
    TransactionConflict,
    #[error("Invalid shard state header")]
    InvalidShardStateHeader,
}
