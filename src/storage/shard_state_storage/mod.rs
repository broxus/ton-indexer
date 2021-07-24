use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{SeekFrom, Write};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use dashmap::DashMap;
use tokio::sync::{RwLock, RwLockWriteGuard};
use ton_types::CellImpl;

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
        ShardStateReplaceTransaction::new(state).await
    }
}

struct ShardStateStorageState {
    shard_state_db: sled::Tree,
    dynamic_boc_db: DynamicBocDb,
}

pub struct ShardStateReplaceTransaction<'a> {
    state: RwLockWriteGuard<'a, ShardStateStorageState>,
    file: tokio::fs::File,
    executed: bool,
    reader: ShardStatePacketReader,
    boc_header: Option<BocHeader>,
    cells_read: usize,
}

impl<'a> ShardStateReplaceTransaction<'a> {
    async fn new(
        state: RwLockWriteGuard<'a, ShardStateStorageState>,
    ) -> Result<ShardStateReplaceTransaction<'a>> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .open("tempshardstate")
            .await?;

        Ok(Self {
            state,
            file,
            executed: false,
            reader: ShardStatePacketReader::new(),
            boc_header: None,
            cells_read: 0,
        })
    }

    pub async fn process_packet(&mut self, packet: Vec<u8>) -> Result<bool> {
        use tokio::io::AsyncWriteExt;

        self.reader.set_next_packet(packet);

        let header = loop {
            match &self.boc_header {
                Some(header) => break header,
                None => {
                    self.boc_header = match self.reader.read_header()? {
                        Some(header) => {
                            log::info!("HEADER: {:?}", header);
                            Some(header)
                        }
                        None => return Ok(false),
                    };
                    continue;
                }
            }
        };

        log::info!("CELLS READ: {} of {}", self.cells_read, header.cell_count);

        let mut chunk_size = 0u32;
        let mut buffer = [0; 256]; // At most 2 + 128 + 4 * 4

        while self.cells_read < header.cell_count {
            let cell_size = match self.reader.read_cell(header.ref_size, &mut buffer)? {
                Some(cell_size) => cell_size,
                None => {
                    self.file.write_u32_le(chunk_size).await?;
                    log::info!("CHUNK SIZE: {} bytes", chunk_size);
                    return Ok(false);
                }
            };

            buffer[cell_size] = cell_size as u8;
            self.file.write_all(&buffer[..cell_size + 1]).await?;

            chunk_size += cell_size as u32 + 1;
            self.cells_read += 1;
        }

        if chunk_size > 0 {
            self.file.write_u32_le(chunk_size).await?;
            log::info!("CHUNK SIZE: {} bytes", chunk_size);
        }

        log::info!("CELLS READ: {} of {}", self.cells_read, header.cell_count);

        if header.has_crc && self.reader.read_crc()?.is_none() {
            return Ok(false);
        }

        Ok(true)
    }

    pub async fn finalize(mut self, block_id: &ton_block::BlockIdExt) -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

        const HASHES_ENTRY_SIZE: usize = 32 + 8 + 4;
        const MAX_DATA_SIZE: usize = 128;

        let header = match &self.boc_header {
            Some(header) => header,
            None => {
                return Err(ShardStateStorageError::InvalidShardStatePacket)
                    .context("BOC header not found")
            }
        };

        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .open("tempshardstate_hashes")
            .await?;
        file.set_len(header.cell_count as u64 * HASHES_ENTRY_SIZE as u64)
            .await?;

        let mut tail = [0; 4];
        let mut chunk_buffer = Vec::with_capacity(1 << 20);

        // Allocate
        let mut temp_buffer = vec![0u8; MAX_DATA_SIZE + HASHES_ENTRY_SIZE];
        let (data_buffer, reference_buffer) = temp_buffer.split_at_mut(MAX_DATA_SIZE);

        let total_size = self.file.seek(SeekFrom::End(0)).await?;
        log::info!("TOTAL SIZE: {}", total_size);

        let mut tree_bits: usize = 0;
        let mut tree_cells: u32 = 0;

        let mut cell_index = header.cell_count;
        let mut total_read = 0;
        let mut chunk_size = 0;
        while total_read < total_size {
            self.file.seek(SeekFrom::Current(-4 - chunk_size)).await?;
            self.file.read_exact(&mut tail).await?;
            total_read += 4;

            let mut remaining_bytes = u32::from_le_bytes(tail) as usize;
            chunk_buffer.resize(remaining_bytes, 0);

            chunk_size = remaining_bytes as i64;

            self.file.seek(SeekFrom::Current(-chunk_size - 4)).await?;
            self.file.read_exact(&mut chunk_buffer).await?;
            total_read += chunk_size as u64;

            log::info!("PROCESSING CHUNK OF SIZE: {}", chunk_size);

            while remaining_bytes > 0 {
                cell_index -= 1;
                let cell_size = chunk_buffer[remaining_bytes - 1] as usize;
                remaining_bytes -= cell_size + 1;

                let cell = RawCell::from_stored_data(
                    &mut std::io::Cursor::new(
                        &chunk_buffer[remaining_bytes..remaining_bytes + cell_size],
                    ),
                    header.ref_size,
                    header.cell_count,
                    cell_index,
                    data_buffer,
                )?;

                let mut cell_bits = cell.bit_len;
                let mut cell_cells = 1;

                for index in cell.reference_indices {
                    file.seek(SeekFrom::Start(index as u64 * HASHES_ENTRY_SIZE as u64))
                        .await?;
                    file.read_exact(reference_buffer).await?;

                    cell_bits += usize::from_le_bytes(reference_buffer[32..40].try_into().unwrap());
                    cell_cells += u32::from_le_bytes(reference_buffer[40..44].try_into().unwrap());
                }

                tree_bits += cell_bits;
                tree_cells += cell_cells;

                reference_buffer[32..40].copy_from_slice(&cell_bits.to_le_bytes());
                reference_buffer[40..44].copy_from_slice(&cell_cells.to_le_bytes());

                file.seek(SeekFrom::Start(
                    cell_index as u64 * HASHES_ENTRY_SIZE as u64,
                ))
                .await?;
                file.write_all(reference_buffer).await?;

                chunk_buffer.truncate(remaining_bytes);
            }

            log::info!(
                "READ: {}, BITS: {}, CELLS: {}",
                total_read,
                tree_bits,
                tree_cells
            );
        }

        log::info!("DONE PROCESSING: {} of {}", total_read, total_size);

        let key = block_id.to_vec()?;

        //self.state.shard_state_db.insert(key, root_cell_hash)?;
        Ok(())
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
    #[error("Invalid shard state packet")]
    InvalidShardStatePacket,
}
