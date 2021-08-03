use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use dashmap::DashMap;
use nekoton_utils::NoFailure;
use num_traits::ToPrimitive;
use sha2::Sha256;
use tokio::fs::File;
use tokio::sync::{RwLock, RwLockWriteGuard};

use self::mapped_file::*;
use self::parser::*;
use super::storage_cell::StorageCell;
use crate::storage::StoredValue;
use crate::utils::*;

mod mapped_file;
mod parser;

pub struct ShardStateStorage {
    downloads_dir: Arc<PathBuf>,
    state: RwLock<ShardStateStorageState>,
}

impl ShardStateStorage {
    pub async fn with_db<P>(shard_state_db: Tree, cell_db: Tree, file_db_path: &P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let downloads_dir = Arc::new(file_db_path.as_ref().join("downloads"));
        tokio::fs::create_dir_all(downloads_dir.as_ref()).await?;

        Ok(Self {
            state: RwLock::new(ShardStateStorageState {
                shard_state_db,
                dynamic_boc_db: DynamicBocDb::with_db(cell_db),
            }),
            downloads_dir,
        })
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

    pub async fn begin_replace(
        &'_ self,
        block_id: &ton_block::BlockIdExt,
        clear_db: bool,
    ) -> Result<(ShardStateReplaceTransaction<'_>, FilesContext)> {
        let state = self.state.write().await;
        if clear_db {
            state.shard_state_db.clear()?;
            state.dynamic_boc_db.clear()?;
        }

        let ctx = FilesContext::new(self.downloads_dir.as_ref(), block_id).await?;
        Ok((ShardStateReplaceTransaction::new(state).await?, ctx))
    }
}

struct ShardStateStorageState {
    shard_state_db: Tree,
    dynamic_boc_db: DynamicBocDb,
}

pub struct ShardStateReplaceTransaction<'a> {
    state: RwLockWriteGuard<'a, ShardStateStorageState>,
    reader: ShardStatePacketReader,
    boc_header: Option<BocHeader>,
    cells_read: usize,
}

impl<'a> ShardStateReplaceTransaction<'a> {
    async fn new(
        state: RwLockWriteGuard<'a, ShardStateStorageState>,
    ) -> Result<ShardStateReplaceTransaction<'a>> {
        Ok(Self {
            state,
            reader: ShardStatePacketReader::new(),
            boc_header: None,
            cells_read: 0,
        })
    }

    pub async fn process_packet(
        &mut self,
        ctx: &mut FilesContext,
        packet: Vec<u8>,
    ) -> Result<bool> {
        use tokio::io::AsyncWriteExt;

        let cells_file = ctx.cells_file()?;

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

        let mut chunk_size = 0u32;
        let mut buffer = [0; 256]; // At most 2 + 128 + 4 * 4

        while self.cells_read < header.cell_count {
            let cell_size = match self.reader.read_cell(header.ref_size, &mut buffer)? {
                Some(cell_size) => cell_size,
                None => break,
            };

            buffer[cell_size] = cell_size as u8;
            cells_file.write_all(&buffer[..cell_size + 1]).await?;

            chunk_size += cell_size as u32 + 1;
            self.cells_read += 1;
        }

        log::info!("CELLS READ: {} of {}", self.cells_read, header.cell_count);

        if chunk_size > 0 {
            cells_file.write_u32_le(chunk_size).await?;
            log::info!("CREATING CHUNK OF SIZE: {} bytes", chunk_size);
        }

        if self.cells_read < header.cell_count {
            return Ok(false);
        }

        if header.has_crc && self.reader.read_crc()?.is_none() {
            return Ok(false);
        }

        Ok(true)
    }

    pub async fn finalize(
        self,
        ctx: &mut FilesContext,
        block_id: ton_block::BlockIdExt,
    ) -> Result<Arc<ShardStateStuff>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        // 2^7 bits + 1 bytes
        const MAX_DATA_SIZE: usize = 128;

        let header = match &self.boc_header {
            Some(header) => header,
            None => {
                return Err(ShardStateStorageError::InvalidShardStatePacket)
                    .context("BOC header not found")
            }
        };

        let hashes_file = ctx.create_mapped_hashes_file(header.cell_count * HashesEntry::LEN)?;
        let cells_file = ctx.cells_file()?;

        let mut tail = [0; 4];
        let mut entries_buffer = EntriesBuffer::new();
        let mut pruned_branches = HashMap::new();

        // Allocate on heap to prevent big future size
        let mut chunk_buffer = Vec::with_capacity(1 << 20);
        let mut output_buffer = Vec::with_capacity(1 << 10);
        let mut data_buffer = vec![0u8; MAX_DATA_SIZE];

        let total_size = cells_file.seek(SeekFrom::End(0)).await?;
        log::info!("TOTAL SIZE: {}", total_size);

        let mut cell_index = header.cell_count;
        let mut total_read = 0;
        let mut chunk_size = 0;
        while total_read < total_size {
            cells_file.seek(SeekFrom::Current(-4 - chunk_size)).await?;
            cells_file.read_exact(&mut tail).await?;
            total_read += 4;

            let mut remaining_bytes = u32::from_le_bytes(tail) as usize;
            chunk_buffer.resize(remaining_bytes, 0);

            chunk_size = remaining_bytes as i64;

            cells_file.seek(SeekFrom::Current(-chunk_size - 4)).await?;
            cells_file.read_exact(&mut chunk_buffer).await?;
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
                    &mut data_buffer,
                )?;

                for (&index, buffer) in cell
                    .reference_indices
                    .iter()
                    .zip(entries_buffer.iter_child_buffers())
                {
                    // SAFETY: `buffer` is guaranteed to be in separate memory area
                    unsafe { hashes_file.read_exact_at(index as usize * HashesEntry::LEN, buffer) }
                }

                self.finalize_cell(
                    cell_index as u32,
                    cell,
                    &mut pruned_branches,
                    &mut entries_buffer,
                    &mut output_buffer,
                )?;

                // SAFETY: `entries_buffer` is guaranteed to be in separate memory area
                unsafe {
                    hashes_file.write_all_at(
                        cell_index * HashesEntry::LEN,
                        entries_buffer.current_entry_buffer(),
                    )
                };

                chunk_buffer.truncate(remaining_bytes);
            }

            log::info!("READ: {}", total_read);
        }

        log::info!("DONE PROCESSING: {} of {} bytes", total_read, total_size);

        let block_id_key = block_id.to_vec()?;

        // Current entry contains root cell
        let current_entry = entries_buffer.split_children(&[]).0;
        self.state
            .shard_state_db
            .insert(block_id_key.as_slice(), current_entry.as_reader().hash(3))?;

        // Load stored shard state
        match self.state.shard_state_db.get(block_id_key)? {
            Some(root) => {
                let cell_id = ton_types::UInt256::from_be_bytes(root.as_ref());
                let cell = self.state.dynamic_boc_db.load_cell(cell_id)?;

                Ok(Arc::new(ShardStateStuff::new(
                    block_id,
                    ton_types::Cell::with_cell_impl_arc(cell),
                )?))
            }
            None => Err(ShardStateStorageError::NotFound.into()),
        }
    }

    fn finalize_cell(
        &self,
        cell_index: u32,
        cell: RawCell<'_>,
        pruned_branches: &mut HashMap<u32, Vec<u8>>,
        entries_buffer: &mut EntriesBuffer,
        output_buffer: &mut Vec<u8>,
    ) -> Result<()> {
        use sha2::Digest;

        let (mut current_entry, children) = entries_buffer.split_children(&cell.reference_indices);

        current_entry.clear();

        // Prepare mask and counters
        let data_size = (cell.bit_len / 8) + if cell.bit_len % 8 != 0 { 1 } else { 0 };

        let mut children_mask = ton_types::LevelMask::with_mask(0);
        let mut tree_bits_count = cell.bit_len as u64;
        let mut tree_cell_count = 1;

        for (_, child) in children.iter() {
            children_mask |= child.level_mask();
            tree_bits_count += child.tree_bits_count();
            tree_cell_count += child.tree_cell_count();
        }

        let mut is_merkle_cell = false;
        let mut is_pruned_cell = false;
        let level_mask = match cell.cell_type {
            ton_types::CellType::Ordinary => children_mask,
            ton_types::CellType::PrunedBranch => {
                is_pruned_cell = true;
                ton_types::LevelMask::with_mask(cell.level_mask)
            }
            ton_types::CellType::LibraryReference => ton_types::LevelMask::with_mask(0),
            ton_types::CellType::MerkleProof => {
                is_merkle_cell = true;
                ton_types::LevelMask::for_merkle_cell(children_mask)
            }
            ton_types::CellType::MerkleUpdate => {
                is_merkle_cell = true;
                ton_types::LevelMask::for_merkle_cell(children_mask)
            }
            ton_types::CellType::Unknown => {
                return Err(ShardStateStorageError::InvalidCell).context("Unknown cell type")
            }
        };

        if cell.level_mask != level_mask.mask() {
            return Err(ShardStateStorageError::InvalidCell).context("Level mask mismatch");
        }

        // Save mask and counters
        current_entry.set_level_mask(level_mask);
        current_entry.set_cell_type(cell.cell_type);
        current_entry.set_tree_bits_count(tree_bits_count);
        current_entry.set_tree_cell_count(tree_cell_count);

        // Calculate hashes
        let hash_count = if is_pruned_cell {
            1
        } else {
            level_mask.level() + 1
        };

        let mut max_depths = [0u16; 4];
        for i in 0..hash_count {
            let mut hasher = Sha256::new();

            let level_mask = if is_pruned_cell {
                level_mask
            } else {
                ton_types::LevelMask::with_level(i)
            };

            let (d1, d2) = ton_types::BagOfCells::calculate_descriptor_bytes(
                cell.bit_len,
                cell.reference_indices.len() as u8,
                level_mask.mask(),
                cell.cell_type != ton_types::CellType::Ordinary,
                false,
            );

            hasher.update(&[d1, d2]);

            if i == 0 {
                hasher.update(&cell.data[..data_size]);
            } else {
                hasher.update(current_entry.get_hash_slice(i - 1));
            }

            for (index, child) in children.iter() {
                let child_depth = if child.cell_type() == ton_types::CellType::PrunedBranch {
                    let child_data = pruned_branches
                        .get(index)
                        .ok_or(ShardStateStorageError::InvalidCell)
                        .context("Pruned branch data not found")?;
                    child.pruned_branch_depth(i, child_data)
                } else {
                    child.depth(if is_merkle_cell { i + 1 } else { i })
                };
                hasher.update(&child_depth.to_be_bytes());

                let depth = &mut max_depths[i as usize];
                *depth = std::cmp::max(*depth, child_depth + 1);

                if *depth > ton_types::MAX_DEPTH {
                    return Err(ShardStateStorageError::InvalidCell)
                        .context("Max tree depth exceeded");
                }

                current_entry.set_depth(i, *depth);
            }

            for (index, child) in children.iter() {
                if child.cell_type() == ton_types::CellType::PrunedBranch {
                    let child_data = pruned_branches
                        .get(index)
                        .ok_or(ShardStateStorageError::InvalidCell)
                        .context("Pruned branch data not found")?;
                    let child_hash = child.pruned_branch_hash(i, child_data);
                    hasher.update(child_hash);
                } else {
                    let child_hash = child.hash(if is_merkle_cell { i + 1 } else { i });
                    hasher.update(child_hash);
                }
            }

            current_entry.set_hash(i, hasher.finalize().as_slice());
        }

        // Update pruned branches
        if is_pruned_cell {
            pruned_branches.insert(cell_index, cell.data[..data_size].to_vec());
        }

        // Write cell data
        output_buffer.clear();

        output_buffer.write_all(&[cell.cell_type.to_u8().unwrap()])?;
        output_buffer.write_all(&(cell.bit_len as u16).to_le_bytes())?;
        output_buffer.write_all(&cell.data[0..(cell.bit_len + 8) / 8])?;
        output_buffer.write_all(&[cell.level_mask, 0, 1, hash_count])?; // level_mask, store_hashes, has_hashes, hash_count
        for i in 0..hash_count {
            output_buffer.write_all(current_entry.get_hash_slice(i))?;
        }
        output_buffer.write_all(&[1, hash_count])?; // has_depths, depth_count(same as hash_count)
        for i in 0..hash_count {
            output_buffer.write_all(current_entry.get_depth_slice(i))?;
        }

        // Write cell references
        output_buffer.write_all(&[cell.reference_indices.len() as u8])?;
        for (_, child) in children.iter() {
            output_buffer.write_all(child.hash(3))?; // repr hash
        }

        // Write counters
        output_buffer.write_all(current_entry.get_tree_bits_count_slice())?;
        output_buffer.write_all(current_entry.get_tree_cell_count_slice())?;

        // Save serialized data
        let db = &self.state.dynamic_boc_db.cell_db.db;
        if is_pruned_cell {
            let repr_hash = current_entry
                .as_reader()
                .pruned_branch_hash(3, &cell.data[..data_size]);
            db.insert(repr_hash, output_buffer.as_slice())?;
        } else {
            db.insert(current_entry.as_reader().hash(3), output_buffer.as_slice())?;
        };

        // Done
        Ok(())
    }
}

pub struct FilesContext {
    cells_path: PathBuf,
    cells_file: Option<File>,
    hashes_path: PathBuf,
}

impl FilesContext {
    async fn new<P>(downloads_dir: &P, block_id: &ton_block::BlockIdExt) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let block_id = format!(
            "({},{:016x},{})",
            block_id.shard_id.workchain_id(),
            block_id.shard_id.shard_prefix_with_tag(),
            block_id.seq_no
        );

        let cells_path = downloads_dir
            .as_ref()
            .join(format!("state_cells_{}", block_id));
        let hashes_path = downloads_dir
            .as_ref()
            .join(format!("state_hashes_{}", block_id));

        let cells_file = Some(
            tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .read(true)
                .open(&cells_path)
                .await
                .context("Failed to create cells file")?,
        );

        Ok(Self {
            cells_path,
            cells_file,
            hashes_path,
        })
    }

    pub async fn clear(mut self) -> Result<()> {
        if let Some(file) = self.cells_file.take() {
            file.set_len(0).await?;
            file.sync_all().await?;
        };

        tokio::fs::remove_file(self.cells_path).await?;
        tokio::fs::remove_file(self.hashes_path).await?;
        Ok(())
    }

    fn cells_file(&mut self) -> Result<&mut File> {
        match &mut self.cells_file {
            Some(file) => Ok(file),
            None => Err(ShardStateStorageError::AlreadyFinalized.into()),
        }
    }

    fn create_mapped_hashes_file(&self, length: usize) -> Result<MappedFile> {
        let mapped_file = MappedFile::new(&self.hashes_path, length)?;
        Ok(mapped_file)
    }
}

struct EntriesBuffer(Box<[[u8; HashesEntry::LEN]; 5]>);

impl EntriesBuffer {
    fn new() -> Self {
        Self(Box::new([[0; HashesEntry::LEN]; 5]))
    }

    fn current_entry_buffer(&mut self) -> &mut [u8; HashesEntry::LEN] {
        &mut self.0[0]
    }

    fn iter_child_buffers(&mut self) -> impl Iterator<Item = &mut [u8; HashesEntry::LEN]> {
        self.0.iter_mut().skip(1)
    }

    fn split_children<'a, 'b>(
        &'a mut self,
        references: &'b [u32],
    ) -> (HashesEntryWriter<'a>, EntriesBufferChildren<'b>)
    where
        'a: 'b,
    {
        let [first, tail @ ..] = &mut *self.0;
        (
            HashesEntryWriter(first),
            EntriesBufferChildren(references, tail),
        )
    }
}

struct EntriesBufferChildren<'a>(&'a [u32], &'a [[u8; HashesEntry::LEN]]);

impl EntriesBufferChildren<'_> {
    fn iter(&self) -> impl Iterator<Item = (&u32, HashesEntry)> {
        self.0
            .iter()
            .zip(self.1)
            .map(|(index, item)| (index, HashesEntry(item)))
    }
}

struct HashesEntryWriter<'a>(&'a mut [u8]);

impl HashesEntryWriter<'_> {
    fn as_reader(&self) -> HashesEntry {
        HashesEntry(self.0)
    }

    fn clear(&mut self) {
        for byte in &mut *self.0 {
            *byte = 0;
        }
    }

    fn set_level_mask(&mut self, level_mask: ton_types::LevelMask) {
        self.0[0] = level_mask.mask();
    }

    fn set_cell_type(&mut self, cell_type: ton_types::CellType) {
        self.0[1] = cell_type.into();
    }

    fn set_tree_bits_count(&mut self, count: u64) {
        self.get_tree_bits_count_slice()
            .copy_from_slice(&count.to_le_bytes());
    }

    fn get_tree_bits_count_slice(&mut self) -> &mut [u8] {
        &mut self.0[4..12]
    }

    fn set_tree_cell_count(&mut self, count: u64) {
        self.get_tree_cell_count_slice()
            .copy_from_slice(&count.to_le_bytes());
    }

    fn get_tree_cell_count_slice(&mut self) -> &mut [u8] {
        &mut self.0[12..20]
    }

    fn set_hash(&mut self, i: u8, hash: &[u8]) {
        self.get_hash_slice(i).copy_from_slice(hash);
    }

    fn get_hash_slice(&mut self, i: u8) -> &mut [u8] {
        let offset = HashesEntry::HASHES_OFFSET + 32 * i as usize;
        &mut self.0[offset..offset + 32]
    }

    fn set_depth(&mut self, i: u8, depth: u16) {
        self.get_depth_slice(i)
            .copy_from_slice(&depth.to_le_bytes());
    }

    fn get_depth_slice(&mut self, i: u8) -> &mut [u8] {
        let offset = HashesEntry::DEPTHS_OFFSET + 2 * i as usize;
        &mut self.0[offset..offset + 2]
    }
}

struct HashesEntry<'a>(&'a [u8]);

impl<'a> HashesEntry<'a> {
    // 4 bytes - info (1 byte level mask, 1 byte cell type, 2 bytes padding)
    // 8 bytes - tree bits count
    // 4 bytes - cell count
    // 32 * 4 bytes - hashes
    // 2 * 4 bytes - depths
    const LEN: usize = 4 + 8 + 8 + 32 * 4 + 2 * 4;
    const HASHES_OFFSET: usize = 4 + 8 + 8;
    const DEPTHS_OFFSET: usize = 4 + 8 + 8 + 32 * 4;

    fn level_mask(&self) -> ton_types::LevelMask {
        ton_types::LevelMask::with_mask(self.0[0])
    }

    fn cell_type(&self) -> ton_types::CellType {
        ton_types::CellType::from(self.0[1])
    }

    fn tree_bits_count(&self) -> u64 {
        u64::from_le_bytes(self.0[4..12].try_into().unwrap())
    }

    fn tree_cell_count(&self) -> u64 {
        u64::from_le_bytes(self.0[12..20].try_into().unwrap())
    }

    fn hash(&self, n: u8) -> &[u8] {
        let offset = Self::HASHES_OFFSET + 32 * self.level_mask().calc_hash_index(n as usize);
        &self.0[offset..offset + 32]
    }

    fn depth(&self, n: u8) -> u16 {
        let offset = Self::DEPTHS_OFFSET + 2 * self.level_mask().calc_hash_index(n as usize);
        u16::from_le_bytes([self.0[offset], self.0[offset + 1]])
    }

    fn pruned_branch_hash<'b>(&self, n: u8, data: &'b [u8]) -> &'b [u8]
    where
        'a: 'b,
    {
        let level_mask = self.level_mask();
        let index = level_mask.calc_hash_index(n as usize);
        let level = level_mask.level() as usize;

        if index == level {
            let offset = Self::HASHES_OFFSET;
            &self.0[offset..offset + 32]
        } else {
            let offset = 1 + 1 + index * 32;
            &data[offset..offset + 32]
        }
    }

    fn pruned_branch_depth(&self, n: u8, data: &[u8]) -> u16 {
        let level_mask = self.level_mask();
        let index = level_mask.calc_hash_index(n as usize);
        let level = level_mask.level() as usize;

        if index == level {
            let offset = Self::DEPTHS_OFFSET;
            u16::from_le_bytes([self.0[offset], self.0[offset + 1]])
        } else {
            let offset = 1 + 1 + level * 32 + index * 2;
            u16::from_be_bytes([data[offset], data[offset + 1]])
        }
    }
}

#[derive(Clone)]
pub struct DynamicBocDb {
    cell_db: CellDb,
    cells: Arc<DashMap<ton_types::UInt256, Weak<StorageCell>>>,
}

impl DynamicBocDb {
    fn with_db(db: Tree) -> Self {
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
    db: Tree,
}

impl CellDb {
    pub fn contains(&self, hash: &ton_types::UInt256) -> Result<bool> {
        let has_key = self.db.contains_key(hash.as_slice())?;
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
    #[error("Already finalized")]
    AlreadyFinalized,
    #[error("Cell db transaction conflict")]
    TransactionConflict,
    #[error("Invalid shard state packet")]
    InvalidShardStatePacket,
    #[error("Invalid cell")]
    InvalidCell,
}
