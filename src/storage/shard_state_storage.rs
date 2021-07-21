use std::collections::HashMap;
use std::convert::TryInto;
use std::io::{Read, Write};
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use crc::crc32::{self, Hasher32};
use dashmap::DashMap;
use tokio::sync::{RwLock, RwLockWriteGuard};
use ton_types::{ByteOrderRead, CellImpl, UInt256};

use super::storage_cell::StorageCell;
use crate::storage::StoredValue;
use crate::utils::*;

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
    pending_cells: HashMap<u32, RawCell>,
}

impl<'a> ShardStateReplaceTransaction<'a> {
    fn new(state: RwLockWriteGuard<'a, ShardStateStorageState>) -> Self {
        Self {
            state,
            executed: false,
            reader: ShardStatePacketReader::new(),
            boc_header: None,
            cells_read: 0,
            pending_cells: HashMap::new(),
        }
    }

    pub fn finalize(self, block_id: &ton_block::BlockIdExt, root_cell_hash: &[u8]) -> Result<()> {
        let key = block_id.to_vec()?;
        self.state.shard_state_db.insert(key, root_cell_hash)?;
        Ok(())
    }

    pub fn process(&mut self, data: Vec<u8>, last: bool) -> Result<Option<UInt256>> {
        self.reader.next_packet = data;

        loop {
            let header = match &self.boc_header {
                Some(header) => header,
                None => {
                    let mut reader = self.reader.begin();
                    let header = BocHeader::from_reader(&mut reader)?;
                    reader.end();

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

            while self.cells_read < header.cell_count {
                let mut reader = self.reader.begin();

                match RawCell::from_reader(
                    &mut reader,
                    header.ref_size,
                    header.cell_count,
                    self.cells_read,
                )? {
                    Some(_cell) => {
                        println!("READ CELL");
                        reader.end();
                    }
                    None if last => return Ok(Some(UInt256::default())),
                    None => return Ok(None),
                };
            }
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

struct BocHeader {
    root_index: Option<usize>,
    index_included: bool,
    has_crc: bool,
    ref_size: usize,
    offset_size: usize,
    cell_count: usize,
}

impl BocHeader {
    fn from_reader<R>(src: &mut R) -> Result<Self>
    where
        R: Read,
    {
        const BOC_INDEXED_TAG: u32 = 0x68ff65f3;
        const BOC_INDEXED_CRC32_TAG: u32 = 0xacc3a728;
        const BOC_GENERIC_TAG: u32 = 0xb5ee9c72;

        let magic = src.read_be_u32()?;
        let first_byte = src.read_byte()?;

        let index_included;
        let mut has_crc = false;
        let ref_size;

        match magic {
            BOC_INDEXED_TAG => {
                ref_size = first_byte as usize;
                index_included = true;
            }
            BOC_INDEXED_CRC32_TAG => {
                ref_size = first_byte as usize;
                index_included = true;
                has_crc = true;
            }
            BOC_GENERIC_TAG => {
                index_included = first_byte & 0b1000_0000 != 0;
                has_crc = first_byte & 0b0100_0000 != 0;
                ref_size = (first_byte & 0b0000_0111) as usize;
            }
            _ => {
                return Err(ShardStateStorageError::InvalidShardStateHeader)
                    .context("Invalid flags")
            }
        }

        if ref_size == 0 || ref_size > 4 {
            return Err(ShardStateStorageError::InvalidShardStateHeader)
                .context("Ref size must be in range [1;4]");
        }

        let offset_size = src.read_byte()? as usize;
        if offset_size == 0 || offset_size > 8 {
            return Err(ShardStateStorageError::InvalidShardStateHeader)
                .context("Offset size must be in range [1;8]");
        }

        let cell_count = src.read_be_uint(ref_size)?;
        let root_count = src.read_be_uint(ref_size)?;
        src.read_be_uint(ref_size)?; // skip absent

        if root_count != 1 {
            return Err(ShardStateStorageError::InvalidShardStateHeader)
                .context("Expected one root cell");
        }
        if root_count > cell_count {
            return Err(ShardStateStorageError::InvalidShardStateHeader)
                .context("Root count is greater then cell count");
        }

        src.read_be_uint(offset_size)?; // skip total cells size

        let root_index = if magic == BOC_GENERIC_TAG {
            Some(src.read_be_uint(ref_size)?)
        } else {
            None
        };

        Ok(Self {
            root_index,
            index_included,
            has_crc,
            ref_size,
            offset_size,
            cell_count,
        })
    }
}

macro_rules! try_read {
    ($expr:expr) => {
        match $expr {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
    };
}

struct RawCell {
    cell_type: ton_types::CellType,
    level: u8,
    data: Vec<u8>,
    reference_indices: Vec<u32>,
    reference_hashes: Vec<UInt256>,
    hashes: Option<[[u8; 32]; 4]>,
    depths: Option<[u16; 4]>,
}

impl RawCell {
    fn from_reader<R>(
        src: &mut R,
        ref_size: usize,
        cell_count: usize,
        cell_index: usize,
    ) -> Result<Option<Self>>
    where
        R: Read,
    {
        let d1 = try_read!(src.read_byte());
        let l = d1 >> 5;
        let h = (d1 & 0b0001_0000) != 0;
        let s = (d1 & 0b0000_1000) != 0;
        let r = (d1 & 0b0000_0111) as usize;
        let absent = r == 7 && h;

        if absent {
            let data_size = 32 * ((ton_types::LevelMask::with_level(l).level() + 1) as usize);
            let mut cell_data = vec![0; data_size + 1];
            try_read!(src.read_exact(&mut cell_data[..data_size]));
            cell_data[data_size] = 0x80;

            return Ok(Some(RawCell {
                cell_type: ton_types::CellType::Ordinary,
                level: l,
                data: cell_data,
                reference_indices: Vec::new(),
                reference_hashes: Vec::new(),
                hashes: None,
                depths: None,
            }));
        }

        if r > 4 {
            return Err(ShardStateStorageError::InvalidShardStateCell)
                .context("Cell must contain at most 4 references");
        }

        let d2 = try_read!(src.read_byte());
        let data_size = ((d2 >> 1) + if d2 & 1 != 0 { 1 } else { 0 }) as usize;
        let no_completion_tag = d2 & 1 == 0;

        let (hashes, depths) = if h {
            let mut hashes = [[0u8; 32]; 4];
            let mut depths = [0; 4];

            let level = ton_types::LevelMask::with_mask(l).level() as usize;
            for hash in hashes.iter_mut().take(level + 1) {
                try_read!(src.read_exact(hash));
            }
            for depth in depths.iter_mut().take(level + 1) {
                *depth = try_read!(src.read_be_uint(2)) as u16;
            }
            (Some(hashes), Some(depths))
        } else {
            (None, None)
        };

        let mut cell_data = vec![0; data_size + if no_completion_tag { 1 } else { 0 }];
        try_read!(src.read_exact(&mut cell_data[..data_size]));

        if no_completion_tag {
            cell_data[data_size] = 0x80;
        }

        let cell_type = if !s {
            ton_types::CellType::Ordinary
        } else {
            ton_types::CellType::from(cell_data[0])
        };

        let mut reference_indices = Vec::with_capacity(r);
        if r > 0 {
            for _ in 0..r {
                let index = try_read!(src.read_be_uint(ref_size));
                if index > cell_count || index <= cell_index {
                    return Err(ShardStateStorageError::InvalidShardStateCell)
                        .context("Reference index out of range");
                } else {
                    reference_indices.push(index as u32);
                }
            }
        }

        Ok(Some(RawCell {
            cell_type,
            level: l,
            data: cell_data,
            reference_indices,
            reference_hashes: Vec::with_capacity(r),
            hashes,
            depths,
        }))
    }

    fn hash(&self, mut index: usize) -> [u8; 32] {
        index = ton_types::LevelMask::with_mask(self.level).calc_hash_index(index);
        if self.cell_type == ton_types::CellType::PrunedBranch {
            if index != self.level as usize {
                let offset = 1 + 1 + index * 32;
                self.data[offset..offset + 32].try_into().unwrap()
            } else if let Some(hashes) = &self.hashes {
                hashes[0]
            } else {
                unreachable!()
            }
        } else if let Some(hashes) = &self.hashes {
            hashes[index]
        } else {
            unreachable!()
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum ReaderAction {
    Incomplete,
    Complete,
}

struct ShardStatePacketReader {
    hasher: crc32::Digest,
    offset: usize,
    current_packet: Vec<u8>,
    next_packet: Vec<u8>,
    bytes_to_skip: usize,
}

impl ShardStatePacketReader {
    fn new() -> Self {
        Self {
            hasher: crc32::Digest::new(crc32::CASTAGNOLI),
            offset: 0,
            current_packet: Default::default(),
            next_packet: Default::default(),
            bytes_to_skip: 0,
        }
    }

    fn begin(&'_ mut self) -> ShardStatePacketReaderTransaction<'_> {
        let offset = self.offset;
        ShardStatePacketReaderTransaction {
            reader: self,
            reading_next_packet: false,
            offset,
        }
    }

    fn set_skip(&mut self, n: usize) {
        self.bytes_to_skip = n;
    }

    fn process_skip(&mut self) -> ReaderAction {
        let mut n = std::mem::take(&mut self.bytes_to_skip);

        let remaining = self.current_packet.len() - self.offset;
        if n > remaining {
            n -= remaining;
            self.offset = 0;
            self.current_packet = std::mem::take(&mut self.next_packet);
        } else {
            self.offset += n;
            return ReaderAction::Complete;
        }

        if n > self.current_packet.len() {
            n -= self.current_packet.len();
            self.current_packet = Vec::new();
            self.bytes_to_skip = n;
            ReaderAction::Incomplete
        } else {
            self.offset += n;
            ReaderAction::Complete
        }
    }
}

struct ShardStatePacketReaderTransaction<'a> {
    reader: &'a mut ShardStatePacketReader,
    reading_next_packet: bool,
    offset: usize,
}

impl<'a> ShardStatePacketReaderTransaction<'a> {
    fn end(self) {
        if self.reading_next_packet {
            // Write to the hasher until the end of current packet
            self.reader
                .hasher
                .write(&self.reader.current_packet[self.reader.offset..]);

            // Write to the hasher current bytes
            self.reader
                .hasher
                .write(&self.reader.next_packet[..self.offset]);

            // Replace current packet
            self.reader.current_packet = std::mem::take(&mut self.reader.next_packet);
        } else {
            // Write to the hasher current bytes
            self.reader
                .hasher
                .write(&self.reader.current_packet[..self.offset]);
        }

        // Bump offset
        self.reader.offset = self.offset;
    }
}

impl<'a> Read for ShardStatePacketReaderTransaction<'a> {
    fn read(&mut self, mut buf: &mut [u8]) -> std::io::Result<usize> {
        let mut result = 0;

        loop {
            let current_packet = match self.reading_next_packet {
                // Reading non-empty current packet
                false if self.offset < self.reader.current_packet.len() => {
                    &self.reader.current_packet
                }

                // Current packet is empty - retry and switch to next
                false => {
                    self.reading_next_packet = true;
                    self.offset = 0;
                    continue;
                }

                // Reading non-empty next packet
                true if self.offset < self.reader.next_packet.len() => &self.reader.next_packet,

                // Reading next packet which is empty
                true => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "packet buffer underflow",
                    ))
                }
            };

            let n = std::cmp::min(current_packet.len() - self.offset, buf.len());
            for i in 0..n {
                buf[i] = current_packet[self.offset + i];
            }

            result += n;
            self.offset += n;

            let tmp = buf;
            buf = &mut tmp[n..];

            if buf.is_empty() {
                return Ok(result);
            }
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
    #[error("Invalid shard state cell")]
    InvalidShardStateCell,
}
