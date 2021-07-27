use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use ton_types::{ByteOrderRead, CellImpl};

use super::shard_state_storage::DynamicBocDb;
use crate::utils::*;

pub struct StorageCell {
    boc_db: DynamicBocDb,
    cell_data: ton_types::CellData,
    references: RwLock<Vec<StorageCellReference>>,
    tree_bits_count: Arc<AtomicU64>,
    tree_cell_count: Arc<AtomicU64>,
}

impl StorageCell {
    pub fn repr_hash(&self) -> ton_types::UInt256 {
        self.hash(ton_types::MAX_LEVEL as usize)
    }

    pub fn deserialize(boc_db: DynamicBocDb, data: &[u8]) -> anyhow::Result<Self> {
        let mut reader = std::io::Cursor::new(data);

        let cell_data = ton_types::CellData::deserialize(&mut reader).convert()?;
        let references_count = reader.read_byte()?;
        let mut references = Vec::with_capacity(references_count as usize);

        for _ in 0..references_count {
            let hash = ton_types::UInt256::from(reader.read_u256()?);
            references.push(StorageCellReference::Unloaded(hash));
        }

        let (tree_bits_count, tree_cell_count) = match reader.read_le_u64() {
            Ok(tree_bits_count) => match reader.read_le_u64() {
                Ok(tree_cell_count) => (tree_bits_count, tree_cell_count),
                Err(_) => (0, 0),
            },
            Err(_) => (0, 0),
        };

        Ok(Self {
            boc_db,
            cell_data,
            references: RwLock::new(references),
            tree_bits_count: Arc::new(AtomicU64::new(tree_bits_count)),
            tree_cell_count: Arc::new(AtomicU64::new(tree_cell_count)),
        })
    }

    #[allow(unused)]
    pub fn deserialize_references(data: &[u8]) -> anyhow::Result<Vec<StorageCellReference>> {
        let mut reader = std::io::Cursor::new(data);

        let _cell_data = ton_types::CellData::deserialize(&mut reader).convert()?;
        let references_count = reader.read_byte()?;
        let mut references = Vec::with_capacity(references_count as usize);

        for _ in 0..references_count {
            let hash = ton_types::UInt256::from(reader.read_u256()?);
            references.push(StorageCellReference::Unloaded(hash));
        }
        Ok(references)
    }

    pub fn serialize(cell: &dyn CellImpl) -> anyhow::Result<Vec<u8>> {
        let references_count = cell.references_count() as u8;

        let mut data = Vec::new();
        cell.cell_data().serialize(&mut data).convert()?;
        data.write_all(&[references_count])?;

        for i in 0..references_count {
            data.write_all(cell.reference(i as usize).convert()?.repr_hash().as_slice())?;
        }

        data.write_all(&cell.tree_bits_count().to_le_bytes())?;
        data.write_all(&cell.tree_cell_count().to_le_bytes())?;

        Ok(data)
    }

    fn reference(&self, index: usize) -> anyhow::Result<Arc<StorageCell>> {
        let hash = match &self.references.read().get(index) {
            Some(StorageCellReference::Unloaded(hash)) => *hash,
            Some(StorageCellReference::Loaded(cell)) => return Ok(cell.clone()),
            None => return Err(StorageCellError::AccessingInvalidReference.into()),
        };

        let storage_cell = self.boc_db.load_cell(hash)?;
        self.references.write()[index] = StorageCellReference::Loaded(storage_cell.clone());

        Ok(storage_cell)
    }
}

impl CellImpl for StorageCell {
    fn data(&self) -> &[u8] {
        self.cell_data.data()
    }

    fn cell_data(&self) -> &ton_types::CellData {
        &self.cell_data
    }

    fn bit_length(&self) -> usize {
        self.cell_data.bit_length() as usize
    }

    fn references_count(&self) -> usize {
        self.references.read().len()
    }

    fn reference(&self, index: usize) -> ton_types::Result<ton_types::Cell> {
        Ok(ton_types::Cell::with_cell_impl_arc(
            self.reference(index)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
        ))
    }

    fn cell_type(&self) -> ton_types::CellType {
        self.cell_data.cell_type()
    }

    fn level_mask(&self) -> ton_types::LevelMask {
        self.cell_data.level_mask()
    }

    fn hash(&self, index: usize) -> ton_types::UInt256 {
        self.cell_data.hash(index)
    }

    fn depth(&self, index: usize) -> u16 {
        self.cell_data.depth(index)
    }

    fn store_hashes(&self) -> bool {
        self.cell_data.store_hashes()
    }

    fn tree_bits_count(&self) -> u64 {
        self.tree_bits_count.load(Ordering::Acquire)
    }

    fn tree_cell_count(&self) -> u64 {
        self.tree_cell_count.load(Ordering::Acquire)
    }
}

impl Drop for StorageCell {
    fn drop(&mut self) {
        self.boc_db.drop_cell(&self.repr_hash())
    }
}

impl PartialEq for StorageCell {
    fn eq(&self, other: &Self) -> bool {
        if self.cell_data != other.cell_data {
            return false;
        }

        let references = self.references.read();
        let other_references = self.references.read();
        references.len() == other_references.len()
            && same_references(&references, &other_references)
    }
}

fn same_references(
    references: &[StorageCellReference],
    other_references: &[StorageCellReference],
) -> bool {
    for i in 0..references.len() {
        if references[i].hash() != other_references[i].hash() {
            return false;
        }
    }
    true
}

#[derive(Clone, PartialEq)]
pub enum StorageCellReference {
    Loaded(Arc<StorageCell>),
    Unloaded(ton_types::UInt256),
}

impl StorageCellReference {
    pub fn hash(&self) -> ton_types::UInt256 {
        match self {
            Self::Loaded(cell) => cell.repr_hash(),
            Self::Unloaded(hash) => *hash,
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum StorageCellError {
    #[error("Accessing invalid cell reference")]
    AccessingInvalidReference,
}
