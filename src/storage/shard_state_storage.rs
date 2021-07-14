use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::{Arc, Weak};

use anyhow::Result;
use dashmap::DashMap;

use super::storage_cell::StorageCell;
use crate::storage::StoredValue;
use crate::utils::*;

pub struct ShardStateStorage {
    shard_state_db: sled::Tree,
    dynamic_boc_db: Arc<DynamicBocDb>,
}

impl ShardStateStorage {
    pub fn with_db(shard_state_db: sled::Tree, cell_db_path: sled::Tree) -> Self {
        Self {
            shard_state_db,
            dynamic_boc_db: Arc::new(DynamicBocDb::with_db(cell_db_path)),
        }
    }

    pub fn store_state(
        &self,
        block_id: &ton_block::BlockIdExt,
        root: ton_types::Cell,
    ) -> Result<()> {
        let cell_id = root.repr_hash();
        self.dynamic_boc_db.store_dynamic_boc(root)?;

        let key = block_id.to_vec()?;
        self.shard_state_db.insert(key, cell_id.as_slice())?;

        Ok(())
    }

    pub fn load_state(&self, block_id: &ton_block::BlockIdExt) -> Result<ton_types::Cell> {
        match self.shard_state_db.get(block_id.to_vec()?)? {
            Some(root) => {
                let cell_id = ton_types::UInt256::from_be_bytes(root.as_ref());
                let cell = self.dynamic_boc_db.load_cell(cell_id)?;
                Ok(ton_types::Cell::with_cell_impl_arc(cell))
            }
            None => Err(ShardStateStorageError::NotFound.into()),
        }
    }
}

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
        use sled::Transactional;

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

    pub fn load_cell(self: &Arc<Self>, hash: ton_types::UInt256) -> Result<Arc<StorageCell>> {
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
    pub fn with_db(db: sled::Tree) -> Self {
        Self { db }
    }

    pub fn contains(&self, hash: &ton_types::UInt256) -> Result<bool> {
        let has_key = self.db.contains_key(hash.as_ref())?;
        Ok(has_key)
    }

    pub fn load(
        &self,
        boc_db: &Arc<DynamicBocDb>,
        hash: &ton_types::UInt256,
    ) -> Result<StorageCell> {
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
}
