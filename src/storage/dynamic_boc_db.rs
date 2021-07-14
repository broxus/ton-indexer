use std::sync::{Arc, Weak};

use anyhow::Result;
use dashmap::DashMap;

use super::storage_cell::StorageCell;

pub struct DynamicBocDb {
    cell_db: CellDb,
    cells: Arc<DashMap<ton_types::UInt256, Weak<StorageCell>>>,
}

impl DynamicBocDb {
    pub fn with_db(db: sled::Tree) -> Self {
        Self {
            cell_db: CellDb { db },
            cells: Arc::new(DashMap::new()),
        }
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

    fn store_tree_of_cells(&self, cell: &dyn ton_types::CellImpl) -> Result<usize> {
        enum NodeOrigin {
            FromParent,
        }

        let mut origin = NodeOrigin::FromParent;
        let mut current_parent: Option<(Arc<dyn ton_types::CellImpl>, usize)> = None;
        let mut current: Option<(Arc<dyn ton_types::CellImpl>, usize)> = None;

        // while let Some((current, reference_count)) = current {
        //     match origin {
        //         NodeOrigin::FromParent => {
        //             // TODO: store
        //
        //             if reference_count == 0 {
        //                 break;
        //             }
        //             current_parent = todo!();
        //         }
        //     }
        // }

        Ok(todo!())
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
            None => Err(DynamicBocDbError::CellNotFound.into()),
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum DynamicBocDbError {
    #[error("Cell not found in cell db")]
    CellNotFound,
}
