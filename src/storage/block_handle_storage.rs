use std::sync::{Arc, Weak};

use anyhow::Result;
use tiny_adnl::utils::*;
use ton_types::UInt256;

use crate::storage::{columns, StoredValue};

use super::block_handle::*;
use super::block_meta::*;
use super::tree::*;

pub struct BlockHandleStorage {
    cache: Arc<FxDashMap<ton_block::BlockIdExt, Weak<BlockHandle>>>,
    db: Tree<columns::BlockHandles>,
}

impl BlockHandleStorage {
    pub fn with_db(db: Tree<columns::BlockHandles>) -> Self {
        Self {
            cache: Arc::new(Default::default()),
            db,
        }
    }

    pub fn load_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>> {
        Ok(loop {
            if let Some(weak) = self.cache.get(block_id) {
                if let Some(handle) = weak.upgrade() {
                    break Some(handle);
                }
            }

            if let Some(meta) = self.db.get(block_id.root_hash.as_slice())? {
                let meta = BlockMeta::from_slice(meta.as_ref())?;
                if let Some(handle) = self.create_handle(block_id.clone(), meta)? {
                    break Some(handle);
                }
            } else {
                break None;
            }
        })
    }

    pub fn key_block_iter(&self) -> Result<impl Iterator<Item = (UInt256, BlockMeta)> + '_> {
        use rocksdb::IteratorMode;
        let db = self.db.raw_db_handle();
        let iterator = db.iterator_cf(&self.db.get_cf()?, IteratorMode::End);
        let iter = iterator
            .into_iter()
            .filter_map(|(k, v)| {
                let key = if k.len() == 32 {
                    UInt256::from_slice(&k)
                } else {
                    return None;
                };
                match BlockMeta::from_slice(&v) {
                    Ok(a) => Some((key, a)),
                    Err(_) => None,
                }
            })
            .filter(|(_, v)| v.is_key_block());
        Ok(iter)
    }

    pub fn store_handle(&self, handle: &Arc<BlockHandle>) -> Result<()> {
        self.db
            .insert(handle.id().root_hash.as_slice(), handle.meta().to_vec()?)?;
        Ok(())
    }

    pub fn create_handle(
        &self,
        block_id: ton_block::BlockIdExt,
        meta: BlockMeta,
    ) -> Result<Option<Arc<BlockHandle>>> {
        use dashmap::mapref::entry::Entry;

        let handle = match self.cache.entry(block_id.clone()) {
            Entry::Vacant(entry) => {
                let handle = Arc::new(BlockHandle::with_values(block_id, meta, self.cache.clone()));
                entry.insert(Arc::downgrade(&handle));
                handle
            }
            Entry::Occupied(_) => return Ok(None),
        };

        self.store_handle(&handle)?;
        Ok(Some(handle))
    }
}
