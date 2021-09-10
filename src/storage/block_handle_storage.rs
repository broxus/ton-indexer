use std::collections::HashSet;
use std::sync::{Arc, Weak};

use anyhow::Result;
use tiny_adnl::utils::*;

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

    /// returns number of dropped blocks and set of key blocks
    pub fn drop_handles_data<'a>(
        &self,
        ids: impl Iterator<Item = &'a ton_block::BlockIdExt>,
    ) -> Result<(usize, HashSet<ton_block::BlockIdExt>)> {
        let mut total = 0;
        let mut key_blocks = HashSet::new();
        for id in ids {
            let h = match self.load_handle(id)? {
                Some(a) => a,
                None => continue,
            };
            if h.meta().is_key_block() {
                key_blocks.insert(id.clone());
                continue;
            }
            if h.meta().has_data() {
                h.meta().set_has_data();
            }
            drop(h);
            total += 1;
        }
        Ok((total, key_blocks))
    }
}
