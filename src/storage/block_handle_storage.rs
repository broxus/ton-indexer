/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - simplified storing
///
use std::collections::HashSet;
use std::sync::{Arc, Weak};

use anyhow::Result;
use tiny_adnl::utils::*;

use super::block_handle::*;
use super::block_meta::*;
use super::tree::*;
use super::{columns, StoredValue};

pub struct BlockHandleStorage {
    cache: Arc<FxDashMap<ton_block::BlockIdExt, Weak<BlockHandle>>>,
    db: Tree<columns::BlockHandles>,
    key_blocks: Tree<columns::KeyBlocks>,
}

impl BlockHandleStorage {
    pub fn with_db(db: Tree<columns::BlockHandles>, key_blocks: Tree<columns::KeyBlocks>) -> Self {
        Self {
            cache: Arc::new(Default::default()),
            db,
            key_blocks,
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
        let id = handle.id();

        self.db
            .insert(id.root_hash.as_slice(), handle.meta().to_vec()?)?;

        if handle.meta().is_key_block() {
            self.key_blocks
                .insert(id.seq_no.to_be_bytes(), id.to_vec()?)?;
        }

        Ok(())
    }

    pub fn find_key_block(&self, seq_no: u32) -> Result<Option<ton_block::BlockIdExt>> {
        self.key_blocks
            .get(seq_no.to_be_bytes())?
            .map(|id| ton_block::BlockIdExt::from_slice(&id))
            .transpose()
    }

    pub fn find_key_block_handle(&self, seq_no: u32) -> Result<Option<Arc<BlockHandle>>> {
        match self.find_key_block(seq_no)? {
            Some(block_id) => self.load_handle(&block_id),
            None => Ok(None),
        }
    }

    pub fn key_block_iterator(&self, since: Option<u32>) -> Result<KeyBlocksIterator> {
        let mut iterator = KeyBlocksIterator {
            raw_iterator: self.key_blocks.raw_iterator()?,
        };
        if let Some(seq_no) = since {
            iterator.seek(seq_no);
        }
        Ok(iterator)
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
        let mut untouched = HashSet::new();
        for id in ids {
            let h = match self.load_handle(id)? {
                Some(a) => a,
                None => continue,
            };
            if h.meta().is_key_block() {
                untouched.insert(id.clone());
                continue;
            }
            if h.meta().has_data() {
                h.meta().set_has_data();
            } else {
                untouched.insert(id.clone());
            }
            drop(h);
            total += 1;
        }
        Ok((total, untouched))
    }
}

pub struct KeyBlocksIterator<'a> {
    raw_iterator: rocksdb::DBRawIterator<'a>,
}

impl KeyBlocksIterator<'_> {
    pub fn seek(&mut self, seq_no: u32) {
        self.raw_iterator.seek(seq_no.to_be_bytes());
    }
}

impl Iterator for KeyBlocksIterator<'_> {
    type Item = Result<ton_block::BlockIdExt>;

    fn next(&mut self) -> Option<Self::Item> {
        self.raw_iterator
            .value()
            .map(|value| ton_block::BlockIdExt::from_slice(value))
    }
}
