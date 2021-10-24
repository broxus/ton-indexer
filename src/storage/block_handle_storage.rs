/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - simplified storing
///
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
    pub fn with_db(db: &Arc<rocksdb::DB>) -> Result<Self> {
        Ok(Self {
            cache: Arc::new(Default::default()),
            db: Tree::new(db)?,
            key_blocks: Tree::new(db)?,
        })
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
        } else {
            iterator.raw_iterator.seek_to_first();
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

        if handle.meta().is_key_block() {
            self.key_blocks
                .insert(handle.id().seq_no.to_be_bytes(), handle.id().to_vec()?)?;
        }

        Ok(Some(handle))
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
        let value = self
            .raw_iterator
            .value()
            .map(|value| ton_block::BlockIdExt::from_slice(value))?;
        self.raw_iterator.next();
        Some(value)
    }
}
