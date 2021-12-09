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
use super::{columns, StoredValue, TopBlocks};
use crate::utils::*;

pub struct BlockHandleStorage {
    cache: Arc<FxDashMap<ton_block::BlockIdExt, Weak<BlockHandle>>>,
    block_handles: Tree<columns::BlockHandles>,
    key_blocks: Tree<columns::KeyBlocks>,
}

impl BlockHandleStorage {
    pub fn with_db(db: &Arc<rocksdb::DB>) -> Result<Self> {
        Ok(Self {
            cache: Arc::new(Default::default()),
            block_handles: Tree::new(db)?,
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

            if let Some(meta) = self.block_handles.get(block_id.root_hash.as_slice())? {
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

        self.block_handles
            .insert(id.root_hash.as_slice(), handle.meta().to_vec()?)?;

        if handle.meta().is_key_block() {
            self.key_blocks
                .insert(handle.id().seq_no.to_be_bytes(), handle.id().to_vec()?)?;
        }

        Ok(())
    }

    pub fn find_last_key_block(&self) -> Result<Arc<BlockHandle>> {
        let mut iter = self.key_blocks.raw_iterator()?;
        iter.seek_to_last();

        // Load key block from current iterator value
        let key_block_id = iter
            .value()
            .map(ton_block::BlockIdExt::from_slice)
            .transpose()?
            .ok_or(BlockHandleStorageError::KeyBlockNotFound)?;

        self.load_handle(&key_block_id)?.ok_or_else(|| {
            BlockHandleStorageError::KeyBlockHandleNotFound(key_block_id.seq_no).into()
        })
    }

    pub fn find_prev_key_block(&self, seq_no: u32) -> Result<Option<Arc<BlockHandle>>> {
        if seq_no == 0 {
            return Ok(None);
        }

        // Create iterator and move it to the previous key block before the specified
        let mut iter = self.key_blocks.raw_iterator()?;
        iter.seek_for_prev((seq_no - 1u32).to_be_bytes());

        // Load key block from current iterator value
        iter.value()
            .map(ton_block::BlockIdExt::from_slice)
            .transpose()?
            .map(|key_block_id| {
                self.load_handle(&key_block_id)?.ok_or_else(|| {
                    BlockHandleStorageError::KeyBlockHandleNotFound(key_block_id.seq_no).into()
                })
            })
            .transpose()
    }

    pub fn find_prev_persistent_key_block(&self, seq_no: u32) -> Result<Option<Arc<BlockHandle>>> {
        if seq_no == 0 {
            return Ok(None);
        }

        // Create iterator and move it to the previous key block before the specified
        let mut iter = self.key_blocks.raw_iterator()?;
        iter.seek_for_prev((seq_no - 1u32).to_be_bytes());

        // Loads key block from current iterator value and moves it backward
        let mut get_key_block = move || -> Result<Option<Arc<BlockHandle>>> {
            // Load key block id
            let key_block_id = match iter
                .value()
                .map(ton_block::BlockIdExt::from_slice)
                .transpose()?
            {
                Some(prev_key_block) => prev_key_block,
                None => return Ok(None),
            };

            // Load block handle for this id
            let handle = self.load_handle(&key_block_id)?.ok_or(
                BlockHandleStorageError::KeyBlockHandleNotFound(key_block_id.seq_no),
            )?;

            // Move iterator backward
            iter.prev();

            // Done
            Ok(Some(handle))
        };

        // Load previous key block
        let mut key_block = match get_key_block()? {
            Some(id) => id,
            None => return Ok(None),
        };

        // Load previous key blocks and check if the `key_block` is for persistent state
        while let Some(prev_key_block) = get_key_block()? {
            if is_persistent_state(
                key_block.meta().gen_utime(),
                prev_key_block.meta().gen_utime(),
            ) {
                // Found
                return Ok(Some(key_block));
            }
            key_block = prev_key_block;
        }

        // Not found
        Ok(None)
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

        Ok(Some(handle))
    }

    pub fn gc_handles_cache(&self, top_blocks: &TopBlocks) -> usize {
        let mut total_removed = 0;

        self.cache.retain(|block_id, value| {
            let value = match value.upgrade() {
                Some(value) => value,
                None => {
                    total_removed += 1;
                    return false;
                }
            };

            if block_id.is_masterchain() && value.meta().is_key_block()
                || top_blocks.contains(block_id)
            {
                // Keep key blocks and latest blocks
                true
            } else {
                // Remove all outdated
                total_removed += 1;
                value.meta().clear_data_and_proof();
                false
            }
        });

        total_removed
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
            .map(ton_block::BlockIdExt::from_slice)?;
        self.raw_iterator.next();
        Some(value)
    }
}

#[derive(thiserror::Error, Debug)]
enum BlockHandleStorageError {
    #[error("Key block not found")]
    KeyBlockNotFound,
    #[error("Key block handle not found: {}", .0)]
    KeyBlockHandleNotFound(u32),
}
