/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - simplified storing
///
use std::sync::{Arc, Weak};

use anyhow::Result;
use ton_types::FxDashMap;

use super::{columns, BlockHandle, BlockMeta, BlockMetaData, StoredValue, Tree};
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

    pub fn store_block_applied(&self, handle: &Arc<BlockHandle>) -> Result<bool> {
        if handle.meta().set_is_applied() {
            self.store_handle(handle)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn assign_mc_ref_seq_no(
        &self,
        handle: &Arc<BlockHandle>,
        mc_ref_seq_no: u32,
    ) -> Result<()> {
        if handle.set_masterchain_ref_seqno(mc_ref_seq_no)? {
            self.store_handle(handle)?;
        }
        Ok(())
    }

    pub fn create_or_load_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
        meta_data: BlockMetaData,
    ) -> Result<(Arc<BlockHandle>, HandleCreationStatus)> {
        if let Some(handle) = self.load_handle(block_id)? {
            return Ok((handle, HandleCreationStatus::Fetched));
        }

        if let Some(handle) =
            self.create_handle(block_id.clone(), BlockMeta::with_data(meta_data))?
        {
            return Ok((handle, HandleCreationStatus::Created));
        }

        if let Some(handle) = self.load_handle(block_id)? {
            return Ok((handle, HandleCreationStatus::Fetched));
        }

        Err(BlockHandleStorageError::FailedToCreateBlockHandle.into())
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

    pub fn store_handle(&self, handle: &BlockHandle) -> Result<()> {
        let id = handle.id();

        self.block_handles
            .insert(id.root_hash.as_slice(), handle.meta().to_vec())?;

        if handle.is_key_block() {
            self.key_blocks
                .insert(id.seq_no.to_be_bytes(), id.to_vec())?;
        }

        Ok(())
    }

    pub fn load_key_block_handle(&self, seq_no: u32) -> Result<Arc<BlockHandle>> {
        let key_block_id = self
            .key_blocks
            .get(seq_no.to_be_bytes())?
            .map(|value| ton_block::BlockIdExt::from_slice(value.as_ref()))
            .transpose()?
            .ok_or(BlockHandleStorageError::KeyBlockNotFound)?;

        self.load_handle(&key_block_id)?.ok_or_else(|| {
            BlockHandleStorageError::KeyBlockHandleNotFound(key_block_id.seq_no).into()
        })
    }

    pub fn find_last_key_block(&self) -> Result<Arc<BlockHandle>> {
        let mut iter = self.key_blocks.raw_iterator();
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
        let mut iter = self.key_blocks.raw_iterator();
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
        let mut iter = self.key_blocks.raw_iterator();
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

    pub fn key_blocks_iterator(
        &self,
        direction: KeyBlocksDirection,
    ) -> impl Iterator<Item = Result<ton_block::BlockIdExt>> + '_ {
        let mut raw_iterator = self.key_blocks.raw_iterator();
        let reverse = match direction {
            KeyBlocksDirection::ForwardFrom(seq_no) => {
                raw_iterator.seek(seq_no.to_be_bytes());
                false
            }
            KeyBlocksDirection::Backward => {
                raw_iterator.seek_to_last();
                true
            }
        };

        KeyBlocksIterator {
            raw_iterator,
            reverse,
        }
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

            if block_id.seq_no == 0
                || block_id.is_masterchain() && value.is_key_block()
                || top_blocks.contains(block_id)
            {
                // Keep zero state, key blocks and latest blocks
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

    fn create_handle(
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

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum HandleCreationStatus {
    Created,
    Fetched,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum KeyBlocksDirection {
    ForwardFrom(u32),
    Backward,
}

struct KeyBlocksIterator<'a> {
    raw_iterator: rocksdb::DBRawIterator<'a>,
    reverse: bool,
}

impl Iterator for KeyBlocksIterator<'_> {
    type Item = Result<ton_block::BlockIdExt>;

    fn next(&mut self) -> Option<Self::Item> {
        let value = self
            .raw_iterator
            .value()
            .map(ton_block::BlockIdExt::from_slice)?;
        if self.reverse {
            self.raw_iterator.prev();
        } else {
            self.raw_iterator.next();
        }

        Some(value)
    }
}

#[derive(thiserror::Error, Debug)]
enum BlockHandleStorageError {
    #[error("Failed to create block handle")]
    FailedToCreateBlockHandle,
    #[error("Key block not found")]
    KeyBlockNotFound,
    #[error("Key block handle not found: {}", .0)]
    KeyBlockHandleNotFound(u32),
}
