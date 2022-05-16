use std::sync::Arc;

use anyhow::Result;

use super::{columns, read_block_id_le, write_block_id_le, BlockHandle, Column, StoredValue, Tree};

/// Stores relations between blocks
pub struct BlockConnectionStorage {
    block_handles: Tree<columns::BlockHandles>,
    key_blocks: Tree<columns::KeyBlocks>,
    prev1_block_db: Tree<columns::Prev1>,
    prev2_block_db: Tree<columns::Prev2>,
    next1_block_db: Tree<columns::Next1>,
    next2_block_db: Tree<columns::Next2>,
}

impl BlockConnectionStorage {
    pub fn with_db(db: &Arc<rocksdb::DB>) -> Result<Self> {
        Ok(Self {
            block_handles: Tree::new(db)?,
            key_blocks: Tree::new(db)?,
            prev1_block_db: Tree::new(db)?,
            prev2_block_db: Tree::new(db)?,
            next1_block_db: Tree::new(db)?,
            next2_block_db: Tree::new(db)?,
        })
    }

    pub fn store_connection(
        &self,
        handle: &BlockHandle,
        direction: BlockConnection,
        connected_block_id: &ton_block::BlockIdExt,
    ) -> Result<()> {
        // Use strange match because all columns have different types
        let store = match direction {
            BlockConnection::Prev1 => {
                if handle.meta().has_prev1() {
                    return Ok(());
                }
                store_block_connection_impl(&self.prev1_block_db, handle, connected_block_id)?;
                handle.meta().set_has_prev1()
            }
            BlockConnection::Prev2 => {
                if handle.meta().has_prev2() {
                    return Ok(());
                }
                store_block_connection_impl(&self.prev2_block_db, handle, connected_block_id)?;
                handle.meta().set_has_prev2()
            }
            BlockConnection::Next1 => {
                if handle.meta().has_next1() {
                    return Ok(());
                }
                store_block_connection_impl(&self.next1_block_db, handle, connected_block_id)?;
                handle.meta().set_has_next1()
            }
            BlockConnection::Next2 => {
                if handle.meta().has_next2() {
                    return Ok(());
                }
                store_block_connection_impl(&self.next2_block_db, handle, connected_block_id)?;
                handle.meta().set_has_next2()
            }
        };

        if store {
            let id = handle.id();

            if handle.is_key_block() {
                let mut write_batch = rocksdb::WriteBatch::default();

                write_batch.put_cf(
                    &self.block_handles.get_cf(),
                    id.root_hash.as_slice(),
                    handle.meta().to_vec(),
                );
                write_batch.put_cf(
                    &self.key_blocks.get_cf(),
                    id.seq_no.to_be_bytes(),
                    id.to_vec(),
                );

                self.block_handles.raw_db_handle().write(write_batch)?;
            } else {
                self.block_handles
                    .insert(id.root_hash.as_slice(), handle.meta().to_vec())?;
            }
        }

        Ok(())
    }

    pub fn load_connection(
        &self,
        block_id: &ton_block::BlockIdExt,
        direction: BlockConnection,
    ) -> Result<ton_block::BlockIdExt> {
        match direction {
            BlockConnection::Prev1 => load_block_connection_impl(&self.prev1_block_db, block_id),
            BlockConnection::Prev2 => load_block_connection_impl(&self.prev2_block_db, block_id),
            BlockConnection::Next1 => load_block_connection_impl(&self.next1_block_db, block_id),
            BlockConnection::Next2 => load_block_connection_impl(&self.next2_block_db, block_id),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BlockConnection {
    Prev1,
    Prev2,
    Next1,
    Next2,
}

#[inline]
fn store_block_connection_impl<T>(
    db: &Tree<T>,
    handle: &BlockHandle,
    block_id: &ton_block::BlockIdExt,
) -> Result<()>
where
    T: Column,
{
    db.insert(
        handle.id().root_hash.as_slice(),
        write_block_id_le(block_id),
    )
}

#[inline]
fn load_block_connection_impl<T>(
    db: &Tree<T>,
    block_id: &ton_block::BlockIdExt,
) -> Result<ton_block::BlockIdExt>
where
    T: Column,
{
    match db.get(block_id.root_hash.as_slice())? {
        Some(value) => read_block_id_le(value.as_ref())
            .ok_or_else(|| BlockConnectionStorageError::InvalidBlockId.into()),
        None => Err(BlockConnectionStorageError::NotFound.into()),
    }
}

#[derive(Debug, thiserror::Error)]
enum BlockConnectionStorageError {
    #[error("Invalid connection block id")]
    InvalidBlockId,
    #[error("Block connection not found")]
    NotFound,
}
