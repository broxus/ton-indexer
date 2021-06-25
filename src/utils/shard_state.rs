use std::io::{Cursor, Write};

use anyhow::{anyhow, Result};
use ton_block::{Deserializable, HashmapAugType, Serializable};
use ton_types::{Cell, UInt256};

use super::NoFailure;

#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ShardStateStuff {
    block_id: ton_block::BlockIdExt,
    shard_state: ton_block::ShardStateUnsplit,
    shard_state_extra: Option<ton_block::McStateExtra>,
    root: Cell,
}

impl ShardStateStuff {
    pub fn new(block_id: ton_block::BlockIdExt, root: Cell) -> Result<Self> {
        let shard_state =
            ton_block::ShardStateUnsplit::construct_from(&mut root.clone().into()).convert()?;

        if shard_state.shard() != block_id.shard() {
            return Err(anyhow!("State's shard block_id is not equal to given one"));
        }

        if shard_state.shard().shard_prefix_with_tag() != block_id.shard().shard_prefix_with_tag() {
            return Err(anyhow!("State's shard id is not equal to given one"));
        } else if shard_state.seq_no() != block_id.seq_no {
            return Err(anyhow!("State's seqno is not equal to given one"));
        }

        let mut stuff = Self::default();
        stuff.block_id = block_id;
        stuff.shard_state_extra = shard_state.read_custom().convert()?;
        stuff.shard_state = shard_state;
        stuff.root = root;
        Ok(stuff)
    }

    pub fn with_state(
        block_id: ton_block::BlockIdExt,
        shard_state: ton_block::ShardStateUnsplit,
    ) -> Result<Self> {
        let mut stuff = Self::default();
        stuff.block_id = block_id;
        stuff.root = shard_state.serialize().convert()?;
        stuff.shard_state = shard_state;
        stuff.shard_state_extra = stuff.shard_state.read_custom().convert()?;
        Ok(stuff)
    }

    pub fn construct_split_root(left: Cell, right: Cell) -> Result<Cell> {
        ton_block::ShardStateSplit { left, right }
            .serialize()
            .convert()
    }

    pub fn deserialize_zerostate(id: ton_block::BlockIdExt, bytes: &[u8]) -> Result<Self> {
        if id.seq_no() != 0 {
            return Err(anyhow!("Given id has non-zero seq number"));
        }

        let file_hash = UInt256::calc_file_hash(&bytes);
        if file_hash != id.file_hash {
            return Err(anyhow!("Wrong zero state's {} file hash", id));
        }

        let root = ton_types::deserialize_tree_of_cells(&mut Cursor::new(bytes)).convert()?;
        if &root.repr_hash() != id.root_hash() {
            return Err(anyhow!("Wrong zero state's {} root hash", id));
        }

        Self::new(id, root)
    }

    pub fn deserialize(id: ton_block::BlockIdExt, bytes: &[u8]) -> Result<Self> {
        if id.seq_no() == 0 {
            return Err(anyhow!("Use `deserialize_zerostate` method for zerostate"));
        }
        let root = ton_types::deserialize_tree_of_cells(&mut Cursor::new(bytes)).convert()?;

        Self::new(id, root)
    }

    pub fn state(&self) -> &ton_block::ShardStateUnsplit {
        &self.shard_state
    }

    pub fn shard_state_extra(&self) -> Result<&ton_block::McStateExtra> {
        self.shard_state_extra.as_ref().ok_or_else(|| {
            anyhow!(
                "Masterchain state of {} must contain McStateExtra",
                self.block_id()
            )
        })
    }

    pub fn shards(&self) -> Result<&ton_block::ShardHashes> {
        Ok(self.shard_state_extra()?.shards())
    }

    pub fn root_cell(&self) -> &Cell {
        &self.root
    }

    pub fn shard(&self) -> &ton_block::ShardIdent {
        &self.block_id.shard()
    }

    pub fn block_id(&self) -> &ton_block::BlockIdExt {
        &self.block_id
    }

    pub fn write_to<T: Write>(&self, dst: &mut T) -> Result<()> {
        ton_types::serialize_tree_of_cells(&self.root, dst).convert()?;
        Ok(())
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut bytes = Cursor::new(Vec::<u8>::new());
        self.write_to(&mut bytes)?;
        Ok(bytes.into_inner())
    }

    pub fn config_params(&self) -> Result<&ton_block::ConfigParams> {
        Ok(&self.shard_state_extra()?.config)
    }

    pub fn has_prev_block(&self, block_id: &ton_block::BlockIdExt) -> Result<bool> {
        Ok(self
            .shard_state_extra()?
            .prev_blocks
            .get(&block_id.seq_no())
            .convert()?
            .map(|id| {
                &id.blk_ref().root_hash == block_id.root_hash()
                    && &id.blk_ref().file_hash == block_id.file_hash()
            })
            .unwrap_or_default())
    }
}
