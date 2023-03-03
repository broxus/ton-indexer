use std::io::Read;
use std::sync::Arc;

use anyhow::{Context, Result};
use rustc_hash::FxHashMap;
use ton_types::ByteOrderRead;

use crate::db::*;
use crate::utils::{StoredValue, StoredValueBuffer, TopBlocks};

pub struct GcStateStorage {
    db: Arc<Db>,
}

impl GcStateStorage {
    pub fn new(db: Arc<Db>) -> Result<Arc<Self>> {
        let storage = Arc::new(Self { db });
        let _ = storage.load()?;
        Ok(storage)
    }

    pub fn load(&self) -> Result<GcState> {
        Ok(match self.db.node_states.get(STATES_GC_STATE_KEY)? {
            Some(value) => {
                GcState::from_slice(&value).context("Failed to decode states GC state")?
            }
            None => {
                let state = GcState {
                    current_marker: 1, // NOTE: zero marker is reserved for persistent state
                    step: None,
                };
                self.update(&state)?;
                state
            }
        })
    }

    pub fn update(&self, state: &GcState) -> Result<()> {
        let node_states = &self.db.node_states;
        node_states
            .insert(STATES_GC_STATE_KEY, state.to_vec())
            .context("Failed to update shards GC state")
    }

    pub fn clear_last_blocks(&self) -> Result<()> {
        let mut iter = self.db.node_states.prefix_iterator(GC_LAST_BLOCK_KEY);
        loop {
            let key = match iter.key() {
                Some(key) => key,
                None => break iter.status()?,
            };

            if key.starts_with(GC_LAST_BLOCK_KEY) {
                self.db.node_states.remove(key)?;
            }

            iter.next();
        }
        Ok(())
    }

    pub fn load_last_blocks(&self) -> Result<FxHashMap<ton_block::ShardIdent, u32>> {
        let mut result = FxHashMap::default();

        let mut iter = self.db.node_states.prefix_iterator(GC_LAST_BLOCK_KEY);
        loop {
            let (key, mut value) = match iter.item() {
                Some(item) => item,
                None => break iter.status()?,
            };

            if key.starts_with(GC_LAST_BLOCK_KEY) {
                let shard_ident = LastShardBlockKey::from_slice(key)
                    .context("Failed to load last shard id")?
                    .0;
                let top_block = (&mut value)
                    .read_le_u32()
                    .context("Failed to load top block")?;
                result.insert(shard_ident, top_block);
            }

            iter.next();
        }

        Ok(result)
    }
}

#[derive(Debug)]
pub struct GcState {
    pub current_marker: u8,
    pub step: Option<Step>,
}

impl GcState {
    /// 0x00 marker is used for persistent state
    /// 0xff marker is used for persistent state transition
    pub fn next_marker(&self) -> u8 {
        match self.current_marker {
            // Saturate marker
            254 | 255 => 1,
            // Increment marker otherwise
            marker => marker + 1,
        }
    }
}

impl StoredValue for GcState {
    const SIZE_HINT: usize = 512;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        let (step, blocks) = match &self.step {
            None => (0, None),
            Some(Step::Mark(blocks)) => (1, Some(blocks)),
            Some(Step::SweepCells(blocks)) => (2, Some(blocks)),
            Some(Step::SweepBlocks(blocks)) => (3, Some(blocks)),
        };
        buffer.write_raw_slice(&[self.current_marker, step]);

        if let Some(blocks) = blocks {
            blocks.serialize(buffer);
        }
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        let mut data = [0u8; 2];
        reader.read_exact(&mut data)?;
        let step = match data[1] {
            0 => None,
            1 => Some(Step::Mark(TopBlocks::deserialize(reader)?)),
            2 => Some(Step::SweepCells(TopBlocks::deserialize(reader)?)),
            3 => Some(Step::SweepBlocks(TopBlocks::deserialize(reader)?)),
            _ => return Err(GcStateStorageError::InvalidStatesGcStep.into()),
        };

        Ok(Self {
            current_marker: data[0],
            step,
        })
    }
}

#[derive(Debug)]
pub struct LastShardBlockKey(pub ton_block::ShardIdent);

impl StoredValue for LastShardBlockKey {
    const SIZE_HINT: usize = GC_LAST_BLOCK_KEY.len() + ton_block::ShardIdent::SIZE_HINT;

    type OnStackSlice = [u8; Self::SIZE_HINT];

    #[inline(always)]
    fn serialize<T: StoredValueBuffer>(&self, buffer: &mut T) {
        buffer.write_raw_slice(GC_LAST_BLOCK_KEY);
        self.0.serialize(buffer);
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where
        Self: Sized,
    {
        if reader.len() > GC_LAST_BLOCK_KEY.len() {
            *reader = &(*reader)[GC_LAST_BLOCK_KEY.len()..];
        }

        ton_block::ShardIdent::deserialize(reader).map(Self)
    }
}

#[derive(Debug)]
pub enum Step {
    Mark(TopBlocks),
    SweepCells(TopBlocks),
    SweepBlocks(TopBlocks),
}

const STATES_GC_STATE_KEY: &[u8] = b"states_gc_state";
const GC_LAST_BLOCK_KEY: &[u8] = b"gc_last_block";

#[derive(thiserror::Error, Debug)]
enum GcStateStorageError {
    #[error("Invalid states GC step")]
    InvalidStatesGcStep,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fully_on_stack() {
        assert!(!LastShardBlockKey(ton_block::ShardIdent::default())
            .to_vec()
            .spilled());
    }

    #[test]
    fn correct_last_shared_block_key_repr() {
        let key = LastShardBlockKey(
            ton_block::ShardIdent::with_tagged_prefix(-1, ton_block::SHARD_FULL).unwrap(),
        );

        let mut data = Vec::new();
        key.serialize(&mut data);

        let deserialized_key = LastShardBlockKey::from_slice(&data).unwrap();
        assert_eq!(deserialized_key.0, key.0);
    }
}
