/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use std::collections::hash_map;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use once_cell::sync::Lazy;
use rustc_hash::FxHashMap;
use ton_block::{Deserializable, Serializable};
use ton_types::{Cell, UInt256};

/// Full persistent state block id (relative to the masterchain)
pub struct FullStateId {
    pub mc_block_id: ton_block::BlockIdExt,
    pub block_id: ton_block::BlockIdExt,
}

#[derive(Clone)]
pub struct ShardStateStuff {
    block_id: ton_block::BlockIdExt,
    shard_state: ton_block::ShardStateUnsplit,
    shard_state_extra: Option<ton_block::McStateExtra>,
    handle: Arc<RefMcStateHandle>,
    root: Cell,
}

impl ShardStateStuff {
    pub fn new(
        block_id: ton_block::BlockIdExt,
        root: Cell,
        min_ref_mc_state: &Arc<MinRefMcState>,
    ) -> Result<Self> {
        let shard_state = ton_block::ShardStateUnsplit::construct_from(&mut root.clone().into())?;

        if shard_state.shard() != block_id.shard() {
            return Err(anyhow!("State's shard block_id is not equal to given one"));
        }

        if shard_state.shard().shard_prefix_with_tag() != block_id.shard().shard_prefix_with_tag() {
            return Err(anyhow!("State's shard id is not equal to given one"));
        } else if shard_state.seq_no() != block_id.seq_no {
            return Err(anyhow!("State's seqno is not equal to given one"));
        }

        let handle = min_ref_mc_state.insert(shard_state.min_ref_mc_seqno());

        Ok(Self {
            block_id,
            shard_state_extra: shard_state.read_custom()?,
            shard_state,
            root,
            handle,
        })
    }

    pub fn construct_split_root(left: Cell, right: Cell) -> Result<Cell> {
        ton_block::ShardStateSplit { left, right }.serialize()
    }

    pub fn deserialize_zerostate(id: ton_block::BlockIdExt, mut bytes: &[u8]) -> Result<Self> {
        if id.seq_no() != 0 {
            return Err(anyhow!("Given id has non-zero seq number"));
        }

        let file_hash = UInt256::calc_file_hash(bytes);
        if file_hash != id.file_hash {
            return Err(anyhow!("Wrong zero state's {id} file hash"));
        }

        let root = ton_types::deserialize_tree_of_cells(&mut bytes)?;
        if root.repr_hash() != id.root_hash() {
            return Err(anyhow!("Wrong zero state's {id} root hash"));
        }

        Self::new(id, root, &*ZEROSTATE_REFS)
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

    #[allow(unused)]
    pub fn shards(&self) -> Result<&ton_block::ShardHashes> {
        Ok(self.shard_state_extra()?.shards())
    }

    #[inline(always)]
    pub fn root_cell(&self) -> &Cell {
        &self.root
    }

    #[inline(always)]
    pub fn ref_mc_state_handle(&self) -> &Arc<RefMcStateHandle> {
        &self.handle
    }

    #[allow(unused)]
    pub fn shard(&self) -> &ton_block::ShardIdent {
        self.block_id.shard()
    }

    pub fn block_id(&self) -> &ton_block::BlockIdExt {
        &self.block_id
    }

    pub fn config_params(&self) -> Result<&ton_block::ConfigParams> {
        Ok(&self.shard_state_extra()?.config)
    }
}

pub struct RefMcStateHandle {
    min_ref_mc_state: Arc<MinRefMcState>,
    mc_seq_no: u32,
}

impl Drop for RefMcStateHandle {
    fn drop(&mut self) {
        self.min_ref_mc_state.remove(self.mc_seq_no);
    }
}

#[derive(Default)]
pub struct MinRefMcState {
    counters: parking_lot::RwLock<(Option<u32>, FxHashMap<u32, AtomicU32>)>,
}

impl MinRefMcState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            counters: Default::default(),
        })
    }

    pub fn seq_no(&self) -> Option<u32> {
        self.counters.read().0
    }

    fn insert(self: &Arc<Self>, mc_seq_no: u32) -> Arc<RefMcStateHandle> {
        // Fast path, just increase existing counter
        let counters = self.counters.read();
        if let Some(counter) = (*counters).1.get(&mc_seq_no) {
            counter.fetch_add(1, Ordering::Release);
            return Arc::new(RefMcStateHandle {
                min_ref_mc_state: self.clone(),
                mc_seq_no,
            });
        }
        drop(counters);

        // Fallback to exclusive write
        let mut counters = self.counters.write();
        let (min_ref_seq_no, counters) = &mut *counters;
        match counters.entry(mc_seq_no) {
            hash_map::Entry::Vacant(entry) => {
                entry.insert(AtomicU32::new(1));

                match min_ref_seq_no {
                    Some(seqno) if mc_seq_no < *seqno => *seqno = mc_seq_no,
                    None => *min_ref_seq_no = Some(mc_seq_no),
                    _ => {}
                }
            }
            hash_map::Entry::Occupied(entry) => {
                entry.get().fetch_add(1, Ordering::Release);
            }
        }

        Arc::new(RefMcStateHandle {
            min_ref_mc_state: self.clone(),
            mc_seq_no,
        })
    }

    fn remove(&self, mc_seq_no: u32) {
        // Fast path, just decrease existing counter
        let counters = self.counters.read();
        if let Some(counter) = (*counters).1.get(&mc_seq_no) {
            if counter.fetch_sub(1, Ordering::AcqRel) > 1 {
                return;
            }
        } else {
            return;
        }
        drop(counters);

        // Fallback to exclusive write to update current min
        let mut counters = self.counters.write();
        let (min_ref_seq_no, counters) = &mut *counters;
        match counters.entry(mc_seq_no) {
            hash_map::Entry::Occupied(entry) if entry.get().load(Ordering::Acquire) == 0 => {
                entry.remove();
                if matches!(min_ref_seq_no, Some(seq_no) if *seq_no == mc_seq_no) {
                    *min_ref_seq_no = counters.keys().min().copied();
                }
            }
            _ => {}
        }
    }
}

pub fn is_persistent_state(block_utime: u32, prev_utime: u32) -> bool {
    block_utime >> 17 != prev_utime >> 17
}

static ZEROSTATE_REFS: Lazy<Arc<MinRefMcState>> = Lazy::new(MinRefMcState::new);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_ref_mc_state() {
        let state = Arc::new(MinRefMcState::default());

        {
            let _handle = state.insert(10);
            assert_eq!(state.seq_no(), Some(10));
        }
        assert_eq!(state.seq_no(), None);

        {
            let handle1 = state.insert(10);
            assert_eq!(state.seq_no(), Some(10));
            let _handle2 = state.insert(15);
            assert_eq!(state.seq_no(), Some(10));
            let handle3 = state.insert(10);
            assert_eq!(state.seq_no(), Some(10));
            drop(handle3);
            assert_eq!(state.seq_no(), Some(10));
            drop(handle1);
            assert_eq!(state.seq_no(), Some(15));
        }
        assert_eq!(state.seq_no(), None);
    }
}
