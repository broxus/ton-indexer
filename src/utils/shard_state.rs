/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use std::collections::hash_map;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use everscale_types::models::*;
use everscale_types::prelude::*;
use sha2::Digest;

use super::FastHashMap;

/// Full persistent state block id (relative to the masterchain)
pub struct FullStateId {
    pub mc_block_id: BlockId,
    pub block_id: BlockId,
}

#[derive(Clone)]
pub struct ShardStateStuff {
    block_id: BlockId,
    shard_state: ShardStateUnsplit,
    shard_state_extra: Option<McStateExtra>,
    handle: Arc<RefMcStateHandle>,
    root: Cell,
}

impl ShardStateStuff {
    pub fn new(
        block_id: BlockId,
        root: Cell,
        min_ref_mc_state: &Arc<MinRefMcState>,
    ) -> Result<Self> {
        let shard_state = root.parse::<ShardStateUnsplit>()?;

        if shard_state.shard_ident != block_id.shard {
            return Err(anyhow!("State's shard block_id is not equal to given one"));
        }

        if shard_state.shard_ident.prefix() != block_id.shard.prefix() {
            return Err(anyhow!("State's shard id is not equal to given one"));
        } else if shard_state.seqno != block_id.seqno {
            return Err(anyhow!("State's seqno is not equal to given one"));
        }

        let handle = min_ref_mc_state.insert(shard_state.min_ref_mc_seqno);

        Ok(Self {
            block_id,
            shard_state_extra: shard_state.load_custom()?,
            shard_state,
            root,
            handle,
        })
    }

    pub fn construct_split_root(left: Cell, right: Cell) -> Result<Cell> {
        CellBuilder::build_from(ShardStateSplit {
            left: Lazy::from_raw(left),
            right: Lazy::from_raw(right),
        })
        .map_err(From::from)
    }

    pub fn deserialize_zerostate(id: BlockId, bytes: &[u8]) -> Result<Self> {
        if id.seqno != 0 {
            return Err(anyhow!("Given id has non-zero seq number"));
        }

        let file_hash: [u8; 32] = sha2::Sha256::digest(bytes).into();
        if file_hash != id.file_hash {
            return Err(anyhow!("Wrong zero state's {id} file hash"));
        }

        let root = Boc::decode(bytes)?;
        if root.repr_hash() != &id.root_hash {
            return Err(anyhow!("Wrong zero state's {id} root hash"));
        }

        Self::new(id, root, &ZEROSTATE_REFS)
    }

    pub fn state(&self) -> &ShardStateUnsplit {
        &self.shard_state
    }

    pub fn shard_state_extra(&self) -> Result<&McStateExtra> {
        self.shard_state_extra.as_ref().ok_or_else(|| {
            anyhow!(
                "Masterchain state of {} must contain McStateExtra",
                self.block_id()
            )
        })
    }

    #[allow(unused)]
    pub fn shards(&self) -> Result<&ShardHashes> {
        Ok(&self.shard_state_extra()?.shards)
    }

    #[inline(always)]
    pub fn root_cell(&self) -> &Cell {
        &self.root
    }

    #[inline(always)]
    pub fn ref_mc_state_handle(&self) -> &Arc<RefMcStateHandle> {
        &self.handle
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    pub fn config_params(&self) -> Result<&BlockchainConfig> {
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
    counters: parking_lot::RwLock<(Option<u32>, FastHashMap<u32, AtomicU32>)>,
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
        if let Some(counter) = counters.1.get(&mc_seq_no) {
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
        if let Some(counter) = counters.1.get(&mc_seq_no) {
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

static ZEROSTATE_REFS: once_cell::sync::Lazy<Arc<MinRefMcState>> =
    once_cell::sync::Lazy::new(MinRefMcState::new);

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
