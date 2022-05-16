use std::sync::atomic::{AtomicU32, Ordering};

use super::persistent_state_keeper::*;

#[derive(Default)]
pub struct RuntimeStorage {
    last_known_key_block_seqno: AtomicU32,
    persistent_state_keeper: PersistentStateKeeper,
}

impl RuntimeStorage {
    pub fn update_last_known_key_block_seqno(&self, seqno: u32) -> bool {
        self.last_known_key_block_seqno
            .fetch_max(seqno, Ordering::AcqRel)
            < seqno
    }

    pub fn last_known_key_block_seqno(&self) -> u32 {
        self.last_known_key_block_seqno.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn persistent_state_keeper(&self) -> &PersistentStateKeeper {
        &self.persistent_state_keeper
    }
}
