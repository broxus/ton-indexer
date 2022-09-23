use std::sync::Arc;

use super::persistent_state_keeper::*;
use super::BlockHandleStorage;

pub struct RuntimeStorage {
    persistent_state_keeper: PersistentStateKeeper,
}

impl RuntimeStorage {
    pub fn new(block_handle_storage: &Arc<BlockHandleStorage>) -> Self {
        Self {
            persistent_state_keeper: PersistentStateKeeper::new(block_handle_storage),
        }
    }

    #[inline(always)]
    pub fn persistent_state_keeper(&self) -> &PersistentStateKeeper {
        &self.persistent_state_keeper
    }
}
