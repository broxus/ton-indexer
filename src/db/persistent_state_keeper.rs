use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::db::BlockHandle;
use crate::utils::*;

#[derive(Default)]
pub struct PersistentStateKeeper {
    persistent_state_changed: Notify,
    current_persistent_state: RwLock<Option<Arc<BlockHandle>>>,
    last_utime: AtomicU32,
}

impl PersistentStateKeeper {
    pub fn update(&self, block_handle: &Arc<BlockHandle>) {
        if !block_handle.is_key_block() {
            return;
        }

        let block_utime = block_handle.meta().gen_utime();
        let prev_utime = self.last_utime();

        if prev_utime > block_utime {
            return;
        }

        if is_persistent_state(block_utime, prev_utime) {
            *self.current_persistent_state.write() = Some(block_handle.clone());
            self.persistent_state_changed.notify_waiters();
        }
    }

    pub fn last_utime(&self) -> u32 {
        self.last_utime.load(Ordering::Acquire)
    }

    pub fn current(&self) -> Option<Arc<BlockHandle>> {
        self.current_persistent_state.read().clone()
    }

    pub fn new_state_found(&self) -> tokio::sync::futures::Notified {
        self.persistent_state_changed.notified()
    }
}
