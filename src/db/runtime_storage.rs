use super::persistent_state_keeper::*;

#[derive(Default)]
pub struct RuntimeStorage {
    persistent_state_keeper: PersistentStateKeeper,
}

impl RuntimeStorage {
    #[inline(always)]
    pub fn persistent_state_keeper(&self) -> &PersistentStateKeeper {
        &self.persistent_state_keeper
    }
}
