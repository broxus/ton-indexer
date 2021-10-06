/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced verbose `lockfree` crate with `dashmap`
///
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::shard_state::ShardStateStuff;
use lru_time_cache::LruCache;
use parking_lot::Mutex;
pub struct ShardStateCache {
    map: Arc<ShardStatesMap>,
}

type ShardStatesMap = Mutex<LruCache<ton_block::BlockIdExt, Arc<ShardStateStuff>>>;

impl ShardStateCache {
    pub fn new(ttl_sec: u64) -> Self {
        let map = Arc::new(ShardStatesMap::new(
            LruCache::with_expiry_duration_and_capacity(Duration::from_secs(ttl_sec), 100_000),
        ));
        Self { map }
    }

    pub fn get(&self, block_id: &ton_block::BlockIdExt) -> Option<Arc<ShardStateStuff>> {
        let mut map = self.map.lock();
        if let Some(a) = map.get(block_id) {
            return Some(a.clone());
        }
        None
    }

    pub fn set<F>(&self, block_id: &ton_block::BlockIdExt, factory: F)
    where
        F: FnOnce(Option<&ShardStateStuff>) -> Option<Arc<ShardStateStuff>>,
    {
        let mut map = self.map.lock();
        if let Some(value) = factory(None) {
            map.insert(block_id.clone(), value);
        };
    }
}
