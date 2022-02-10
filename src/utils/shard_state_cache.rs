use std::sync::Arc;
use std::time::Duration;

use lru_time_cache::LruCache;
use parking_lot::Mutex;

use super::shard_state::ShardStateStuff;
use crate::config::ShardStateCacheOptions;
use crate::storage::TopBlocks;

pub struct ShardStateCache {
    map: Option<ShardStatesMap>,
}

type ShardStatesMap = Mutex<LruCache<ton_block::BlockIdExt, Arc<ShardStateStuff>>>;

impl ShardStateCache {
    pub fn new(config: Option<ShardStateCacheOptions>) -> Self {
        Self {
            map: config.map(|config| {
                ShardStatesMap::new(LruCache::with_expiry_duration_and_capacity(
                    Duration::from_secs(config.ttl_sec),
                    config.capacity,
                ))
            }),
        }
    }

    pub fn get(&self, block_id: &ton_block::BlockIdExt) -> Option<Arc<ShardStateStuff>> {
        self.map
            .as_ref()
            .and_then(|map| map.lock().get(block_id).cloned())
    }

    pub fn set<F>(&self, block_id: &ton_block::BlockIdExt, factory: F)
    where
        F: FnOnce() -> Arc<ShardStateStuff>,
    {
        if let Some(map) = &self.map {
            map.lock().insert(block_id.clone(), factory());
        }
    }

    pub fn remove(&self, top_blocks: &TopBlocks) {
        if let Some(map) = &self.map {
            let mut map = map.lock();
            map.retain(|(key, _)| top_blocks.contains(key));
        }
    }

    pub fn is_empty(&self) -> bool {
        if let Some(map) = &self.map {
            map.lock().is_empty()
        } else {
            true
        }
    }

    pub fn len(&self) -> usize {
        if let Some(map) = &self.map {
            map.lock().len()
        } else {
            0
        }
    }
}
