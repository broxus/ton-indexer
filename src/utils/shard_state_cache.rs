use std::sync::Arc;
use std::time::Duration;

use lru_time_cache::LruCache;
use parking_lot::Mutex;

use super::shard_state::ShardStateStuff;
use super::top_blocks::*;
use crate::config::ShardStateCacheOptions;

/// LRU cache for shard states
///
/// [`ShardStateStuff`]
pub struct ShardStateCache {
    map: Option<ShardStatesMap>,
}

impl ShardStateCache {
    /// Creates cache object. If `config` is `None`, cache is disabled
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

    /// Retrieves a reference to the value stored under key, or None if the key doesn't exist
    /// or the cache is disabled.
    ///
    /// Also removes expired elements and updates the time
    pub fn get(&self, block_id: &ton_block::BlockIdExt) -> Option<Arc<ShardStateStuff>> {
        if let Some(map) = &self.map {
            map.lock().get(block_id).cloned()
        } else {
            None
        }
    }

    /// Inserts a key-value pair into the cache (if enabled).
    pub fn set<F>(&self, block_id: &ton_block::BlockIdExt, factory: F)
    where
        F: FnOnce() -> Arc<ShardStateStuff>,
    {
        if let Some(map) = &self.map {
            map.lock().insert(block_id.clone(), factory());
        }
    }

    /// Removes all outdated elements from the cache before the top blocks
    pub fn remove(&self, top_blocks: &TopBlocks) {
        if let Some(map) = &self.map {
            let mut map = map.lock();
            map.retain(|(key, _)| top_blocks.contains(key));
        }
    }

    /// Removes all elements from the cache
    pub fn clear(&self) {
        if let Some(map) = &self.map {
            map.lock().clear();
        }
    }

    /// Returns true if the cache is disabled or there are no non-expired entries in the cache
    pub fn is_empty(&self) -> bool {
        if let Some(map) = &self.map {
            map.lock().is_empty()
        } else {
            true
        }
    }

    /// Returns number of elements in the cache
    pub fn len(&self) -> usize {
        if let Some(map) = &self.map {
            map.lock().len()
        } else {
            0
        }
    }
}

type ShardStatesMap = Mutex<LruCache<ton_block::BlockIdExt, Arc<ShardStateStuff>>>;
