use std::sync::Arc;
use std::time::Duration;

use super::shard_state::ShardStateStuff;
use super::top_blocks::*;
use super::FastDashMap;
use crate::config::ShardStateCacheOptions;

/// LRU cache for shard states
///
/// [`ShardStateStuff`]
pub struct ShardStateCache {
    ttl: Option<Duration>,
    map: Option<ShardStatesMap>,
}

impl ShardStateCache {
    /// Creates cache object. If `config` is `None`, cache is disabled
    pub fn new(config: Option<ShardStateCacheOptions>) -> Self {
        let config = config.map(|config| {
            let ttl = Duration::from_secs(config.ttl_sec);
            (ttl, ShardStatesMap::default())
        });

        match config {
            // Cache is enabled and should be cleared every TTL interval
            Some((ttl, map)) => Self {
                ttl: Some(ttl),
                map: Some(map),
            },
            // Cache is disabled
            None => Self {
                ttl: None,
                map: None,
            },
        }
    }

    pub fn ttl(&self) -> Option<Duration> {
        self.ttl
    }

    /// Retrieves a reference to the value stored under key, or None if the key doesn't exist
    /// or the cache is disabled.
    ///
    /// Also removes expired elements and updates the time
    pub fn get(&self, block_id: &ton_block::BlockIdExt) -> Option<Arc<ShardStateStuff>> {
        if let Some(map) = &self.map {
            let entry = map.get(block_id)?;
            Some(entry.value().clone())
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
            map.insert(block_id.clone(), factory());
        }
    }

    /// Removes all outdated elements from the cache before the top blocks
    pub fn remove(&self, top_blocks: &TopBlocks) {
        if let Some(map) = &self.map {
            metrics::gauge!("shard_state_cache_size").set(map.len() as f64); // measuring worst case
            map.retain(|key, _| top_blocks.contains(key));
        }
    }

    /// Removes all elements from the cache
    pub fn clear(&self) {
        if let Some(map) = &self.map {
            map.clear();
        }
    }

    /// Returns true if the cache is disabled or there are no non-expired entries in the cache
    pub fn is_empty(&self) -> bool {
        if let Some(map) = &self.map {
            map.is_empty()
        } else {
            true
        }
    }
}

type ShardStatesMap = FastDashMap<ton_block::BlockIdExt, Arc<ShardStateStuff>>;
