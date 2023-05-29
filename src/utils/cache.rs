use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::FastHasherState;

#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CacheStats {
    pub hits: u64,
    pub requests: u64,
    pub occupied: u64,
    pub hits_ratio: f64,
    pub size_bytes: u64,
}

// we are using `mini moka` because it's faster than `moka` and has the ability to iterate over all keys
// which is necessary for storing popularity stats for cache warm up.
// `fast_cache` in 2 times faster than `mini moka` but has no iteration interface
pub struct MokaCache<K, V> {
    inner: mini_moka::sync::Cache<K, V, FastHasherState>,
    requests: AtomicU64,
    hits: AtomicU64,
}

impl<K, V> MokaCache<K, V>
where
    K: Hash + Eq + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: mini_moka::sync::Cache::builder()
                .max_capacity(capacity as u64)
                .build_with_hasher(FastHasherState::default()),
            requests: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        }
    }

    pub fn with_cache(cache: mini_moka::sync::Cache<K, V, FastHasherState>) -> Self {
        Self {
            inner: cache,
            requests: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        }
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        Arc<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let res = self.inner.get(key);
        self.requests.fetch_add(1, Ordering::Relaxed);
        if res.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }

        res
    }

    pub fn insert(&self, key: K, value: V) {
        self.inner.insert(key, value);
    }

    pub fn stats(&self) -> CacheStats {
        let size = self.inner.weighted_size();
        let req = self.requests.load(Ordering::Relaxed);
        let hits = self.hits.load(Ordering::Relaxed);
        let hit_ratio = if req > 0 {
            hits as f64 / req as f64
        } else {
            0.0
        } * 100.0;

        CacheStats {
            hits,
            requests: req,
            occupied: self.inner.entry_count(),
            hits_ratio: hit_ratio,
            size_bytes: size,
        }
    }
}
