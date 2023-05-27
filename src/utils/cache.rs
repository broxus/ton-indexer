use std::borrow::Borrow;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::FastHasherState;

#[derive(Default, Debug, Clone, Copy)]
pub struct CacheStats {
    pub hits: u64,
    pub requests: u64,
    pub occupied: usize,
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
}
