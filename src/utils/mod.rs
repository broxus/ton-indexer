use histogram::Histogram;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::io::Write;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub use archive_package::*;
pub use block::*;
pub use block_proof::*;
pub use mapped_file::*;
pub use operations_pool::*;
pub use package_entry_id::*;
pub use progress_bar::*;
pub use shard_state::*;
pub use shard_state_cache::*;
pub use stored_value::*;
pub use top_blocks::*;
pub use with_archive_data::*;

mod archive_package;
mod block;
mod block_proof;
mod mapped_file;
mod operations_pool;
mod package_entry_id;
mod progress_bar;
mod shard_state;
mod shard_state_cache;
mod stored_value;
mod top_blocks;
mod with_archive_data;

pub(crate) type FastHashSet<K> = HashSet<K, FastHasherState>;
pub(crate) type FastHashMap<K, V> = HashMap<K, V, FastHasherState>;
pub(crate) type FastDashSet<K> = dashmap::DashSet<K, FastHasherState>;
pub(crate) type FastDashMap<K, V> = dashmap::DashMap<K, V, FastHasherState>;
pub(crate) type FastHasherState = ahash::RandomState;

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
    pub fn with_capacity(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: mini_moka::sync::Cache::builder()
                .max_capacity(capacity as u64)
                .build_with_hasher(FastHasherState::default()),
            requests: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        })
    }

    pub fn with_cache(cache: mini_moka::sync::Cache<K, V, FastHasherState>) -> Arc<Self> {
        Arc::new(Self {
            inner: cache,
            requests: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        })
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        Arc<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let res = self.inner.get(key);
        self.requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if res.is_some() {
            self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        res
    }

    pub fn insert(&self, key: K, value: V) {
        self.inner.insert(key, value);
    }

    pub fn stats(&self) {
        let size_bytes = bytesize::ByteSize(self.inner.weighted_size());
        let req = self.requests.load(std::sync::atomic::Ordering::Relaxed);
        let hits = self.hits.load(std::sync::atomic::Ordering::Relaxed);
        let hit_ratio = if req > 0 {
            hits as f64 / req as f64
        } else {
            0.0
        } * 100.0;

        tracing::info!(
            occupied = self.inner.entry_count(),
            requests = req,
            hits = hits,
            hit_ratio = hit_ratio,
            size = ?size_bytes,
            "Cache stats",
        );
    }
}

pub fn print_hist(histogram: &Histogram) -> Option<()> {
    fn print_percentile(percentile: f64, hist: &Histogram, mut io: impl Write) -> Option<()> {
        let bucket = hist.percentile(percentile).ok()?;

        writeln!(
            io,
            "Percentile {}%  from {} to {}  => {} count",
            percentile,
            bucket.low(),
            bucket.high(),
            bucket.count()
        )
        .ok()?;

        Some(())
    }
    let mut io = std::io::stdout().lock();

    print_percentile(0.1, histogram, &mut io)?;
    print_percentile(10.0, histogram, &mut io)?;
    print_percentile(25.0, histogram, &mut io)?;
    print_percentile(50.0, histogram, &mut io)?;
    print_percentile(75.0, histogram, &mut io)?;
    print_percentile(90.0, histogram, &mut io)?;
    print_percentile(95.0, histogram, &mut io)?;
    print_percentile(99.0, histogram, &mut io)?;
    print_percentile(99.9, histogram, &mut io)?;
    print_percentile(99.99, histogram, &mut io)?;
    print_percentile(99.999, histogram, &mut io)?;
    Some(())
}
