use histogram::Histogram;
use schnellru::ByLength;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::io::Write;
use std::sync::Mutex;

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

pub struct FastLruCache<K, V> {
    inner: Mutex<(
        schnellru::LruMap<K, V, ByLength, FastHasherState>,
        LruCacheStats,
    )>,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct LruCacheStats {
    pub hits: u64,
    pub requests: u64,
    pub occupied: usize,
}

impl<K, V> FastLruCache<K, V>
where
    K: Hash + PartialEq,
    V: Clone,
{
    pub fn new(capacity: u32) -> Self {
        Self {
            inner: Mutex::new((
                schnellru::LruMap::with_hasher(ByLength::new(capacity), Default::default()),
                LruCacheStats::default(),
            )),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        let mut lock = self.inner.lock().unwrap();

        lock.0.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let mut lock = self.inner.lock().unwrap();

        let res = lock.0.get(key).cloned();
        lock.1.requests += 1;
        if res.is_some() {
            lock.1.hits += 1;
        }

        res
    }

    pub fn stats(&self) -> LruCacheStats {
        let lock = self.inner.lock().unwrap();
        let mut stats = lock.1;
        stats.occupied = lock.0.len();

        stats
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
