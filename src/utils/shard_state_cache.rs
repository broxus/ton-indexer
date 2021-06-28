use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;

use super::shard_state::ShardStateStuff;

pub struct ShardStateCache {
    map: Arc<ShardStatesMap>,
}

type ShardStatesMap = DashMap<ton_block::BlockIdExt, (Arc<ShardStateStuff>, AtomicU64)>;

impl ShardStateCache {
    pub fn new(ttl_sec: u64) -> Self {
        let map = Arc::new(ShardStatesMap::default());

        tokio::spawn({
            let map = Arc::downgrade(&map);

            async move {
                while let Some(map) = map.upgrade() {
                    tokio::time::sleep(Duration::from_millis(ttl_sec * 100)).await;

                    let now = now();
                    map.retain(|_, (_, time)| {
                        let time = time.load(Ordering::Acquire);
                        now.saturating_sub(time) > ttl_sec
                    });
                }
            }
        });

        Self { map }
    }

    pub fn get(&self, block_id: &ton_block::BlockIdExt) -> Option<Arc<ShardStateStuff>> {
        let item = self.map.get(block_id)?;
        item.value().1.store(now(), Ordering::Release);
        Some(item.value().0.clone())
    }

    pub fn set<F>(&self, block_id: &ton_block::BlockIdExt, factory: F) -> bool
    where
        F: FnOnce(Option<&ShardStateStuff>) -> Option<Arc<ShardStateStuff>>,
    {
        use dashmap::mapref::entry::Entry;

        let now = now();

        match self.map.entry(block_id.clone()) {
            Entry::Occupied(entry) => match factory(Some(entry.get().0.as_ref())) {
                Some(value) => {
                    entry.replace_entry((value, AtomicU64::new(now)));
                    true
                }
                None => false,
            },
            Entry::Vacant(entry) => match factory(None) {
                Some(value) => {
                    entry.insert((value, AtomicU64::new(now)));
                    true
                }
                None => false,
            },
        }
    }
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
