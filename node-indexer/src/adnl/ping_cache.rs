use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Weak};

use anyhow::Result;
use dashmap::DashMap;
use tokio::sync::Barrier;
use ton_api::ton;

#[derive(Default)]
pub struct PingCache {
    seqno: AtomicI64,
    queries: DashMap<i64, PingQueryState>,
}

impl PingCache {
    pub fn add_query(self: &Arc<Self>) -> (i64, PendingPingQuery) {
        let seqno = self.seqno.fetch_add(1, Ordering::Relaxed);

        let barrier = Arc::new(Barrier::new(2));
        let query = PingQueryState::Sent(barrier.clone());

        self.queries.insert(seqno, query);

        let pending_query = PendingPingQuery {
            seqno,
            barrier,
            cache: Arc::downgrade(self),
        };

        (seqno, pending_query)
    }

    pub async fn update_query(&self, seqno: i64, answer: bool) -> Result<bool> {
        use dashmap::mapref::entry::Entry;

        let old = match self.queries.entry(seqno) {
            Entry::Vacant(_) => None,
            Entry::Occupied(entry) => match entry.get() {
                PingQueryState::Sent(_) => {
                    let (_, old) = entry.replace_entry(match answer {
                        true => PingQueryState::Received,
                        false => PingQueryState::Timeout,
                    });
                    Some(old)
                }
                _ => None,
            },
        };

        match old {
            Some(PingQueryState::Sent(barrier)) => {
                barrier.wait().await;
                Ok(true)
            }
            Some(_) => Err(PingCacheError::UnexpectedState.into()),
            None => return Ok(false),
        }
    }
}

pub struct PendingPingQuery {
    seqno: i64,
    barrier: Arc<Barrier>,
    cache: Weak<PingCache>,
}

impl PendingPingQuery {
    pub fn as_tl(&self) -> ton::rpc::tcp::Ping {
        ton::rpc::tcp::Ping {
            random_id: self.seqno,
        }
    }

    pub async fn wait(self) -> Result<()> {
        self.barrier.wait().await;
        let cache = match self.cache.upgrade() {
            Some(cache) => cache,
            None => return Err(PingCacheError::CacheDropped.into()),
        };

        match cache.queries.remove(&self.seqno) {
            Some((_, PingQueryState::Received)) => Ok(()),
            Some((_, PingQueryState::Timeout)) => Err(PingCacheError::TimeoutReached.into()),
            Some(_) => Err(PingCacheError::InvalidQueryState.into()),
            None => Err(PingCacheError::UnknownId.into()),
        }
    }
}

enum PingQueryState {
    Sent(Arc<Barrier>),
    Received,
    Timeout,
}

#[derive(thiserror::Error, Debug)]
enum PingCacheError {
    #[error("Ping cache was dropped")]
    CacheDropped,
    #[error("Invalid ping query state")]
    InvalidQueryState,
    #[error("Unknown ping seqno")]
    UnknownId,
    #[error("Unexpected ping query state")]
    UnexpectedState,
    #[error("Ping timeout reached")]
    TimeoutReached,
}
