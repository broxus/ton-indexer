use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use bb8::PooledConnection;
use ton_api::ton;

use super::adnl_pool::AdnlManageConnection;
use super::connection::*;
use super::errors::*;
use shared_deps::ton_api::ton;
use shared_deps::ton_block::ShardIdent;
use dashmap::DashMap;

type ShardCache = DashMap<ShardIdent, ton::>;

pub struct LastBlock {
    id: parking_lot::RwLock<Option<(QueryResult<ton::ton_node::blockidext::BlockIdExt>, Instant)>>,
    shard_cache:ShardCache,
    threshold: Duration,
    in_process: AtomicBool,
}

impl LastBlock {
    pub fn new(threshold: &Duration) -> Self {
        Self {
            id: parking_lot::RwLock::new(None),
            threshold: *threshold,
            in_process: AtomicBool::new(false),
            shard_cache:DashMap::default()
        }
    }

    pub async fn get_last_block(
        &self,
        connection: &mut PooledConnection<'_, AdnlManageConnection>,
    ) -> QueryResult<ton::ton_node::blockidext::BlockIdExt> {
        let now = {
            let id = self.id.read();

            let now = Instant::now();

            match &*id {
                Some((result, last)) => {
                    if now.duration_since(*last) < self.threshold
                        || self
                            .in_process
                            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                            .is_err()
                    {
                        return result.clone();
                    }
                    now
                }
                None => now,
            }
        };

        log::debug!("Getting mc block");

        let id = query(connection, ton::rpc::lite_server::GetMasterchainInfo)
            .await
            .map(|result| result.only().last);

        log::debug!("Got mc block");

        let mut new_id = self.id.write();
        *new_id = Some((id.clone(), now));
        self.in_process.store(false, Ordering::Release);

        id
    }
}
