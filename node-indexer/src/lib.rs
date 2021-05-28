use adnl::client::{AdnlClient, AdnlClientConfig};
use parking_lot::{Mutex, RwLock};

use crate::adnl_pool::AdnlManageConnection;
use crate::errors::{QueryError, QueryResult};
use crate::last_block::LastBlock;
use bb8::{Pool, PooledConnection};
use futures::{Sink, SinkExt};
use shared_deps::anyhow::Result;
use shared_deps::ton_api::ton;
use shared_deps::ton_block::{Deserializable, ShardDescr, ShardIdent};
use shared_deps::{tokio, NoFailure};
use shared_deps::{ton_block, ton_types};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use shared_deps::ton_api::ton::ton_node::blockid::BlockId;

mod adnl_pool;
mod errors;
mod last_block;

struct NodeClient {
    pool: Pool<AdnlManageConnection>,
    last_block: LastBlock,
    config: Config,

}

impl NodeClient {
    pub async fn new(config: AdnlClientConfig, current_block: Option<()>) -> Result<Self> {
        Ok(Self {
            current_block: current_block.map(|x| Mutex::new(x)).unwrap_or_default(),
            connection: AdnlClient::connect(&config).await.convert()?,
        })
    }

    pub fn proccessing_loop(sink: impl Sink<ton_block::Block>) {}
}

async fn proccessing_loop_inner(sink: impl Sink<ton_block::Block>) {
    sink.send();
}

pub struct State {
    pool: Pool<AdnlManageConnection>,
    last_block: LastBlock,
    address_subscriptions: RwLock<AddressSubscriptionsMap>,
    config: Config,
}

impl NodeClient {
    pub async fn spawn_indexer(self: &Arc<Self>) -> QueryResult<()> {
        let indexer = Arc::downgrade(self);
        let mut connection = self.acquire_connection().await?;
        let mut curr_mc_block_id = self.last_block.get_last_block(&mut connection).await?;

        tokio::spawn(async move {
            let (producer, consumer) = tokio::sync::mpsc::unbounded_channel();
            loop {
                let indexer = match indexer.upgrade() {
                    Some(indexer) => indexer,
                    None => return,
                };

                tokio::time::sleep(indexer.config.indexer_interval).await;
                log::debug!("Indexer step");

                let mut connection = match indexer.acquire_connection().await {
                    Ok(connection) => connection,
                    Err(_) => continue,
                };

                match indexer
                    .indexer_step(&mut connection, &curr_mc_block_id)
                    .await
                {
                    Ok(next_block_id) => curr_mc_block_id = next_block_id,
                    Err(e) => {
                        log::error!("Indexer step error: {:?}", e);
                    }
                }
            }
        });

        Ok(())
    }

    async fn indexer_step(
        &self,
        producer: UnboundedSender<ton_block::Block>,
        connection: &mut PooledConnection<'_, AdnlManageConnection>,
        prev_mc_block_id: &ton::ton_node::blockidext::BlockIdExt,
    ) -> Result<ton::ton_node::blockidext::BlockIdExt> {
        let curr_mc_block_id = self.last_block.get_last_block(connection).await?;
        if prev_mc_block_id == &curr_mc_block_id {
            return Ok(curr_mc_block_id);
        }

        let curr_mc_block = query_block(connection, curr_mc_block_id.clone()).await?;
        let extra = curr_mc_block
            .extra
            .read_struct()
            .and_then(|extra| extra.read_custom())
            .map_err(|e| anyhow::anyhow!("Failed to parse block info: {:?}", e))?;

        let extra = match extra {
            Some(extra) => extra,
            None => return Ok(curr_mc_block_id),
        };

        let mut workchain = -1;
        extra
            .shards()
            .iterate_shards(|shard_id, shard| {
                log::debug!("Shard id: {:?}, shard block: {}", shard_id, shard.seq_no);
                tokio::spawn( async move { process_shard(shard_id, shard, producer.clone()).await });
                Ok(true)
            })
            .map_err(|e| anyhow::anyhow!("Failed to iterate shards: {:?}", e))?;

        log::debug!("Next masterchain block id: {:?}", curr_mc_block_id);
        Ok(curr_mc_block_id)
    }
}

pub async fn process_shard(
    shard_id: ShardIdent,
    shard: ShardDescr,
    producer: UnboundedSender<ton_block::Block>,
) {
    let shards = shard.seq_no;
    let req = ton::rpc::lite_server::LookupBlock {
        mode: 0,
        id: BlockId{
            workchain:shard_id.workchain_id() ,
            shard: ,
            seqno: 0
        },
        lt: None,
        utime: None,
    };
}

pub async fn query_block(
    connection: &mut PooledConnection<'_, AdnlManageConnection>,
    id: ton::ton_node::blockidext::BlockIdExt,
) -> QueryResult<ton_block::Block> {
    let block = query(connection, ton::rpc::lite_server::GetBlock { id }).await?;

    let block = ton_block::Block::construct_from_bytes(&block.only().data.0)
        .map_err(|_| QueryError::InvalidBlock)?;

    Ok(block)
}
