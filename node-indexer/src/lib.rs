use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bb8::{Pool, PooledConnection};
use futures::{Sink, SinkExt};

use ton_api::ton;
use ton_api::ton::ton_node::blockid::BlockId;
use ton_block::{Deserializable, ShardDescr, ShardIdent};

use crate::adnl_config::AdnlConfig;
use crate::adnl_pool::AdnlManageConnection;
use crate::errors::{QueryError, QueryResult};
use crate::last_block::LastBlock;

pub mod adnl_config;
mod adnl_pool;
mod errors;
mod last_block;

#[derive(Debug, Clone)]
pub struct Config {
    indexer_interval: Duration,
    adnl: AdnlConfig,
    threshold: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            indexer_interval: Duration::from_secs(1),
            adnl: AdnlConfig::default_mainnet_config(),
            threshold: Duration::from_secs(1),
        }
    }
}
type ShardBlocks = Arc<dashmap::DashMap<i64, i32>>;

pub struct NodeClient {
    pool: Pool<AdnlManageConnection>,
    last_block: LastBlock,
    config: Config,
    shard_cache: ShardBlocks,
}

impl NodeClient {
    pub async fn new(config: Config) -> Result<Self> {
        let manager = AdnlManageConnection::new(config.adnl.tonlib_config()?);
        let pool = Pool::builder()
            .max_lifetime(None)
            .max_size(10)
            .build(manager)
            .await?;
        Ok(Self {
            pool,
            last_block: LastBlock::new(&config.threshold),
            config,
            shard_cache: ShardBlocks::default(),
        })
    }
}

async fn acquire_connection(
    pool: &Pool<AdnlManageConnection>,
) -> QueryResult<PooledConnection<'_, AdnlManageConnection>> {
    pool.get().await.map_err(|e| {
        log::error!("connection error: {:#?}", e);
        QueryError::ConnectionError
    })
}

impl NodeClient {
    async fn acquire_connection(&self) -> QueryResult<PooledConnection<'_, AdnlManageConnection>> {
        acquire_connection(&self.pool).await
    }

    pub async fn spawn_indexer(
        self: &Arc<Self>,
        sink: impl Sink<ton_block::Block> + Clone + Send + Sync + Unpin + 'static,
    ) -> QueryResult<()> {
        let indexer = Arc::downgrade(self);
        let mut connection = self.acquire_connection().await?;
        let mut curr_mc_block_id = self.last_block.get_last_block(&mut connection).await?;

        tokio::spawn(async move {
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
                    .clone()
                    .indexer_step(sink.clone(), &mut connection, &curr_mc_block_id)
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
        self: Arc<Self>,
        tx: impl Sink<ton_block::Block> + Clone + Send + Sync + Unpin + 'static,
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

        extra
            .shards()
            .iterate_shards(|shard_id, shard| {
                log::debug!("Shard id: {:?}, shard block: {}", shard_id, shard.seq_no);
                let idxr = self.clone();
                let tx = tx.clone();
                tokio::spawn(async move { idxr.process_shard(shard_id, shard, tx).await });
                Ok(true)
            })
            .map_err(|e| anyhow::anyhow!("Failed to iterate shards: {:?}", e))?;

        log::debug!("Next masterchain block id: {:?}", curr_mc_block_id);
        Ok(curr_mc_block_id)
    }

    pub async fn process_shard(
        self: Arc<Self>,
        shard_id: ShardIdent,
        shard: ShardDescr,
        tx: impl Sink<ton_block::Block> + Clone + Send + Sync + Unpin + 'static,
    ) {
        let workchain = shard_id.workchain_id();
        let shard_id = shard_id.shard_prefix_with_tag() as i64;
        let last_known_block = *self
            .shard_cache
            .entry(shard_id)
            .or_insert(shard.seq_no as i32)
            .value();

        for seq_no in last_known_block..(shard.seq_no as i32) {
            let mut tx = tx.clone();
            let pool = self.pool.clone();
            tokio::spawn(async move {
                let mut connection = match acquire_connection(&pool).await {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("Failed aquiring connection: {:?}", e);
                        return;
                    }
                };
                let block = query_block_by_seqno(
                    &mut connection,
                    BlockId {
                        workchain,
                        shard: shard_id,
                        seqno: seq_no,
                    },
                )
                .await;
                match block {
                    Ok(a) => {
                        if let Err(e) = tx.send(a).await {
                            log::error!("Failed sending");
                        }
                    }
                    Err(e) => {
                        log::error!("Query error: {:?}", e);
                    }
                }
            });
        }
    }
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

pub async fn query_block_by_seqno(
    connection: &mut PooledConnection<'_, AdnlManageConnection>,
    id: ton::ton_node::blockid::BlockId,
) -> QueryResult<ton_block::Block> {
    let block_id = query(
        connection,
        ton::rpc::lite_server::LookupBlock {
            mode: 0x1,
            id,
            lt: None,
            utime: None,
        },
    )
    .await?;

    query_block(connection, block_id.only().id).await
}

pub async fn query<T>(
    connection: &mut PooledConnection<'_, AdnlManageConnection>,
    query: T,
) -> QueryResult<T::Reply>
where
    T: ton_api::Function,
{
    let query_bytes = query
        .boxed_serialized_bytes()
        .map_err(|_| QueryError::FailedToSerialize)?;

    let response = connection
        .query(&ton::TLObject::new(ton::rpc::lite_server::Query {
            data: query_bytes.into(),
        }))
        .await
        .map_err(|_| QueryError::ConnectionError)?;

    match response.downcast::<T::Reply>() {
        Ok(reply) => Ok(reply),
        Err(error) => match error.downcast::<ton::lite_server::Error>() {
            Ok(error) => Err(QueryError::LiteServer(error)),
            Err(_) => Err(QueryError::Unknown),
        },
    }
}

mod test {
    use super::Config;
    use futures::StreamExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_blocks() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let config = super::Config::default();
        let node = Arc::new(super::NodeClient::new(config).await.unwrap());
        node.spawn_indexer(tx).await.unwrap();
        while let Some(a) = tokio_stream::wrappers::UnboundedReceiverStream::new(rx)
            .next()
            .await
        {
            println!("a");
            return;
        }
    }
}
