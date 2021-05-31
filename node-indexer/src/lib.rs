use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bb8::{Pool, PooledConnection};
use either::Either;
use futures::Sink;
use ton_api::ton;
use ton_api::ton::ton_node::blockid::BlockId;
use ton_block::{Block, Deserializable, ShardDescr, ShardIdent};

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

enum IndexerStepResult {
    NoChanges,
    NewBlock {
        good: Vec<Block>,
        bad: Vec<BlockId>,
        mc_block_id: ton::ton_node::blockidext::BlockIdExt,
    },
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
        let curr_mc_block_id = self.last_block.get_last_block(&mut connection).await?;

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
                    .indexer_step(&mut connection, &curr_mc_block_id)
                    .await
                {
                    Ok(a) => match a {
                        IndexerStepResult::NoChanges => {
                            log::info!("Mc block not changed")
                        }
                        IndexerStepResult::NewBlock { .. } => {
                            todo!()
                        }
                    },
                    Err(e) => {
                        log::error!("Fatal indexer step error: {}", e)
                    }
                }
            }
        });

        Ok(())
    }

    async fn indexer_step(
        self: Arc<Self>,
        connection: &mut PooledConnection<'_, AdnlManageConnection>,
        prev_mc_block_id: &ton::ton_node::blockidext::BlockIdExt,
    ) -> Result<IndexerStepResult> {
        let curr_mc_block_id = self.last_block.get_last_block(connection).await?;
        if prev_mc_block_id == &curr_mc_block_id {
            return Ok(IndexerStepResult::NoChanges);
        }

        let curr_mc_block = query_block(connection, curr_mc_block_id.clone()).await?;
        let extra = curr_mc_block
            .extra
            .read_struct()
            .and_then(|extra| extra.read_custom())
            .map_err(|e| anyhow::anyhow!("Failed to parse block info: {:?}", e))?;

        let extra = match extra {
            Some(extra) => extra,
            None => return Ok(IndexerStepResult::NoChanges),
        };
        let mut futs = Vec::new();
        extra
            .shards()
            .iterate_shards(|shard_id, shard| {
                log::debug!("Shard id: {:?}, shard block: {}", shard_id, shard.seq_no);
                let idxr = self.clone();
                let task = idxr.process_shard(shard_id, shard);
                futs.push(task);
                Ok(true)
            })
            .map_err(|e| anyhow::anyhow!("Failed to iterate shards: {:?}", e))?;

        let (ok, bad) = futures::future::join_all(futs)
            .await
            .into_iter()
            .flatten()
            .fold((Vec::new(), Vec::new()), |(mut ok, mut bad), x| {
                match x {
                    Either::Left(a) => ok.push(a),
                    Either::Right(a) => bad.push(a),
                };
                (ok, bad)
            });

        log::debug!("Next masterchain block id: {:?}", curr_mc_block_id);
        Ok(IndexerStepResult::NewBlock {
            good: ok,
            bad,
            mc_block_id: curr_mc_block_id,
        })
    }

    pub async fn process_shard(
        self: Arc<Self>,
        shard_id: ShardIdent,
        shard: ShardDescr,
    ) -> Vec<Either<Block, BlockId>> {
        let workchain = shard_id.workchain_id();
        let shard_id = shard_id.shard_prefix_with_tag() as i64;
        let last_known_block = *self
            .shard_cache
            .entry(shard_id)
            .or_insert(shard.seq_no as i32)
            .value();
        log::trace!(
            "Processing blocks {}..{} in shard {:016x}",
            last_known_block,
            shard.seq_no,
            shard_id
        );
        let mut futs = Vec::new();
        for seq_no in last_known_block..(shard.seq_no as i32) {
            let pool = self.pool.clone();
            let task = async move {
                let id = BlockId {
                    workchain,
                    shard: shard_id,
                    seqno: seq_no,
                };

                let mut connection = match acquire_connection(&pool).await {
                    Ok(a) => a,
                    Err(e) => {
                        log::error!("Failed acquiring connection: {:?}", e);
                        return either::Right(id);
                    }
                };
                let block = query_block_by_seqno(&mut connection, id.clone()).await;
                match block {
                    Ok(a) => either::Left(a),
                    Err(e) => {
                        log::error!("Query error: {:?}", e);
                        either::Right(id)
                    }
                }
            };
            futs.push(task);
        }
        futures::future::join_all(futs).await
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
    use std::sync::Arc;

    use futures::StreamExt;
    use ton_block::{Block, GetRepresentationHash};

    use super::Config;

    #[tokio::test]
    async fn test_blocks() {
        env_logger::init();
        let (tx, mut rx) = futures::channel::mpsc::unbounded();
        let config = super::Config::default();
        let node = Arc::new(super::NodeClient::new(config).await.unwrap());
        log::info!("here");
        node.spawn_indexer(tx).await.unwrap();
        while let Some(a) = rx.next().await {
            let data: Block = a;
            let info = data.read_info().unwrap();
            let hash = info.hash().unwrap();
            let seq = info.seq_no();
            let wc = info.shard().workchain_id();
            let shard = info.shard().shard_prefix_with_tag();
            println!(
                "Hash: {:?} Seq: {} Wc: {}, shard: {:016x}",
                hash, seq, wc, shard
            );
            return;
        }
    }
}
