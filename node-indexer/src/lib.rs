use std::collections::HashMap;
use std::convert::TryInto;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bb8::{Pool, PooledConnection};
use either::Either;
use futures::{Sink, SinkExt, StreamExt};
use ton::ton_node::blockid::BlockId;
use ton_api::ton;
use ton_block::{Block, Deserializable, MsgAddressInt, ShardDescr, ShardIdent};

use shared_deps::NoFailure;

use crate::adnl::AdnlClientConfig;
use crate::adnl_pool::AdnlManageConnection;
use crate::errors::{QueryError, QueryResult};
use crate::last_block::LastBlock;

mod adnl;
mod adnl_pool;
mod errors;
mod last_block;

#[derive(Debug, Clone)]
pub struct Config {
    pub indexer_interval: Duration,
    pub adnl: AdnlClientConfig,
    pub threshold: Duration,
    pub pool_size: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            indexer_interval: Duration::from_secs(1),
            adnl: default_mainnet_config(),
            threshold: Duration::from_secs(1),
            pool_size: 100,
        }
    }
}

pub fn default_mainnet_config() -> AdnlClientConfig {
    let key =
        hex::decode("b8d4512fee9e9d08ee899fece99faf3bbcb151447bbb175fcc8cbe4719040ab7").unwrap();

    AdnlClientConfig {
        server_address: SocketAddrV4::new(Ipv4Addr::new(54, 158, 97, 195), 3031),
        server_key: ed25519_dalek::PublicKey::from_bytes(&key).unwrap(),
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
        let manager = AdnlManageConnection::new(config.adnl.clone());
        let pool = Pool::builder()
            .max_size(config.pool_size)
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

struct IndexerStepResult {
    good: Vec<Block>,
    bad: Vec<BlockId>,
}

async fn bad_block_resolver<S>(
    mut bad_block_queue: tokio::sync::mpsc::UnboundedReceiver<BlockId>,
    pool: Pool<AdnlManageConnection>,
    mut sink: S,
) where
    S: Sink<ton_block::Block> + Clone + Send + Sync + Unpin + 'static,
    <S as futures::Sink<ton_block::Block>>::Error: std::error::Error,
{
    let mut bad = HashMap::new();
    while let Some(id) = bad_block_queue.recv().await {
        match query_block_by_seqno(pool.clone(), id.clone()).await {
            Ok(a) => {
                if let Err(e) = sink.send(a).await {
                    log::error!("Failed sending answer: {}", e);
                }
                bad.remove(&id);
            }
            Err(e) => {
                log::error!("Failed quering info about block: {}", e);
                let entry = bad.entry(id.clone()).or_insert(5);
                *entry -= 1;
                if *entry == 0 {
                    bad.remove(&id);
                }
            }
        };
    }
}

async fn get_block_ext_id(
    pool: Pool<AdnlManageConnection>,
    id: BlockId,
) -> Result<ton_api::ton::ton_node::blockidext::BlockIdExt> {
    Ok(query(
        pool.clone(),
        ton::rpc::lite_server::LookupBlock {
            mode: 0x1,
            id,
            lt: None,
            utime: None,
        },
    )
    .await?
    .id()
    .clone())
}

impl NodeClient {
    async fn blocks_producer(
        self: Arc<Self>,
        start_block: Option<BlockId>,
        new_mc_blocks_queue: tokio::sync::mpsc::Sender<ton::ton_node::blockidext::BlockIdExt>,
        pool_size: u32,
    ) -> Result<()> {
        let top_block = self.last_block.get_last_block(self.pool.clone()).await?;

        let mut current_block = match start_block {
            Some(a) => get_block_ext_id(self.pool.clone(), a).await?,
            None => top_block.clone(),
        };

        loop {
            let blocks_diff = top_block.seqno - current_block.seqno;
            if blocks_diff != 0 {
                let query_count = (blocks_diff / 10).max(1).min((pool_size as i32) * 8);
                log::info!("Query count: {}, diff: {}", query_count, blocks_diff);
                let block = get_block_ext_id(
                    self.pool.clone(),
                    BlockId {
                        workchain: current_block.workchain,
                        shard: current_block.shard,
                        seqno: current_block.seqno + query_count,
                    },
                )
                .await?;
                current_block = block;
                new_mc_blocks_queue.send(current_block.clone()).await?;
            } else if current_block == top_block {
                log::info!("Synced");
                log::info!("Current mc height: {}", current_block.seqno);

                let mut block = self.last_block.get_last_block(self.pool.clone()).await?;
                loop {
                    let current_block = self.last_block.get_last_block(self.pool.clone()).await?;
                    if current_block.seqno == block.seqno {
                        tokio::time::sleep(self.config.indexer_interval).await;
                    } else {
                        block = current_block;
                        new_mc_blocks_queue.send(block.clone()).await;
                    }
                }
            } else {
                log::info!("Near head");
                let block = get_block_ext_id(
                    self.pool.clone(),
                    BlockId {
                        workchain: current_block.workchain,
                        shard: current_block.shard,
                        seqno: current_block.seqno + 1,
                    },
                )
                .await?;
                current_block = block;
                new_mc_blocks_queue.send(current_block.clone()).await?;
            }
        }
    }

    pub async fn run_local(
        &self,
        contract_address: MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<nekoton::helpers::abi::ExecutionOutput> {
        use nekoton::core::models::{GenTimings, LastTransactionId};
        use nekoton::helpers::abi::FunctionExt;
        let last_block = self.last_block.get_last_block(self.pool.clone()).await?;
        let id = contract_address
            .address()
            .get_bytestring(0)
            .as_slice()
            .try_into()?;
        let get_state = ton::rpc::lite_server::GetAccountState {
            id: last_block,
            account: ton::lite_server::accountid::AccountId {
                workchain: contract_address.workchain_id(),
                id: ton::int256(id),
            },
        };
        let state = query(self.pool.clone(), get_state).await?.only();
        let state = ton_block::AccountStuff::construct_from_bytes(&state.state).convert()?;
        let latest_lt = state.storage.last_trans_lt;
        function.run_local(
            state,
            GenTimings::Unknown,
            &LastTransactionId::Inexact { latest_lt },
            input,
        )
    }

    pub async fn spawn_indexer<S, McBlocks>(
        self: &Arc<Self>,
        seqno: Option<BlockId>,
        mut sink: S,
        mut mc_blocks: McBlocks,
    ) -> QueryResult<()>
    where
        S: Sink<ton_block::Block> + Clone + Send + Sync + Unpin + 'static,
        <S as futures::Sink<ton_block::Block>>::Error: std::error::Error,
        McBlocks:
            Sink<ton::ton_node::blockidext::BlockIdExt> + Clone + Send + Sync + Unpin + 'static,
        <McBlocks as futures::Sink<ton::ton_node::blockidext::BlockIdExt>>::Error:
            std::error::Error,
    {
        let (bad_blocks_tx, bad_blocks_rx) = tokio::sync::mpsc::unbounded_channel();
        let indexer = Arc::downgrade(self);

        tokio::spawn(bad_block_resolver(
            bad_blocks_rx,
            self.pool.clone(),
            sink.clone(),
        ));

        let (masterchain_blocks_tx, mut masterchain_blocks_rx) = tokio::sync::mpsc::channel(2);

        tokio::spawn(self.clone().blocks_producer(
            seqno,
            masterchain_blocks_tx,
            self.config.pool_size,
        ));
        tokio::spawn(async move {
            while let Some(block) = masterchain_blocks_rx.recv().await {
                let indexer = match indexer.upgrade() {
                    Some(indexer) => indexer,
                    None => return,
                };

                log::debug!("Indexer step");

                match indexer.indexer_step(block.clone()).await {
                    Ok(a) => {
                        let IndexerStepResult { good, bad } = a;
                        mc_blocks.send(block);
                        for block in good {
                            if let Err(e) = sink.send(block).await {
                                log::error!("{:?}", e);
                            }
                        }
                        for bad_block in bad {
                            if let Err(e) = bad_blocks_tx.send(bad_block) {
                                log::error!("Bad blocks channel have broken: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Fatal indexer step error: {}", e)
                    }
                }
            }
        });

        Ok(())
    }

    async fn indexer_step(
        self: &Arc<Self>,
        mc_block: ton::ton_node::blockidext::BlockIdExt,
    ) -> Result<IndexerStepResult> {
        let futs = futures::stream::FuturesUnordered::new();
        let block = query_block(self.pool.clone(), mc_block).await?;
        let extra = block
            .extra
            .read_struct()
            .and_then(|extra| extra.read_custom())
            .map_err(|e| anyhow::anyhow!("Failed to parse block info: {:?}", e))?;

        let extra = match extra {
            Some(extra) => extra,
            None => anyhow::bail!("No extra in block"),
        };

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

        let (ok, bad) = futs.collect::<Vec<_>>().await.into_iter().flatten().fold(
            (Vec::new(), Vec::new()),
            |(mut ok, mut bad), x| {
                match x {
                    Either::Left(a) => ok.push(a),
                    Either::Right(a) => bad.push(a),
                };
                (ok, bad)
            },
        );

        log::info!("Good: {}", ok.len());
        Ok(IndexerStepResult { good: ok, bad })
    }

    pub async fn process_shard(
        self: Arc<Self>,
        shard_id: ShardIdent,
        shard: ShardDescr,
    ) -> Vec<Either<Block, BlockId>> {
        let workchain = shard_id.workchain_id();
        let shard_id = shard_id.shard_prefix_with_tag() as i64;
        let current_seqno = shard.seq_no as i32;

        let last_known_block = *self
            .shard_cache
            .entry(shard_id)
            .or_insert(current_seqno)
            .value();

        log::debug!(
            "Processing blocks {} in shard {:016x}.",
            current_seqno - last_known_block,
            shard_id,
        );
        let futs = futures::stream::FuturesUnordered::new();
        for seq_no in last_known_block..(current_seqno) {
            let pool = self.pool.clone();
            let task = async move {
                let id = BlockId {
                    workchain,
                    shard: shard_id,
                    seqno: seq_no,
                };
                let block = query_block_by_seqno(pool, id.clone()).await;
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
        let res = futs.collect().await;
        self.shard_cache.insert(shard_id, current_seqno);
        res
    }
}

pub async fn query_block(
    connection: Pool<AdnlManageConnection>,
    id: ton::ton_node::blockidext::BlockIdExt,
) -> QueryResult<ton_block::Block> {
    let now = std::time::Instant::now();
    let block = query(connection, ton::rpc::lite_server::GetBlock { id }).await?;
    let spent = std::time::Instant::now() - now;
    log::trace!("Spent in query_block: {:#?}", spent);
    let block = ton_block::Block::construct_from_bytes(&block.only().data.0)
        .map_err(|_| QueryError::InvalidBlock)?;

    Ok(block)
}

pub async fn query_block_by_seqno(
    connection: Pool<AdnlManageConnection>,
    id: ton::ton_node::blockid::BlockId,
) -> QueryResult<ton_block::Block> {
    let block_id = query(
        connection.clone(),
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

pub async fn query<T>(connection: Pool<AdnlManageConnection>, query: T) -> QueryResult<T::Reply>
where
    T: ton_api::Function,
{
    let query_bytes = query
        .boxed_serialized_bytes()
        .map_err(|_| QueryError::FailedToSerialize)?;
    let response = acquire_connection(&connection)
        .await?
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
