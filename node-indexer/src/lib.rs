use std::convert::TryInto;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bb8::{Pool, PooledConnection};
use either::Either;
use futures::{Sink, SinkExt, StreamExt};
use nekoton::core::models::TransactionId;
use nekoton::transport::models::{ExistingContract, RawContractState, RawTransaction};
use tiny_adnl::AdnlTcpClientConfig;
use ton::ton_node::blockid::BlockId;
use ton_api::ton;
use ton_block::{Block, Deserializable, HashmapAugType, MsgAddressInt, ShardDescr, ShardIdent};

use shared_deps::TrustMe;

use crate::adnl_pool::AdnlManageConnection;
use crate::errors::{QueryError, QueryResult};
use crate::last_block::LastBlock;

mod adnl_pool;
mod errors;
mod last_block;

#[derive(Debug, Clone)]
pub struct Config {
    pub indexer_interval: Duration,
    pub adnl: AdnlTcpClientConfig,
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

pub fn default_mainnet_config() -> AdnlTcpClientConfig {
    let key =
        hex::decode("b8d4512fee9e9d08ee899fece99faf3bbcb151447bbb175fcc8cbe4719040ab7").unwrap();

    AdnlTcpClientConfig {
        server_address: SocketAddrV4::new(Ipv4Addr::new(54, 158, 97, 195), 3031),
        server_key: ed25519_dalek::PublicKey::from_bytes(&key).unwrap(),
        socket_read_timeout: Duration::from_secs(10),
        socket_send_timeout: Duration::from_secs(10),
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
    sink: S,
) where
    S: Sink<ton_block::Block> + Clone + Send + Sync + Unpin + 'static,
    <S as futures::Sink<ton_block::Block>>::Error: std::error::Error,
{
    while let Some(id) = bad_block_queue.recv().await {
        tokio::spawn({
            let pool = pool.clone();
            let id = id.clone();
            let mut tx = sink.clone();
            async move {
                let result = tryhard::retry_fn(|| query_block_by_seqno(pool.clone(), id.clone()))
                    .retries(10)
                    .exponential_backoff(Duration::from_secs(1))
                    .await;
                match result {
                    Ok(a) => {
                        if let Err(e) = tx.send(a).await {
                            log::error!("Failed sending via channel: {}", e)
                        }
                    }
                    Err(e) => {
                        log::error!("Failed querying info about block: {}", e);
                    }
                }
            }
        });
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
        async fn get_block_id(
            pool: &Pool<AdnlManageConnection>,
            id: BlockId,
        ) -> Result<ton_api::ton::ton_node::blockidext::BlockIdExt> {
            tryhard::retry_fn(|| async {
                let pool = pool.clone();
                let id = id.clone();
                get_block_ext_id(pool.clone(), id).await
            })
            .retries(100)
            .exponential_backoff(Duration::from_secs(1))
            .max_delay(Duration::from_secs(600))
            .await
        }
        let top_block = tryhard::retry_fn(|| self.last_block.get_last_block(self.pool.clone()))
            .retries(100)
            .await
            .expect("Fatal block producer error");

        let mut current_block = match start_block {
            Some(a) => get_block_id(&self.pool, a)
                .await
                .expect("Fatal block producer error"),
            None => top_block.clone(),
        };

        macro_rules! get_last_block {
            () => {
                tryhard::retry_fn(|| self.last_block.get_last_block(self.pool.clone()))
                    .retries(100)
                    .exponential_backoff(Duration::from_secs(1))
                    .max_delay(Duration::from_secs(600))
                    .await
                    .expect("Fatal block producer error");
            };
        }

        loop {
            let blocks_diff = top_block.seqno - current_block.seqno;
            if blocks_diff != 0 {
                new_mc_blocks_queue
                    .send(current_block.clone())
                    .await
                    .expect("Channel is broken");
                let query_count = (blocks_diff / 10).max(1).min((pool_size as i32) * 8);
                log::info!("Query count: {}, diff: {}", query_count, blocks_diff);
                let block = get_block_id(
                    &self.pool,
                    BlockId {
                        workchain: current_block.workchain,
                        shard: current_block.shard,
                        seqno: current_block.seqno + query_count,
                    },
                )
                .await
                .expect("Fatal block producer error");
                current_block = block;
            } else if current_block == top_block {
                log::info!("Synced");
                log::info!("Current mc height: {}", current_block.seqno);
                let mut block = get_last_block!();
                loop {
                    let current_block = get_last_block!();
                    if current_block.seqno == block.seqno {
                        tokio::time::sleep(self.config.indexer_interval).await;
                    } else {
                        block = current_block;
                        if let Err(e) = new_mc_blocks_queue.send(block.clone()).await {
                            log::error!("Fail sending block id: {}", e);
                        }
                    }
                }
            } else {
                log::error!("Logic has broken");
                let block = get_block_id(
                    &self.pool,
                    BlockId {
                        workchain: current_block.workchain,
                        shard: current_block.shard,
                        seqno: current_block.seqno + 1,
                    },
                )
                .await
                .expect("Fatal block producer error");
                current_block = block;
                new_mc_blocks_queue.send(current_block.clone()).await?;
            }
        }
    }

    /// Return all transactions  for `contract_address`. Latest transaction first
    pub async fn get_all_transactions(
        &self,
        contract_address: MsgAddressInt,
    ) -> Result<Vec<RawTransaction>> {
        let mut all_transactions = Vec::new();
        let mut tx_id = None;
        loop {
            let mut res = self
                .get_transactions(contract_address.clone(), tx_id, 16)
                .await?;
            if res.is_empty() {
                log::debug!("Empty answer, no more transactions");
                break;
            }
            log::debug!("Got {} transactions", res.len());
            // Checked on previous step
            let hash = res.last().as_ref().trust_me().data.prev_trans_hash;
            let lt = res.last().as_ref().trust_me().data.prev_trans_lt;

            log::debug!("Getting txs before {}, lt: {}", hex::encode(&hash), lt);
            let id = TransactionId { lt, hash };
            tx_id = Some(id);
            all_transactions.append(&mut res);
        }
        Ok(all_transactions)
    }

    pub async fn get_contract_state(
        &self,
        contract_address: MsgAddressInt,
    ) -> Result<nekoton::transport::models::RawContractState> {
        use nekoton::core::models::{GenTimings, LastTransactionId};

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
        let response = query(self.pool.clone(), get_state).await?.only();
        let state = match ton_block::Account::construct_from_bytes(&response.state.0) {
            Ok(ton_block::Account::Account(account)) => {
                let q_roots =
                    ton_types::deserialize_cells_tree(&mut std::io::Cursor::new(&response.proof.0))
                        .map_err(|_| anyhow::anyhow!("InvalidAccountStateProof"))?;
                if q_roots.len() != 2 {
                    anyhow::bail!("InvalidAccountStateProof")
                }

                let merkle_proof = ton_block::MerkleProof::construct_from_cell(q_roots[0].clone())
                    .map_err(|_| anyhow::anyhow!("InvalidAccountStateProof"))?;
                let proof_root = merkle_proof.proof.virtualize(1);

                let ss = ton_block::ShardStateUnsplit::construct_from(&mut proof_root.into())
                    .map_err(|_| anyhow::anyhow!("InvalidAccountStateProof"))?;

                let shard_info = ss
                    .read_accounts()
                    .and_then(|accounts| {
                        accounts.get(&ton_types::UInt256::from(
                            // contract_address.get_address().get_bytestring(0),
                            id,
                        ))
                    })
                    .map_err(|_| anyhow::anyhow!("InvalidAccountStateProof"))?;

                if let Some(shard_info) = shard_info {
                    RawContractState::Exists(ExistingContract {
                        account,
                        timings: GenTimings::Known {
                            gen_lt: ss.gen_lt(),
                            gen_utime: (chrono::Utc::now().timestamp() - 10) as u32, // TEMP!!!!!, replace with ss.gen_time(),
                        },
                        last_transaction_id: LastTransactionId::Exact(TransactionId {
                            lt: shard_info.last_trans_lt(),
                            hash: *shard_info.last_trans_hash(),
                        }),
                    })
                } else {
                    RawContractState::NotExists
                }
            }
            _ => RawContractState::NotExists,
        };
        Ok(state)
    }

    pub async fn get_transactions(
        &self,
        address: MsgAddressInt,
        from: Option<TransactionId>,
        count: u8,
    ) -> Result<Vec<RawTransaction>> {
        async fn get_transactions_inner(
            client: &NodeClient,
            address: MsgAddressInt,
            from: Option<TransactionId>,
            count: u8,
        ) -> Result<Vec<u8>> {
            let from = match from {
                Some(id) => id,
                None => match client.get_contract_state(address.clone()).await? {
                    RawContractState::Exists(contract) => {
                        contract.last_transaction_id.to_transaction_id()
                    }
                    RawContractState::NotExists => {
                        let transactions =
                            ton_types::serialize_toc(&ton_types::Cell::default()).unwrap();

                        return Ok(transactions);
                    }
                },
            };

            let response = query(
                client.pool.clone(),
                ton::rpc::lite_server::GetTransactions {
                    count: count as i32,
                    account: ton::lite_server::accountid::AccountId {
                        workchain: address.workchain_id() as i32,
                        id: ton::int256(
                            ton_types::UInt256::from(address.address().get_bytestring(0)).into(),
                        ),
                    },
                    lt: from.lt as i64,
                    hash: from.hash.into(),
                },
            )
            .await?;

            Ok(response.transactions().0.clone())
        }
        let data = get_transactions_inner(self, address, from, count).await?;
        let transactions = match ton_types::deserialize_cells_tree(&mut std::io::Cursor::new(data))
        {
            Ok(a) => a,
            Err(e) => {
                log::error!("Failed deserilizing transactions list: {}", e);
                return Ok(Vec::new());
            }
        };

        let mut result = Vec::with_capacity(transactions.len());
        for item in transactions.into_iter().rev() {
            result.push(RawTransaction {
                hash: item.repr_hash(),
                data: ton_block::Transaction::construct_from_cell(item)
                    .map_err(|_| anyhow::anyhow!("Invalid transaction"))?,
            });
        }
        Ok(result)
    }

    pub async fn run_local(
        &self,
        contract_address: MsgAddressInt,
        function: &ton_abi::Function,
        input: &[ton_abi::Token],
    ) -> Result<nekoton::helpers::abi::ExecutionOutput> {
        use nekoton::helpers::abi::FunctionExt;

        let state = self.get_contract_state(contract_address).await?;
        let state = match state {
            RawContractState::NotExists => {
                anyhow::bail!("Account doesn't exist")
            }
            RawContractState::Exists(a) => a,
        };
        function.run_local(
            state.account,
            state.timings,
            &state.last_transaction_id,
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
        McBlocks: Sink<BlockId> + Clone + Send + Sync + Unpin + 'static,
        <McBlocks as futures::Sink<BlockId>>::Error: std::error::Error,
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
                    None => {
                        log::error!("Indexer refs are empty. Quiting");
                        return;
                    }
                };

                log::debug!("Indexer step. Id: {}", block.seqno);
                let block_id = BlockId {
                    workchain: block.workchain,
                    shard: block.shard,
                    seqno: block.seqno,
                };
                let step_result =
                    tryhard::retry_fn(|| async { indexer.indexer_step(block.clone()).await })
                        .retries(10)
                        .exponential_backoff(Duration::from_secs(1))
                        .await;
                match step_result {
                    Ok(a) => {
                        let IndexerStepResult { good, bad } = a;
                        log::debug!("Good: {}, Bad: {}", good.len(), bad.len());
                        if let Err(e) = mc_blocks.send(block_id).await {
                            log::error!("Failed sending block id: {}", e);
                        }
                        for block in good {
                            if let Err(e) = sink.send(block).await {
                                log::error!("{:?}", e);
                            }
                        }
                        for bad_block in bad {
                            if let Err(e) = bad_blocks_tx.send(bad_block) {
                                log::error!("Bad blocks channel has broken: {:?}", e);
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

        Ok(IndexerStepResult { good: ok, bad })
    }

    async fn process_shard(
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
