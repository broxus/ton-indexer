use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use dashmap::{DashMap, DashSet};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tiny_adnl::utils::*;
use ton_api::ton;

pub use self::boot::*;
use self::db::*;
use self::downloader::*;
use self::node_state::*;
use self::shard_client::*;
use crate::config::*;
use crate::network::*;
use crate::storage::*;
use crate::utils::*;

mod boot;
mod db;
mod downloader;
mod node_state;
mod shard_client;

pub struct Engine {
    db: Arc<dyn Db>,
    network: Arc<NodeNetwork>,

    zero_state_id: ton_block::BlockIdExt,
    init_mc_block_id: ton_block::BlockIdExt,
    last_known_mc_block_seqno: AtomicU32,
    last_known_key_block_seqno: AtomicU32,

    shard_states_cache: ShardStateCache,

    shard_states_operations: OperationsPool<ton_block::BlockIdExt, Arc<ShardStateStuff>>,
}

impl Engine {
    pub async fn new(config: NodeConfig, global_config: GlobalConfig) -> Result<Arc<Self>> {
        let db = SledDb::new(config.sled_db_path(), config.file_db_path()).await?;

        let zero_state_id = global_config.zero_state.clone();

        let mut init_mc_block_id = zero_state_id.clone();
        if let Ok(block_id) = InitMcBlockId::load_from_db(db.as_ref()) {
            if block_id.0.seqno > init_mc_block_id.seq_no as i32 {
                init_mc_block_id = convert_block_id_ext_api2blk(&block_id.0)?;
            }
        }

        let network = NodeNetwork::new(config, global_config).await?;
        network.start().await?;

        let engine = Arc::new(Self {
            db,
            network,
            zero_state_id,
            init_mc_block_id,
            last_known_mc_block_seqno: AtomicU32::new(0),
            last_known_key_block_seqno: AtomicU32::new(0),
            shard_states_cache: ShardStateCache::new(120),
            shard_states_operations: OperationsPool::new("shard_states_operations"),
        });

        engine
            .get_full_node_overlay(ton_block::BASE_WORKCHAIN_ID, ton_block::SHARD_FULL)
            .await?;

        Ok(engine)
    }

    pub fn db(&self) -> &Arc<dyn Db> {
        &self.db
    }

    pub fn network(&self) -> &Arc<NodeNetwork> {
        &self.network
    }

    pub fn zero_state_id(&self) -> &ton_block::BlockIdExt {
        &self.zero_state_id
    }

    pub fn init_mc_block_id(&self) -> &ton_block::BlockIdExt {
        &self.init_mc_block_id
    }

    pub fn set_init_mc_block_id(&self, block_id: &ton_block::BlockIdExt) {
        InitMcBlockId(convert_block_id_ext_blk2api(block_id))
            .store_into_db(self.db.as_ref())
            .ok();
    }

    pub fn update_last_known_mc_block_seqno(&self, seqno: u32) -> bool {
        self.last_known_mc_block_seqno
            .fetch_max(seqno, Ordering::SeqCst)
            < seqno
    }

    pub fn update_last_known_key_block_seqno(&self, seqno: u32) -> bool {
        self.last_known_key_block_seqno
            .fetch_max(seqno, Ordering::SeqCst)
            < seqno
    }

    pub async fn get_masterchain_overlay(&self) -> Result<Arc<dyn FullNodeOverlayClient>> {
        self.get_full_node_overlay(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL)
            .await
    }

    pub async fn get_full_node_overlay(
        &self,
        workchain: i32,
        shard: u64,
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {
        let (full_id, short_id) = self.network.compute_overlay_id(workchain, shard)?;
        self.network.get_overlay(full_id, short_id).await
    }

    pub async fn listen_broadcasts(
        self: &Arc<Self>,
        shard_ident: ton_block::ShardIdent,
    ) -> Result<()> {
        let overlay = self
            .get_full_node_overlay(
                shard_ident.workchain_id(),
                shard_ident.shard_prefix_with_tag(),
            )
            .await?;
        let engine = self.clone();

        tokio::spawn(async move {
            loop {
                match overlay.wait_broadcast().await {
                    Ok((ton::ton_node::Broadcast::TonNode_BlockBroadcast(block), peer_id)) => {
                        log::info!("Got block broadcast from {}", peer_id);

                        let engine = engine.clone();
                        tokio::spawn(async move {
                            match process_block_broadcast(&engine, *block).await {
                                Ok(_) => {
                                    log::info!("Processed block broadcast");
                                }
                                Err(e) => {
                                    log::error!("Failed to process block broadcast: {}", e);
                                }
                            }
                        });
                    }
                    Ok((broadcast, peer_id)) => {
                        log::info!("Got unknown broadcast: {:?} from {}", broadcast, peer_id);
                    }
                    Err(e) => {
                        log::error!("Failed to wait broadcast for shard {}: {}", shard_ident, e);
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn download_zerostate(
        &self,
        block_id: &ton_block::BlockIdExt,
        max_attempts: Option<u32>,
    ) -> Result<Arc<ShardStateStuff>> {
        self.create_download_context(
            "download_zerostate",
            Arc::new(ZeroStateDownloader),
            block_id,
            max_attempts,
            Some(DownloaderTimeouts {
                initial: 10,
                max: 3000,
                multiplier: 1.2,
            }),
        )
        .await?
        .download()
        .await
    }

    pub async fn download_state(
        &self,
        block_id: &ton_block::BlockIdExt,
        masterchain_block_id: &ton_block::BlockIdExt,
        active_peers: &Arc<DashSet<AdnlNodeIdShort>>,
    ) -> Result<Arc<ShardStateStuff>> {
        let overlay = self
            .get_full_node_overlay(
                block_id.shard_id.workchain_id(),
                block_id.shard_id.shard_prefix_with_tag(),
            )
            .await?;

        let neighbour = loop {
            match overlay
                .check_persistent_state(block_id, masterchain_block_id, active_peers)
                .await
            {
                Ok(Some(peer)) => break peer,
                Ok(None) => {
                    log::trace!("Failed to download state: state not found");
                }
                Err(e) => {
                    log::trace!("Failed to download state: {}", e);
                }
            };
        };

        let mut offset = 0;
        let parts = Arc::new(DashMap::new());
        let max_size = 1 << 18;
        let total_size = Arc::new(AtomicUsize::new(usize::MAX));
        let threads = 3;

        let results = std::iter::repeat_with(|| {
            let overlay = overlay.clone();
            let parts = parts.clone();
            let total_size = total_size.clone();
            let neighbour = neighbour.clone();
            let mut thread_offset = offset;
            offset += max_size;

            async move {
                let mut peer_attempt = 0;
                let mut part_attempt = 0;

                loop {
                    log::info!(
                        "-------------------------- Downloading part: {}",
                        thread_offset
                    );

                    if thread_offset >= total_size.load(Ordering::Acquire) {
                        log::info!("-------- Downloaded part: {}", thread_offset);
                        return Result::<_, anyhow::Error>::Ok(());
                    }

                    match overlay
                        .download_persistent_state_part(
                            block_id,
                            masterchain_block_id,
                            thread_offset,
                            max_size,
                            neighbour.clone(),
                            peer_attempt,
                        )
                        .await
                    {
                        Ok(part) => {
                            part_attempt = 0;
                            let part_len = part.len();
                            parts.insert(thread_offset, part);

                            if part_len < max_size {
                                total_size.store(thread_offset + part_len, Ordering::Release);
                                return Ok(());
                            }

                            thread_offset += max_size * threads;
                        }
                        Err(e) => {
                            part_attempt += 1;
                            peer_attempt += 1;

                            log::error!("Failed to download persistent state part: {}", e);
                            if part_attempt > 10 {
                                return Err(EngineError::RanOutOfAttempts.into());
                            }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        })
        .take(threads)
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

        log::info!("DOWNLOAD RESULTS: {:?}", results);

        results
            .into_iter()
            .find(|result| result.is_err())
            .unwrap_or(Ok(()))?;

        let total_size = total_size.load(Ordering::Acquire);
        debug_assert!(total_size < usize::MAX);

        let mut state = Vec::with_capacity(total_size);

        let mut offset = 0;
        while let Some(part) = parts.get(&offset) {
            state.extend_from_slice(part.value());
            offset += part.len();
        }
        debug_assert_eq!(total_size, state.len());

        Ok(Arc::new(ShardStateStuff::deserialize(
            block_id.clone(),
            &state,
        )?))
    }

    pub async fn download_next_masterchain_block(
        &self,
        prev_block_id: &ton_block::BlockIdExt,
        max_attempts: Option<u32>,
    ) -> Result<(BlockStuff, BlockProofStuff)> {
        if !prev_block_id.is_masterchain() {
            return Err(EngineError::NonMasterchainNextBlock.into());
        }

        self.create_download_context(
            "download_next_masterchain_block",
            Arc::new(NextBlockDownloader),
            prev_block_id,
            max_attempts,
            Some(DownloaderTimeouts {
                initial: 50,
                max: 1000,
                multiplier: 1.1,
            }),
        )
        .await?
        .download()
        .await
    }

    pub async fn download_block(
        &self,
        block_id: &ton_block::BlockIdExt,
        max_attempts: Option<u32>,
        timeouts: Option<DownloaderTimeouts>,
    ) -> Result<(BlockStuff, BlockProofStuff)> {
        self.create_download_context(
            "download_block",
            Arc::new(BlockDownloader),
            block_id,
            max_attempts,
            timeouts,
        )
        .await?
        .download()
        .await
    }

    pub async fn download_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        is_link: bool,
        is_key_block: bool,
        max_attempts: Option<u32>,
    ) -> Result<BlockProofStuff> {
        self.create_download_context(
            "create_download_context",
            Arc::new(BlockProofDownloader {
                is_link,
                is_key_block,
            }),
            block_id,
            max_attempts,
            None,
        )
        .await?
        .download()
        .await
    }

    pub async fn download_next_key_blocks_ids(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Vec<ton_block::BlockIdExt>> {
        let mc_overlay = self.get_masterchain_overlay().await?;
        mc_overlay.download_next_key_blocks_ids(block_id, 5).await
    }

    pub fn load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>> {
        self.db.load_block_handle(block_id)
    }

    pub fn find_block_by_seq_no(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        seq_no: u32,
    ) -> Result<Arc<BlockHandle>> {
        self.db.find_block_by_seq_no(account_prefix, seq_no)
    }

    pub fn find_block_by_utime(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        utime: u32,
    ) -> Result<Arc<BlockHandle>> {
        self.db.find_block_by_utime(account_prefix, utime)
    }

    pub fn find_block_by_lt(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        lt: u64,
    ) -> Result<Arc<BlockHandle>> {
        self.db.find_block_by_lt(account_prefix, lt)
    }

    pub fn load_state(&self, block_id: &ton_block::BlockIdExt) -> Result<Arc<ShardStateStuff>> {
        if let Some(state) = self.shard_states_cache.get(block_id) {
            Ok(state)
        } else {
            let state = Arc::new(self.db.load_shard_state(block_id)?);
            self.shard_states_cache
                .set(block_id, |_| Some(state.clone()));
            Ok(state)
        }
    }

    pub async fn store_state(
        &self,
        handle: &Arc<BlockHandle>,
        state: &Arc<ShardStateStuff>,
    ) -> Result<()> {
        self.shard_states_cache.set(handle.id(), |existing| {
            existing.is_none().then(|| state.clone())
        });

        self.db.store_shard_state(handle, state.as_ref())?;

        self.shard_states_operations
            .do_or_wait(state.block_id(), None, futures::future::ok(state.clone()))
            .await?;

        Ok(())
    }

    pub async fn load_block_data(&self, handle: &Arc<BlockHandle>) -> Result<BlockStuff> {
        self.db.load_block_data(handle.as_ref()).await
    }

    pub async fn store_block_data(&self, block: &BlockStuff) -> Result<StoreBlockResult> {
        let store_block_result = self.db.store_block_data(block).await?;
        if store_block_result.handle.id().shard().is_masterchain() {
            if store_block_result.handle.meta().is_key_block() {
                self.update_last_known_key_block_seqno(store_block_result.handle.id().seq_no);
            }
            self.update_last_known_mc_block_seqno(store_block_result.handle.id().seq_no);
        }
        Ok(store_block_result)
    }

    pub async fn load_block_proof(
        &self,
        handle: &Arc<BlockHandle>,
        is_link: bool,
    ) -> Result<BlockProofStuff> {
        self.db.load_block_proof(handle, is_link).await
    }

    pub async fn store_block_proof(
        &self,
        block_ud: &ton_block::BlockIdExt,
        handle: Option<Arc<BlockHandle>>,
        proof: &BlockProofStuff,
    ) -> Result<Arc<BlockHandle>> {
        Ok(self
            .db
            .store_block_proof(block_ud, handle, proof)
            .await?
            .handle)
    }

    pub async fn store_zerostate(
        &self,
        block_id: &ton_block::BlockIdExt,
        state: &Arc<ShardStateStuff>,
    ) -> Result<Arc<BlockHandle>> {
        let handle =
            self.db
                .create_or_load_block_handle(block_id, None, Some(state.state().gen_time()))?;
        self.store_state(&handle, state).await?;
        Ok(handle)
    }

    async fn create_download_context<'a, T>(
        &'a self,
        name: &'a str,
        downloader: Arc<dyn Downloader<Item = T>>,
        block_id: &'a ton_block::BlockIdExt,
        max_attempts: Option<u32>,
        timeouts: Option<DownloaderTimeouts>,
    ) -> Result<DownloadContext<'a, T>> {
        Ok(DownloadContext {
            name,
            block_id,
            max_attempts,
            timeouts,
            client: self
                .get_full_node_overlay(
                    block_id.shard().workchain_id(),
                    block_id.shard().shard_prefix_with_tag(),
                )
                .await?,
            db: self.db.as_ref(),
            downloader,
        })
    }
}

impl<'a, T> DownloadContext<'a, T> {
    async fn load_full_block(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuff, BlockProofStuff)>> {
        Ok(match self.db.load_block_handle(block_id)? {
            Some(handle) => {
                let mut is_link = false;
                if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) {
                    Some((
                        self.db.load_block_data(&handle).await?,
                        self.db.load_block_proof(&handle, is_link).await?,
                    ))
                } else {
                    None
                }
            }
            None => None,
        })
    }
}

struct BlockDownloader;

#[async_trait::async_trait]
impl Downloader for BlockDownloader {
    type Item = (BlockStuff, BlockProofStuff);

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        if let Some(full_block) = context.load_full_block(context.block_id).await? {
            return Ok(Some(full_block));
        }

        context.client.download_block_full(context.block_id).await
    }
}

struct BlockProofDownloader {
    is_link: bool,
    is_key_block: bool,
}

#[async_trait::async_trait]
impl Downloader for BlockProofDownloader {
    type Item = BlockProofStuff;

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        if let Some(handle) = context.db.load_block_handle(context.block_id)? {
            let mut is_link = false;
            if handle.has_proof_or_link(&mut is_link) {
                return Ok(Some(context.db.load_block_proof(&handle, is_link).await?));
            }
        }

        context
            .client
            .download_block_proof(context.block_id, self.is_link, self.is_key_block)
            .await
    }
}

struct NextBlockDownloader;

#[async_trait::async_trait]
impl Downloader for NextBlockDownloader {
    type Item = (BlockStuff, BlockProofStuff);

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        if let Some(prev_handle) = context.db.load_block_handle(context.block_id)? {
            if prev_handle.meta().has_next1() {
                let next_block_id = context
                    .db
                    .load_block_connection(context.block_id, BlockConnection::Next1)?;

                if let Some(full_block) = context.load_full_block(&next_block_id).await? {
                    return Ok(Some(full_block));
                }
            }
        }

        context
            .client
            .download_next_block_full(context.block_id)
            .await
    }
}

struct ZeroStateDownloader;

#[async_trait::async_trait]
impl Downloader for ZeroStateDownloader {
    type Item = Arc<ShardStateStuff>;

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        context.client.download_zero_state(context.block_id).await
    }
}

#[derive(thiserror::Error, Debug)]
enum EngineError {
    #[error("Downloading next block is only allowed for masterchain")]
    NonMasterchainNextBlock,
    #[error("Ran out of attempts")]
    RanOutOfAttempts,
}
