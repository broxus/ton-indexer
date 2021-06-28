use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tiny_adnl::utils::*;

use self::db::*;
use self::downloader::*;
use self::node_state::*;
use crate::config::*;
use crate::network::*;
use crate::storage::*;
use crate::utils::*;

mod boot;
mod db;
mod downloader;
mod node_state;

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
        let db = Arc::new(InMemoryDb::new());

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

    pub fn load_block_proof(
        &self,
        handle: &Arc<BlockHandle>,
        is_link: bool,
    ) -> Result<BlockProofStuff> {
        self.db.load_block_proof(handle, is_link)
    }

    pub fn store_block_proof(
        &self,
        block_ud: &ton_block::BlockIdExt,
        handle: Option<Arc<BlockHandle>>,
        proof: &BlockProofStuff,
    ) -> Result<Arc<BlockHandle>> {
        Ok(self.db.store_block_proof(block_ud, handle, proof)?.handle)
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
    fn load_full_block(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuff, BlockProofStuff)>> {
        Ok(match self.db.load_block_handle(block_id)? {
            Some(handle) => {
                let mut is_link = false;
                if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) {
                    Some((
                        self.db.load_block_data(&handle)?,
                        self.db.load_block_proof(&handle, is_link)?,
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
        if let Some(full_block) = context.load_full_block(context.block_id)? {
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
                return Ok(Some(context.db.load_block_proof(&handle, is_link)?));
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

                if let Some(full_block) = context.load_full_block(&next_block_id)? {
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
}
