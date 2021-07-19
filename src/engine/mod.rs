use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use anyhow::Result;
use dashmap::DashSet;
use tiny_adnl::utils::*;
use ton_api::ton;

use crate::config::*;
use crate::network::*;
use crate::storage::*;
use crate::utils::*;

use self::complex_operations::*;
use self::db::*;
use self::downloader::*;
use self::node_state::*;

pub mod complex_operations;
mod db;
mod downloader;
mod node_state;

pub struct Engine {
    db: Arc<dyn Db>,
    network: Arc<NodeNetwork>,

    init_mc_block_id: ton_block::BlockIdExt,
    last_known_mc_block_seqno: AtomicU32,
    last_known_key_block_seqno: AtomicU32,

    shard_states_cache: ShardStateCache,

    shard_states_operations: OperationsPool<ton_block::BlockIdExt, Arc<ShardStateStuff>>,
    block_applying_operations: OperationsPool<ton_block::BlockIdExt, ()>,
    next_block_applying_operations: OperationsPool<ton_block::BlockIdExt, ton_block::BlockIdExt>,
    download_block_operations: OperationsPool<ton_block::BlockIdExt, (BlockStuff, BlockProofStuff)>,
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
            init_mc_block_id,
            last_known_mc_block_seqno: AtomicU32::new(0),
            last_known_key_block_seqno: AtomicU32::new(0),
            shard_states_cache: ShardStateCache::new(120),
            shard_states_operations: OperationsPool::new("shard_states_operations"),
            block_applying_operations: OperationsPool::new("block_applying_operations"),
            next_block_applying_operations: OperationsPool::new("next_block_applying_operations"),
            download_block_operations: OperationsPool::new("download_block_operations"),
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
                    Ok((ton::ton_node::Broadcast::TonNode_BlockBroadcast(block), _)) => {
                        let engine = engine.clone();
                        tokio::spawn(async move {
                            if let Err(e) = process_block_broadcast(&engine, *block).await {
                                log::error!("Failed to process block broadcast: {:?}", e);
                            }
                        });
                    }
                    Err(e) => {
                        log::error!("Failed to wait broadcast for shard {}: {}", shard_ident, e);
                    }
                    _ => { /* do nothing */ }
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
    ) -> Result<(BlockStuff, BlockProofStuff)> {
        loop {
            if let Some(handle) = self.load_block_handle(block_id)? {
                if handle.meta().has_data() {
                    let block = self.load_block_data(&handle).await?;
                    let block_proof = self
                        .load_block_proof(&handle, !block_id.shard().is_masterchain())
                        .await?;
                    return Ok((block, block_proof));
                }
            }

            if let Some(data) = self
                .download_block_operations
                .do_or_wait(
                    block_id,
                    None,
                    self.download_block_worker(block_id, max_attempts, None),
                )
                .await?
            {
                return Ok(data);
            }
        }
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

    pub async fn download_archive(
        &self,
        mc_block_seq_no: u32,
        active_peers: &Arc<DashSet<AdnlNodeIdShort>>,
    ) -> Result<Option<Vec<u8>>> {
        let mc_overlay = self.get_masterchain_overlay().await?;
        mc_overlay
            .download_archive(mc_block_seq_no, active_peers)
            .await
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

    #[allow(unused)]
    pub fn find_block_by_utime(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        utime: u32,
    ) -> Result<Arc<BlockHandle>> {
        self.db.find_block_by_utime(account_prefix, utime)
    }

    #[allow(unused)]
    pub fn find_block_by_lt(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        lt: u64,
    ) -> Result<Arc<BlockHandle>> {
        self.db.find_block_by_lt(account_prefix, lt)
    }

    pub async fn wait_next_applied_mc_block(
        &self,
        prev_handle: &Arc<BlockHandle>,
        timeout_ms: Option<u64>,
    ) -> Result<(Arc<BlockHandle>, BlockStuff)> {
        if !prev_handle.id().shard().is_masterchain() {
            return Err(EngineError::NonMasterchainNextBlock.into());
        }

        loop {
            if prev_handle.meta().has_next1() {
                let next1_id = self
                    .db
                    .load_block_connection(prev_handle.id(), BlockConnection::Next1)?;
                return self.wait_applied_block(&next1_id, timeout_ms).await;
            } else if let Some(next1_id) = self
                .next_block_applying_operations
                .wait(prev_handle.id(), timeout_ms, || {
                    Ok(prev_handle.meta().has_next1())
                })
                .await?
            {
                if let Some(handle) = self.load_block_handle(&next1_id)? {
                    let block = self.load_block_data(&handle).await?;
                    return Ok((handle, block));
                }
            }
        }
    }

    pub async fn wait_applied_block(
        &self,
        block_id: &ton_block::BlockIdExt,
        timeout_ms: Option<u64>,
    ) -> Result<(Arc<BlockHandle>, BlockStuff)> {
        loop {
            if let Some(handle) = self.load_block_handle(block_id)? {
                if handle.meta().is_applied() {
                    let block = self.load_block_data(&handle).await?;
                    return Ok((handle, block));
                }
            }

            self.block_applying_operations
                .wait(block_id, timeout_ms, || {
                    Ok(self
                        .load_block_handle(block_id)?
                        .map(|handle| handle.meta().is_applied())
                        .unwrap_or_default())
                })
                .await?;
        }
    }

    pub async fn wait_state(
        self: &Arc<Self>,
        block_id: &ton_block::BlockIdExt,
        timeout_ms: Option<u64>,
        allow_block_downloading: bool,
    ) -> Result<Arc<ShardStateStuff>> {
        loop {
            let has_state = || {
                Ok(self
                    .load_block_handle(block_id)?
                    .map(|handle| handle.meta().has_state())
                    .unwrap_or_default())
            };

            if has_state()? {
                return self.load_state(block_id);
            }

            if allow_block_downloading {
                let engine = self.clone();
                let block_id = block_id.clone();
                tokio::spawn(async move {
                    if let Err(e) = engine.download_and_apply_block(&block_id, 0, true, 0).await {
                        log::error!(
                            "Error while pre-apply block {} (while waiting state): {}",
                            block_id,
                            e
                        );
                    }
                });
            }

            if let Some(shard_state) = self
                .shard_states_operations
                .wait(block_id, timeout_ms, &has_state)
                .await?
            {
                return Ok(shard_state);
            }
        }
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

    pub async fn check_sync(&self) -> Result<bool> {
        let shards_client_mc_block_id = self.load_shards_client_mc_block_id().await?;
        let last_applied_mc_block_id = self.load_last_applied_mc_block_id().await?;
        if shards_client_mc_block_id.seq_no + MAX_BLOCK_APPLIER_DEPTH
            < last_applied_mc_block_id.seq_no
        {
            return Ok(false);
        }

        let last_mc_block_handle = self
            .load_block_handle(&last_applied_mc_block_id)?
            .ok_or_else(|| {
                log::info!("Handle not found");
                EngineError::FailedToLoadLastMasterchainBlockHandle
            })?;

        if last_mc_block_handle.meta().gen_utime() + 600 > now() as u32 {
            return Ok(true);
        }

        let last_key_block_seqno = self.last_known_key_block_seqno.load(Ordering::Acquire);
        if last_key_block_seqno > last_applied_mc_block_id.seq_no {
            return Ok(false);
        }

        Ok(false)
    }

    pub async fn set_applied(&self, handle: &Arc<BlockHandle>, mc_seq_no: u32) -> Result<bool> {
        if handle.meta().is_applied() {
            return Ok(false);
        }
        self.db.assign_mc_ref_seq_no(handle, mc_seq_no)?;
        self.db.index_handle(handle)?;
        self.db.store_block_applied(handle)
    }

    async fn load_last_applied_mc_block_id(&self) -> Result<ton_block::BlockIdExt> {
        convert_block_id_ext_api2blk(&LastMcBlockId::load_from_db(self.db.as_ref())?.0)
    }

    async fn store_last_applied_mc_block_id(&self, block_id: &ton_block::BlockIdExt) -> Result<()> {
        LastMcBlockId(convert_block_id_ext_blk2api(block_id)).store_into_db(self.db.as_ref())
    }

    async fn load_shards_client_mc_block_id(&self) -> Result<ton_block::BlockIdExt> {
        convert_block_id_ext_api2blk(&ShardsClientMcBlockId::load_from_db(self.db.as_ref())?.0)
    }

    async fn store_shards_client_mc_block_id(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<()> {
        ShardsClientMcBlockId(convert_block_id_ext_blk2api(block_id))
            .store_into_db(self.db.as_ref())
    }

    async fn download_and_apply_block(
        self: &Arc<Self>,
        block_id: &ton_block::BlockIdExt,
        mc_seq_no: u32,
        pre_apply: bool,
        depth: u32,
    ) -> Result<()> {
        if depth > MAX_BLOCK_APPLIER_DEPTH {
            return Err(EngineError::TooDeepRecursion.into());
        }

        loop {
            if let Some(handle) = self.load_block_handle(block_id)? {
                if handle.meta().is_applied() || pre_apply && handle.meta().has_state() {
                    return Ok(());
                }

                if handle.meta().has_data() {
                    while !(pre_apply && handle.meta().has_state() || handle.meta().is_applied()) {
                        let operation = async {
                            let block = self.load_block_data(&handle).await?;
                            apply_block(self, &handle, &block, mc_seq_no, pre_apply, depth).await?;
                            Ok(())
                        };

                        self.block_applying_operations
                            .do_or_wait(handle.id(), None, operation)
                            .await?;
                    }
                    return Ok(());
                }
            }

            log::info!("Start downloading block {} for apply", block_id);

            let (max_attempts, timeouts) = if pre_apply {
                (
                    Some(10),
                    Some(DownloaderTimeouts {
                        initial: 50,
                        max: 500,
                        multiplier: 1.5,
                    }),
                )
            } else {
                (None, None)
            };

            if let Some((block, block_proof)) = self
                .download_block_operations
                .do_or_wait(
                    block_id,
                    None,
                    self.download_block_worker(block_id, max_attempts, timeouts),
                )
                .await?
            {
                if self.load_block_handle(block_id)?.is_some() {
                    continue;
                }

                self.check_block_proof(&block_proof).await?;
                let handle = self.store_block_data(&block).await?.handle;
                let handle = self
                    .store_block_proof(block_id, Some(handle), &block_proof)
                    .await?;

                log::info!("Downloaded block {} for apply", block_id);

                self.apply_block_ext(&handle, &block, mc_seq_no, pre_apply, 0)
                    .await?;
                return Ok(());
            }
        }
    }

    async fn apply_block_ext(
        self: &Arc<Self>,
        handle: &Arc<BlockHandle>,
        block: &BlockStuff,
        mc_seq_no: u32,
        pre_apply: bool,
        depth: u32,
    ) -> Result<()> {
        while !((pre_apply && handle.meta().has_data()) || handle.meta().is_applied()) {
            self.block_applying_operations
                .do_or_wait(
                    handle.id(),
                    None,
                    apply_block(self, handle, block, mc_seq_no, pre_apply, depth),
                )
                .await?;
        }
        Ok(())
    }

    async fn check_block_proof(&self, block_proof: &BlockProofStuff) -> Result<()> {
        if block_proof.is_link() {
            block_proof.check_proof_link()?;
        } else {
            let (virt_block, virt_block_info) = block_proof.pre_check_block_proof()?;
            let prev_key_block_seqno = virt_block_info.prev_key_block_seqno();

            let masterchain_prefix = ton_block::AccountIdPrefixFull::any_masterchain();
            let handle = self
                .db
                .find_block_by_seq_no(&masterchain_prefix, prev_key_block_seqno)?;
            let prev_key_block_proof = self.load_block_proof(&handle, false).await?;

            check_with_prev_key_block_proof(
                &block_proof,
                &prev_key_block_proof,
                &virt_block,
                &virt_block_info,
            )?;
        }
        Ok(())
    }

    async fn download_block_worker(
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

#[derive(thiserror::Error, Debug)]
enum EngineError {
    #[error("Downloading next block is only allowed for masterchain")]
    NonMasterchainNextBlock,
    #[error("Failed to load handle for last masterchain block")]
    FailedToLoadLastMasterchainBlockHandle,
    #[error("Too deep recusion")]
    TooDeepRecursion,
}
