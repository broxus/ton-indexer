use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tiny_adnl::utils::*;
use ton_api::ton;

pub use rocksdb::perf::MemoryUsageStats;

use crate::config::*;
use crate::network::*;
use crate::storage::*;
use crate::utils::*;

use self::complex_operations::*;
use self::db::*;
use self::downloader::*;
use self::node_state::*;
use self::states_gc_resolver::*;

pub mod complex_operations;
mod db;
mod downloader;
mod node_state;
pub mod rpc_operations;
mod states_gc_resolver;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum EngineStatus {
    Booted,
    Synced,
}

#[async_trait::async_trait]
pub trait Subscriber: Send + Sync {
    async fn engine_status_changed(&self, status: EngineStatus) {
        let _unused_by_default = status;
    }

    async fn process_block(
        &self,
        block: &BlockStuff,
        block_proof: Option<&BlockProofStuff>,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        let _unused_by_default = block;
        let _unused_by_default = block_proof;
        let _unused_by_default = shard_state;
        Ok(())
    }
}

pub struct Engine {
    is_working: AtomicBool,
    db: Arc<Db>,
    states_gc_resolver: Arc<DefaultStateGcResolver>,
    subscribers: Vec<Arc<dyn Subscriber>>,
    network: Arc<NodeNetwork>,
    old_blocks_policy: OldBlocksPolicy,
    init_mc_block_id: ton_block::BlockIdExt,
    last_known_mc_block_seqno: AtomicU32,
    last_known_key_block_seqno: AtomicU32,

    parallel_archive_downloads: u32,
    shard_state_cache_enabled: bool,
    shard_states_cache: ShardStateCache,

    shard_states_operations: OperationsPool<ton_block::BlockIdExt, Arc<ShardStateStuff>>,
    block_applying_operations: OperationsPool<ton_block::BlockIdExt, ()>,
    next_block_applying_operations: OperationsPool<ton_block::BlockIdExt, ton_block::BlockIdExt>,
    download_block_operations: OperationsPool<ton_block::BlockIdExt, (BlockStuff, BlockProofStuff)>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Engine {
    pub async fn new(
        config: NodeConfig,
        global_config: GlobalConfig,
        subscribers: Vec<Arc<dyn Subscriber>>,
    ) -> Result<Arc<Self>> {
        let old_blocks_policy = config.old_blocks_policy;
        let shard_state_cache_enabled = config.shard_state_cache_enabled;
        let db = Db::new(
            &config.rocks_db_path,
            &config.file_db_path,
            config.max_db_memory_usage,
        )
        .await?;
        // let gced = db
        //     .garbage_collect(GcType::KeepNotOlderThen(86400))
        //     .await
        //     .unwrap();
        // log::info!("Gced {}", gced);
        // std::process::exit(0);
        let zero_state_id = global_config.zero_state.clone();

        let mut init_mc_block_id = zero_state_id.clone();
        if let Ok(block_id) = InitMcBlockId::load_from_db(db.as_ref()) {
            if block_id.0.seqno > init_mc_block_id.seq_no as i32 {
                init_mc_block_id = convert_block_id_ext_api2blk(&block_id.0)?;
            }
        }

        let network = NodeNetwork::new(
            config.ip_address.into(),
            config.adnl_keys.build_keystore()?,
            config.adnl_options,
            config.rldp_options,
            config.dht_options,
            config.neighbours_options,
            config.overlay_shard_options,
            global_config,
        )
        .await?;
        network.start().await?;
        log::info!("Network started");

        let states_gc_resolver = Arc::new(DefaultStateGcResolver::default());
        db.start_states_gc(
            states_gc_resolver.clone(),
            Duration::from_secs(config.state_gc_interval_ms),
        );

        let engine = Arc::new(Self {
            is_working: AtomicBool::new(true),
            db,
            states_gc_resolver,
            subscribers,
            network,
            old_blocks_policy,
            init_mc_block_id,
            last_known_mc_block_seqno: AtomicU32::new(0),
            last_known_key_block_seqno: AtomicU32::new(0),
            parallel_archive_downloads: config.parallel_archive_downloads,
            shard_state_cache_enabled,
            shard_states_cache: ShardStateCache::new(120),
            shard_states_operations: OperationsPool::new("shard_states_operations"),
            block_applying_operations: OperationsPool::new("block_applying_operations"),
            next_block_applying_operations: OperationsPool::new("next_block_applying_operations"),
            download_block_operations: OperationsPool::new("download_block_operations"),
        });

        engine
            .get_full_node_overlay(ton_block::BASE_WORKCHAIN_ID, ton_block::SHARD_FULL)
            .await?;
        log::info!("Overlay connected");
        Ok(engine)
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        // Start full node overlay service
        let service = FullNodeOverlayService::new(self);

        let (_, masterchain_overlay_id) = self
            .network
            .compute_overlay_id(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL)?;
        self.network
            .add_subscriber(masterchain_overlay_id, service.clone());

        let (_, basechain_overlay_id) = self
            .network
            .compute_overlay_id(ton_block::BASE_WORKCHAIN_ID, ton_block::SHARD_FULL)?;
        self.network.add_subscriber(basechain_overlay_id, service);
        // Boot
        let BootData {
            last_mc_block_id,
            shards_client_mc_block_id,
        } = boot(self).await?;
        log::info!(
            "Initialized (last block: {}, shards client block id: {})",
            last_mc_block_id,
            shards_client_mc_block_id
        );

        self.notify_subscribers_with_status(EngineStatus::Booted)
            .await;

        // Start listening broadcasts
        self.listen_broadcasts(ton_block::ShardIdent::masterchain())
            .await?;
        self.listen_broadcasts(ton_block::ShardIdent::with_tagged_prefix(
            ton_block::BASE_WORKCHAIN_ID,
            ton_block::SHARD_FULL,
        )?)
        .await?;

        match self.old_blocks_policy {
            OldBlocksPolicy::Ignore => { /* do nothing */ }
            OldBlocksPolicy::Sync { from_seqno } => {
                background_sync(self, from_seqno).await?;
            }
        }
        // Synchronize
        if !self.is_synced()? {
            sync(self).await?;
        }
        log::info!("Synced!");

        self.notify_subscribers_with_status(EngineStatus::Synced)
            .await;

        let last_mc_block_id = self.load_last_applied_mc_block_id()?;
        let shards_client_mc_block_id = self.load_shards_client_mc_block_id()?;

        // Start walking through the blocks
        tokio::spawn({
            let engine = self.clone();
            async move {
                if let Err(e) = walk_masterchain_blocks(&engine, last_mc_block_id).await {
                    log::error!(
                        "FATAL ERROR while walking though masterchain blocks: {:?}",
                        e
                    );
                }
            }
        });

        tokio::spawn({
            let engine = self.clone();
            async move {
                if let Err(e) = walk_shard_blocks(&engine, shards_client_mc_block_id).await {
                    log::error!("FATAL ERROR while walking though shard blocks: {:?}", e);
                }
            }
        });

        // Engine started
        Ok(())
    }

    /// Initiates shutdown
    pub fn shutdown(&self) {
        self.is_working.store(false, Ordering::Release);
        self.network.shutdown();
    }

    pub fn is_working(&self) -> bool {
        self.is_working.load(Ordering::Acquire)
    }

    pub fn get_memory_usage_stats(&self) -> Result<RocksdbStats> {
        self.db.get_memory_usage_stats()
    }

    pub async fn broadcast_external_message(
        &self,
        to: &ton_block::AccountIdPrefixFull,
        data: &[u8],
    ) -> Result<()> {
        let overlay = self
            .get_full_node_overlay(to.workchain_id, to.prefix)
            .await?;
        overlay.broadcast_external_message(data).await
    }

    pub fn init_mc_block_id(&self) -> &ton_block::BlockIdExt {
        &self.init_mc_block_id
    }

    fn set_init_mc_block_id(&self, block_id: &ton_block::BlockIdExt) {
        InitMcBlockId(convert_block_id_ext_blk2api(block_id))
            .store_into_db(self.db.as_ref())
            .ok();
    }

    fn update_last_known_mc_block_seqno(&self, seqno: u32) -> bool {
        self.last_known_mc_block_seqno
            .fetch_max(seqno, Ordering::SeqCst)
            < seqno
    }

    fn update_last_known_key_block_seqno(&self, seqno: u32) -> bool {
        self.last_known_key_block_seqno
            .fetch_max(seqno, Ordering::SeqCst)
            < seqno
    }

    async fn get_masterchain_overlay(&self) -> Result<Arc<dyn FullNodeOverlayClient>> {
        self.get_full_node_overlay(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL)
            .await
    }

    async fn get_full_node_overlay(
        &self,
        workchain: i32,
        shard: u64,
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {
        let (full_id, short_id) = self.network.compute_overlay_id(workchain, shard)?;
        self.network.get_overlay(full_id, short_id).await
    }

    async fn listen_broadcasts(self: &Arc<Self>, shard_ident: ton_block::ShardIdent) -> Result<()> {
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

    async fn download_zerostate(
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

    async fn download_next_masterchain_block(
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

    async fn download_block(
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

    async fn download_block_proof(
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

    async fn download_next_key_blocks_ids(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Vec<ton_block::BlockIdExt>> {
        let mc_overlay = self.get_masterchain_overlay().await?;
        mc_overlay.download_next_key_blocks_ids(block_id, 5).await
    }

    async fn download_archive(
        &self,
        mc_block_seq_no: u32,
        active_peers: &Arc<ActivePeers>,
    ) -> Result<Option<Vec<u8>>> {
        let mc_overlay = self.get_masterchain_overlay().await?;
        mc_overlay
            .download_archive(mc_block_seq_no, active_peers)
            .await
    }

    fn load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>> {
        self.db.load_block_handle(block_id)
    }

    fn find_block_by_seq_no(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        seq_no: u32,
    ) -> Result<Arc<BlockHandle>> {
        self.db.find_block_by_seq_no(account_prefix, seq_no)
    }

    #[allow(unused)]
    fn find_block_by_utime(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        utime: u32,
    ) -> Result<Arc<BlockHandle>> {
        self.db.find_block_by_utime(account_prefix, utime)
    }

    #[allow(unused)]
    fn find_block_by_lt(
        &self,
        account_prefix: &ton_block::AccountIdPrefixFull,
        lt: u64,
    ) -> Result<Arc<BlockHandle>> {
        self.db.find_block_by_lt(account_prefix, lt)
    }

    async fn wait_next_applied_mc_block(
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

    async fn wait_applied_block(
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
                return self.load_state(block_id).await;
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

    pub async fn load_state(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Arc<ShardStateStuff>> {
        if self.shard_state_cache_enabled {
            if let Some(state) = self.shard_states_cache.get(block_id) {
                return Ok(state);
            }
        }

        let state = Arc::new(self.db.load_shard_state(block_id).await?);

        if self.shard_state_cache_enabled {
            self.shard_states_cache
                .set(block_id, |_| Some(state.clone()));
        }

        Ok(state)
    }

    async fn store_state(
        &self,
        handle: &Arc<BlockHandle>,
        state: &Arc<ShardStateStuff>,
    ) -> Result<()> {
        if self.shard_state_cache_enabled {
            self.shard_states_cache.set(handle.id(), |existing| {
                existing.is_none().then(|| state.clone())
            });
        }

        self.db.store_shard_state(handle, state.as_ref()).await?;

        self.shard_states_operations
            .do_or_wait(state.block_id(), None, futures::future::ok(state.clone()))
            .await?;

        Ok(())
    }

    async fn load_block_data(&self, handle: &Arc<BlockHandle>) -> Result<BlockStuff> {
        self.db.load_block_data(handle.as_ref()).await
    }

    async fn store_block_data(&self, block: &BlockStuff) -> Result<StoreBlockResult> {
        let store_block_result = self.db.store_block_data(block).await?;
        if store_block_result.handle.id().shard().is_masterchain() {
            if store_block_result.handle.meta().is_key_block() {
                self.update_last_known_key_block_seqno(store_block_result.handle.id().seq_no);
            }
            self.update_last_known_mc_block_seqno(store_block_result.handle.id().seq_no);
        }
        Ok(store_block_result)
    }

    async fn load_block_proof(
        &self,
        handle: &Arc<BlockHandle>,
        is_link: bool,
    ) -> Result<BlockProofStuff> {
        self.db.load_block_proof(handle, is_link).await
    }

    async fn store_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        handle: Option<Arc<BlockHandle>>,
        proof: &BlockProofStuff,
    ) -> Result<Arc<BlockHandle>> {
        Ok(self
            .db
            .store_block_proof(block_id, handle, proof)
            .await?
            .handle)
    }

    async fn store_zerostate(
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

    async fn load_prev_key_block(&self, block_id: u32) -> Result<ton_block::BlockIdExt> {
        let current_block = self.load_last_applied_mc_block_id()?;
        let current_shard_state = self.load_state(&current_block).await?;
        let prev_blocks = &current_shard_state.shard_state_extra()?.prev_blocks;

        let possible_ref_blk = prev_blocks
            .get_prev_key_block(block_id)?
            .context("No keyblock found")?;

        let ref_blk = match possible_ref_blk.seq_no {
            seqno if seqno < block_id => possible_ref_blk,
            seqno if seqno == block_id => prev_blocks
                .get_prev_key_block(seqno)?
                .context("No keyblock found")?,
            _ => anyhow::bail!("Failed to find previous key block"),
        };

        let prev_key_block = ton_block::BlockIdExt {
            shard_id: ton_block::ShardIdent::masterchain(),
            seq_no: ref_blk.seq_no,
            root_hash: ref_blk.root_hash,
            file_hash: ref_blk.file_hash,
        };
        Ok(prev_key_block)
    }

    pub fn is_synced(&self) -> Result<bool> {
        let shards_client_mc_block_id = self.load_shards_client_mc_block_id()?;
        let last_applied_mc_block_id = self.load_last_applied_mc_block_id()?;
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

    fn set_applied(&self, handle: &Arc<BlockHandle>, mc_seq_no: u32) -> Result<bool> {
        if handle.meta().is_applied() {
            return Ok(false);
        }
        self.db.assign_mc_ref_seq_no(handle, mc_seq_no)?;
        self.db.index_handle(handle)?;
        self.db.store_block_applied(handle)
    }

    pub fn load_last_applied_mc_block_id(&self) -> Result<ton_block::BlockIdExt> {
        convert_block_id_ext_api2blk(&LastMcBlockId::load_from_db(self.db.as_ref())?.0)
    }

    fn store_last_applied_mc_block_id(&self, block_id: &ton_block::BlockIdExt) -> Result<()> {
        LastMcBlockId(convert_block_id_ext_blk2api(block_id)).store_into_db(self.db.as_ref())
    }

    pub fn load_shards_client_mc_block_id(&self) -> Result<ton_block::BlockIdExt> {
        convert_block_id_ext_api2blk(&ShardsClientMcBlockId::load_from_db(self.db.as_ref())?.0)
    }

    fn store_shards_client_mc_block_id(&self, block_id: &ton_block::BlockIdExt) -> Result<()> {
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

            log::trace!("Start downloading block {} for apply", block_id);

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

                log::trace!("Downloaded block {} for apply", block_id);

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
        while !(pre_apply && handle.meta().has_data() || handle.meta().is_applied()) {
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

    async fn notify_subscribers_with_status(&self, status: EngineStatus) {
        for subscriber in &self.subscribers {
            subscriber.engine_status_changed(status).await;
        }
    }

    async fn notify_subscribers_with_block(
        &self,
        handle: &Arc<BlockHandle>,
        block: &BlockStuff,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        if self.subscribers.is_empty() {
            return Ok(());
        }

        if handle.id().shard().is_masterchain() {
            let block_proof = self.load_block_proof(handle, false).await?;
            for subscriber in &self.subscribers {
                subscriber
                    .process_block(block, Some(&block_proof), shard_state)
                    .await?;
            }
        } else {
            for subscriber in &self.subscribers {
                subscriber.process_block(block, None, shard_state).await?;
            }
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
                block_proof,
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

    pub async fn gc(&self, gc_type: BlocksGcType) -> Result<usize> {
        self.db.garbage_collect(gc_type).await
    }
}

#[derive(Debug)]
pub enum BlocksGcType {
    KeepLastNBlocks(u32),
    KeepNotOlderThen(u32),
}

#[derive(thiserror::Error, Debug)]
enum EngineError {
    #[error("Downloading next block is only allowed for masterchain")]
    NonMasterchainNextBlock,
    #[error("Failed to load handle for last masterchain block")]
    FailedToLoadLastMasterchainBlockHandle,
    #[error("Too deep recursion")]
    TooDeepRecursion,
}
