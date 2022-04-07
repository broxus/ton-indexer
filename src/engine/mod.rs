/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - removed validator stuff
/// - slightly changed application of blocks
///
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
pub use rocksdb::perf::MemoryUsageStats;
use tiny_adnl::utils::*;
use tiny_adnl::{NeighboursMetrics, OverlayShardMetrics};
use ton_api::ton;

pub use db::{DbMetrics, RocksdbStats};
use global_config::GlobalConfig;

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
pub mod rpc_operations;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum EngineStatus {
    Booted,
    Synced,
}

pub struct Engine {
    is_working: AtomicBool,
    db: Arc<Db>,
    states_gc_options: Option<StateGcOptions>,
    blocks_gc_state: Option<BlocksGcState>,
    subscribers: Vec<Arc<dyn Subscriber>>,
    network: Arc<NodeNetwork>,
    old_blocks_policy: OldBlocksPolicy,
    archives_enabled: bool,
    zero_state_id: ton_block::BlockIdExt,
    init_mc_block_id: ton_block::BlockIdExt,
    last_known_key_block_seqno: AtomicU32,
    hard_forks: FxHashSet<ton_block::BlockIdExt>,

    sync_options: SyncOptions,

    shard_states_operations: ShardStatesOperationsPool,
    block_applying_operations: BlockApplyingOperationsPool,
    next_block_applying_operations: NextBlockApplyingOperationsPool,
    download_block_operations: DownloadBlockOperationsPool,
    last_blocks: BlockCaches,

    metrics: Arc<EngineMetrics>,
}

type ShardStatesOperationsPool = OperationsPool<ton_block::BlockIdExt, Arc<ShardStateStuff>>;
type BlockApplyingOperationsPool = OperationsPool<ton_block::BlockIdExt, ()>;
type NextBlockApplyingOperationsPool = OperationsPool<ton_block::BlockIdExt, ton_block::BlockIdExt>;
type DownloadBlockOperationsPool =
    OperationsPool<ton_block::BlockIdExt, (BlockStuffAug, BlockProofStuffAug)>;

struct BlockCaches {
    pub last_mc: LastMcBlockId,
    pub init_block: InitMcBlockId,
    pub shard_client_mc_block: ShardsClientMcBlockId,
}

struct BlocksGcState {
    ty: BlocksGcKind,
    max_blocks_per_batch: Option<usize>,
    enabled: AtomicBool,
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
        let old_blocks_policy = config.sync_options.old_blocks_policy;
        let db = Db::new(
            &config.rocks_db_path,
            &config.file_db_path,
            config.max_db_memory_usage,
        )
        .await
        .context("Failed to create DB")?;

        let zero_state_id = global_config.zero_state.clone();

        let mut init_mc_block_id = zero_state_id.clone();
        let init_block =
            InitMcBlockId::new(db.raw()).context("Failed to get init masterchain block")?;
        if let Ok(block_id) = init_block.load_from_db() {
            if block_id.seqno > init_mc_block_id.seq_no as i32 {
                init_mc_block_id = convert_block_id_ext_api2blk(&block_id)?;
            }
        }

        let hard_forks = global_config.hard_forks.clone().into_iter().collect();

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
        .await
        .context("Failed to init network")?;
        network.start().await.context("Failed to start network")?;

        log::info!("Network started");

        let engine = Arc::new(Self {
            is_working: AtomicBool::new(true),
            db: db.clone(),
            states_gc_options: config.state_gc_options,
            blocks_gc_state: config.blocks_gc_options.map(|options| BlocksGcState {
                ty: options.kind,
                max_blocks_per_batch: options.max_blocks_per_batch,
                enabled: AtomicBool::new(options.enable_for_sync),
            }),
            subscribers,
            network,
            old_blocks_policy,
            archives_enabled: config.archives_enabled,
            zero_state_id,
            init_mc_block_id,
            last_known_key_block_seqno: AtomicU32::new(0),
            hard_forks,
            sync_options: config.sync_options,
            shard_states_operations: OperationsPool::new("shard_states_operations"),
            block_applying_operations: OperationsPool::new("block_applying_operations"),
            next_block_applying_operations: OperationsPool::new("next_block_applying_operations"),
            download_block_operations: OperationsPool::new("download_block_operations"),
            last_blocks: BlockCaches {
                last_mc: LastMcBlockId::new(db.raw())?,
                init_block,
                shard_client_mc_block: ShardsClientMcBlockId::new(db.raw())?,
            },
            metrics: Arc::new(Default::default()),
        });

        engine
            .get_full_node_overlay(ton_block::BASE_WORKCHAIN_ID, ton_block::SHARD_FULL)
            .await
            .context("Failed to create base workchain overlay node")?;

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

        if let Some(blocks_gc) = &self.blocks_gc_state {
            blocks_gc.enabled.store(true, Ordering::Release);

            let handle = self.db.find_last_key_block()?;
            self.db
                .remove_outdated_blocks(handle.id(), blocks_gc.max_blocks_per_batch, blocks_gc.ty)
                .await?;
        }

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

        if let Some(options) = &self.states_gc_options {
            let engine = Arc::downgrade(self);
            let offset = Duration::from_secs(options.offset_sec);
            let interval = Duration::from_secs(options.interval_sec);

            tokio::spawn(async move {
                tokio::time::sleep(offset).await;
                loop {
                    tokio::time::sleep(interval).await;

                    let engine = match engine.upgrade() {
                        Some(engine) => engine,
                        None => return,
                    };

                    let block_id = match engine.load_shards_client_mc_block_id() {
                        Ok(block_id) => block_id,
                        Err(e) => {
                            log::error!("Failed to load last shards client block: {:?}", e);
                            continue;
                        }
                    };

                    if let Err(e) = engine.db.remove_outdated_states(&block_id).await {
                        log::error!("Failed to GC state: {:?}", e);
                    }
                }
            });
        };

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

    pub fn get_db_metrics(&self) -> DbMetrics {
        self.db.metrics()
    }

    pub fn get_memory_usage_stats(&self) -> Result<RocksdbStats> {
        self.db.get_memory_usage_stats()
    }

    pub fn metrics(&self) -> &Arc<EngineMetrics> {
        &self.metrics
    }

    pub fn internal_metrics(&self) -> InternalEngineMetrics {
        InternalEngineMetrics {
            shard_states_operations_len: self.shard_states_operations.len(),
            block_applying_operations_len: self.block_applying_operations.len(),
            next_block_applying_operations_len: self.next_block_applying_operations.len(),
            download_block_operations_len: self.download_block_operations.len(),
        }
    }

    pub fn network_metrics(&self) -> NetworkMetrics {
        self.network.metrics()
    }

    pub fn network_neighbour_metrics(
        &self,
    ) -> impl Iterator<Item = (OverlayIdShort, NeighboursMetrics)> + '_ {
        self.network.neighbour_metrics()
    }

    pub fn network_overlay_metrics(
        &self,
    ) -> impl Iterator<Item = (OverlayIdShort, OverlayShardMetrics)> + '_ {
        self.network.overlay_metrics()
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
        self.last_blocks
            .init_block
            .store_into_db(convert_block_id_ext_blk2api(block_id))
            .ok();
    }

    fn is_hard_fork(&self, block_id: &ton_block::BlockIdExt) -> bool {
        self.hard_forks.contains(block_id)
    }

    fn update_last_known_key_block_seqno(&self, seqno: u32) -> bool {
        self.last_known_key_block_seqno
            .fetch_max(seqno, Ordering::SeqCst)
            < seqno
    }

    async fn get_masterchain_overlay(&self) -> Result<FullNodeOverlayClient> {
        self.get_full_node_overlay(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL)
            .await
    }

    async fn get_full_node_overlay(
        &self,
        workchain: i32,
        shard: u64,
    ) -> Result<FullNodeOverlayClient> {
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
    ) -> Result<(BlockStuffAug, BlockProofStuffAug)> {
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
    ) -> Result<(BlockStuffAug, BlockProofStuffAug)> {
        loop {
            if let Some(handle) = self.load_block_handle(block_id)? {
                if handle.meta().has_data() {
                    let block = self.load_block_data(&handle).await?;
                    let block_proof = self
                        .load_block_proof(&handle, !block_id.shard().is_masterchain())
                        .await?;

                    return Ok((
                        WithArchiveData::loaded(block),
                        WithArchiveData::loaded(block_proof),
                    ));
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
    ) -> Result<BlockProofStuffAug> {
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
        output: &mut (dyn Write + Send),
    ) -> Result<ArchiveDownloadStatus> {
        let mc_overlay = self.get_masterchain_overlay().await?;
        mc_overlay.download_archive(mc_block_seq_no, output).await
    }

    pub(crate) fn load_block_handle(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<BlockHandle>>> {
        self.db.load_block_handle(block_id)
    }

    pub async fn load_last_key_block(&self) -> Result<BlockStuff> {
        let handle = self
            .db
            .find_last_key_block()
            .context("Failed to find last key block")?;
        self.db
            .load_block_data(&handle)
            .await
            .context("Failed to load key block data")
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

    pub async fn load_mc_zero_state(&self) -> Result<Arc<ShardStateStuff>> {
        self.load_state(&self.zero_state_id).await
    }

    pub async fn load_state(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Arc<ShardStateStuff>> {
        let state = Arc::new(self.db.load_shard_state(block_id).await?);
        Ok(state)
    }

    async fn store_state(
        &self,
        handle: &Arc<BlockHandle>,
        state: &Arc<ShardStateStuff>,
    ) -> Result<()> {
        self.db.store_shard_state(handle, state.as_ref()).await?;

        self.shard_states_operations
            .do_or_wait(state.block_id(), None, futures::future::ok(state.clone()))
            .await?;

        Ok(())
    }

    async fn load_block_data(&self, handle: &Arc<BlockHandle>) -> Result<BlockStuff> {
        self.db.load_block_data(handle.as_ref()).await
    }

    async fn store_block_data(&self, block: &BlockStuffAug) -> Result<StoreBlockResult> {
        let store_block_result = self.db.store_block_data(block).await?;

        let handle = &store_block_result.handle;
        if handle.id().shard().is_masterchain() && handle.is_key_block() {
            self.update_last_known_key_block_seqno(store_block_result.handle.id().seq_no);
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
        proof: &BlockProofStuffAug,
    ) -> Result<StoreBlockResult> {
        self.db.store_block_proof(block_id, handle, proof).await
    }

    async fn store_zerostate(
        &self,
        block_id: &ton_block::BlockIdExt,
        state: &Arc<ShardStateStuff>,
    ) -> Result<Arc<BlockHandle>> {
        let (handle, _) =
            self.db
                .create_or_load_block_handle(block_id, None, Some(state.state().gen_time()))?;
        self.store_state(&handle, state).await?;
        Ok(handle)
    }

    #[allow(unused)]
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

    async fn set_applied(&self, handle: &Arc<BlockHandle>, mc_seq_no: u32) -> Result<bool> {
        if handle.meta().is_applied() {
            return Ok(false);
        }
        self.db.assign_mc_ref_seq_no(handle, mc_seq_no)?;

        if self.archives_enabled {
            self.db.archive_block(handle).await?;
        }

        let applied = self.db.store_block_applied(handle)?;

        if handle.is_key_block() {
            if let Some(blocks_gc) = &self.blocks_gc_state {
                if blocks_gc.enabled.load(Ordering::Acquire) {
                    self.db
                        .remove_outdated_blocks(
                            handle.id(),
                            blocks_gc.max_blocks_per_batch,
                            blocks_gc.ty,
                        )
                        .await?
                }
            }
        }

        Ok(applied)
    }

    pub fn load_last_applied_mc_block_id(&self) -> Result<ton_block::BlockIdExt> {
        convert_block_id_ext_api2blk(&self.last_blocks.last_mc.load_from_db()?)
    }

    fn store_last_applied_mc_block_id(&self, block_id: &ton_block::BlockIdExt) -> Result<()> {
        self.last_blocks
            .last_mc
            .store_into_db(convert_block_id_ext_blk2api(block_id))?;
        self.metrics
            .last_mc_block_seqno
            .store(block_id.seq_no, Ordering::Release);
        Ok(())
    }

    pub fn load_shards_client_mc_block_id(&self) -> Result<ton_block::BlockIdExt> {
        convert_block_id_ext_api2blk(&self.last_blocks.shard_client_mc_block.load_from_db()?)
    }

    fn store_shards_client_mc_block_id(&self, block_id: &ton_block::BlockIdExt) -> Result<()> {
        self.last_blocks
            .shard_client_mc_block
            .store_into_db(convert_block_id_ext_blk2api(block_id))?;
        self.metrics
            .last_shard_client_mc_block_seqno
            .store(block_id.seq_no, Ordering::Release);
        Ok(())
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
                // Block is already applied
                if handle.meta().is_applied() || pre_apply && handle.meta().has_state() {
                    return Ok(());
                }

                let mut possibly_updated = false;

                // Check if block was already downloaded
                if handle.meta().has_data() {
                    // While it is still not applied
                    while !(pre_apply && handle.meta().has_state() || handle.meta().is_applied()) {
                        possibly_updated = true;

                        // Apply block
                        let operation = async {
                            let block = self.load_block_data(&handle).await?;
                            apply_block(self, &handle, &block, mc_seq_no, pre_apply, depth).await?;
                            Ok(())
                        };
                        match self
                            .block_applying_operations
                            .do_or_wait(handle.id(), None, operation)
                            .await
                        {
                            // Successfully applied but continue to the next iteration
                            // just to be sure
                            Ok(_) => continue,
                            // For some reason, the data disappeared, so proceed to the downloader
                            Err(_) if !handle.meta().has_data() => break,
                            // Return on other errors
                            Err(e) => return Err(e),
                        }
                    }
                }

                // Block is applied and has data
                if possibly_updated && handle.meta().has_data() {
                    return Ok(());
                }
            }

            log::trace!("Start downloading block {} for apply", block_id);

            // Prepare params
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

            // Download next block
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
                    .await?
                    .handle;

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

        let meta = handle.meta().brief();
        let time_diff = now() as i64 - meta.gen_utime() as i64;

        let ctx = ProcessBlockContext {
            engine: self,
            meta,
            handle,
            block,
            shard_state: Some(shard_state),
        };

        if handle.id().shard().is_masterchain() {
            self.metrics
                .mc_time_diff
                .store(time_diff, Ordering::Release);
            self.metrics
                .last_mc_utime
                .store(meta.gen_utime(), Ordering::Release);

            for subscriber in &self.subscribers {
                subscriber.process_block(ctx).await?;
            }
        } else {
            self.metrics
                .shard_client_time_diff
                .store(time_diff, Ordering::Release);

            for subscriber in &self.subscribers {
                subscriber.process_block(ctx).await?;
            }
        }

        Ok(())
    }

    async fn notify_subscribers_with_archive_block(
        &self,
        handle: &Arc<BlockHandle>,
        block: &BlockStuff,
    ) -> Result<()> {
        let meta = handle.meta().brief();

        let ctx = ProcessBlockContext {
            engine: self,
            meta,
            handle,
            block,
            shard_state: None,
        };

        if handle.id().shard().is_masterchain() {
            for subscriber in &self.subscribers {
                subscriber.process_block(ctx).await?;
            }
        } else {
            for subscriber in &self.subscribers {
                subscriber.process_block(ctx).await?;
            }
        }

        Ok(())
    }

    async fn notify_subscribers_with_full_state(&self, state: &ShardStateStuff) -> Result<()> {
        for subscriber in &self.subscribers {
            subscriber.process_full_state(state).await?;
        }
        Ok(())
    }

    async fn check_block_proof(&self, block_proof: &BlockProofStuff) -> Result<()> {
        if block_proof.is_link() {
            block_proof.check_proof_link()?;
        } else {
            let (virt_block, virt_block_info) = block_proof.pre_check_block_proof()?;
            let prev_key_block_seqno = virt_block_info.prev_key_block_seqno();

            let handle = self.db.load_key_block_handle(prev_key_block_seqno)?;
            let prev_key_block_proof = self.load_block_proof(&handle, false).await?;

            if let Err(e) = check_with_prev_key_block_proof(
                block_proof,
                &prev_key_block_proof,
                &virt_block,
                &virt_block_info,
            ) {
                if !self.is_hard_fork(handle.id()) {
                    return Err(e);
                }
                log::warn!("Received hard fork block {}. Ignoring proof", handle.id());
            }
        }
        Ok(())
    }

    async fn download_block_worker(
        &self,
        block_id: &ton_block::BlockIdExt,
        max_attempts: Option<u32>,
        timeouts: Option<DownloaderTimeouts>,
    ) -> Result<(BlockStuffAug, BlockProofStuffAug)> {
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

#[async_trait::async_trait]
pub trait Subscriber: Send + Sync {
    async fn engine_status_changed(&self, status: EngineStatus) {
        let _unused_by_default = status;
    }

    async fn process_block(&self, ctx: ProcessBlockContext<'_>) -> Result<()> {
        let _unused_by_default = ctx;
        Ok(())
    }

    async fn process_full_state(&self, state: &ShardStateStuff) -> Result<()> {
        let _unused_by_default = state;
        Ok(())
    }
}

#[derive(Copy, Clone)]
pub struct ProcessBlockContext<'a> {
    engine: &'a Engine,
    meta: BriefBlockMeta,
    handle: &'a Arc<BlockHandle>,
    block: &'a BlockStuff,
    shard_state: Option<&'a ShardStateStuff>,
}

impl ProcessBlockContext<'_> {
    #[inline(always)]
    pub fn id(&self) -> &ton_block::BlockIdExt {
        self.handle.id()
    }

    #[inline(always)]
    pub fn meta(&self) -> BriefBlockMeta {
        self.meta
    }

    #[inline(always)]
    pub fn block(&self) -> &ton_block::Block {
        self.block.block()
    }

    #[inline(always)]
    pub fn block_stuff(&self) -> &BlockStuff {
        self.block
    }

    #[inline(always)]
    pub fn shard_state(&self) -> Option<&ton_block::ShardStateUnsplit> {
        self.shard_state.map(ShardStateStuff::state)
    }

    #[inline(always)]
    pub fn shard_state_stuff(&self) -> Option<&ShardStateStuff> {
        self.shard_state
    }

    #[inline(always)]
    pub fn is_masterchain(&self) -> bool {
        self.handle.id().shard_id.workchain_id() == ton_block::MASTERCHAIN_ID
    }

    #[inline(always)]
    pub fn is_from_archive(&self) -> bool {
        self.shard_state.is_none()
    }

    pub async fn load_block_data(&self) -> Result<Vec<u8>> {
        self.engine.db.load_block_data_raw(self.handle).await
    }

    pub async fn load_block_proof(&self) -> Result<BlockProofStuff> {
        self.engine
            .db
            .load_block_proof(self.handle, !self.is_masterchain())
            .await
    }

    pub async fn load_block_proof_data(&self) -> Result<Vec<u8>> {
        self.engine
            .db
            .load_block_proof_raw(self.handle, !self.is_masterchain())
            .await
    }
}

#[derive(Debug, Default)]
pub struct EngineMetrics {
    pub last_mc_block_seqno: AtomicU32,
    pub last_shard_client_mc_block_seqno: AtomicU32,
    pub last_mc_utime: AtomicU32,
    pub mc_time_diff: AtomicI64,
    pub shard_client_time_diff: AtomicI64,
}

#[derive(Debug, Default)]
pub struct InternalEngineMetrics {
    pub shard_states_operations_len: usize,
    pub block_applying_operations_len: usize,
    pub next_block_applying_operations_len: usize,
    pub download_block_operations_len: usize,
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
