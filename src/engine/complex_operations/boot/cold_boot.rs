use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use arc_swap::ArcSwapOption;
use broxus_util::now;
use everscale_types::models::*;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesOrdered;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::engine::complex_operations::download_state::*;
use crate::engine::{Engine, FullStateId};
use crate::network::Neighbour;
use crate::storage::*;
use crate::utils::*;

/// Boot type when the node has not yet started syncing
///
/// Returns last masterchain key block id
pub async fn cold_boot(engine: &Arc<Engine>) -> Result<BlockId> {
    tracing::info!("starting cold boot");

    // Find the last key block (or zerostate) from which we can start downloading other key blocks
    let prev_key_block = prepare_prev_key_block(engine).await?;

    // Ensure that all key blocks until now (with some offset) are downloaded
    download_key_blocks(engine, prev_key_block).await?;

    // Choose the latest key block with persistent state
    let last_key_block = choose_key_block(engine)?;

    if last_key_block.id().seqno == 0 {
        // If the last suitable key block is zerostate, we must download all other zerostates
        let zero_state = engine.load_mc_zero_state().await?;
        download_workchain_zero_state(engine, &zero_state, 0).await?;
    } else {
        // If the last suitable key block is not zerostate, we must download all blocks
        // with their states from shards for that
        download_start_blocks_and_states(engine, last_key_block.id()).await?;
    };

    tracing::info!("finished cold boot");
    Ok(last_key_block.id().clone())
}

/// Searches for the last key block (or zerostate) from which
/// we can start downloading other key blocks
async fn prepare_prev_key_block(engine: &Arc<Engine>) -> Result<PrevKeyBlock> {
    let block_handle_storage = engine.storage.block_handle_storage();
    let block_storage = engine.storage.block_storage();

    let block_id = &engine.init_mc_block_id;

    if block_id.seqno == 0 {
        // Download zerostate when init block id has not yet been changed
        tracing::info!(block_id = %block_id.as_short_id(), "using zero state");
        let (handle, state) = engine.download_zero_state(block_id).await?;
        Ok(PrevKeyBlock::ZeroState { handle, state })
    } else {
        // Ensure that block proof is downloaded for the last known key block
        tracing::info!(block_id = %block_id.as_short_id(), "using key block");

        let handle = match block_handle_storage.load_handle(block_id)? {
            // Check whether block proof is already downloaded
            Some(handle) if handle.meta().has_proof() => {
                if !handle.is_key_block() {
                    return Err(ColdBootError::StartingFromNonKeyBlock.into());
                }

                let proof = block_storage.load_block_proof(&handle, false).await?;
                return Ok(PrevKeyBlock::KeyBlock {
                    handle,
                    proof: Box::new(proof),
                });
            }
            handle => handle,
        };

        // Find previous key block (or zerostate). It is needed for proof verification
        let prev_key_block = block_handle_storage
            .find_prev_key_block(block_id.seqno)?
            .context("Previous key block not found")?;
        let prev_key_block = if prev_key_block.id().seqno == 0 {
            // Previous key block is zerostate
            PrevKeyBlock::ZeroState {
                handle: prev_key_block,
                state: engine.load_mc_zero_state().await?,
            }
        } else {
            // Previous key block is also a key block so it must have proof
            let proof = block_storage
                .load_block_proof(&prev_key_block, false)
                .await
                .context("Failed to found prev key block proof")?;
            PrevKeyBlock::KeyBlock {
                handle: prev_key_block,
                proof: Box::new(proof),
            }
        };

        // Download and save block proof
        let (handle, proof) = loop {
            let proof = engine
                .download_block_proof(block_id, true, None, None)
                .await?;

            match prev_key_block.check_next_proof(engine, &proof) {
                Ok(info) => {
                    let handle = match handle {
                        Some(handle) => handle.into(),
                        None => info.with_mc_seqno(block_id.seqno).into(),
                    };

                    let handle = block_storage
                        .store_block_proof(&proof, handle)
                        .await?
                        .handle;

                    break (handle, proof);
                }
                Err(e) => {
                    tracing::warn!("got invalid block proof for init block: {e:?}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        };

        if !handle.is_key_block() {
            return Err(ColdBootError::StartingFromNonKeyBlock.into());
        }

        Ok(PrevKeyBlock::KeyBlock {
            handle,
            proof: Box::new(proof.data),
        })
    }
}

/// Downloads and saves all key blocks until now
async fn download_key_blocks(engine: &Arc<Engine>, mut prev_key_block: PrevKeyBlock) -> Result<()> {
    const BLOCKS_PER_BATCH: u16 = 5;

    // Create parallel task for downloading key blocks
    let (tasks_tx, mut tasks_rx) = mpsc::unbounded_channel::<BlockId>();
    let (ids_tx, mut ids_rx) = mpsc::unbounded_channel();

    let signal = CancellationToken::new();
    let _guard = signal.clone().drop_guard();

    let good_peer = Arc::new(ArcSwapOption::empty());
    tokio::spawn({
        let mc_client = engine.masterchain_client.clone();
        let good_peer = good_peer.clone();
        async move {
            tokio::pin! { let dropped = signal.cancelled(); }

            while let Some(block_id) = tasks_rx.recv().await {
                'inner: loop {
                    tracing::debug!(
                        block_id = %block_id.as_short_id(),
                        "downloading next key blocks"
                    );

                    let neighbour = good_peer.load_full();
                    let res = tokio::select! {
                        // Download next key blocks ids
                        res = mc_client.download_next_key_blocks_ids(
                            &block_id,
                            BLOCKS_PER_BATCH,
                            neighbour.as_ref()
                        ) => res,
                        // Return in case of main task cancellation
                        _ = &mut dropped => return,
                    };

                    match res {
                        // Send result back to the main task
                        Ok((ids, neighbour)) => {
                            good_peer.store(Some(neighbour.clone()));
                            if ids_tx.send((ids, neighbour)).is_err() {
                                return;
                            }
                            break 'inner;
                        }
                        // Reset good_peer in case of error and retry request
                        Err(e) => {
                            tracing::warn!(
                                block_id = %block_id.as_short_id(),
                                "failed to download key block ids: {e:?}"
                            );
                            good_peer.store(None);
                        }
                    };
                }
            }
        }
    });

    let sync_start_utime = prev_key_block.handle().meta().gen_utime();
    let mut pg = ProgressBarBuilder::new("downloading key blocks")
        .total(now().checked_sub(sync_start_utime).unwrap_or(1))
        .build();

    // Continue downloading key blocks from the last known block
    let mut prev_handle = prev_key_block.handle().clone();
    tasks_tx.send(prev_handle.id().clone()).ok();

    let node_state = engine.storage.node_state();
    while let Some((mut ids, neighbour)) = ids_rx.recv().await {
        // Id for zerostate as a key block is a trap
        ids.retain(|id| id.seqno > 0);

        match ids.last() {
            // Start downloading next key blocks in background
            Some(block_id) => {
                tracing::debug!(last_key_block_id = %block_id.as_short_id());
                tasks_tx.send(block_id.clone()).ok();
            }
            // Allow empty response for syncing from zerostate
            None if prev_handle.id().seqno == 0
                && !is_persistent_state(now(), sync_start_utime) =>
            {
                tracing::debug!("starting from zerostate");
            }
            // Retry request in case of empty response
            None => {
                // Reset good peer, because empty ids is suspicious
                good_peer.store(None);
                tasks_tx.send(prev_handle.id().clone()).ok();
                continue;
            }
        };

        // Download key block proofs in parallel
        // TODO: Invalidate queue (tasks_*) in case of bad block proof
        let mut stream = BlockProofStream::new(engine, &mut prev_key_block, &ids, &neighbour);

        // Process each key block sequentially
        while let Some((handle, proof)) = stream.next().await? {
            // Update init mc block id to the next persistent key block
            let block_utime = handle.meta().gen_utime();
            let prev_utime = prev_handle.meta().gen_utime();
            if is_persistent_state(block_utime, prev_utime) {
                node_state.store_init_mc_block_id(handle.id())?;
            }

            // Update stream context
            prev_handle = handle.clone();
            *stream.prev_key_block = PrevKeyBlock::KeyBlock {
                handle,
                proof: Box::new(proof),
            };
        }

        let last_utime = prev_handle.meta().gen_utime();
        let current_utime = now();

        pg.set_progress(last_utime.saturating_sub(sync_start_utime));

        tracing::debug!(
            last_known_block_id = %prev_handle.id().as_short_id(),
            last_utime,
            current_utime,
        );

        // Prevent infinite key blocks loading
        if last_utime + 2 * KEY_BLOCK_UTIME_STEP > current_utime {
            break;
        }
    }

    pg.complete();
    Ok(())
}

/// Parallel block proof downloader
struct BlockProofStream<'a> {
    engine: &'a Engine,
    prev_key_block: &'a mut PrevKeyBlock,
    ids: &'a [BlockId],
    neighbour: &'a Arc<Neighbour>,
    futures: FuturesOrdered<BoxFuture<'a, (usize, Result<BlockProofStuffAug>)>>,
    index: usize,
}

impl<'a> BlockProofStream<'a> {
    fn new(
        engine: &'a Engine,
        prev_key_block: &'a mut PrevKeyBlock,
        ids: &'a [BlockId],
        neighbour: &'a Arc<Neighbour>,
    ) -> Self {
        Self {
            engine,
            prev_key_block,
            ids,
            neighbour,
            futures: Default::default(),
            index: 0,
        }
    }

    /// Waits next block proof
    async fn next(&mut self) -> Result<Option<(Arc<BlockHandle>, BlockProofStuff)>> {
        // Check ids range end
        let block_id = match self.ids.get(self.index) {
            Some(block_id) => block_id.clone(),
            None => return Ok(None),
        };

        let block_handle_storage = self.engine.storage.block_handle_storage();
        let block_storage = self.engine.storage.block_storage();

        // Check whether block proof is already stored locally
        if let Some(handle) = block_handle_storage.load_handle(&block_id)? {
            if let Ok(proof) = block_storage.load_block_proof(&handle, false).await {
                // Move index forward
                self.index += 1;
                return Ok(Some((handle, proof)));
            }
        }

        loop {
            // Wait for the next resolved future
            let proof = match self.futures.next().await {
                // Skip block proofs which we don't need anymore
                Some((index, _)) if index < self.index => {
                    continue;
                }
                // Next block proof found
                Some((_, proof)) => proof?,
                // No more futures found. Probably unreachable
                None if self.index > self.ids.len() => return Ok(None),
                // Initial state when futures queue is empty
                None => {
                    // Fill tasks queue
                    self.restart();
                    continue;
                }
            };

            // Verify block proof
            match self.prev_key_block.check_next_proof(self.engine, &proof) {
                Ok(info) => {
                    // Save block proof
                    let handle = block_storage
                        .store_block_proof(&proof, info.with_mc_seqno(block_id.seqno).into())
                        .await?
                        .handle;

                    // Move index forward
                    self.index += 1;
                    return Ok(Some((handle, proof.data)));
                }
                Err(e) => {
                    // Refill tasks queue
                    self.restart();

                    tracing::warn!("got invalid key block proof: {e:?}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    /// Fills the stream with new futures
    fn restart(&mut self) {
        // NOTE: `.enumerate()` must be before `.skip()` because the first element index
        // must be the same as `self.index` and so on
        let ids = self.ids.iter().enumerate().skip(self.index);
        self.futures = ids
            .map(|(index, id)| {
                self.engine
                    .download_block_proof(id, true, None, Some(self.neighbour))
                    .map(move |result| (index, result))
                    .boxed()
            })
            .collect();
    }
}

/// Selectes the latest suitable key block with persistent state
fn choose_key_block(engine: &Engine) -> Result<Arc<BlockHandle>> {
    let block_handle_storage = engine.storage.block_handle_storage();
    let mut key_blocks = block_handle_storage
        .key_blocks_iterator(KeyBlocksDirection::Backward)
        .map(|item| {
            item.and_then(|block_id| {
                block_handle_storage
                    .load_handle(&block_id)?
                    .context("Key block handle not found")
            })
        })
        .peekable();

    // Iterate all key blocks in reverse order (from the latest to the oldest)
    while let Some(handle) = key_blocks.next().transpose()? {
        let handle_utime = handle.meta().gen_utime();
        let prev_utime = match key_blocks.peek() {
            Some(Ok(prev_block)) => prev_block.meta().gen_utime(),
            Some(Err(e)) => {
                tracing::warn!("failed to load previous key block: {e:?}");
                return Err(ColdBootError::FailedToLoadKeyBlock.into());
            }
            None => 0,
        };

        let is_persistent = prev_utime == 0 || is_persistent_state(handle_utime, prev_utime);
        tracing::debug!(
            seqno = handle.id().seqno,
            is_persistent,
            "new key block candidate",
        );

        // Skip not persistent or too new key blocks
        if !is_persistent {
            tracing::debug!("ignoring state: not persistent");
            continue;
        } else if handle_utime + INTITAL_SYNC_TIME_SECONDS > now() {
            tracing::debug!("ignoring state: too new");
            continue;
        }

        // Use first suitable key block
        tracing::info!(block_id = %handle.id().as_short_id(), "found best key block handle");
        return Ok(handle);
    }

    Err(ColdBootError::PersistentShardStateNotFound.into())
}

enum PrevKeyBlock {
    ZeroState {
        handle: Arc<BlockHandle>,
        state: Arc<ShardStateStuff>,
    },
    KeyBlock {
        handle: Arc<BlockHandle>,
        proof: Box<BlockProofStuff>,
    },
}

impl PrevKeyBlock {
    fn handle(&self) -> &Arc<BlockHandle> {
        match self {
            Self::ZeroState { handle, .. } => handle,
            Self::KeyBlock { handle, .. } => handle,
        }
    }

    fn check_next_proof(
        &self,
        engine: &Engine,
        next_proof: &BlockProofStuff,
    ) -> Result<BriefBlockInfo> {
        let block_id = next_proof.id();

        let (virt_block, virt_block_info) = next_proof
            .pre_check_block_proof()
            .context("Failed to pre check block proof")?;
        let res = BriefBlockInfo::from(&virt_block_info);

        match self {
            // Check block proof with zero state
            PrevKeyBlock::ZeroState { state, .. } => {
                check_with_master_state(next_proof, state, &virt_block, &virt_block_info)
            }
            // Check block proof with previous key block
            PrevKeyBlock::KeyBlock { proof, .. } => {
                check_with_prev_key_block_proof(next_proof, proof, &virt_block, &virt_block_info)
            }
        }
        .or_else(|e| {
            // Allow invalid proofs for hard forks
            if engine.is_hard_fork(block_id) {
                tracing::warn!(
                    block_id = %block_id.as_short_id(),
                    "received hard fork key block, ignoring proof"
                );
                Ok(())
            } else {
                Err(e)
            }
        })
        .map(move |_| res)
    }
}

async fn download_workchain_zero_state(
    engine: &Arc<Engine>,
    mc_zero_state: &ShardStateStuff,
    workchain: i32,
) -> Result<()> {
    // Get workchain description
    let workchains = mc_zero_state.config_params()?.get_workchains()?;
    let base_workchain = workchains
        .get(&workchain)?
        .ok_or(ColdBootError::BaseWorkchainInfoNotFound)?;

    // Download and save zerostate
    engine
        .download_zero_state(&BlockId {
            shard: ShardIdent::new_full(workchain),
            seqno: 0,
            root_hash: base_workchain.zerostate_root_hash,
            file_hash: base_workchain.zerostate_file_hash,
        })
        .await?;

    Ok(())
}

async fn download_start_blocks_and_states(
    engine: &Arc<Engine>,
    mc_block_id: &BlockId,
) -> Result<TopBlocks> {
    // Download and save masterchain block and state
    let (_, init_mc_block) = download_block_with_state(
        engine,
        FullStateId {
            mc_block_id: mc_block_id.clone(),
            block_id: mc_block_id.clone(),
        },
    )
    .await?;

    let top_blocks = TopBlocks::from_mc_block(&init_mc_block)?;

    tracing::info!(
        block_id = %init_mc_block.id().as_short_id(),
        "downloaded init mc block state"
    );

    // Download and save blocks and states from other shards
    for (_, block_id) in init_mc_block.shard_blocks()? {
        if block_id.seqno == 0 {
            engine.download_zero_state(&block_id).await?;
        } else {
            download_block_with_state(
                engine,
                FullStateId {
                    mc_block_id: mc_block_id.clone(),
                    block_id,
                },
            )
            .await?;
        };
    }

    Ok(top_blocks)
}

async fn download_block_with_state(
    engine: &Arc<Engine>,
    full_state_id: FullStateId,
) -> Result<(Arc<BlockHandle>, BlockStuff)> {
    let block_handle_storage = engine.storage.block_handle_storage();
    let block_storage = engine.storage.block_storage();

    let mc_seqno = full_state_id.mc_block_id.seqno;

    let handle = block_handle_storage
        .load_handle(&full_state_id.block_id)?
        .filter(|handle| handle.meta().has_data());

    // Download block data and proof
    let (block, handle) = match handle {
        Some(handle) => (block_storage.load_block_data(&handle).await?, handle),
        None => {
            let (block, proof, meta_data) = loop {
                let (block, proof) = engine.download_block(&full_state_id.block_id, None).await?;
                match proof.pre_check_block_proof() {
                    Ok((_, block_info)) => {
                        let meta_data = BriefBlockInfo::from(&block_info).with_mc_seqno(mc_seqno);
                        break (block, proof, meta_data);
                    }
                    Err(e) => {
                        tracing::error!("received invalid block: {e:?}");
                        continue;
                    }
                }
            };

            tracing::info!(
                block_id = %full_state_id.block_id.as_short_id(),
                "downloaded block data"
            );

            let mut handle = block_storage
                .store_block_data(&block, meta_data)
                .await?
                .handle;
            if !handle.meta().has_proof() {
                handle = block_storage
                    .store_block_proof(&proof, handle.into())
                    .await?
                    .handle;
            }
            (block.data, handle)
        }
    };

    // Download block state
    if !handle.meta().has_state() {
        let state_update = block.block().load_state_update()?;

        tracing::info!(block_id = %handle.id().as_short_id(), "downloading state");
        let shard_state = download_state(engine, full_state_id).await?;
        tracing::info!(block_id = %handle.id().as_short_id(), "downloaded state");

        let state_hash = shard_state.root_cell().repr_hash();
        if &state_update.new_hash != state_hash {
            return Err(ColdBootError::ShardStateHashMismatch.into());
        }

        engine.store_state(&handle, &shard_state).await?;
        engine
            .notify_subscribers_with_full_state(&shard_state)
            .await?;
    }

    engine.set_applied(&handle, mc_seqno).await?;
    Ok((handle, block))
}

const KEY_BLOCK_UTIME_STEP: u32 = 86400;
const INTITAL_SYNC_TIME_SECONDS: u32 = 300;

#[derive(thiserror::Error, Debug)]
enum ColdBootError {
    #[error("Starting from non-key block")]
    StartingFromNonKeyBlock,
    #[error("Failed to load key block")]
    FailedToLoadKeyBlock,
    #[error("Base workchain info not found")]
    BaseWorkchainInfoNotFound,
    #[error("Downloaded shard state hash mismatch")]
    ShardStateHashMismatch,
    #[error("Persistent shard state not found")]
    PersistentShardStateNotFound,
}
