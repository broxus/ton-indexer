/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - simplified boot
///
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use futures::{FutureExt, StreamExt};
use tiny_adnl::utils::*;
use tokio::sync::mpsc;

use crate::db::*;
use crate::engine::Engine;
use crate::utils::*;

use super::download_state::*;

#[derive(Debug, Clone)]
pub struct BootData {
    pub last_mc_block_id: ton_block::BlockIdExt,
    pub shards_client_mc_block_id: ton_block::BlockIdExt,
}

pub async fn boot(engine: &Arc<Engine>) -> Result<BootData> {
    log::info!("Starting boot");

    let last_mc_block_id = match engine.load_last_applied_mc_block_id() {
        Ok(block_id) => warm_boot(engine, block_id).await?,
        Err(e) => {
            log::warn!("Failed to load last masterchain block id: {e:?}");
            let last_mc_block_id = cold_boot(engine).await?;

            engine.store_last_applied_mc_block_id(&last_mc_block_id)?;

            engine
                .db
                .node_state()
                .store_background_sync_end(&last_mc_block_id)?;

            last_mc_block_id
        }
    };

    let shards_client_mc_block_id = match engine.load_shards_client_mc_block_id() {
        Ok(block_id) => block_id,
        Err(_) => {
            engine.store_shards_client_mc_block_id(&last_mc_block_id)?;
            last_mc_block_id.clone()
        }
    };

    Ok(BootData {
        last_mc_block_id,
        shards_client_mc_block_id,
    })
}

async fn cold_boot(engine: &Arc<Engine>) -> Result<ton_block::BlockIdExt> {
    log::info!("Starting cold boot");
    let boot_data = prepare_cold_boot_data(engine).await?;
    let zero_state = match &boot_data {
        ColdBootData::ZeroState { state, .. } => Some(state.clone()),
        ColdBootData::KeyBlock { .. } => None,
    };
    let key_blocks = get_key_blocks(engine, boot_data).await?;
    let last_key_block = choose_key_block(key_blocks)?;

    let block_id = last_key_block.id();
    match (block_id.seq_no, zero_state) {
        (0, Some(zero_state)) => download_base_wc_zero_state(engine, &zero_state).await?,
        _ => download_start_blocks_and_states(engine, block_id).await?,
    }

    log::info!("Cold boot finished");
    Ok(block_id.clone())
}

async fn warm_boot(
    engine: &Arc<Engine>,
    mut last_mc_block_id: ton_block::BlockIdExt,
) -> Result<ton_block::BlockIdExt> {
    log::info!("Starting warm boot");
    let block_handle_storage = engine.db.block_handle_storage();
    let handle = block_handle_storage
        .load_handle(&last_mc_block_id)?
        .ok_or(BootError::FailedToLoadInitialBlock)?;

    let state = engine.load_state(&last_mc_block_id).await?;
    if last_mc_block_id.seq_no != 0 && !handle.meta().is_key_block() {
        log::info!("Started from non-key block");

        last_mc_block_id = state
            .shard_state_extra()?
            .last_key_block
            .clone()
            .ok_or(BootError::MasterchainStateNotFound)?
            .master_block_id()
            .1;

        log::info!("LAST KEY BLOCK: {last_mc_block_id}");
    }
    log::info!("Warm boot finished");
    Ok(last_mc_block_id)
}

async fn prepare_cold_boot_data(engine: &Arc<Engine>) -> Result<ColdBootData> {
    let block_handle_storage = engine.db.block_handle_storage();
    let block_storage = engine.db.block_storage();

    let block_id = engine.init_mc_block_id();
    log::info!("Cold boot from {block_id}");

    if block_id.seq_no == 0 {
        log::info!("Using zero state");
        let (handle, state) = download_zero_state(engine, block_id).await?;
        Ok(ColdBootData::ZeroState { handle, state })
    } else {
        log::info!("Using key block");
        let handle = match block_handle_storage.load_handle(block_id)? {
            Some(handle) => {
                if handle.meta().has_proof_link() || handle.meta().has_proof() {
                    let proof = match block_storage.load_block_proof(&handle, true).await {
                        Ok(proof) => proof,
                        Err(e) => {
                            log::warn!("Failed to load block proof as link: {e}");
                            block_storage.load_block_proof(&handle, false).await?
                        }
                    };

                    if !handle.is_key_block() {
                        return Err(BootError::StartingFromNonKeyBlock.into());
                    }

                    return Ok(ColdBootData::KeyBlock {
                        handle,
                        proof: Box::new(proof),
                    });
                }
                Some(handle)
            }
            None => None,
        };

        let (handle, proof) = loop {
            match engine
                .download_block_proof(block_id, true, true, None, None)
                .await
            {
                Ok(proof) => match proof.check_proof_link() {
                    Ok(_) => {
                        let handle = block_storage
                            .store_block_proof(block_id, handle, &proof)
                            .await?
                            .handle;
                        break (handle, proof);
                    }
                    Err(e) => {
                        log::warn!("Got invalid block proof for init block: {e:?}");
                    }
                },
                Err(e) => {
                    log::warn!("Failed to download block proof for init block: {e:?}");
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        if !handle.is_key_block() {
            return Err(BootError::StartingFromNonKeyBlock.into());
        }

        Ok(ColdBootData::KeyBlock {
            handle,
            proof: Box::new(proof.data),
        })
    }
}

async fn get_key_blocks(
    engine: &Arc<Engine>,
    mut boot_data: ColdBootData,
) -> Result<Vec<Arc<BlockHandle>>> {
    const BLOCKS_PER_BATCH: u16 = 5;

    let mut handle = boot_data.init_block_handle().clone();

    let mut result = vec![handle.clone()];

    let (tasks_tx, mut tasks_rx) = mpsc::unbounded_channel();
    let (ids_tx, mut ids_rx) = mpsc::unbounded_channel();

    let (_guard, signal) = trigger_on_drop();

    let mc_overlay = engine.get_masterchain_overlay().await?;
    tokio::spawn(async move {
        let mut good_peer = None;

        tokio::pin! { let dropped = signal.cancelled(); }

        while let Some(block_id) = tasks_rx.recv().await {
            'inner: loop {
                log::info!("Downloading next key blocks for: {block_id}");

                let res = tokio::select! {
                    res = mc_overlay.download_next_key_blocks_ids(
                        &block_id,
                        BLOCKS_PER_BATCH,
                        good_peer.as_ref()
                    ) => res,
                    _ = &mut dropped => return,
                };

                match res {
                    Ok((ids, neighbour)) => {
                        good_peer = Some(neighbour.clone());
                        if ids_tx.send((ids, neighbour)).is_err() {
                            return;
                        }
                        break 'inner;
                    }
                    Err(e) => {
                        log::warn!("Failed to download next key block ids for {block_id}: {e:?}");
                        good_peer = None;
                    }
                };
            }
        }
    });

    tasks_tx.send(handle.id().clone()).ok();

    while let Some((ids, neighbour)) = ids_rx.recv().await {
        match ids.last() {
            Some(block_id) => {
                log::info!("Last key block id: {block_id}");
                tasks_tx.send(block_id.clone()).ok();
            }
            None => {
                tasks_tx.send(handle.id().clone()).ok();
                continue;
            }
        };

        // TODO: Invalidate queue (tasks_*) in case of bad block proof
        let mut stream = BlockProofStream::new(engine, &mut boot_data, &ids, &neighbour);
        while let Some((next_handle, proof)) = stream.next().await? {
            let prev_utime = handle.meta().gen_utime();
            if is_persistent_state(next_handle.meta().gen_utime(), prev_utime) {
                engine.set_init_mc_block_id(next_handle.id())?;
            }

            handle = next_handle;
            result.push(handle.clone());
            *stream.boot_data = ColdBootData::KeyBlock {
                handle: handle.clone(),
                proof: Box::new(proof),
            };
        }

        let last_utime = handle.meta().gen_utime() as i32;
        let current_utime = now();

        log::info!(
            "Last known block: {}, utime: {last_utime}, now: {current_utime}",
            handle.id(),
        );

        if last_utime + INTITAL_SYNC_TIME_SECONDS > current_utime
            || last_utime + 2 * KEY_BLOCK_UTIME_STEP > current_utime
        {
            break;
        }
    }

    Ok(result)
}

struct BlockProofStream<'a> {
    engine: &'a Engine,
    boot_data: &'a mut ColdBootData,
    ids: &'a [ton_block::BlockIdExt],
    neighbour: &'a Arc<tiny_adnl::Neighbour>,
    futures: FuturesOrdered<BoxFuture<'a, (usize, Result<BlockProofStuffAug>)>>,
    index: usize,
}

impl<'a> BlockProofStream<'a> {
    fn new(
        engine: &'a Engine,
        boot_data: &'a mut ColdBootData,
        ids: &'a [ton_block::BlockIdExt],
        neighbour: &'a Arc<tiny_adnl::Neighbour>,
    ) -> Self {
        Self {
            engine,
            boot_data,
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

        let block_handle_storage = self.engine.db.block_handle_storage();
        let block_storage = self.engine.db.block_storage();

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
                Some((index, _)) if index < self.index => continue,
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

            let result = match self.boot_data {
                // Check block proof with previous key block
                ColdBootData::KeyBlock {
                    proof: prev_proof, ..
                } => proof
                    .check_with_prev_key_block_proof(prev_proof)
                    .or_else(|e| {
                        if self.engine.is_hard_fork(&block_id) {
                            log::warn!("Received hard fork key block {block_id}. Ignoring proof");
                            Ok(())
                        } else {
                            Err(e)
                        }
                    }),
                // Check block proof with zero state
                ColdBootData::ZeroState { state, .. } => proof.check_with_master_state(state),
            };

            match result {
                Ok(_) => {
                    // Save block proof
                    let handle = block_storage
                        .store_block_proof(&block_id, None, &proof)
                        .await?
                        .handle;

                    // Move index forward
                    self.index += 1;
                    return Ok(Some((handle, proof.data)));
                }
                Err(e) => {
                    // Refill tasks queue
                    self.restart();

                    log::warn!("Got invalid key block proof: {e:?}");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    /// Fills the stream with new futures
    fn restart(&mut self) {
        let ids = self.ids.iter().skip(self.index).enumerate();
        self.futures = ids
            .map(|(index, id)| {
                self.engine
                    .download_block_proof(id, false, true, None, Some(self.neighbour))
                    .map(move |result| (index, result))
                    .boxed()
            })
            .collect();
    }
}

fn choose_key_block(mut key_blocks: Vec<Arc<BlockHandle>>) -> Result<Arc<BlockHandle>> {
    while let Some(handle) = key_blocks.pop() {
        let handle_utime = handle.meta().gen_utime();
        let prev_utime = match key_blocks.last() {
            Some(prev_block) => prev_block.meta().gen_utime(),
            None => 0,
        };

        let is_persistent = prev_utime == 0 || is_persistent_state(handle_utime, prev_utime);
        log::info!(
            "Key block candidate: seqno={}, persistent={}",
            handle.id().seq_no,
            is_persistent
        );

        if !is_persistent || handle_utime as i32 + INTITAL_SYNC_TIME_SECONDS > now() {
            log::info!("Ignoring state: too new");
            continue;
        }

        log::info!("Best key block handle is {}", handle.id());
        return Ok(handle);
    }

    Err(BootError::PersistentShardStateNotFound.into())
}

enum ColdBootData {
    ZeroState {
        handle: Arc<BlockHandle>,
        state: Arc<ShardStateStuff>,
    },
    KeyBlock {
        handle: Arc<BlockHandle>,
        proof: Box<BlockProofStuff>,
    },
}

impl ColdBootData {
    fn init_block_handle(&self) -> &Arc<BlockHandle> {
        match self {
            Self::ZeroState { handle, .. } => handle,
            Self::KeyBlock { handle, .. } => handle,
        }
    }
}

async fn download_base_wc_zero_state(
    engine: &Arc<Engine>,
    zero_state: &ShardStateStuff,
) -> Result<()> {
    let workchains = zero_state.config_params()?.workchains()?;
    let base_workchain = workchains
        .get(&0)?
        .ok_or(BootError::BaseWorkchainInfoNotFound)?;

    log::info!(
        "Base workchain zerostate: {}",
        base_workchain.zerostate_root_hash.to_hex_string()
    );

    download_zero_state(
        engine,
        &ton_block::BlockIdExt {
            shard_id: ton_block::ShardIdent::with_tagged_prefix(
                ton_block::BASE_WORKCHAIN_ID,
                ton_block::SHARD_FULL,
            )?,
            seq_no: 0,
            root_hash: base_workchain.zerostate_root_hash,
            file_hash: base_workchain.zerostate_file_hash,
        },
    )
    .await?;

    Ok(())
}

pub async fn download_zero_state(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
) -> Result<(Arc<BlockHandle>, Arc<ShardStateStuff>)> {
    let db = &engine.db;

    if let Some(handle) = db
        .block_handle_storage()
        .load_handle(block_id)
        .context("Failed to load zerostate handle")?
    {
        if handle.meta().has_state() {
            return Ok((
                handle,
                engine
                    .load_state(block_id)
                    .await
                    .context("Failed to load zerostate")?,
            ));
        }
    }

    loop {
        match engine.download_zerostate(block_id, None).await {
            Ok(state) => {
                let handle = engine.store_zerostate(block_id, &state).await?;
                engine.set_applied(&handle, 0).await?;
                engine.notify_subscribers_with_full_state(&state).await?;
                return Ok((handle, state));
            }
            Err(e) => {
                log::warn!("Failed to download zero state: {}", e);
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn download_start_blocks_and_states(
    engine: &Arc<Engine>,
    masterchain_block_id: &ton_block::BlockIdExt,
) -> Result<()> {
    let (_, init_mc_block) =
        download_block_and_state(engine, masterchain_block_id, masterchain_block_id).await?;

    log::info!("Downloaded init mc block state: {}", init_mc_block.id());

    for (_, block_id) in init_mc_block.shard_blocks()? {
        if block_id.seq_no == 0 {
            download_zero_state(engine, &block_id).await?;
        } else {
            download_block_and_state(engine, &block_id, masterchain_block_id).await?;
        };
    }

    Ok(())
}

async fn download_block_and_state(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
    masterchain_block_id: &ton_block::BlockIdExt,
) -> Result<(Arc<BlockHandle>, BlockStuff)> {
    let block_handle_storage = engine.db.block_handle_storage();
    let block_storage = engine.db.block_storage();

    let handle = block_handle_storage
        .load_handle(block_id)?
        .filter(|handle| handle.meta().has_data());

    log::info!("Downloading block state for {}", block_id);

    let (block, handle) = match handle {
        Some(handle) => (block_storage.load_block_data(&handle).await?, handle),
        None => {
            let (block, proof) = engine.download_block(block_id, None).await?;
            log::info!("Downloaded block {}", block_id);

            let mut handle = block_storage.store_block_data(&block).await?.handle;
            if !handle.meta().has_proof() {
                handle = block_storage
                    .store_block_proof(block_id, Some(handle), &proof)
                    .await?
                    .handle;
            }
            (block.data, handle)
        }
    };

    if !handle.meta().has_state() {
        let state_update = block.block().read_state_update()?;
        log::info!(
            "Download state: {} for {}",
            handle.id(),
            masterchain_block_id
        );

        let shard_state = download_state(engine, handle.id(), masterchain_block_id).await?;
        log::info!("Downloaded state");

        let state_hash = shard_state.root_cell().repr_hash();
        if state_update.new_hash != state_hash {
            return Err(BootError::ShardStateHashMismatch.into());
        }

        log::info!("Received shard state for: {}", shard_state.block_id());
        engine.store_state(&handle, &shard_state).await?;
        engine
            .notify_subscribers_with_full_state(&shard_state)
            .await?;
    }

    engine
        .set_applied(&handle, masterchain_block_id.seq_no)
        .await?;
    Ok((handle, block))
}

const KEY_BLOCK_UTIME_STEP: i32 = 86400;
const INTITAL_SYNC_TIME_SECONDS: i32 = 300;

#[derive(thiserror::Error, Debug)]
enum BootError {
    #[error("Starting from non-key block")]
    StartingFromNonKeyBlock,
    #[error("Failed to load initial block handle")]
    FailedToLoadInitialBlock,
    #[error("Masterchain state not found")]
    MasterchainStateNotFound,
    #[error("Base workchain info not found")]
    BaseWorkchainInfoNotFound,
    #[error("Downloaded shard state hash mismatch")]
    ShardStateHashMismatch,
    #[error("Persistent shard state not found")]
    PersistentShardStateNotFound,
}
