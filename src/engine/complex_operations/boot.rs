use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tiny_adnl::utils::*;

use super::download_state::*;
use crate::engine::node_state::*;
use crate::engine::Engine;
use crate::storage::*;
use crate::utils::*;

#[derive(Debug, Clone)]
pub struct BootData {
    pub last_mc_block_id: ton_block::BlockIdExt,
    pub shards_client_mc_block_id: ton_block::BlockIdExt,
}

pub async fn boot(engine: &Arc<Engine>) -> Result<BootData> {
    let last_mc_block_id = match LastMcBlockId::load_from_db(engine.db.as_ref()) {
        Ok(block_id) => {
            let last_mc_block_id = convert_block_id_ext_api2blk(&block_id.0)?;
            warm_boot(engine, last_mc_block_id).await?
        }
        Err(e) => {
            log::warn!("Failed to load last masterchain block id: {}", e);
            let last_mc_block_id = cold_boot(engine).await?;
            engine
                .store_last_applied_mc_block_id(&last_mc_block_id)
                .await?;
            last_mc_block_id
        }
    };

    let shards_client_mc_block_id = match ShardsClientMcBlockId::load_from_db(engine.db.as_ref()) {
        Ok(block_id) => convert_block_id_ext_api2blk(&block_id.0)?,
        Err(_) => {
            engine
                .store_shards_client_mc_block_id(&last_mc_block_id)
                .await?;
            last_mc_block_id.clone()
        }
    };
    let boot_data = BootData {
        last_mc_block_id,
        shards_client_mc_block_id,
    };
    Ok(boot_data)
}

async fn cold_boot(engine: &Arc<Engine>) -> Result<ton_block::BlockIdExt> {
    let boot_data = prepare_cold_boot_data(engine).await?;
    let key_blocks = get_key_blocks(engine, boot_data).await?;
    let last_key_block = choose_key_block(key_blocks)?;

    let block_id = last_key_block.id();
    download_start_blocks_and_states(engine, block_id).await?;

    Ok(block_id.clone())
}

async fn warm_boot(
    engine: &Arc<Engine>,
    mut last_mc_block_id: ton_block::BlockIdExt,
) -> Result<ton_block::BlockIdExt> {
    let handle = engine
        .load_block_handle(&last_mc_block_id)?
        .ok_or(BootError::FailedToLoadInitialBlock)?;

    let state = engine.load_state(&last_mc_block_id).await?;
    if last_mc_block_id.seq_no != 0 && !handle.meta().is_key_block() {
        last_mc_block_id = state
            .shard_state_extra()?
            .last_key_block
            .clone()
            .ok_or(BootError::MasterchainStateNotFound)?
            .master_block_id()
            .1
    }

    Ok(last_mc_block_id)
}

async fn prepare_cold_boot_data(engine: &Arc<Engine>) -> Result<ColdBootData> {
    let block_id = engine.init_mc_block_id();
    log::info!("Cold boot from {}", block_id);

    if block_id.seq_no == 0 {
        log::info!("Using zero state");
        let (handle, state) = download_zero_state(engine, block_id).await?;
        Ok(ColdBootData::ZeroState { handle, state })
    } else {
        log::info!("Using key block");
        let handle = match engine.load_block_handle(block_id)? {
            Some(handle) => {
                if handle.meta().has_proof_link() || handle.meta().has_proof() {
                    let proof = match engine.load_block_proof(&handle, true).await {
                        Ok(proof) => proof,
                        Err(e) => {
                            log::warn!("Failed to load block proof as link: {}", e);
                            engine.load_block_proof(&handle, false).await?
                        }
                    };

                    if !handle.meta().is_key_block() {
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
                .download_block_proof(block_id, true, true, None)
                .await
            {
                Ok(proof) => match proof.check_proof_link() {
                    Ok(_) => {
                        let handle = engine.store_block_proof(block_id, handle, &proof).await?;
                        break (handle, proof);
                    }
                    Err(e) => {
                        log::warn!("Got invalid block proof for init block: {}", e);
                    }
                },
                Err(e) => {
                    log::warn!("Failed to download block proof for init block: {}", e);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        if !handle.meta().is_key_block() {
            return Err(BootError::StartingFromNonKeyBlock.into());
        }

        Ok(ColdBootData::KeyBlock {
            handle,
            proof: Box::new(proof),
        })
    }
}

async fn get_key_blocks(
    engine: &Arc<Engine>,
    mut boot_data: ColdBootData,
) -> Result<Vec<Arc<BlockHandle>>> {
    let mut handle = boot_data.init_block_handle().clone();

    let mut result = vec![handle.clone()];

    loop {
        log::info!("Downloading next key blocks for: {}", handle.id());

        let ids = match engine.download_next_key_blocks_ids(handle.id()).await {
            Ok(ids) => ids,
            Err(e) => {
                log::warn!(
                    "Failed to download next key block ids for {}: {}",
                    handle.id(),
                    e
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };

        if let Some(block_id) = ids.last() {
            log::info!("Last key block id: {}", block_id);
            for block_id in &ids {
                let prev_utime = handle.meta().gen_utime();
                let (next_handle, proof) =
                    download_key_block_proof(engine, block_id, &boot_data).await?;
                if is_persistent_state(next_handle.meta().gen_utime(), prev_utime) {
                    engine.set_init_mc_block_id(block_id);
                }

                handle = next_handle;
                result.push(handle.clone());
                boot_data = ColdBootData::KeyBlock {
                    handle: handle.clone(),
                    proof: Box::new(proof),
                };
            }
        }

        let last_utime = handle.meta().gen_utime() as i32;
        let current_utime = now();

        log::info!(
            "Last known block: {}, utime: {}, now: {}",
            handle.id(),
            last_utime,
            current_utime
        );

        if last_utime + INTITAL_SYNC_TIME_SECONDS > current_utime
            || last_utime + 2 * KEY_BLOCK_UTIME_STEP > current_utime
        {
            return Ok(result);
        }
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

        let ttl = persistent_state_ttl(handle_utime);
        let time_to_download = 3600;
        if ttl < now() as u32 + time_to_download {
            log::info!("Best key block is expiring shortly: expire_at={}", ttl);
        }

        log::info!("Best key block handle is {}", handle.id());
        return Ok(handle);
    }

    Err(BootError::PersistentShardStateNotFound.into())
}

async fn download_key_block_proof(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
    boot_data: &ColdBootData,
) -> Result<(Arc<BlockHandle>, BlockProofStuff)> {
    if let Some(handle) = engine.load_block_handle(block_id)? {
        if let Ok(proof) = engine.load_block_proof(&handle, false).await {
            return Ok((handle, proof));
        }
    }

    loop {
        let proof = engine
            .download_block_proof(block_id, false, true, None)
            .await?;
        let result = match boot_data {
            ColdBootData::KeyBlock {
                proof: prev_proof, ..
            } => proof.check_with_prev_key_block_proof(prev_proof),
            ColdBootData::ZeroState { state, .. } => proof.check_with_master_state(state),
        };

        match result {
            Ok(_) => {
                let handle = engine.store_block_proof(block_id, None, &proof).await?;
                return Ok((handle, proof));
            }
            Err(e) => {
                log::warn!("Got invalid key block proof: {}", e);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
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

#[allow(unused)]
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
    if let Some(handle) = engine.load_block_handle(block_id)? {
        if handle.meta().has_state() {
            return Ok((handle, engine.load_state(block_id).await?));
        }
    }

    loop {
        match engine.download_zerostate(block_id, None).await {
            Ok(state) => {
                let handle = engine.store_zerostate(block_id, &state).await?;
                engine.set_applied(&handle, 0).await?;
                engine.notify_subscribers_with_state(&state).await?;
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
    let active_peers = Arc::new(ActivePeers::default());

    let (_, init_mc_block) = download_block_and_state(
        engine,
        masterchain_block_id,
        masterchain_block_id,
        &active_peers,
    )
    .await?;

    log::info!("Downloaded init mc block state: {}", init_mc_block.id());

    for (_, block_id) in init_mc_block.shards_blocks()? {
        if block_id.seq_no == 0 {
            download_zero_state(engine, &block_id).await?;
        } else {
            download_block_and_state(engine, &block_id, masterchain_block_id, &active_peers)
                .await?;
        };
    }

    Ok(())
}

async fn download_block_and_state(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
    masterchain_block_id: &ton_block::BlockIdExt,
    active_peers: &Arc<ActivePeers>,
) -> Result<(Arc<BlockHandle>, BlockStuff)> {
    let handle = engine
        .load_block_handle(block_id)?
        .filter(|handle| handle.meta().has_data());

    log::info!("Downloading block state for {}", block_id);

    let (block, handle) = match handle {
        Some(handle) => (engine.load_block_data(&handle).await?, handle),
        None => {
            let (block, proof) = engine.download_block(block_id, None).await?;
            log::info!("Downloaded block {}", block_id);

            let mut handle = engine.store_block_data(&block).await?.handle;
            if !handle.meta().has_proof() {
                handle = engine
                    .store_block_proof(block_id, Some(handle), &proof)
                    .await?;
            }
            (block, handle)
        }
    };

    if !handle.meta().has_state() {
        let state_update = block.block().read_state_update()?;
        log::info!(
            "Download state: {} for {}",
            handle.id(),
            masterchain_block_id
        );

        let shard_state = download_state(
            engine,
            handle.id(),
            masterchain_block_id,
            handle.id().is_masterchain(),
            active_peers,
        )
        .await?;
        log::info!("Downloaded state");

        let state_hash = shard_state.root_cell().repr_hash();
        if state_update.new_hash != state_hash {
            return Err(BootError::ShardStateHashMismatch.into());
        }

        log::info!("Received shard state for: {}", shard_state.block_id());
        engine.store_state(&handle, &shard_state).await?;
        engine.notify_subscribers_with_state(&shard_state).await?;
    }

    engine
        .set_applied(&handle, masterchain_block_id.seq_no)
        .await?;
    Ok((handle, block))
}

fn is_persistent_state(block_utime: u32, prev_utime: u32) -> bool {
    block_utime / (1 << 17) != prev_utime / (1 << 17)
}

fn persistent_state_ttl(utime: u32) -> u32 {
    let x = utime / (1 << 17);
    let b = x.trailing_zeros();
    utime + ((1 << 18) << b)
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
