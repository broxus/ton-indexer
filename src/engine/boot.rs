use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tiny_adnl::utils::*;

use super::node_state::*;
use super::Engine;
use crate::storage::*;
use crate::utils::*;

async fn boot(engine: &Arc<Engine>) -> Result<ton_block::BlockIdExt> {
    let mut last_mc_block_id = match LastMcBlockId::load_from_db(engine.db().as_ref()) {
        Ok(block_id) => {
            let last_mc_block_id = convert_block_id_ext_api2blk(&block_id.0)?;
            warm_boot(engine, last_mc_block_id).await?
        }
        Err(e) => {
            log::warn!("Failed to load last masterchain block id: {}", e);
            cold_boot(engine).await?
        }
    };

    todo!()
}

async fn cold_boot(engine: &Arc<Engine>) -> Result<ton_block::BlockIdExt> {
    let boot_data = prepare_cold_boot_data(engine).await?;
    let key_blocks = get_key_blocks(engine, boot_data).await?;

    log::warn!("loaded: {}", key_blocks.len());

    todo!()
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
                    let proof = match engine.load_block_proof(&handle, true) {
                        Ok(proof) => proof,
                        Err(e) => {
                            log::warn!("Failed to load block proof as link: {}", e);
                            engine.load_block_proof(&handle, false)?
                        }
                    };

                    if !handle.meta().is_key_block() {
                        return Err(BootError::StartingFromNonKeyBlock.into());
                    }

                    return Ok(ColdBootData::KeyBlock { handle, proof });
                }
                Some(handle)
            }
            None => None,
        };

        let (handle, proof) = loop {
            match engine
                .download_block_proof(&block_id, true, true, None)
                .await
            {
                Ok(proof) => match proof.check_proof_link() {
                    Ok(_) => {
                        let handle = engine.store_block_proof(&block_id, handle, &proof)?;
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

        Ok(ColdBootData::KeyBlock { handle, proof })
    }
}

async fn get_key_blocks(
    engine: &Arc<Engine>,
    mut boot_data: ColdBootData,
) -> Result<Vec<Arc<BlockHandle>>> {
    let download_new_key_blocks_until = now() as u32 + 600;

    let mut handle = boot_data.init_block_handle().clone();
    let mut key_blocks = vec![handle.clone()];
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
                key_blocks.push(handle.clone());
                boot_data = ColdBootData::KeyBlock {
                    handle: handle.clone(),
                    proof,
                };
            }
        }

        if let Some(last_handle) = key_blocks.last() {
            let last_utime = last_handle.meta().gen_utime() as i32;
            log::info!(
                "Last known block: {}, utime: {}, now: {}",
                handle.id(),
                last_utime,
                now()
            );

            if now() - last_utime < 2 * 86400 {
                return Ok(key_blocks);
            }
        }
    }
}

async fn download_key_block_proof(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
    boot_data: &ColdBootData,
) -> Result<(Arc<BlockHandle>, BlockProofStuff)> {
    if let Some(handle) = engine.load_block_handle(block_id)? {
        if let Ok(proof) = engine.load_block_proof(&handle, false) {
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
                let handle = engine.store_block_proof(block_id, None, &proof)?;
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
        proof: BlockProofStuff,
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

async fn warm_boot(
    engine: &Arc<Engine>,
    mut last_mc_block_id: ton_block::BlockIdExt,
) -> Result<ton_block::BlockIdExt> {
    todo!()
}

async fn download_zero_state(
    engine: &Engine,
    block_id: &ton_block::BlockIdExt,
) -> Result<(Arc<BlockHandle>, Arc<ShardStateStuff>)> {
    if let Some(handle) = engine.load_block_handle(block_id)? {
        if handle.meta().has_state() {
            return Ok((handle, engine.load_state(block_id)?));
        }
    }

    loop {
        match engine.download_zerostate(block_id, None).await {
            Ok(state) => {
                let handle = engine.store_zerostate(block_id, &state).await?;
                return Ok((handle, state));
            }
            Err(e) => {
                log::warn!("Failed to download zero state: {}", e);
            }
        }

        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn is_persistent_state(block_utime: u32, prev_utime: u32) -> bool {
    block_utime >> 17 != prev_utime >> 17
}

#[derive(thiserror::Error, Debug)]
enum BootError {
    #[error("Starting from non-key block")]
    StartingFromNonKeyBlock,
}
