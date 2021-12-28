/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - simplified block walking
///
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use ton_api::ton;

use crate::engine::db::BlockConnection;
use crate::engine::Engine;
use crate::utils::*;

pub async fn walk_masterchain_blocks(
    engine: &Arc<Engine>,
    mut mc_block_id: ton_block::BlockIdExt,
) -> Result<()> {
    while engine.is_working() {
        log::info!("walk_masterchain_blocks: {}", mc_block_id);
        mc_block_id = match load_next_masterchain_block(engine, &mc_block_id).await {
            Ok(id) => id,
            Err(e) => {
                log::error!(
                    "Failed to load next masterchain block for {}: {:?}",
                    mc_block_id,
                    e
                );
                continue;
            }
        }
    }
    Ok(())
}

pub async fn walk_shard_blocks(
    engine: &Arc<Engine>,
    mc_block_id: ton_block::BlockIdExt,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(1));

    let mut handle = engine
        .load_block_handle(&mc_block_id)?
        .ok_or(ShardClientError::ShardchainBlockHandleNotFound)?;

    while engine.is_working() {
        log::info!("walk_shard_blocks: {}", handle.id());
        let (next_handle, next_block) = engine.wait_next_applied_mc_block(&handle, None).await?;
        handle = next_handle;

        tokio::spawn({
            let engine = engine.clone();
            let permit = semaphore.clone().acquire_owned().await?;
            async move {
                if let Err(e) = load_shard_blocks(&engine, permit, next_block).await {
                    log::error!("Failed to load shard blocks: {:?}", e);
                }
            }
        });
    }
    Ok(())
}

async fn load_next_masterchain_block(
    engine: &Arc<Engine>,
    mc_block_id: &ton_block::BlockIdExt,
) -> Result<ton_block::BlockIdExt> {
    if let Some(handle) = engine.load_block_handle(mc_block_id)? {
        if handle.meta().has_next1() {
            let next1_id = engine
                .db
                .load_block_connection(mc_block_id, BlockConnection::Next1)?;
            engine
                .download_and_apply_block(&next1_id, next1_id.seq_no, false, 0)
                .await?;
            return Ok(next1_id);
        }
    } else {
        return Err(ShardClientError::MasterchainBlockNotFound.into());
    }

    let (block, block_proof) = engine
        .download_next_masterchain_block(mc_block_id, None)
        .await?;
    if block.id().seq_no != mc_block_id.seq_no + 1 {
        return Err(ShardClientError::BlockIdMismatch.into());
    }

    let prev_state = engine.wait_state(mc_block_id, None, true).await?;
    block_proof.check_with_master_state(&prev_state)?;

    let mut next_handle = match engine.load_block_handle(block.id())? {
        Some(next_handle) => {
            if !next_handle.meta().has_data() {
                return Err(ShardClientError::InvalidBlockHandle.into());
            }
            next_handle
        }
        None => engine.store_block_data(&block).await?.handle,
    };

    if !next_handle.meta().has_proof() {
        next_handle = engine
            .store_block_proof(block.id(), Some(next_handle), &block_proof)
            .await?;
    }

    engine
        .apply_block_ext(&next_handle, &block, next_handle.id().seq_no, false, 0)
        .await?;
    Ok(block.id().clone())
}

async fn load_shard_blocks(
    engine: &Arc<Engine>,
    permit: OwnedSemaphorePermit,
    masterchain_block: BlockStuff,
) -> Result<()> {
    let mc_seq_no = masterchain_block.id().seq_no;
    let mut tasks = Vec::new();
    for (_, shard_block_id) in masterchain_block.shards_blocks()? {
        if let Some(handle) = engine.load_block_handle(&shard_block_id)? {
            if handle.meta().is_applied() {
                continue;
            }
        }

        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            while let Err(e) = engine
                .download_and_apply_block(&shard_block_id, mc_seq_no, false, 0)
                .await
            {
                log::error!("Failed to apply shard block: {}: {:?}", shard_block_id, e);
            }
        }));
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .find(|item| item.is_err())
        .unwrap_or(Ok(()))?;

    engine.store_shards_client_mc_block_id(masterchain_block.id())?;

    std::mem::drop(permit);
    Ok(())
}

pub async fn process_block_broadcast(
    engine: &Arc<Engine>,
    broadcast: ton::ton_node::broadcast::BlockBroadcast,
) -> Result<()> {
    let block_id = convert_block_id_ext_api2blk(&broadcast.id)?;
    if let Some(handle) = engine.load_block_handle(&block_id)? {
        if handle.meta().has_data() {
            return Ok(());
        }
    }

    let proof = BlockProofStuff::deserialize(
        block_id.clone(),
        broadcast.proof.0.clone(),
        !block_id.shard_id.is_masterchain(),
    )?;
    let block_info = proof.virtualize_block()?.0.read_info()?;

    let prev_key_block_seqno = block_info.prev_key_block_seqno();
    let last_applied_mc_block_id = engine.load_last_applied_mc_block_id()?;
    if prev_key_block_seqno > last_applied_mc_block_id.seq_no {
        return Ok(());
    }

    let (key_block_proof, validator_set, catchain_config) = {
        let handle = engine.db.load_key_block_handle(prev_key_block_seqno)?;
        let proof = engine.load_block_proof(&handle, false).await?;
        let (validator_set, catchain_config) = proof.get_cur_validators_set()?;
        (proof, validator_set, catchain_config)
    };

    validate_broadcast(&broadcast, &block_id, &validator_set, &catchain_config)?;

    if block_id.shard_id.is_masterchain() {
        proof.check_with_prev_key_block_proof(&key_block_proof)?;
    } else {
        proof.check_proof_link()?;
    }

    let block = BlockStuff::deserialize_checked(block_id, broadcast.data.0)?;
    let mut handle = engine.store_block_data(&block).await?.handle;

    if !handle.meta().has_proof() {
        handle = engine
            .store_block_proof(block.id(), Some(handle), &proof)
            .await?;
    }

    if block.id().shard_id.is_masterchain() {
        if block.id().seq_no == last_applied_mc_block_id.seq_no + 1 {
            engine
                .apply_block_ext(&handle, &block, block.id().seq_no, false, 0)
                .await?;
        }
    } else {
        let master_ref = block
            .block()
            .read_info()
            .and_then(|info| info.read_master_ref())?
            .ok_or(ShardClientError::InvalidBlockExtra)?;

        let shards_client_mc_block_id = engine.load_shards_client_mc_block_id()?;
        if shards_client_mc_block_id.seq_no + 8 >= master_ref.master.seq_no {
            engine
                .apply_block_ext(&handle, &block, shards_client_mc_block_id.seq_no, true, 0)
                .await?;
        }
    }

    Ok(())
}

fn validate_broadcast(
    broadcast: &ton::ton_node::broadcast::BlockBroadcast,
    block_id: &ton_block::BlockIdExt,
    validator_set: &ton_block::ValidatorSet,
    catchain_config: &ton_block::CatchainConfig,
) -> Result<()> {
    let (validators, validators_hash_short) = validator_set.calc_subset(
        catchain_config,
        block_id.shard_id.shard_prefix_with_tag(),
        block_id.shard_id.workchain_id(),
        broadcast.catchain_seqno as u32,
        ton_block::UnixTime32(0),
    )?;

    if validators_hash_short != broadcast.validator_set_hash as u32 {
        return Err(anyhow!(
            "Bad validator set hash in broadcast with block {}, calculated: {}, found: {}",
            block_id,
            validators_hash_short,
            broadcast.validator_set_hash
        ));
    }

    // Extract signatures
    let mut block_pure_signatures = ton_block::BlockSignaturesPure::default();
    for signature in &broadcast.signatures.0 {
        block_pure_signatures.add_sigpair(ton_block::CryptoSignaturePair {
            node_id_short: ton_types::UInt256::from(&signature.who.0),
            sign: ton_block::CryptoSignature::from_bytes(&signature.signature)?,
        });
    }

    // Check signatures
    let data_to_sign =
        ton_block::Block::build_data_for_sign(&block_id.root_hash, &block_id.file_hash);
    let total_weight: u64 = validators.iter().map(|v| v.weight).sum();
    let weight = block_pure_signatures.check_signatures(validators, &data_to_sign)?;

    if weight * 3 <= total_weight * 2 {
        return Err(anyhow!(
            "Too small signatures weight in broadcast with block {}",
            block_id,
        ));
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
enum ShardClientError {
    #[error("Masterchain block not found")]
    MasterchainBlockNotFound,
    #[error("Shardchain block handle not found")]
    ShardchainBlockHandleNotFound,
    #[error("Block id mismatch")]
    BlockIdMismatch,
    #[error("Invalid block handle")]
    InvalidBlockHandle,
    #[error("Invalid block extra")]
    InvalidBlockExtra,
}
