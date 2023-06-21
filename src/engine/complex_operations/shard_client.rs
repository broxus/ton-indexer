/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - simplified block walking
///
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::engine::Engine;
use crate::proto;
use crate::storage::{BlockConnection, BlockHandle};
use crate::utils::*;

pub async fn walk_masterchain_blocks(
    engine: &Arc<Engine>,
    mut block_id: ton_block::BlockIdExt,
) -> Result<()> {
    while engine.is_working() {
        tracing::info!(
            block_id = %block_id.display(),
            "walking through masterchain blocks"
        );
        block_id = match load_next_masterchain_block(engine, &block_id).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(
                    block_id = %block_id.display(),
                    "failed to load next masterchain block: {e:?}"
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

    let block_handle_storage = engine.storage.block_handle_storage();
    let mut handle = block_handle_storage
        .load_handle(&mc_block_id)?
        .ok_or(ShardClientError::ShardchainBlockHandleNotFound)?;

    while engine.is_working() {
        tracing::info!(
            block_id = %handle.id().display(),
            "walking through shard blocks"
        );
        let (next_handle, next_block) = engine.wait_next_applied_mc_block(&handle, None).await?;
        handle = next_handle.clone();

        let engine = engine.clone();
        let permit = semaphore.clone().acquire_owned().await?;
        tokio::spawn(async move {
            if let Err(e) = load_shard_blocks(&engine, permit, next_handle, &next_block).await {
                tracing::error!("failed to load shard blocks: {e:?}");
            }
        });
    }
    Ok(())
}

async fn load_next_masterchain_block(
    engine: &Arc<Engine>,
    prev_block_id: &ton_block::BlockIdExt,
) -> Result<ton_block::BlockIdExt> {
    let block_handle_storage = engine.storage.block_handle_storage();
    let block_connection_storage = engine.storage.block_connection_storage();
    let block_storage = engine.storage.block_storage();

    if let Some(handle) = block_handle_storage.load_handle(prev_block_id)? {
        if handle.meta().has_next1() {
            let next1_id =
                block_connection_storage.load_connection(prev_block_id, BlockConnection::Next1)?;
            engine
                .download_and_apply_block(&next1_id, next1_id.seq_no, false, 0)
                .await?;
            return Ok(next1_id);
        }
    } else {
        return Err(ShardClientError::MasterchainBlockNotFound.into());
    }

    let (block, block_proof) = engine
        .download_next_masterchain_block(prev_block_id, None)
        .await?;
    let block_id = block.id();

    if block_id.seq_no != prev_block_id.seq_no + 1 {
        return Err(ShardClientError::BlockIdMismatch.into());
    } else if block_proof.is_link() {
        return Err(ShardClientError::InvalidBlockProof.into());
    }

    let (virt_block, virt_block_info) = block_proof.pre_check_block_proof()?;
    let brief_info = BriefBlockInfo::from(&virt_block_info);

    // TODO: use key block proof
    let prev_state = engine.wait_state(prev_block_id, None, true).await?;
    check_with_master_state(&block_proof, &prev_state, &virt_block, &virt_block_info)?;

    let mut handle = match block_handle_storage.load_handle(block_id)? {
        // Handle exists and it has block data specified
        Some(next_handle) if next_handle.meta().has_data() => next_handle,
        // Handle doesn't exist or doesn't have block data
        handle => {
            if handle.is_some() {
                tracing::warn!(
                    block_id = %block_id.display(),
                    "partially initialized handle detected"
                );
            }
            block_storage
                .store_block_data(&block, brief_info.with_mc_seq_no(block_id.seq_no))
                .await?
                .handle
        }
    };

    if !handle.meta().has_proof() {
        handle = block_storage
            .store_block_proof(&block_proof, handle.into())
            .await?
            .handle;
    }

    engine
        .apply_block_ext(&handle, &block, handle.id().seq_no, false, 0)
        .await?;

    Ok(block_id.clone())
}

async fn load_shard_blocks(
    engine: &Arc<Engine>,
    permit: OwnedSemaphorePermit,
    masterchain_block_handle: Arc<BlockHandle>,
    masterchain_block: &BlockStuff,
) -> Result<()> {
    let block_handle_storage = engine.storage.block_handle_storage();

    let mc_seq_no = masterchain_block.id().seq_no;
    let mut tasks = Vec::new();
    for (_, shard_block_id) in masterchain_block.shard_blocks()? {
        if matches!(
            block_handle_storage.load_handle(&shard_block_id)?,
            Some(handle) if handle.meta().is_applied()
        ) {
            continue;
        }

        let engine = engine.clone();
        tasks.push(tokio::spawn(async move {
            while let Err(e) = engine
                .download_and_apply_block(&shard_block_id, mc_seq_no, false, 0)
                .await
            {
                tracing::error!(
                    block_id = %shard_block_id.display(),
                    "failed to apply shard block: {e:?}"
                );
            }
        }));
    }

    futures_util::future::join_all(tasks)
        .await
        .into_iter()
        .find(|item| item.is_err())
        .unwrap_or(Ok(()))?;

    engine
        .on_blocks_edge(&masterchain_block_handle, masterchain_block)
        .await?;

    drop(permit);
    Ok(())
}

pub async fn process_block_broadcast(
    engine: &Arc<Engine>,
    mut broadcast: proto::BlockBroadcast,
) -> Result<()> {
    let block_handle_storage = engine.storage.block_handle_storage();
    let block_storage = engine.storage.block_storage();

    if matches!(
        block_handle_storage.load_handle(&broadcast.id)?,
        Some(handle) if handle.meta().has_data()
    ) {
        return Ok(());
    }

    let proof = BlockProofStuff::deserialize(
        broadcast.id.clone(),
        &broadcast.proof,
        !broadcast.id.shard_id.is_masterchain(),
    )?;
    let virt_block_info = proof.virtualize_block()?.0.read_info()?;
    let meta_data = BriefBlockInfo::from(&virt_block_info);

    let last_applied_mc_block_id = engine.load_last_applied_mc_block_id()?;
    if virt_block_info.prev_key_block_seqno() > last_applied_mc_block_id.seq_no {
        return Ok(());
    }

    let last_mc_state = engine.load_state(&last_applied_mc_block_id).await?;
    validate_broadcast(&mut broadcast, &last_mc_state)?;

    let block_id = &broadcast.id;
    if block_id.shard_id.is_masterchain() {
        proof.check_with_master_state(&last_mc_state)?;
    } else {
        proof.check_proof_link()?;
    }

    let block = BlockStuff::deserialize_checked(block_id.clone(), &broadcast.data)?;
    let block = BlockStuffAug::new(block, broadcast.data);
    let mut handle = match block_storage
        .store_block_data(&block, meta_data.with_mc_seq_no(0))
        .await?
    {
        result if result.updated => result.handle,
        // Skipped apply for block broadcast because the block is already being processed
        _ => return Ok(()),
    };

    if !handle.meta().has_proof() {
        handle = match block_storage
            .store_block_proof(
                &BlockProofStuffAug::new(proof, broadcast.proof),
                handle.into(),
            )
            .await?
        {
            result if result.updated => result.handle,
            // Skipped apply for block broadcast because the block is already being processed
            _ => return Ok(()),
        };
    }

    if block_id.shard_id.is_masterchain() {
        if block_id.seq_no == last_applied_mc_block_id.seq_no + 1 {
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
    broadcast: &mut proto::BlockBroadcast,
    last_mc_state: &ShardStateStuff,
) -> Result<()> {
    let block_id = &broadcast.id;

    #[allow(unused_labels)]
    let subset = 'subset: {
        let config_params = last_mc_state.config_params()?;
        let validator_set = config_params.validator_set()?;

        #[cfg(feature = "venom")]
        if !broadcast.id.shard().is_masterchain()
            && config_params.has_capability(ton_block::GlobalCapabilities::CapFastFinality)
        {
            let shard_hashes = last_mc_state.shards()?;
            break 'subset ValidatorSubsetInfo::compute_for_workchain_venom(
                &validator_set,
                &broadcast.id,
                shard_hashes,
                broadcast.catchain_seqno,
            )?;
        }

        ValidatorSubsetInfo::compute_standard(
            &validator_set,
            &broadcast.id,
            &config_params.catchain_config()?,
            broadcast.catchain_seqno,
        )?
    };

    if subset.short_hash != broadcast.validator_set_hash {
        return Err(anyhow!(
            "Bad validator set hash in broadcast with block {}, calculated: {}, found: {}",
            block_id,
            subset.short_hash,
            broadcast.validator_set_hash
        ));
    }

    // Extract signatures
    let mut block_pure_signatures = ton_block::BlockSignaturesPure::default();
    for signature in std::mem::take(&mut broadcast.signatures) {
        block_pure_signatures.add_sigpair(signature);
    }

    // Check signatures
    let data_to_sign =
        ton_block::Block::build_data_for_sign(&block_id.root_hash, &block_id.file_hash);
    let total_weight: u64 = subset.validators.iter().map(|v| v.weight).sum();
    let weight = block_pure_signatures.check_signatures(&subset.validators, &data_to_sign)?;

    if weight * 3 <= total_weight * 2 {
        return Err(anyhow!(
            "Too small signatures weight in broadcast with block {block_id}"
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
    #[error("Invalid block proof")]
    InvalidBlockProof,
    #[error("Invalid block extra")]
    InvalidBlockExtra,
}
