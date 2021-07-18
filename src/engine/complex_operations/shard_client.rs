use std::sync::Arc;

use anyhow::{anyhow, Result};
use ton_api::ton;

use crate::engine::Engine;
use crate::utils::*;

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
    let block_info = proof.virtualize_block()?.0.read_info().convert()?;

    let prev_key_block_seqno = block_info.prev_key_block_seqno();

    let mut key_block_proof = None;

    let (validator_set, catchain_config) = {
        let masterchain_prefix = ton_block::AccountIdPrefixFull::any_masterchain();
        let handle = engine.find_block_by_seq_no(&masterchain_prefix, prev_key_block_seqno)?;
        let proof = engine.load_block_proof(&handle, false).await?;
        let validator_set_ext = proof.get_cur_validators_set()?;
        key_block_proof = Some(proof);
        validator_set_ext
    };

    validate_broadcast(&broadcast, &block_id, &validator_set, &catchain_config)?;

    if block_id.shard_id.is_masterchain() {
        if let Some(key_block_proof) = key_block_proof {
            proof.check_with_prev_key_block_proof(&key_block_proof)?;
        }
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

    // TODO: apply

    Ok(())
}

fn validate_broadcast(
    broadcast: &ton::ton_node::broadcast::BlockBroadcast,
    block_id: &ton_block::BlockIdExt,
    validator_set: &ton_block::ValidatorSet,
    catchain_config: &ton_block::CatchainConfig,
) -> Result<()> {
    let (validators, validators_hash_short) = validator_set
        .calc_subset(
            catchain_config,
            block_id.shard_id.shard_prefix_with_tag(),
            block_id.shard_id.workchain_id(),
            broadcast.catchain_seqno as u32,
            ton_block::UnixTime32(0),
        )
        .convert()?;

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
            sign: ton_block::CryptoSignature::from_bytes(&signature.signature).convert()?,
        });
    }

    // Check signatures
    let data_to_sign =
        ton_block::Block::build_data_for_sign(&block_id.root_hash, &block_id.file_hash);
    let total_weight: u64 = validators.iter().map(|v| v.weight).sum();
    let weight = block_pure_signatures
        .check_signatures(validators, &data_to_sign)
        .convert()?;

    if weight * 3 <= total_weight * 2 {
        return Err(anyhow!(
            "Too small signatures weight in broadcast with block {}",
            block_id,
        ));
    }

    Ok(())
}
