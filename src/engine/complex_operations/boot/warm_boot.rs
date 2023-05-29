use std::sync::Arc;

use anyhow::Result;
use everscale_types::models::*;

use crate::engine::Engine;

/// Boot type when already synced or started syncing (there are states for each workchain).
///
/// Returns last masterchain key block id
pub async fn warm_boot(engine: &Arc<Engine>, mut last_mc_block_id: BlockId) -> Result<BlockId> {
    tracing::info!("starting warm boot");
    let block_handle_storage = engine.storage.block_handle_storage();
    let handle = block_handle_storage
        .load_handle(&last_mc_block_id)?
        .ok_or(WarmBootError::FailedToLoadInitialBlock)?;

    let state = engine.load_state(&last_mc_block_id).await?;
    if last_mc_block_id.seq_no != 0 && !handle.meta().is_key_block() {
        tracing::info!("started from non-key block");

        last_mc_block_id = state
            .shard_state_extra()?
            .last_key_block
            .clone()
            .ok_or(WarmBootError::MasterchainStateNotFound)?
            .master_block_id()
            .1;

        tracing::info!(last_mc_block_id = %last_mc_block_id.display());
    }

    tracing::info!("warm boot finished");
    Ok(last_mc_block_id)
}

#[derive(Debug, thiserror::Error)]
enum WarmBootError {
    #[error("Failed to load initial block handle")]
    FailedToLoadInitialBlock,
    #[error("Masterchain state not found")]
    MasterchainStateNotFound,
}
