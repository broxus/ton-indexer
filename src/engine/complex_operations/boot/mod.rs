use std::sync::Arc;

use anyhow::Result;

use crate::engine::Engine;
use crate::utils::*;

use self::cold_boot::*;
use self::warm_boot::*;

mod cold_boot;
mod warm_boot;
pub use cold_boot::{
    choose_key_block, cold_boot, download_workchain_zero_state, prepare_prev_key_block,
};

/// Ensures that all shard states are downloaded.
///
/// Returns last masterchain key block id and last shard client block id
pub async fn boot(engine: &Arc<Engine>) -> Result<()> {
    tracing::info!("starting boot");

    let last_key_block_id = match engine.load_last_applied_mc_block_id() {
        Ok(block_id) => warm_boot(engine, block_id).await?,
        Err(e) => {
            tracing::warn!("failed to load last masterchain block id: {e}. node is not synced yet");
            let last_mc_block_id = cold_boot(engine).await?;

            engine.store_last_applied_mc_block_id(&last_mc_block_id)?;

            engine
                .storage
                .node_state()
                .store_historical_sync_end(&last_mc_block_id)?;

            last_mc_block_id
        }
    };

    let shards_client_mc_block_id = match engine.load_shards_client_mc_block_id() {
        Ok(block_id) => block_id,
        Err(_) => {
            engine.store_shards_client_mc_block_id(&last_key_block_id)?;

            // NOTE: clippy can't detect the usage later in macros
            #[allow(clippy::redundant_clone)]
            last_key_block_id.clone()
        }
    };

    tracing::info!(
        last_key_block_id = %last_key_block_id.display(),
        shards_client_mc_block_id = %shards_client_mc_block_id.display(),
        "boot finished"
    );
    Ok(())
}
