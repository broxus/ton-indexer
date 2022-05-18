use std::sync::Arc;

use anyhow::Result;

use crate::engine::Engine;

use self::cold_boot::*;
use self::warm_boot::*;

mod cold_boot;
mod warm_boot;

/// Ensures that all shard states are downloaded.
///
/// Returns last masterchain key block id and last shard client block id
pub async fn boot(engine: &Arc<Engine>) -> Result<()> {
    log::info!("Starting boot");

    let last_key_block_id = match engine.load_last_applied_mc_block_id() {
        Ok(block_id) => warm_boot(engine, block_id).await?,
        Err(e) => {
            log::warn!("Failed to load last masterchain block id: {e}. Node is not synced yet");
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
            engine.store_shards_client_mc_block_id(&last_key_block_id)?;
            last_key_block_id.clone()
        }
    };

    log::info!("Boot finished");
    log::info!("Last key block: {last_key_block_id}");
    log::info!("Last shards client block: {shards_client_mc_block_id}");

    Ok(())
}
