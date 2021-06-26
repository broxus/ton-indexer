use std::sync::Arc;

use anyhow::Result;

use super::node_state::*;
use super::Engine;
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
    let block_id = engine.init_mc_block_id();
    log::info!("Cold boot from {}", block_id);

    if block_id.seq_no == 0 {
        log::info!("Downloading zero state");
    }

    todo!()
}

async fn warm_boot(
    engine: &Arc<Engine>,
    mut last_mc_block_id: ton_block::BlockIdExt,
) -> Result<ton_block::BlockIdExt> {
    todo!()
}
