use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tiny_adnl::utils::*;
use tiny_adnl::{OverlaySubscriber, QueryAnswer, QueryConsumingResult};
use ton_api::ton::{self, TLObject};
use ton_api::IntoBoxed;

pub use crate::config::*;
use crate::engine::*;
use crate::network::*;

mod config;
mod engine;
mod network;
mod storage;
mod utils;

pub async fn start(node_config: NodeConfig, global_config: GlobalConfig) -> Result<()> {
    let engine = Engine::new(node_config, global_config).await?;

    start_full_node_service(engine)?;

    Ok(())
}

async fn start_cold(
    client: &dyn FullNodeOverlayClient,
    initial_block: &ton_block::BlockIdExt,
) -> Result<()> {
    log::info!("Starting from block: {}", initial_block);
    if !initial_block.shard_id.is_masterchain() {
        return Err(anyhow!("Initial block must be from masterchain"));
    }

    if initial_block.seq_no == 0 {
        log::info!("Starting from zero state");
    }

    Ok(())
}

fn start_full_node_service(engine: Arc<Engine>) -> Result<()> {
    let service = FullNodeOverlayService::new();

    let network = engine.network();

    let (_, masterchain_overlay_id) =
        network.compute_overlay_id(ton_block::MASTERCHAIN_ID, ton_block::SHARD_FULL)?;
    network.add_subscriber(masterchain_overlay_id, service.clone());

    let (_, basechain_overlay_id) =
        network.compute_overlay_id(ton_block::BASE_WORKCHAIN_ID, ton_block::SHARD_FULL)?;
    network.add_subscriber(basechain_overlay_id, service.clone());

    Ok(())
}

async fn download_zero_state(
    client: &dyn FullNodeOverlayClient,
    block_id: &ton_block::BlockIdExt,
) -> Result<()> {
    loop {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
