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
use crate::utils::NoFailure;

mod config;
mod engine;
mod network;
mod storage;
mod utils;

pub async fn start(node_config: NodeConfig, global_config: GlobalConfig) -> Result<()> {
    let engine = Engine::new(node_config, global_config).await?;

    start_full_node_service(engine.clone())?;

    let last_masterchain_block_id = boot(&engine).await?;

    log::info!("Initialized (last block: {})", last_masterchain_block_id);

    engine
        .listen_broadcasts(ton_block::ShardIdent::masterchain())
        .await?;
    engine
        .listen_broadcasts(
            ton_block::ShardIdent::with_tagged_prefix(
                ton_block::BASE_WORKCHAIN_ID,
                ton_block::SHARD_FULL,
            )
            .convert()?,
        )
        .await?;

    futures::future::pending().await
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
