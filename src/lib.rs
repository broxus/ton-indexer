use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tiny_adnl::utils::*;
use tiny_adnl::{OverlaySubscriber, QueryAnswer, QueryConsumingResult};
use ton_api::ton::{self, TLObject};
use ton_api::IntoBoxed;

pub use crate::config::*;
use crate::network::*;

mod config;
mod engine;
mod network;
mod utils;

pub async fn start(node_config: NodeConfig, global_config: GlobalConfig) -> Result<()> {
    let zero_state = global_config.zero_state.clone();

    let node_network = NodeNetwork::new(node_config, global_config).await?;

    let service = FullNodeOverlayService::new();
    node_network.add_masterchain_subscriber(service.clone());

    let overlay = node_network.start().await?;

    // loop {
    //     match overlay.wait_broadcast().await {
    //         Ok((broadcast, peer_id)) => {
    //             //log::warn!("Broadcast {:?} from {}", broadcast, peer_id)
    //             match broadcast {
    //                 ton::ton_node::Broadcast::TonNode_BlockBroadcast(block) => {
    //                     log::warn!("Got new block: {}", block.id.seqno);
    //                 }
    //                 _ => {}
    //             }
    //         }
    //         Err(e) => {
    //             log::error!("Failed to wait broadcast: {:?}", e);
    //         }
    //     }
    // }

    loop {
        log::info!("Fetching zerostate");
        match overlay.download_zero_state(&zero_state).await {
            Ok(Some((data, _))) => {
                log::warn!("{:#?}", data);
                break Ok(());
            }
            Ok(None) => {
                log::warn!("Zerostate not found");
            }
            Err(e) => {
                log::error!("Failed to load key blocks: {}", e);
            }
        };

        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // loop {
    //     log::info!("Fetching key block ids {}", block_id.seq_no);
    //     match overlay.download_next_key_blocks_ids(&block_id, 5).await {
    //         Ok(blocks) => {
    //             log::info!("Got key block ids for:");
    //             for block in blocks.iter() {
    //                 log::warn!("--- keyblock: {}", block.seq_no);
    //             }
    //
    //             if let Some(block) = blocks.last() {
    //                 block_id = block.clone();
    //             }
    //
    //             continue;
    //         }
    //         Err(e) => {
    //             log::error!("Failed to load key blocks: {}", e);
    //         }
    //     }
    //
    //     tokio::time::sleep(Duration::from_millis(10)).await;
    // }
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

async fn download_zero_state(
    client: &dyn FullNodeOverlayClient,
    block_id: &ton_block::BlockIdExt,
) -> Result<()> {
    loop {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
