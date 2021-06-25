use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tiny_adnl::utils::*;
use tiny_adnl::{OverlaySubscriber, QueryAnswer, QueryConsumingResult};
use ton_api::ton::{self, TLObject};
use ton_api::IntoBoxed;

pub use crate::config::Config;
use crate::network::*;

mod config;
mod network;
mod utils;

pub async fn start(config: Config) -> Result<()> {
    let zero_state = config.zero_state.clone();

    let node_network = NodeNetwork::new(config).await?;

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

    let mut block_id = ton_block::BlockIdExt {
        shard_id: ton_block::ShardIdent::with_tagged_prefix(
            zero_state.workchain,
            zero_state.shard as u64,
        )
        .unwrap(),
        seq_no: zero_state.seqno as u32,
        root_hash: zero_state.root_hash.into(),
        file_hash: zero_state.file_hash.into(),
    };

    loop {
        log::info!("Fetching zerostate");
        match overlay.download_zero_state(&block_id).await {
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
