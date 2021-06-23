mod block;
mod config;
mod network;
mod utils;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tiny_adnl::utils::*;
use tiny_adnl::{OverlaySubscriber, QueryAnswer, QueryConsumingResult};
use ton_api::ton::{self, TLObject};
use ton_api::IntoBoxed;

pub use crate::config::Config;
use crate::network::{FullNodeOverlayClient, NodeNetwork};

struct FullNodeService;

#[async_trait::async_trait]
impl OverlaySubscriber for FullNodeService {
    async fn try_consume_query(
        &self,
        _local_id: &AdnlNodeIdShort,
        _peer_id: &AdnlNodeIdShort,
        query: TLObject,
    ) -> Result<QueryConsumingResult> {
        log::info!("Got query: {:?}", query);

        let query = match query.downcast::<ton::rpc::ton_node::GetCapabilities>() {
            Ok(_) => {
                log::warn!("Got capabilities query");

                let answer = TLObject::new(
                    ton::ton_node::capabilities::Capabilities {
                        version: 2,
                        capabilities: 1,
                    }
                    .into_boxed(),
                );

                return Ok(QueryConsumingResult::Consumed(Some(QueryAnswer::Object(
                    answer,
                ))));
            }
            Err(query) => query,
        };

        Ok(QueryConsumingResult::Consumed(None))
    }
}

pub async fn start(config: Config) -> Result<()> {
    let zero_state = config.zero_state.clone();

    let node_network = NodeNetwork::new(config).await?;

    let service = Arc::new(FullNodeService);
    node_network.add_masterchain_subscriber(service.clone());

    let overlay = node_network.start().await?;

    loop {
        match overlay.wait_broadcast().await {
            Ok((broadcast, peer_id)) => {
                //log::warn!("Broadcast {:?} from {}", broadcast, peer_id)
                match broadcast {
                    ton::ton_node::Broadcast::TonNode_BlockBroadcast(block) => {
                        log::warn!("Got new block: {}", block.id.seqno);
                    }
                    _ => {}
                }
            }
            Err(e) => {
                log::error!("Failed to wait broadcast: {:?}", e);
            }
        }
    }

    // let mut block_id = ton_block::BlockIdExt {
    //     shard_id: ton_block::ShardIdent::with_tagged_prefix(
    //         zero_state.workchain,
    //         zero_state.shard as u64,
    //     )
    //     .unwrap(),
    //     seq_no: zero_state.seqno as u32,
    //     root_hash: zero_state.root_hash.into(),
    //     file_hash: zero_state.file_hash.into(),
    // };
    //
    // loop {
    //     log::info!("Downloading block {}", block_id.seq_no);
    //     match overlay.download_next_key_blocks_ids(&block_id, 5).await {
    //         Ok(blocks) => {
    //             log::info!("Downloaded key blocks:");
    //             for block in blocks.iter() {
    //                 log::warn!("--- keyblock id: {}", block.seq_no);
    //             }
    //
    //             if let Some(block) = blocks.last() {
    //                 block_id = block.clone();
    //             }
    //
    //             blocks.into_iter().map(|block_id| {
    //                 overlay.download_next_block_full(&block_id)
    //             }).collect::<futures::stream::FuturesUnordered>()
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
