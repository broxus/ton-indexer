use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use dashmap::DashSet;
use tiny_adnl::utils::*;
use tokio::sync::{mpsc, oneshot};

use crate::engine::Engine;
use crate::utils::*;

pub async fn download_state(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
    masterchain_block_id: &ton_block::BlockIdExt,
    active_peers: &Arc<DashSet<AdnlNodeIdShort>>,
) -> Result<Arc<ShardStateStuff>> {
    let overlay = engine
        .get_full_node_overlay(
            block_id.shard_id.workchain_id(),
            block_id.shard_id.shard_prefix_with_tag(),
        )
        .await?;

    let neighbour = loop {
        match overlay
            .check_persistent_state(block_id, masterchain_block_id, active_peers)
            .await
        {
            Ok(Some(peer)) => break peer,
            Ok(None) => {
                log::trace!("Failed to download state: state not found");
            }
            Err(e) => {
                log::trace!("Failed to download state: {}", e);
            }
        };
    };

    let (result_tx, result_rx) = oneshot::channel();
    let (packets_tx, packets_rx) = mpsc::channel(1);

    tokio::spawn({
        let engine = engine.clone();
        let block_id = block_id.clone();
        async move { result_tx.send(background_process(&engine, block_id, packets_rx).await) }
    });

    let max_size = 1 << 20;
    let mut offset = 0;
    let mut peer_attempt = 0;
    let mut part_attempt = 0;

    'outer: loop {
        log::info!("-------------------------- Downloading part: {}", offset);

        match overlay
            .download_persistent_state_part(
                block_id,
                masterchain_block_id,
                offset,
                max_size,
                neighbour.clone(),
                peer_attempt,
            )
            .await
        {
            Ok(part) => {
                part_attempt = 0;
                offset += max_size;

                let last = part.len() < max_size;

                if packets_tx.send(part).await.is_err() || last {
                    break 'outer;
                }
            }
            Err(e) => {
                part_attempt += 1;
                peer_attempt += 1;

                log::error!("Failed to download persistent state part: {}", e);
                if part_attempt > 10 {
                    return Err(DownloadStateError::RanOutOfAttempts.into());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    log::info!("DOWNLOADED: {} bytes", offset);

    let result = result_rx.await?;
    log::info!("RESULT: {:?}", result);

    // Ok(Arc::new(ShardStateStuff::deserialize(
    //     block_id.clone(),
    //     &state,
    // )?))
    todo!()
}

async fn background_process(
    engine: &Arc<Engine>,
    block_id: ton_block::BlockIdExt,
    mut packets_rx: PacketsRx,
) -> Result<()> {
    let mut transaction = engine.db.shard_state_storage().begin_replace().await?;

    let mut full = false;
    while let Some(packet) = packets_rx.recv().await {
        if transaction.process_packet(packet).await? {
            full = true;
            break;
        }
    }

    packets_rx.close();
    while packets_rx.recv().await.is_some() {}

    if !full {
        return Err(DownloadStateError::UnexpectedEof.into());
    }

    transaction.finalize(&block_id).await?;

    Ok(())
}

type PacketsRx = mpsc::Receiver<Vec<u8>>;

#[derive(thiserror::Error, Debug)]
enum DownloadStateError {
    #[error("Ran out of attempts")]
    RanOutOfAttempts,
    #[error("Unexpected eof")]
    UnexpectedEof,
}
