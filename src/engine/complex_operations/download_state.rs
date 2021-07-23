use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use dashmap::DashSet;
use tiny_adnl::utils::*;

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

    let mut transaction = engine.db.shard_state_storage().begin_replace().await?;

    let mut offset = 0;
    let mut state = Vec::new();
    let max_size = 1 << 20;
    let mut total_size = usize::MAX;
    let mut peer_attempt = 0;
    let mut part_attempt = 0;

    'outer: while offset < total_size {
        loop {
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
                    let part_len = part.len();
                    let last = part_len < max_size;

                    transaction.process(part, last)?;

                    if last {
                        total_size = offset + part_len;
                        break 'outer;
                    }

                    offset += max_size;
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
    }

    log::info!("DOWNLOADED: {} bytes", total_size);

    transaction.finalize(block_id, todo!())?;

    debug_assert!(total_size < usize::MAX);

    Ok(Arc::new(ShardStateStuff::deserialize(
        block_id.clone(),
        &state,
    )?))
}

#[derive(thiserror::Error, Debug)]
enum DownloadStateError {
    #[error("Ran out of attempts")]
    RanOutOfAttempts,
}
