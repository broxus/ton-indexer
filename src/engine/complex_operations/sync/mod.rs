use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Context, Result};
use everscale_types::models::*;

use crate::engine::Engine;
use crate::storage::*;
use crate::utils::*;

use self::archives_stream::*;
use self::block_maps::*;
pub use self::historical_sync::*;

mod archive_writers_pool;
mod archives_stream;
mod block_maps;
mod historical_sync;

pub async fn sync(engine: &Arc<Engine>) -> Result<()> {
    tracing::info!(target: "sync", "started normal sync");

    let mut last_mc_block_id = engine.last_applied_block()?;
    tracing::info!(
        target: "sync",
        last_mc_block_id = %last_mc_block_id,
        "creating archives stream"
    );

    let mut archives = ArchivesStream::new(engine, last_mc_block_id.seqno + 1.., None);

    let mut last_gen_utime = 0;
    loop {
        let archive = archives.recv().await;
        if let Err(e) = import_package_with_apply(
            engine,
            archive.clone(),
            &last_mc_block_id,
            &mut last_gen_utime,
        )
        .await
        {
            tracing::error!(
                target: "sync",
                block_id = %last_mc_block_id,
                "failed to apply queued archive: {e:?}"
            );
            continue;
        }

        if engine.is_synced()? {
            break;
        }

        last_mc_block_id = engine.last_applied_block()?;
        archive.accept_with_time(last_gen_utime, None); // TODO
    }

    Ok(())
}

#[tracing::instrument(
    skip(engine, maps, last_mc_block_id),
    fields(%last_mc_block_id)
)]
async fn import_package_with_apply(
    engine: &Arc<Engine>,
    maps: Arc<BlockMaps>,
    last_mc_block_id: &BlockId,
    last_gen_utime: &mut u32,
) -> Result<()> {
    if maps.mc_block_ids.is_empty() {
        return Err(SyncError::EmptyArchivePackage.into());
    }

    let import_start = std::time::Instant::now();

    import_mc_blocks_with_apply(engine, &maps, last_mc_block_id, last_gen_utime).await?;
    import_shard_blocks_with_apply(engine, &maps).await?;

    let elapsed_ms = import_start.elapsed().as_millis();
    tracing::info!(
        target: "sync",
        block_id = %last_mc_block_id,
        elapsed_ms,
        "imported archive package"
    );
    Ok(())
}

async fn import_mc_blocks_with_apply(
    engine: &Arc<Engine>,
    maps: &BlockMaps,
    mut last_mc_block_id: &BlockId,
    last_gen_utime: &mut u32,
) -> Result<()> {
    let db = &engine.storage;

    for id in maps.mc_block_ids.values() {
        // Skip already processed blocks
        if id.seqno <= last_mc_block_id.seqno {
            if id.seqno == last_mc_block_id.seqno && last_mc_block_id != id {
                return Err(SyncError::MasterchainBlockIdMismatch.into());
            }
            continue;
        }

        // Ensure that we have all previous blocks
        if id.seqno != last_mc_block_id.seqno + 1 {
            tracing::error!(
                target: "sync",
                seqno = id.seqno,
                expected = last_mc_block_id.seqno + 1,
                "masterchain block seqno mismatch",
            );
            return Err(SyncError::BlocksSkippedInArchive.into());
        }

        // Move last applied mc block
        last_mc_block_id = id;

        // Skip already applied blocks
        if let Some(handle) = db.block_handle_storage().load_handle(id)? {
            if handle.meta().is_applied() {
                continue;
            }
        }

        // Save block
        let entry = maps
            .blocks
            .get(id)
            .context("Failed to get masterchain block entry")?;

        let (block, block_proof) = entry.get_data()?;
        let handle = engine.save_block(block, block_proof, id.seqno).await?;
        *last_gen_utime = handle.meta().gen_utime();

        // Apply block
        engine
            .apply_block_ext(&handle, block, id.seqno, false, 0)
            .await?;
    }

    let block_time_diff = {
        let diff = broxus_util::now().saturating_sub(*last_gen_utime);
        humantime::format_duration(std::time::Duration::from_secs(diff as u64))
    };

    tracing::info!(
        target: "sync",
        last_mc_block_id = %last_mc_block_id,
        %block_time_diff,
        "imported masterchain blocks from archive"
    );
    Ok(())
}

async fn import_shard_blocks_with_apply(engine: &Arc<Engine>, maps: &Arc<BlockMaps>) -> Result<()> {
    let db = &engine.storage;

    // Save all shardchain blocks
    for (id, entry) in &maps.blocks {
        if !id.shard.is_masterchain() {
            let (block, block_proof) = entry.get_data()?;
            engine.save_block(block, block_proof, 0).await?;
        }
    }

    // Iterate through all masterchain blocks in archive
    let mut last_applied_mc_block_id = engine.load_shards_client_mc_block_id()?;
    for mc_block_id in maps.mc_block_ids.values() {
        let mc_seqno = mc_block_id.seqno;
        if mc_seqno <= last_applied_mc_block_id.seqno {
            continue;
        }

        // Load masterchain block data and top shardchain blocks
        let masterchain_handle = db
            .block_handle_storage()
            .load_handle(mc_block_id)?
            .ok_or(SyncError::MasterchainBlockNotFound)?;
        let masterchain_block = db
            .block_storage()
            .load_block_data(&masterchain_handle)
            .await?;
        let shard_blocks = masterchain_block.shard_blocks()?;

        // Start applying blocks for each shard
        let mut tasks = Vec::with_capacity(shard_blocks.len());
        for (_, id) in shard_blocks {
            let engine = engine.clone();
            let maps = maps.clone();
            tasks.push(tokio::spawn(async move {
                let db = &engine.storage;

                let handle = db
                    .block_handle_storage()
                    .load_handle(&id)?
                    .ok_or(SyncError::ShardchainBlockHandleNotFound)?;

                // Skip applied blocks
                if handle.meta().is_applied() {
                    return Ok(());
                }

                // Special case for zerostate blocks
                if id.seqno == 0 {
                    engine.download_zero_state(&id).await?;
                    return Ok(());
                }

                // Get block data or load it from db
                let block_storage = db.block_storage();
                let block = match maps.blocks.get(&id) {
                    Some(entry) => match &entry.block {
                        Some(block) => Some(Cow::Borrowed(&block.data)),
                        None => block_storage
                            .load_block_data(&handle)
                            .await
                            .ok()
                            .map(Cow::Owned),
                    },
                    None => block_storage
                        .load_block_data(&handle)
                        .await
                        .ok()
                        .map(Cow::Owned),
                };

                // TODO:
                //  potential "too deep recursion" error
                //  if too many shardchain blocks are missing

                // Apply shardchain blocks recursively
                match block {
                    Some(block) => {
                        engine
                            .apply_block_ext(&handle, block.as_ref(), mc_seqno, false, 0)
                            .await
                    }
                    None => {
                        tracing::info!(target: "sync", mc_seqno, "downloading shardchain block");
                        engine
                            .download_and_apply_block(handle.id(), mc_seqno, false, 0)
                            .await
                    }
                }
            }));
        }

        futures_util::future::try_join_all(tasks)
            .await?
            .into_iter()
            .find(|item| item.is_err())
            .unwrap_or(Ok(()))?;

        engine.store_shards_client_mc_block_id(mc_block_id)?;
        last_applied_mc_block_id = mc_block_id.clone();
    }

    Ok(())
}

impl Engine {
    fn last_applied_block(&self) -> Result<BlockId> {
        let mc_block_id = self.load_last_applied_mc_block_id()?;
        let sc_block_id = self.load_shards_client_mc_block_id()?;

        Ok(match (mc_block_id, sc_block_id) {
            (mc_block_id, sc_block_id) if mc_block_id.seqno > sc_block_id.seqno => sc_block_id,
            (mc_block_id, _) => mc_block_id,
        })
    }

    async fn save_block(
        &self,
        block: &BlockStuffAug,
        block_proof: &BlockProofStuffAug,
        mc_seqno: u32,
    ) -> Result<Arc<BlockHandle>> {
        let block_storage = self.storage.block_storage();

        let info = self.check_block_proof(block_proof).await?;

        let handle = block_storage
            .store_block_data(block, info.with_mc_seqno(mc_seqno))
            .await?
            .handle;
        let handle = block_storage
            .store_block_proof(block_proof, handle.into())
            .await?
            .handle;

        Ok(handle)
    }
}

#[derive(thiserror::Error, Debug)]
enum SyncError {
    #[error("Empty archive package")]
    EmptyArchivePackage,
    #[error("Masterchain block id mismatch")]
    MasterchainBlockIdMismatch,
    #[error("Some blocks are missing in archive")]
    BlocksSkippedInArchive,
    #[error("Masterchain block not found")]
    MasterchainBlockNotFound,
    #[error("Shardchain block handle not found")]
    ShardchainBlockHandleNotFound,
}
