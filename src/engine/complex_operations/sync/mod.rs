use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::db::*;
use crate::engine::Engine;
use crate::utils::*;

use self::archive_downloader::*;
use self::block_maps::*;
pub use self::historical_sync::*;

mod archive_downloader;
mod archive_writers_pool;
mod block_maps;
mod historical_sync;

pub async fn sync(engine: &Arc<Engine>) -> Result<()> {
    log::info!("sync: Started normal sync");

    let mut last_mc_block_id = engine.last_applied_block()?;
    log::info!("sync: Creating archives stream from {last_mc_block_id}");

    let mut archives_stream = ArchiveDownloader::new(engine, last_mc_block_id.seq_no + 1.., None);

    let mut last_gen_utime = 0;
    while let Some(archive) = archives_stream.recv().await {
        if let Err(e) = import_package_with_apply(
            engine,
            archive.clone(),
            &last_mc_block_id,
            &mut last_gen_utime,
        )
        .await
        {
            log::error!("sync: Failed to apply queued archive for block {last_mc_block_id}: {e:?}");
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

async fn import_package_with_apply(
    engine: &Arc<Engine>,
    maps: Arc<BlockMaps>,
    last_mc_block_id: &ton_block::BlockIdExt,
    last_gen_utime: &mut u32,
) -> Result<()> {
    if maps.mc_block_ids.is_empty() {
        return Err(SyncError::EmptyArchivePackage.into());
    }

    let import_start = std::time::Instant::now();

    import_mc_blocks_with_apply(engine, &maps, last_mc_block_id, last_gen_utime).await?;
    import_shard_blocks_with_apply(engine, &maps).await?;

    let elapsed = import_start.elapsed().as_millis();
    log::info!("sync: Imported archive package for block {last_mc_block_id}. Took: {elapsed} ms");

    Ok(())
}

async fn import_mc_blocks_with_apply(
    engine: &Arc<Engine>,
    maps: &BlockMaps,
    mut last_mc_block_id: &ton_block::BlockIdExt,
    last_gen_utime: &mut u32,
) -> Result<()> {
    let db = &engine.db;

    for id in maps.mc_block_ids.values() {
        // Skip already processed blocks
        if id.seq_no <= last_mc_block_id.seq_no {
            if id.seq_no == last_mc_block_id.seq_no && last_mc_block_id != id {
                return Err(SyncError::MasterchainBlockIdMismatch.into());
            }
            continue;
        }

        // Ensure that we have all previous blocks
        if id.seq_no != last_mc_block_id.seq_no + 1 {
            log::error!(
                "sync: Failed to apply mc block. Seqno: {}, expected: {}",
                id.seq_no,
                last_mc_block_id.seq_no + 1
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
            .context("sync: Failed to get masterchain block entry")?;

        let (block, block_proof) = entry.get_data()?;
        let handle = engine.save_block(block, block_proof, id.seq_no).await?;
        *last_gen_utime = handle.meta().gen_utime();

        // Apply block
        engine
            .apply_block_ext(&handle, block, id.seq_no, false, 0)
            .await?;
    }

    log::info!("sync: Last applied masterchain block id: {last_mc_block_id}");
    Ok(())
}

async fn import_shard_blocks_with_apply(engine: &Arc<Engine>, maps: &Arc<BlockMaps>) -> Result<()> {
    let db = &engine.db;

    // Save all shardchain blocks
    for (id, entry) in &maps.blocks {
        if !id.shard_id.is_masterchain() {
            let (block, block_proof) = entry.get_data()?;
            engine.save_block(block, block_proof, 0).await?;
        }
    }

    // Iterate through all masterchain blocks in archive
    let mut last_applied_mc_block_id = engine.load_shards_client_mc_block_id()?;
    for mc_block_id in maps.mc_block_ids.values() {
        let mc_seq_no = mc_block_id.seq_no;
        if mc_seq_no <= last_applied_mc_block_id.seq_no {
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
                let db = &engine.db;

                let handle = db
                    .block_handle_storage()
                    .load_handle(&id)?
                    .ok_or(SyncError::ShardchainBlockHandleNotFound)?;

                // Skip applied blocks
                if handle.meta().is_applied() {
                    return Ok(());
                }

                // Special case for zerostate blocks
                if id.seq_no == 0 {
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
                            .apply_block_ext(&handle, block.as_ref(), mc_seq_no, false, 0)
                            .await
                    }
                    None => {
                        log::info!("sync: Downloading shardchain block for {}", mc_seq_no);
                        engine
                            .download_and_apply_block(handle.id(), mc_seq_no, false, 0)
                            .await
                    }
                }
            }));
        }

        futures::future::try_join_all(tasks)
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
    fn last_applied_block(&self) -> Result<ton_block::BlockIdExt> {
        let mc_block_id = self.load_last_applied_mc_block_id()?;
        let sc_block_id = self.load_shards_client_mc_block_id()?;
        log::info!("sync: Last applied block id: {}", mc_block_id);
        log::info!("sync: Last shards client block id: {}", sc_block_id);

        Ok(match (mc_block_id, sc_block_id) {
            (mc_block_id, sc_block_id) if mc_block_id.seq_no > sc_block_id.seq_no => sc_block_id,
            (mc_block_id, _) => mc_block_id,
        })
    }

    async fn save_block(
        &self,
        block: &BlockStuffAug,
        block_proof: &BlockProofStuffAug,
        mc_seq_no: u32,
    ) -> Result<Arc<BlockHandle>> {
        let block_storage = self.db.block_storage();

        let info = self.check_block_proof(block_proof).await?;

        let handle = block_storage
            .store_block_data(block, info.with_mc_seq_no(mc_seq_no))
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
