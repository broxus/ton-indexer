use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::engine::Engine;
use crate::storage::*;
use crate::utils::*;

use self::archive_downloader::*;
use self::block_maps::*;

mod archive_downloader;
mod archive_writers_pool;
mod block_maps;

pub async fn sync(engine: &Arc<Engine>) -> Result<()> {
    log::info!("sync: Started sync");

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
        if let Some(handle) = engine.load_block_handle(id)? {
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
        let handle = save_block(engine, id, block, block_proof).await?;
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
    // Save all shardchain blocks
    for (id, entry) in &maps.blocks {
        if !id.shard_id.is_masterchain() {
            let (block, block_proof) = entry.get_data()?;
            save_block(engine, id, block, block_proof).await?;
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
        let masterchain_handle = engine
            .load_block_handle(mc_block_id)?
            .ok_or(SyncError::MasterchainBlockNotFound)?;
        let masterchain_block = engine.load_block_data(&masterchain_handle).await?;
        let shard_blocks = masterchain_block.shard_blocks()?;

        // Start applying blocks for each shard
        let mut tasks = Vec::with_capacity(shard_blocks.len());
        for (_, id) in shard_blocks {
            let engine = engine.clone();
            let maps = maps.clone();
            tasks.push(tokio::spawn(async move {
                let handle = engine
                    .load_block_handle(&id)?
                    .ok_or(SyncError::ShardchainBlockHandleNotFound)?;

                // Skip applied blocks
                if handle.meta().is_applied() {
                    return Ok(());
                }

                // Special case for zerostate blocks
                if id.seq_no == 0 {
                    super::boot::download_zero_state(&engine, &id).await?;
                    return Ok(());
                }

                // Get block data or load it from db
                let block = match maps.blocks.get(&id) {
                    Some(entry) => match &entry.block {
                        Some(block) => Some(Cow::Borrowed(&block.data)),
                        None => engine.load_block_data(&handle).await.ok().map(Cow::Owned),
                    },
                    None => engine.load_block_data(&handle).await.ok().map(Cow::Owned),
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

pub async fn background_sync(engine: &Arc<Engine>, from_seqno: u32) -> Result<()> {
    let (from, to) = engine.background_sync_range(from_seqno)?;

    log::info!("sync: Started background sync from {from} to {to}");

    let mut ctx = BackgroundSyncContext { engine, from, to };

    let mut archive_downloader = ArchiveDownloader::new(engine, from..=to, None);
    while let Some(archive) = archive_downloader.recv().await {
        match ctx.handle(archive.clone()).await? {
            SyncStatus::Done => break,
            SyncStatus::InProgress(edge) => archive.accept(Some(edge)),
            SyncStatus::NoBlocksInArchive => continue,
        }
    }

    log::info!("sync: Background sync complete");
    Ok(())
}

struct BackgroundSyncContext<'a> {
    engine: &'a Arc<Engine>,
    from: u32,
    to: u32,
}

impl BackgroundSyncContext<'_> {
    async fn handle(&mut self, maps: Arc<BlockMaps>) -> Result<SyncStatus> {
        let (lowest_id, highest_id) = match (maps.lowest_mc_id(), maps.highest_mc_id()) {
            (Some(lowest), Some(highest)) => (lowest, highest),
            _ => return Ok(SyncStatus::NoBlocksInArchive),
        };
        log::debug!("sync: Saving archive. Low id: {lowest_id}. High id: {highest_id}");

        let block_edge = maps.build_block_maps_edge(highest_id)?;

        self.import_mc_blocks(&maps).await?;
        self.import_shard_blocks(&maps).await?;

        self.engine
            .db
            .background_sync_store()
            .store_background_sync_start(highest_id)?;
        log::info!("sync: Saved archive from {} to {}", lowest_id, highest_id);

        Ok({
            if highest_id.seq_no >= self.to {
                SyncStatus::Done
            } else {
                SyncStatus::InProgress(block_edge)
            }
        })
    }

    async fn import_mc_blocks(&mut self, maps: &Arc<BlockMaps>) -> Result<()> {
        for id in maps.mc_block_ids.values() {
            // Skip already saved blocks
            if id.seq_no <= self.from {
                continue;
            }
            // Skip blocks after specified range
            if id.seq_no > self.to {
                break;
            }

            // Save block
            let entry = maps
                .blocks
                .get(id)
                .context("sync: Failed to get masterchain block entry")?;

            let (block, block_proof) = entry.get_data()?;
            let handle = save_block(self.engine, id, block, block_proof).await?;

            // Assign valid masterchain id
            self.engine.db.assign_mc_ref_seq_no(&handle, id.seq_no)?;

            // Archive block
            if self.engine.archive_options.is_some() {
                self.engine.db.archive_block(&handle).await?;
            }

            // Notify subscribers
            self.engine
                .notify_subscribers_with_archive_block(&handle, &block.data)
                .await?;
        }

        // Done
        Ok(())
    }

    async fn import_shard_blocks(&mut self, maps: &Arc<BlockMaps>) -> Result<()> {
        for (id, entry) in &maps.blocks {
            if !id.shard_id.is_masterchain() {
                let (block, block_proof) = entry.get_data()?;
                save_block(self.engine, id, block, block_proof).await?;
            }
        }

        for mc_block_id in maps.mc_block_ids.values() {
            let mc_seq_no = mc_block_id.seq_no;
            // Skip already saved blocks
            if mc_seq_no <= self.from {
                continue;
            }
            // Skip blocks after specified range
            if mc_seq_no > self.to {
                break;
            }

            let mc_handle = self
                .engine
                .load_block_handle(mc_block_id)?
                .ok_or(SyncError::MasterchainBlockNotFound)?;
            let mc_block = self.engine.load_block_data(&mc_handle).await?;
            let shard_blocks = mc_block.shard_blocks()?;

            let mut tasks = Vec::with_capacity(shard_blocks.len());
            for (_, id) in shard_blocks {
                let engine = self.engine.clone();
                let maps = maps.clone();
                tasks.push(tokio::spawn(async move {
                    let handle = engine
                        .load_block_handle(&id)?
                        .ok_or(SyncError::ShardchainBlockHandleNotFound)?;

                    let block = maps
                        .blocks
                        .get(&id)
                        .and_then(|entry| entry.block.as_ref())
                        .ok_or(SyncError::ShardchainBlockHandleNotFound)?;

                    // Assign valid masterchain id
                    engine.db.assign_mc_ref_seq_no(&handle, id.seq_no)?;

                    // Archive block
                    if engine.archive_options.is_some() {
                        engine.db.archive_block(&handle).await?;
                    }

                    // Notify subscribers
                    engine
                        .notify_subscribers_with_archive_block(&handle, &block.data)
                        .await?;

                    Result::<_, anyhow::Error>::Ok(())
                }));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
enum SyncStatus {
    Done,
    InProgress(BlockMapsEdge),
    NoBlocksInArchive,
}

async fn save_block(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
    block: &BlockStuffAug,
    block_proof: &BlockProofStuffAug,
) -> Result<Arc<BlockHandle>> {
    if block_proof.is_link() {
        block_proof.check_proof_link()?;
    } else {
        let (virt_block, virt_block_info) = block_proof.pre_check_block_proof()?;
        let handle = {
            let prev_key_block_seqno = virt_block_info.prev_key_block_seqno();
            engine
                .db
                .load_key_block_handle(prev_key_block_seqno)
                .context("Failed to load key block handle")?
        };

        if handle.id().seq_no == 0 {
            let zero_state = engine
                .load_mc_zero_state()
                .await
                .context("Failed to load mc zero state")?;
            block_proof.check_with_master_state(&zero_state)?;
        } else {
            let prev_key_block_proof = engine
                .load_block_proof(&handle, false)
                .await
                .context("Failed to load prev key block proof")?;
            if let Err(e) = check_with_prev_key_block_proof(
                block_proof,
                &prev_key_block_proof,
                &virt_block,
                &virt_block_info,
            ) {
                if !engine.is_hard_fork(handle.id()) {
                    return Err(e);
                }
                log::warn!("Received hard fork block {}. Ignoring proof", handle.id());
            };
        }
    }

    let handle = engine.store_block_data(block).await?.handle;
    let handle = engine
        .store_block_proof(block_id, Some(handle), block_proof)
        .await?
        .handle;
    Ok(handle)
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

    fn background_sync_range(&self, from_seqno: u32) -> Result<(u32, u32)> {
        let store = self.db.background_sync_store();

        log::info!("sync: Download previous history sync since block {from_seqno}");

        let low = match store.load_background_sync_start()? {
            Some(low) => {
                log::warn!(
                    "sync: Ignoring `from_seqno` param, using saved seqno: {}",
                    low.seq_no
                );
                low.seq_no
            }
            None => from_seqno.saturating_sub(1),
        };

        let high = store.load_background_sync_end()?.seq_no;

        Ok((low, high))
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
