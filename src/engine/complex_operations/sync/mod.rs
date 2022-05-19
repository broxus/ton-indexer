use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Context, Result};
use ton_types::FxDashSet;

use crate::db::*;
use crate::engine::Engine;
use crate::utils::*;

use self::archive_downloader::*;
use self::block_maps::*;

mod archive_downloader;
mod archive_writers_pool;
mod block_maps;

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
        let handle = save_block(engine, block, block_proof, id.seq_no).await?;
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
            save_block(engine, block, block_proof, 0).await?;
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

pub async fn background_sync(engine: &Arc<Engine>, from_seqno: u32) -> Result<()> {
    let (from, to) = engine.background_sync_range(from_seqno)?;
    if from + 1 >= to {
        return Ok(());
    }

    log::info!("sync: Started background sync from {from} to {to}");

    let mut ctx = BackgroundSyncContext::new(engine, from, to);

    let mut archive_downloader = ArchiveDownloader::new(engine, from..=to, None);
    while let Some(archive) = archive_downloader.recv().await {
        match ctx.handle(archive.clone()).await {
            Ok(SyncStatus::Done) => break,
            Ok(_) => {
                archive.accept(ctx.last_archive_edge.clone());
            }
            Err(e) => {
                log::error!("sync: Failed to save archive: {e:?}");
            }
        }
    }

    log::info!("sync: Background sync complete");
    Ok(())
}

struct BackgroundSyncContext<'a> {
    engine: &'a Arc<Engine>,
    last_archive_edge: Option<BlockMapsEdge>,
    from: u32,
    to: u32,
}

impl<'a> BackgroundSyncContext<'a> {
    fn new(engine: &'a Arc<Engine>, from: u32, to: u32) -> Self {
        Self {
            engine,
            last_archive_edge: None,
            from,
            to,
        }
    }

    async fn handle(&mut self, maps: Arc<BlockMaps>) -> Result<SyncStatus> {
        let (lowest_id, highest_id) = match (maps.lowest_mc_id(), maps.highest_mc_id()) {
            (Some(lowest), Some(highest)) => (lowest, highest),
            _ => return Ok(SyncStatus::NoBlocksInArchive),
        };
        log::debug!("sync: Saving archive. Low id: {lowest_id}. High id: {highest_id}");

        let mut block_edge = maps.build_block_maps_edge(lowest_id)?;

        self.import_blocks(&maps, &mut block_edge).await?;
        log::info!("sync: Saved archive from {} to {}", lowest_id, highest_id);

        Ok({
            if highest_id.seq_no >= self.to {
                SyncStatus::Done
            } else {
                self.last_archive_edge = Some(block_edge);
                SyncStatus::InProgress
            }
        })
    }

    async fn import_blocks(
        &mut self,
        maps: &Arc<BlockMaps>,
        edge: &mut BlockMapsEdge,
    ) -> Result<()> {
        let block_handle_storage = self.engine.db.block_handle_storage();
        let block_storage = self.engine.db.block_storage();
        let node_state = self.engine.db.node_state();

        let splits = Arc::new(FxDashSet::default());

        for mc_block_id in maps.mc_block_ids.values() {
            let mc_seq_no = mc_block_id.seq_no;
            // Skip blocks after specified range
            if mc_seq_no > self.to {
                break;
            }

            // Save block
            let entry = maps
                .blocks
                .get(mc_block_id)
                .context("sync: Failed to get masterchain block entry")?;

            let (mc_block, mc_block_proof) = entry.get_data()?;
            let mc_info = self.engine.check_block_proof(mc_block_proof).await?;

            let shard_blocks = mc_block.shard_blocks()?;

            let new_edge = BlockMapsEdge {
                mc_block_seq_no: mc_seq_no,
                top_shard_blocks: shard_blocks
                    .iter()
                    .map(|(key, id)| (*key, id.seq_no))
                    .collect(),
            };

            // Skip already saved blocks
            if mc_seq_no <= self.from {
                *edge = new_edge;
                continue;
            }

            let (mc_handle, _) = block_handle_storage
                .create_or_load_handle(mc_block.id(), mc_info.with_mc_seq_no(mc_seq_no))?;

            // Archive block
            if self.engine.archive_options.is_some() {
                block_storage.move_into_archive_with_data(
                    &mc_handle,
                    mc_block_proof.is_link(),
                    mc_block.new_archive_data()?,
                    mc_block_proof.new_archive_data()?,
                )?;
            }

            // Notify subscribers
            self.engine
                .notify_subscribers_with_archive_block(
                    &mc_handle,
                    &mc_block.data,
                    mc_block.new_archive_data()?,
                    mc_block_proof.new_archive_data()?,
                )
                .await?;

            splits.clear();
            let mut tasks = Vec::with_capacity(shard_blocks.len());
            for (_, id) in shard_blocks {
                // Skip blocks which were referenced in previous mc block (no new blocks werMe
                // produced in this shard)
                if !edge.is_before(&id) {
                    continue;
                }

                let engine = self.engine.clone();
                let splits = splits.clone();
                let maps = maps.clone();
                let edge = edge.clone();
                tasks.push(tokio::spawn(async move {
                    let block_handle_storage = engine.db.block_handle_storage();
                    let block_storage = engine.db.block_storage();

                    let mut blocks_to_add = Vec::new();

                    // For each block starting from the latest one (which was referenced by the mc block)
                    let mut stack = Vec::from([id]);
                    while let Some(id) = stack.pop() {
                        let (block_data, block_proof) = maps
                            .blocks
                            .get(&id)
                            .map(|entry| entry.get_data())
                            .transpose()?
                            .ok_or(SyncError::IncompleteBlockData)?;
                        let info = engine.check_block_proof(block_proof).await?;

                        let (prev1, prev2) = block_data.data.construct_prev_id()?;

                        if info.after_split && !splits.insert(prev1.clone()) {
                            // Two blocks from different shards, referenced by the same mc block,
                            // could have the same ancestor in case of split. So when we find
                            // a block after split, we check if it's already processed and skip
                            // all preceding blocks in that case.
                            continue;
                        }

                        // Push left predecessor
                        if edge.is_before(&prev1) {
                            stack.push(prev1);
                        }

                        // Push right predecessor
                        if let Some(prev2) = prev2 {
                            if edge.is_before(&prev2) {
                                stack.push(prev2);
                            }
                        }

                        blocks_to_add.push((info, block_data, block_proof));
                    }

                    // Sort blocks by time (to increase processing locality) and seqno
                    blocks_to_add.sort_unstable_by_key(|(info, block_data, _)| {
                        (info.gen_utime, block_data.data.id().seq_no)
                    });

                    // Apply blocks
                    for (info, block, block_proof) in blocks_to_add {
                        let (handle, _) = block_handle_storage
                            .create_or_load_handle(block.id(), info.with_mc_seq_no(mc_seq_no))?;

                        // Archive block
                        if engine.archive_options.is_some() {
                            block_storage.move_into_archive_with_data(
                                &handle,
                                block_proof.is_link(),
                                block.new_archive_data()?,
                                block_proof.new_archive_data()?,
                            )?;
                        }

                        // Notify subscribers
                        engine
                            .notify_subscribers_with_archive_block(
                                &handle,
                                &block.data,
                                block.new_archive_data()?,
                                block_proof.new_archive_data()?,
                            )
                            .await?;
                    }

                    Result::<_, anyhow::Error>::Ok(())
                }));
            }

            futures::future::try_join_all(tasks)
                .await?
                .into_iter()
                .find(|item| item.is_err())
                .unwrap_or(Ok(()))?;

            *edge = new_edge;

            node_state.store_background_sync_start(mc_block_id)?;
        }

        Ok(())
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum SyncStatus {
    Done,
    InProgress,
    NoBlocksInArchive,
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
        let state = self.db.node_state();

        let low = match state.load_background_sync_start()? {
            Some(low) => low.seq_no,
            None => from_seqno.saturating_sub(1),
        };

        let high = state.load_background_sync_end()?.seq_no;

        Ok((low, high))
    }
}

async fn save_block(
    engine: &Arc<Engine>,
    block: &BlockStuffAug,
    block_proof: &BlockProofStuffAug,
    mc_seq_no: u32,
) -> Result<Arc<BlockHandle>> {
    let info = engine.check_block_proof(block_proof).await?;

    let block_storage = engine.db.block_storage();
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
    #[error("Incomplete block data")]
    IncompleteBlockData,
}
