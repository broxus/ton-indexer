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

    let mut archives_stream = ArchiveDownloader::new(engine, last_mc_block_id.seq_no + 1..);

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
        archive.accept_with_time(last_gen_utime);
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

    import_mc_blocks_with_apply(engine, maps.clone(), last_mc_block_id, last_gen_utime).await?;
    import_shard_blocks_with_apply(engine, maps).await?;

    let elapsed = import_start.elapsed().as_millis();
    log::info!("sync: Imported archive package for block {last_mc_block_id}. Took: {elapsed} ms");

    Ok(())
}

async fn import_mc_blocks_with_apply(
    engine: &Arc<Engine>,
    maps: Arc<BlockMaps>,
    mut last_mc_block_id: &ton_block::BlockIdExt,
    last_gen_utime: &mut u32,
) -> Result<()> {
    for id in maps.mc_block_ids.values() {
        if id.seq_no <= last_mc_block_id.seq_no {
            if id.seq_no == last_mc_block_id.seq_no && last_mc_block_id != id {
                return Err(SyncError::MasterchainBlockIdMismatch.into());
            }
            continue;
        }

        if id.seq_no != last_mc_block_id.seq_no + 1 {
            log::error!(
                "sync: Failed to apply mc block. Seqno: {}, expected: {}",
                id.seq_no,
                last_mc_block_id.seq_no + 1
            );
            return Err(SyncError::BlocksSkippedInArchive.into());
        }

        last_mc_block_id = id;
        if let Some(handle) = engine.load_block_handle(last_mc_block_id)? {
            if handle.meta().is_applied() {
                continue;
            }
        }

        let entry = maps.blocks.get(last_mc_block_id).unwrap();

        let (block, block_proof) = entry.get_data()?;
        let handle = save_block(engine, last_mc_block_id, block, block_proof, None).await?;

        *last_gen_utime = handle.meta().gen_utime();

        engine
            .apply_block_ext(&handle, block, last_mc_block_id.seq_no, false, 0)
            .await?;
    }

    log::info!("sync: Last applied masterchain block id: {last_mc_block_id}");
    Ok(())
}

async fn import_shard_blocks_with_apply(engine: &Arc<Engine>, maps: Arc<BlockMaps>) -> Result<()> {
    // Save all shardchain blocks
    for (id, entry) in &maps.blocks {
        if !id.shard_id.is_masterchain() {
            let (block, block_proof) = entry.get_data()?;
            save_block(engine, id, block, block_proof, None).await?;
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

    let mut ctx = BackgroundSyncContext {
        engine,
        to,
        prev_key_block_id: None,
    };

    let mut archive_downloader = ArchiveDownloader::new(engine, from..=to);
    while let Some(archive) = archive_downloader.recv().await {
        if ctx.handle(archive.clone()).await? == SyncStatus::Done {
            break;
        }
        archive.accept();
    }

    log::info!("sync: Background sync complete");
    Ok(())
}

struct BackgroundSyncContext<'a> {
    engine: &'a Arc<Engine>,
    to: u32,
    prev_key_block_id: Option<ton_block::BlockIdExt>,
}

impl BackgroundSyncContext<'_> {
    async fn handle(&mut self, maps: Arc<BlockMaps>) -> Result<SyncStatus> {
        let (lowest_id, highest_id) = match (maps.lowest_mc_id(), maps.highest_mc_id()) {
            (Some(lowest), Some(highest)) => (lowest, highest),
            _ => return Ok(SyncStatus::NoBlocksInArchive),
        };
        log::debug!("sync: Saving archive. Low id: {lowest_id}. High id: {highest_id}");

        for (id, entry) in &maps.blocks {
            let (block, proof) = entry.get_data()?;

            let handle = match self.engine.load_block_handle(block.id())? {
                Some(handle) => handle,
                None => profl::span!(
                    "save_background_sync_block",
                    save_block(
                        self.engine,
                        id,
                        block,
                        proof,
                        self.prev_key_block_id.as_ref()
                    )
                    .await
                    .context("Failed saving block")?
                ),
            };

            self.engine
                .notify_subscribers_with_archive_block(&handle, block)
                .await
                .context("Failed to process archive block")?;

            if handle.is_key_block() {
                self.prev_key_block_id = Some(id.clone());
            }
        }

        self.engine
            .db
            .background_sync_store()
            .store_background_sync_start(highest_id)?;
        log::info!("sync: Saved archive from {} to {}", lowest_id, highest_id);

        Ok({
            if highest_id.seq_no >= self.to {
                SyncStatus::Done
            } else {
                SyncStatus::InProgress
            }
        })
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum SyncStatus {
    Done,
    InProgress,
    NoBlocksInArchive,
}

async fn save_block(
    engine: &Arc<Engine>,
    block_id: &ton_block::BlockIdExt,
    block: &BlockStuffAug,
    block_proof: &BlockProofStuffAug,
    prev_key_block_id: Option<&ton_block::BlockIdExt>,
) -> Result<Arc<BlockHandle>> {
    if block_proof.is_link() {
        block_proof.check_proof_link()?;
    } else {
        let (virt_block, virt_block_info) = block_proof.pre_check_block_proof()?;
        let handle = match prev_key_block_id {
            Some(block_id) => engine
                .db
                .load_block_handle(block_id)?
                .context("Prev key block not found")?,
            None => {
                let prev_key_block_seqno = virt_block_info.prev_key_block_seqno();
                engine
                    .db
                    .load_key_block_handle(prev_key_block_seqno)
                    .context("Failed to load key block handle")?
            }
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
            None => from_seqno,
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
