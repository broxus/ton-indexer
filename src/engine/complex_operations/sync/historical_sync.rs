use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::{Context, Result};
use ton_types::FxDashSet;

use crate::engine::Engine;

use super::archive_downloader::*;
use super::block_maps::*;

pub async fn historical_sync(engine: &Arc<Engine>, from_seqno: u32) -> Result<()> {
    let (from, to) = engine.historical_sync_range(from_seqno)?;
    if from + 1 >= to {
        return Ok(());
    }

    log::info!("sync: Started historical sync from {from} to {to}");

    let mut ctx = HistoricalSyncContext::new(engine, from, to);

    let mut archive_downloader = ArchiveDownloader::new(engine, from..=to, None);
    while let Some(archive) = archive_downloader.recv().await {
        match ctx.handle(archive.clone()).await {
            Ok(ControlFlow::Break(())) => break,
            Ok(_) => {
                archive.accept(ctx.last_archive_edge.clone());
            }
            Err(e) => {
                log::error!("sync: Failed to save archive: {e:?}");
            }
        }
    }

    log::info!("sync: Historical sync complete");
    Ok(())
}

struct HistoricalSyncContext<'a> {
    engine: &'a Arc<Engine>,
    last_archive_edge: Option<BlockMapsEdge>,
    from: u32,
    to: u32,
}

impl<'a> HistoricalSyncContext<'a> {
    fn new(engine: &'a Arc<Engine>, from: u32, to: u32) -> Self {
        Self {
            engine,
            last_archive_edge: None,
            from,
            to,
        }
    }

    async fn handle(&mut self, maps: Arc<BlockMaps>) -> Result<ControlFlow<()>> {
        let (lowest_id, highest_id) = match (maps.lowest_mc_id(), maps.highest_mc_id()) {
            (Some(lowest), Some(highest)) => (lowest, highest),
            _ => return Err(HistoricalSyncError::EmptyArchivePackage.into()),
        };
        log::debug!("sync: Saving archive. Low id: {lowest_id}. High id: {highest_id}");

        let mut block_edge = maps.build_block_maps_edge(lowest_id)?;

        self.process_blocks(&maps, &mut block_edge).await?;
        log::info!("sync: Saved archive from {lowest_id} to {highest_id}");

        Ok({
            if highest_id.seq_no >= self.to {
                ControlFlow::Break(())
            } else {
                self.last_archive_edge = Some(block_edge);
                ControlFlow::Continue(())
            }
        })
    }

    async fn process_blocks(
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
                            .ok_or(HistoricalSyncError::IncompleteBlockData)?;
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

            node_state.store_historical_sync_start(mc_block_id)?;
        }

        Ok(())
    }
}

impl Engine {
    fn historical_sync_range(&self, from_seqno: u32) -> Result<(u32, u32)> {
        let state = self.db.node_state();

        let low = match state.load_historical_sync_start()? {
            Some(low) => low.seq_no,
            None => from_seqno.saturating_sub(1),
        };

        let high = state.load_historical_sync_end()?.seq_no;

        Ok((low, high))
    }
}

#[derive(Debug, thiserror::Error)]
enum HistoricalSyncError {
    #[error("Empty archive package")]
    EmptyArchivePackage,
    #[error("Incomplete block data")]
    IncompleteBlockData,
}
