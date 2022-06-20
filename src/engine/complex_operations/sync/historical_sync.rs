use std::ops::ControlFlow;
use std::sync::Arc;

use anyhow::Result;
use ton_types::FxDashSet;

use super::archives_stream::*;
use super::block_maps::*;
use crate::engine::Engine;
use crate::utils::*;

pub async fn historical_sync(engine: &Arc<Engine>, from_seqno: u32) -> Result<()> {
    let (from, to) = engine.historical_sync_range(from_seqno)?;
    if from + 1 >= to {
        return Ok(());
    }

    log::info!("sync: Started historical sync from {from} to {to}");

    let mut ctx = HistoricalSyncContext::new(engine, from, to);

    let mut archives = ArchivesStream::new(engine, from..=to, None);
    while let Some(archive) = archives.recv().await {
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
        let node_state = self.engine.db.node_state();

        let splits = Arc::new(FxDashSet::default());

        for mc_block_id in maps.mc_block_ids.values() {
            let mc_seq_no = mc_block_id.seq_no;
            // Skip blocks after specified range
            if mc_seq_no > self.to {
                break;
            }

            // Save block
            let (info, block, proof) = self.engine.prepare_archive_block(maps, mc_block_id).await?;

            let shard_blocks = block.shard_blocks()?;
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

            self.engine
                .save_archive_block(info, block, proof, mc_seq_no)
                .await?;

            splits.clear();
            let mut tasks = Vec::with_capacity(shard_blocks.len());
            for (_, id) in shard_blocks {
                // Skip blocks which were referenced in previous mc block (no new blocks were
                // produced in this shard)
                if !edge.is_before(&id) {
                    continue;
                }

                let engine = self.engine.clone();
                let splits = splits.clone();
                let maps = maps.clone();
                let edge = edge.clone();
                tasks.push(tokio::spawn(async move {
                    let mut blocks_to_add = Vec::new();

                    // For each block starting from the latest one (which was referenced by the mc block)
                    let mut stack = Vec::from([id]);
                    while let Some(id) = stack.pop() {
                        let (info, block, proof) = engine.prepare_archive_block(&maps, &id).await?;
                        let (prev1, prev2) = block.data.construct_prev_id()?;

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

                        blocks_to_add.push((info, block, proof));
                    }

                    // Sort blocks by time (to increase processing locality) and seqno
                    blocks_to_add.sort_unstable_by_key(|(info, block_data, _)| {
                        (info.gen_utime, block_data.data.id().seq_no)
                    });

                    // Apply blocks
                    for (info, block, block_proof) in blocks_to_add {
                        engine
                            .save_archive_block(info, block, block_proof, mc_seq_no)
                            .await?;
                    }

                    Ok::<_, anyhow::Error>(())
                }));
            }

            futures_util::future::try_join_all(tasks)
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
    async fn prepare_archive_block<'a>(
        &self,
        maps: &'a BlockMaps,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<(BriefBlockInfo, &'a BlockStuffAug, &'a BlockProofStuffAug)> {
        let entry = maps.blocks.get(block_id);
        let (block, proof) = entry
            .ok_or(HistoricalSyncError::IncompleteBlockData)?
            .get_data()?;
        let info = self.check_block_proof(proof).await?;
        Ok((info, block, proof))
    }

    async fn save_archive_block(
        &self,
        info: BriefBlockInfo,
        block: &BlockStuffAug,
        proof: &BlockProofStuffAug,
        mc_seq_no: u32,
    ) -> Result<()> {
        let block_handle_storage = self.db.block_handle_storage();
        let block_storage = self.db.block_storage();

        let (handle, _) = block_handle_storage
            .create_or_load_handle(block.id(), info.with_mc_seq_no(mc_seq_no))?;

        // Archive block
        if self.archive_options.is_some() {
            block_storage.move_into_archive_with_data(
                &handle,
                proof.is_link(),
                block.new_archive_data()?,
                proof.new_archive_data()?,
            )?;
        }

        // Notify subscribers
        self.notify_subscribers_with_archive_block(
            &handle,
            &block.data,
            block.new_archive_data()?,
            proof.new_archive_data()?,
        )
        .await
    }

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
