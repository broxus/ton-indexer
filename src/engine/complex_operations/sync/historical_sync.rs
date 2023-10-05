use std::ops::ControlFlow;
use std::sync::Arc;

use crate::engine::complex_operations::sync::import_package_with_apply;
use crate::engine::complex_operations::{
    choose_key_block, download_workchain_zero_state, prepare_prev_key_block,
};
use anyhow::{Context, Result};
use futures_util::{Stream, StreamExt};

use super::archives_stream::*;
use super::block_maps::*;
use crate::engine::Engine;
use crate::utils::*;

pub async fn historical_sync(engine: &Arc<Engine>, from_seqno: u32) -> Result<()> {
    let (from, to) = engine.historical_sync_range(from_seqno)?;
    if from + 1 >= to {
        return Ok(());
    }

    tracing::info!(
        target: "sync",
        from_seq_no = from,
        to_seq_no = to,
        "started historical sync"
    );

    let mut ctx = HistoricalSyncContext::new(engine, from, to);

    let mut archives = ArchivesStream::new(engine, from..=to, None);
    loop {
        let archive = archives.recv().await;
        match ctx.handle(archive.clone()).await {
            Ok(ControlFlow::Break(())) => break,
            Ok(_) => {
                archive.accept(ctx.last_archive_edge.clone());
            }
            Err(e) => {
                tracing::error!(target: "sync", "failed to save archive: {e:?}");
            }
        }
    }

    tracing::info!(target: "sync", "historical sync complete");
    Ok(())
}

pub async fn boot_from_archives(
    engine: &Arc<Engine>,
    mut archive_stream: impl Stream<Item = Vec<u8>> + Unpin,
) -> Result<()> {
    let prev_key_block = prepare_prev_key_block(engine).await?;
    // let last_key_block = choose_key_block(engine)?;
    let zero_state = engine.load_mc_zero_state().await.context("No zero state")?;

    download_workchain_zero_state(engine, &zero_state, ton_block::BASE_WORKCHAIN_ID).await?;
    engine.store_last_applied_mc_block_id(prev_key_block.handle().id())?;
    engine.store_shards_client_mc_block_id(&prev_key_block.handle().id())?;

    let mut last_gen_utime = 0;

    while let Some(pack) = archive_stream.next().await {
        let pack = BlockMaps::new(&pack)?;
        tracing::info!(
            target: "sync",
            lowest_id = %pack.lowest_mc_id().context("lowest id not found")?.display(),
            highest_id = %pack.highest_mc_id().context("highest id not found")?.display(),
            "importing archive");
        import_package_with_apply(
            engine,
            pack,
            &engine.last_applied_block()?,
            &mut last_gen_utime,
        )
        .await?;
        // import_shard_blocks_with_apply(engine, &pack).await?;
        // for master_block in pack.mc_block_ids.values() {
        //     let mc_seq_no = master_block.seq_no;
        //     tracing::info!("Applying archive {}", mc_seq_no);
        //     let master_block = pack
        //         .blocks
        //         .get(master_block)
        //         .context("master block not found")?;
        //     let master_proof = master_block
        //         .proof
        //         .as_ref()
        //         .context("master block proof not found")?;
        //     let master_block = master_block
        //         .block
        //         .as_ref()
        //         .context("should be master block")?;
        //
        //     let shard_blocks = master_block.shard_blocks()?;
        //     let handle = engine
        //         .save_block(master_block, master_proof, mc_seq_no)
        //         .await?;
        //     // what is pre apply?
        //     engine
        //         .apply_block_ext(&handle, &master_block.data, mc_seq_no, false, 0)
        //         .await?;
        //     tracing::info!("Applied mc block {mc_seq_no}");
        //
        //     for shard_block_id in shard_blocks.values() {
        //         if shard_block_id.seq_no == 0 {
        //             // skip zerostate
        //             continue;
        //         }
        //         let shard_block = pack
        //             .blocks
        //             .get(shard_block_id)
        //             .context("shard block not found")?;
        //         let shard_proof = shard_block
        //             .proof
        //             .as_ref()
        //             .context("shard proof not found")?;
        //         let shard_block = shard_block
        //             .block
        //             .as_ref()
        //             .context("should be shard block")?;
        //
        //         let handle = engine
        //             .save_block(shard_block, shard_proof, mc_seq_no)
        //             .await?;
        //         engine
        //             .apply_block_ext(&handle, &shard_block.data, mc_seq_no, false, 0)
        //             .await?;
        //     }
        // }
    }
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
        tracing::debug!(
            target: "sync",
            lowest_id = %lowest_id.display(),
            highest_id = %highest_id.display(),
            "saving archive"
        );

        let mut block_edge = self.last_archive_edge.clone();

        self.process_blocks(&maps, &mut block_edge).await?;
        tracing::info!(
            target: "sync",
            lowest_id = %lowest_id.display(),
            highest_id = %highest_id.display(),
            "saved archive"
        );

        Ok({
            if highest_id.seq_no >= self.to {
                ControlFlow::Break(())
            } else {
                self.last_archive_edge = block_edge;
                ControlFlow::Continue(())
            }
        })
    }

    async fn process_blocks(
        &mut self,
        maps: &Arc<BlockMaps>,
        edge: &mut Option<BlockMapsEdge>,
    ) -> Result<()> {
        let node_state = self.engine.storage.node_state();

        let splits = Arc::new(FastDashSet::default());

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
                *edge = Some(new_edge);
                continue;
            }

            self.engine
                .save_archive_block(info, block, proof, mc_seq_no)
                .await?;

            splits.clear();
            let mut tasks = Vec::with_capacity(shard_blocks.len());
            for (_, id) in shard_blocks {
                fn should_process(
                    maps: &BlockMaps,
                    edge: &Option<BlockMapsEdge>,
                    id: &ton_block::BlockIdExt,
                ) -> bool {
                    match edge {
                        // Process blocks only if they are after the current edge
                        Some(edge) => edge.is_before(id),
                        // Always process all blocks in archive when block edge is not specified
                        None => maps.blocks.contains_key(id),
                    }
                }

                // Skip blocks which were referenced in previous mc block (no new blocks were
                // produced in this shard)
                if !should_process(maps, edge, &id) {
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
                        if should_process(&maps, &edge, &prev1) {
                            stack.push(prev1);
                        }

                        // Push right predecessor
                        if let Some(prev2) = prev2 {
                            if should_process(&maps, &edge, &prev2) {
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

            *edge = Some(new_edge);

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
        let block_handle_storage = self.storage.block_handle_storage();
        let block_storage = self.storage.block_storage();

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
        self.notify_subscriber_with_archive_block(
            &handle,
            &block.data,
            block.new_archive_data()?,
            proof.new_archive_data()?,
        )
        .await?;

        if handle.id().shard_id.is_masterchain() {
            self.on_masterchain_block(&handle).await?;
        }

        Ok(())
    }

    fn historical_sync_range(&self, from_seqno: u32) -> Result<(u32, u32)> {
        let state = self.storage.node_state();

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
