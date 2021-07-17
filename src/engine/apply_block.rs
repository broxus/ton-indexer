use std::sync::Arc;

use anyhow::Result;

use super::Engine;
use crate::engine::db::BlockConnection;
use crate::storage::*;
use crate::utils::*;

pub const MAX_RECURSION_DEPTH: u32 = 16;

pub async fn apply_block(
    engine: &Arc<Engine>,
    handle: &Arc<BlockHandle>,
    block: &BlockStuff,
    mc_seq_no: u32,
    pre_apply: bool,
    recursion_depth: u32,
) -> Result<()> {
    if handle.id() != block.id() {
        return Err(ApplyBlockError::BlockIdMismatch.into());
    }

    let (prev1_id, prev2_id) = block.construct_prev_id()?;
    check_prev_blocks(
        engine,
        &prev1_id,
        &prev2_id,
        mc_seq_no,
        pre_apply,
        recursion_depth,
    )
    .await?;

    let _shard_state = if handle.meta().has_state() {
        engine.load_state(handle.id())?
    } else {
        compute_shard_state(engine, handle, block, &prev1_id, &prev2_id).await?
    };

    if !pre_apply {
        update_block_connections(engine, handle, &prev1_id, &prev2_id)?;
        // TODO: process
    }

    Ok(())
}

async fn check_prev_blocks(
    engine: &Arc<Engine>,
    prev1_id: &ton_block::BlockIdExt,
    prev2_id: &Option<ton_block::BlockIdExt>,
    mc_seq_no: u32,
    pre_apply: bool,
    recursion_depth: u32,
) -> Result<()> {
    match prev2_id {
        Some(prev2_id) => {
            let mut futures = Vec::with_capacity(2);
            futures.push(engine.download_and_apply_block(
                prev1_id,
                mc_seq_no,
                pre_apply,
                recursion_depth + 1,
            ));
            futures.push(engine.download_and_apply_block(
                prev2_id,
                mc_seq_no,
                pre_apply,
                recursion_depth + 1,
            ));

            futures::future::join_all(futures)
                .await
                .into_iter()
                .find(|r| r.is_err())
                .unwrap_or(Ok(()))?;
        }
        None => {
            engine
                .download_and_apply_block(prev1_id, mc_seq_no, pre_apply, recursion_depth + 1)
                .await?;
        }
    }
    Ok(())
}

fn update_block_connections(
    engine: &Arc<Engine>,
    handle: &Arc<BlockHandle>,
    prev1_id: &ton_block::BlockIdExt,
    prev2_id: &Option<ton_block::BlockIdExt>,
) -> Result<()> {
    let db = &engine.db;

    let prev1_handle = engine
        .load_block_handle(&prev1_id)?
        .ok_or_else(|| ApplyBlockError::Prev1BlockHandleNotFound)?;

    match prev2_id {
        Some(prev2_id) => {
            let prev2_handle = engine
                .load_block_handle(&prev2_id)?
                .ok_or_else(|| ApplyBlockError::Prev2BlockHandleNotFound)?;

            db.store_block_connection(&prev1_handle, BlockConnection::Next1, handle.id())?;
            db.store_block_connection(&prev2_handle, BlockConnection::Next1, handle.id())?;
            db.store_block_connection(handle, BlockConnection::Prev1, prev1_id)?;
            db.store_block_connection(handle, BlockConnection::Prev2, prev2_id)?;
        }
        None => {
            let prev1_shard = prev1_handle.id().shard_id;
            let shard = handle.id().shard_id;

            if prev1_shard != shard && prev1_shard.split().convert()?.1 == shard {
                db.store_block_connection(&prev1_handle, BlockConnection::Next2, handle.id())?;
            } else {
                db.store_block_connection(&prev1_handle, BlockConnection::Next1, handle.id())?;
            }
            db.store_block_connection(handle, BlockConnection::Prev1, prev1_id)?;
        }
    }

    Ok(())
}

async fn compute_shard_state(
    engine: &Arc<Engine>,
    handle: &Arc<BlockHandle>,
    block: &BlockStuff,
    prev1_id: &ton_block::BlockIdExt,
    prev2_id: &Option<ton_block::BlockIdExt>,
) -> Result<Arc<ShardStateStuff>> {
    let prev_shard_state_root = match prev2_id {
        Some(prev2_id) => {
            let left = engine
                .wait_state(prev1_id, None, true)
                .await?
                .root_cell()
                .clone();
            let right = engine
                .wait_state(prev2_id, None, true)
                .await?
                .root_cell()
                .clone();
            ShardStateStuff::construct_split_root(left, right)?
        }
        None => engine
            .wait_state(prev1_id, None, true)
            .await?
            .root_cell()
            .clone(),
    };

    let merkle_update = block.block().read_state_update().convert()?;

    let shard_state = tokio::task::spawn_blocking({
        let block_id = block.id().clone();
        move || -> Result<Arc<ShardStateStuff>> {
            let shard_state_root = merkle_update.apply_for(&prev_shard_state_root).convert()?;
            Ok(Arc::new(ShardStateStuff::new(block_id, shard_state_root)?))
        }
    })
    .await??;

    engine.store_state(handle, &shard_state).await?;
    Ok(shard_state)
}

#[derive(thiserror::Error, Debug)]
enum ApplyBlockError {
    #[error("Block id mismatch")]
    BlockIdMismatch,
    #[error("Prev1 block handle not found")]
    Prev1BlockHandleNotFound,
    #[error("Prev2 block handle not found")]
    Prev2BlockHandleNotFound,
}
