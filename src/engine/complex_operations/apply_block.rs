/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - slightly changed application of blocks
///
use std::sync::Arc;

use anyhow::Result;
use futures::future::{BoxFuture, FutureExt};

use crate::engine::db::BlockConnection;
use crate::engine::Engine;
use crate::profile;
use crate::storage::*;
use crate::utils::*;

pub const MAX_BLOCK_APPLIER_DEPTH: u32 = 16;

pub fn apply_block<'a>(
    engine: &'a Arc<Engine>,
    handle: &'a Arc<BlockHandle>,
    block: &'a BlockStuff,
    mc_seq_no: u32,
    pre_apply: bool,
    depth: u32,
) -> BoxFuture<'a, Result<()>> {
    async move {
        if pre_apply && handle.meta().has_data() || handle.meta().is_applied() {
            return Ok(());
        }

        if handle.id() != block.id() {
            return Err(ApplyBlockError::BlockIdMismatch.into());
        }

        let (prev1_id, prev2_id) = profile::span!("ensure_prev_blocks_downloaded", {
            let (prev1_id, prev2_id) = block.construct_prev_id()?;

            ensure_prev_blocks_downloaded(
                engine, &prev1_id, &prev2_id, mc_seq_no, pre_apply, depth,
            )
            .await?;
            (prev1_id, prev2_id)
        });

        let shard_state = if handle.meta().has_state() {
            engine.load_state(handle.id()).await?
        } else {
            compute_and_store_shard_state(engine, handle, block, &prev1_id, &prev2_id).await?
        };

        if !pre_apply {
            update_block_connections(engine, handle, &prev1_id, &prev2_id)?;
            engine
                .notify_subscribers_with_block(handle, block, &shard_state)
                .await?;

            if block.id().is_masterchain() {
                profile::span!(
                    "store_last_applied_mc_block_id",
                    engine.store_last_applied_mc_block_id(block.id())?
                );

                // TODO: update shard blocks

                engine.set_applied(handle, mc_seq_no).await?;

                let id = handle.id().clone();
                engine
                    .next_block_applying_operations
                    .do_or_wait(&prev1_id, None, async move { Ok(id) })
                    .await?;
            } else {
                engine.set_applied(handle, mc_seq_no).await?;
            }
        }

        Ok(())
    }
    .boxed()
}

async fn ensure_prev_blocks_downloaded(
    engine: &Arc<Engine>,
    prev1_id: &ton_block::BlockIdExt,
    prev2_id: &Option<ton_block::BlockIdExt>,
    mc_seq_no: u32,
    pre_apply: bool,
    depth: u32,
) -> Result<()> {
    match prev2_id {
        Some(prev2_id) => {
            let futures = vec![
                engine.download_and_apply_block(prev1_id, mc_seq_no, pre_apply, depth + 1),
                engine.download_and_apply_block(prev2_id, mc_seq_no, pre_apply, depth + 1),
            ];

            futures::future::join_all(futures)
                .await
                .into_iter()
                .find(|r| r.is_err())
                .unwrap_or(Ok(()))?;
        }
        None => {
            engine
                .download_and_apply_block(prev1_id, mc_seq_no, pre_apply, depth + 1)
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
    let prev1_handle = profile::span!(
        "load_prev1_handle",
        engine
            .load_block_handle(prev1_id)?
            .ok_or(ApplyBlockError::Prev1BlockHandleNotFound)?
    );

    match prev2_id {
        Some(prev2_id) => {
            let prev2_handle = engine
                .load_block_handle(prev2_id)?
                .ok_or(ApplyBlockError::Prev2BlockHandleNotFound)?;

            db.store_block_connection(&prev1_handle, BlockConnection::Next1, handle.id())?;
            db.store_block_connection(&prev2_handle, BlockConnection::Next1, handle.id())?;
            db.store_block_connection(handle, BlockConnection::Prev1, prev1_id)?;
            db.store_block_connection(handle, BlockConnection::Prev2, prev2_id)?;
        }
        None => {
            let prev1_shard = prev1_handle.id().shard_id;
            let shard = handle.id().shard_id;

            if prev1_shard != shard && prev1_shard.split()?.1 == shard {
                db.store_block_connection(&prev1_handle, BlockConnection::Next2, handle.id())?;
            } else {
                db.store_block_connection(&prev1_handle, BlockConnection::Next1, handle.id())?;
            }
            db.store_block_connection(handle, BlockConnection::Prev1, prev1_id)?;
        }
    }

    Ok(())
}

async fn compute_and_store_shard_state(
    engine: &Arc<Engine>,
    handle: &Arc<BlockHandle>,
    block: &BlockStuff,
    prev1_id: &ton_block::BlockIdExt,
    prev2_id: &Option<ton_block::BlockIdExt>,
) -> Result<Arc<ShardStateStuff>> {
    let prev_shard_state_root = match prev2_id {
        Some(prev2_id) => {
            if prev1_id.shard().is_masterchain() {
                return Err(ApplyBlockError::InvalidMasterchainBlockSequence.into());
            }

            let left = profile::span!(
                "wait_prev_shard_state_root_left",
                engine
                    .wait_state(prev1_id, None, true)
                    .await?
                    .root_cell()
                    .clone()
            );

            let right = profile::span!(
                "wait_prev_shard_state_root_right",
                engine
                    .wait_state(prev2_id, None, true)
                    .await?
                    .root_cell()
                    .clone()
            );

            profile::span!(
                "construct_split_root",
                ShardStateStuff::construct_split_root(left, right)?
            )
        }
        None => engine
            .wait_state(prev1_id, None, true)
            .await?
            .root_cell()
            .clone(),
    };

    let merkle_update = profile::span!(
        "make_state_merkle_update",
        block.block().read_state_update()?
    );

    let shard_state = tokio::task::spawn_blocking({
        let block_id = block.id().clone();
        move || -> Result<Arc<ShardStateStuff>> {
            let shard_state_root = merkle_update.apply_for(&prev_shard_state_root)?;

            profile::span!(
                "make_shard_state_stuff",
                Ok(Arc::new(ShardStateStuff::new(block_id, shard_state_root)?))
            )
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
    #[error("Invalid masterchain block sequence")]
    InvalidMasterchainBlockSequence,
}
