use anyhow::Result;
use ton_api::ton;

use super::Engine;
use crate::engine::db::BlockConnection;
use crate::utils::*;

#[async_trait::async_trait]
pub trait RpcService: Send + Sync {
    async fn get_next_key_block_ids(
        &self,
        query: ton::rpc::ton_node::GetNextKeyBlockIds,
    ) -> Result<ton::ton_node::KeyBlocks>;

    async fn download_next_block_full(
        &self,
        query: ton::rpc::ton_node::DownloadNextBlockFull,
    ) -> Result<ton::ton_node::DataFull>;
}

#[async_trait::async_trait]
impl RpcService for Engine {
    async fn get_next_key_block_ids(
        &self,
        query: ton::rpc::ton_node::GetNextKeyBlockIds,
    ) -> Result<ton::ton_node::KeyBlocks> {
        const NEXT_KEY_BLOCKS_LIMIT: i32 = 8;

        let limit = std::cmp::min(query.max_size, NEXT_KEY_BLOCKS_LIMIT) as usize;

        let get_next_key_block_ids = || async move {
            let last_mc_block_id = self.load_last_applied_mc_block_id().await?;
            let last_mc_state = self.load_state(&last_mc_block_id).await?;
            let prev_blocks = &last_mc_state.shard_state_extra()?.prev_blocks;

            let start_block_id = convert_block_id_ext_api2blk(&query.block)?;

            if query.block.seqno != 0 {
                prev_blocks.check_key_block(&start_block_id, Some(true))?;
            }

            let mut ids = Vec::with_capacity(limit);

            let mut seq_no = start_block_id.seq_no;
            while let Some(id) = prev_blocks.get_next_key_block(seq_no + 1)? {
                seq_no = id.seq_no;

                ids.push(convert_block_id_ext_blk2api(&id.master_block_id().1));
                if ids.len() >= limit {
                    break;
                }
            }

            Result::<_, anyhow::Error>::Ok(ids)
        };

        let (incomplete, error, blocks) = match get_next_key_block_ids().await {
            Ok(ids) => (ids.len() < limit, false, ids),
            Err(e) => {
                log::warn!("get_next_key_block_ids failed: {:?}", e);
                (false, true, Vec::new())
            }
        };

        Ok(ton::ton_node::KeyBlocks::TonNode_KeyBlocks(Box::new(
            ton::ton_node::keyblocks::KeyBlocks {
                blocks: blocks.into(),
                incomplete: incomplete.into(),
                error: error.into(),
            },
        )))
    }

    async fn download_next_block_full(
        &self,
        query: ton::rpc::ton_node::DownloadNextBlockFull,
    ) -> Result<ton::ton_node::DataFull> {
        let prev_block_id = convert_block_id_ext_api2blk(&query.prev_block)?;
        match self.load_block_handle(&prev_block_id)? {
            Some(handle) if handle.meta().has_next1() => handle,
            _ => return Ok(ton::ton_node::DataFull::TonNode_DataFullEmpty),
        };

        let db = &self.db;
        let next_block_id = db.load_block_connection(&prev_block_id, BlockConnection::Next1)?;

        let mut is_link = false;
        Ok(match self.load_block_handle(&next_block_id)? {
            Some(handle) if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) => {
                let block = db.load_block_data_raw(&handle).await?;
                let proof = db.load_block_proof_raw(&handle, is_link).await?;

                ton::ton_node::DataFull::TonNode_DataFull(Box::new(
                    ton::ton_node::datafull::DataFull {
                        id: convert_block_id_ext_blk2api(&next_block_id),
                        proof: ton::bytes(proof),
                        block: ton::bytes(block),
                        is_link: if is_link {
                            ton::Bool::BoolTrue
                        } else {
                            ton::Bool::BoolFalse
                        },
                    },
                ))
            }
            _ => ton::ton_node::DataFull::TonNode_DataFullEmpty,
        })
    }
}
