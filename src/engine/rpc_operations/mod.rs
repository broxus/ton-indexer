use anyhow::Result;
use std::sync::Arc;
use ton_api::ton;

use super::Engine;
use crate::engine::db::BlockConnection;
use crate::utils::*;

#[async_trait::async_trait]
pub trait RpcService: Send + Sync {
    async fn get_next_block_description(
        self: Arc<Self>,
        query: ton::rpc::ton_node::GetNextBlockDescription,
    ) -> Result<ton::ton_node::BlockDescription>;

    async fn prepare_block_proof(
        self: Arc<Self>,
        query: ton::rpc::ton_node::PrepareBlockProof,
    ) -> Result<ton::ton_node::PreparedProof>;

    async fn prepare_key_block_proof(
        self: Arc<Self>,
        query: ton::rpc::ton_node::PrepareKeyBlockProof,
    ) -> Result<ton::ton_node::PreparedProof>;

    async fn prepare_block(
        self: Arc<Self>,
        query: ton::rpc::ton_node::PrepareBlock,
    ) -> Result<ton::ton_node::Prepared>;

    async fn get_next_key_block_ids(
        self: Arc<Self>,
        query: ton::rpc::ton_node::GetNextKeyBlockIds,
    ) -> Result<ton::ton_node::KeyBlocks>;

    async fn download_next_block_full(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadNextBlockFull,
    ) -> Result<ton::ton_node::DataFull>;

    async fn download_block_full(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadBlockFull,
    ) -> Result<ton::ton_node::DataFull>;

    async fn download_block(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadBlock,
    ) -> Result<Vec<u8>>;

    async fn download_block_proof(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadBlockProof,
    ) -> Result<Vec<u8>>;

    async fn download_key_block_proof(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadKeyBlockProof,
    ) -> Result<Vec<u8>>;

    async fn download_block_proof_link(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadBlockProofLink,
    ) -> Result<Vec<u8>>;

    async fn download_key_block_proof_link(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadKeyBlockProofLink,
    ) -> Result<Vec<u8>>;
}

impl Engine {
    fn prepare_block_proof_internal(
        &self,
        block_id: &ton_block::BlockIdExt,
        allow_partial: bool,
        key_block: bool,
    ) -> Result<ton::ton_node::PreparedProof> {
        Ok(match self.load_block_handle(block_id)? {
            Some(handle) if key_block && !handle.meta().is_key_block() => {
                return Err(RpcServiceError::NotKeyBlock.into())
            }
            Some(handle)
                if !handle.meta().has_proof()
                    && (!allow_partial || !handle.meta().has_proof_link()) =>
            {
                ton::ton_node::PreparedProof::TonNode_PreparedProofEmpty
            }
            Some(handle) if handle.meta().has_proof() && handle.id().shard_id.is_masterchain() => {
                ton::ton_node::PreparedProof::TonNode_PreparedProof
            }
            Some(_) => ton::ton_node::PreparedProof::TonNode_PreparedProofLink,
            None => ton::ton_node::PreparedProof::TonNode_PreparedProofEmpty,
        })
    }

    async fn download_block_proof_internal(
        &self,
        block_id: &ton_block::BlockIdExt,
        is_link: bool,
    ) -> Result<Vec<u8>> {
        match self.load_block_handle(block_id)? {
            Some(handle)
                if is_link && handle.meta().has_proof_link()
                    || !is_link && handle.meta().has_proof() =>
            {
                self.db.load_block_proof_raw(&handle, is_link).await
            }
            _ if is_link => Err(RpcServiceError::BlockProofLinkNotFound.into()),
            _ => Err(RpcServiceError::BlockProofNotFound.into()),
        }
    }
}

#[async_trait::async_trait]
impl RpcService for Engine {
    async fn get_next_block_description(
        self: Arc<Self>,
        query: ton::rpc::ton_node::GetNextBlockDescription,
    ) -> Result<ton::ton_node::BlockDescription> {
        let db = &self.db;
        let prev_block_id = convert_block_id_ext_api2blk(&query.prev_block)?;

        Ok(
            match db.load_block_connection(&prev_block_id, BlockConnection::Next1) {
                Ok(id) => ton::ton_node::BlockDescription::TonNode_BlockDescription(Box::new(
                    ton::ton_node::blockdescription::BlockDescription {
                        id: convert_block_id_ext_blk2api(&id),
                    },
                )),
                Err(_) => ton::ton_node::BlockDescription::TonNode_BlockDescriptionEmpty,
            },
        )
    }

    async fn prepare_block_proof(
        self: Arc<Self>,
        query: ton::rpc::ton_node::PrepareBlockProof,
    ) -> Result<ton::ton_node::PreparedProof> {
        self.prepare_block_proof_internal(
            &convert_block_id_ext_api2blk(&query.block)?,
            query.allow_partial.into(),
            false,
        )
    }

    async fn prepare_key_block_proof(
        self: Arc<Self>,
        query: ton::rpc::ton_node::PrepareKeyBlockProof,
    ) -> Result<ton::ton_node::PreparedProof> {
        self.prepare_block_proof_internal(
            &convert_block_id_ext_api2blk(&query.block)?,
            query.allow_partial.into(),
            true,
        )
    }

    async fn prepare_block(
        self: Arc<Self>,
        query: ton::rpc::ton_node::PrepareBlock,
    ) -> Result<ton::ton_node::Prepared> {
        let block_id = convert_block_id_ext_api2blk(&query.block)?;
        Ok(match self.load_block_handle(&block_id)? {
            Some(handle) if handle.meta().has_data() => ton::ton_node::Prepared::TonNode_Prepared,
            _ => ton::ton_node::Prepared::TonNode_NotFound,
        })
    }

    async fn get_next_key_block_ids(
        self: Arc<Self>,
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
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadNextBlockFull,
    ) -> Result<ton::ton_node::DataFull> {
        let db = &self.db;
        let prev_block_id = convert_block_id_ext_api2blk(&query.prev_block)?;

        let next_block_id = match self.load_block_handle(&prev_block_id)? {
            Some(handle) if handle.meta().has_next1() => {
                db.load_block_connection(&prev_block_id, BlockConnection::Next1)?
            }
            _ => return Ok(ton::ton_node::DataFull::TonNode_DataFullEmpty),
        };

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

    async fn download_block_full(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadBlockFull,
    ) -> Result<ton::ton_node::DataFull> {
        let db = &self.db;
        let block_id = convert_block_id_ext_api2blk(&query.block)?;

        let mut is_link = false;
        Ok(match self.load_block_handle(&block_id)? {
            Some(handle) if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) => {
                let block = db.load_block_data_raw(&handle).await?;
                let proof = db.load_block_proof_raw(&handle, is_link).await?;

                ton::ton_node::DataFull::TonNode_DataFull(Box::new(
                    ton::ton_node::datafull::DataFull {
                        id: convert_block_id_ext_blk2api(&block_id),
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

    async fn download_block(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadBlock,
    ) -> Result<Vec<u8>> {
        let block_id = convert_block_id_ext_api2blk(&query.block)?;
        match self.load_block_handle(&block_id)? {
            Some(handle) if handle.meta().has_data() => self.db.load_block_data_raw(&handle).await,
            _ => Err(RpcServiceError::BlockNotFound.into()),
        }
    }

    async fn download_block_proof(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadBlockProof,
    ) -> Result<Vec<u8>> {
        self.download_block_proof_internal(&convert_block_id_ext_api2blk(&query.block)?, false)
            .await
    }

    async fn download_key_block_proof(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadKeyBlockProof,
    ) -> Result<Vec<u8>> {
        self.download_block_proof_internal(&convert_block_id_ext_api2blk(&query.block)?, false)
            .await
    }

    async fn download_block_proof_link(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadBlockProofLink,
    ) -> Result<Vec<u8>> {
        self.download_block_proof_internal(&convert_block_id_ext_api2blk(&query.block)?, true)
            .await
    }

    async fn download_key_block_proof_link(
        self: Arc<Self>,
        query: ton::rpc::ton_node::DownloadKeyBlockProofLink,
    ) -> Result<Vec<u8>> {
        self.download_block_proof_internal(&convert_block_id_ext_api2blk(&query.block)?, true)
            .await
    }
}

#[derive(thiserror::Error, Debug)]
enum RpcServiceError {
    #[error("Block was not found")]
    BlockNotFound,
    #[error("Block proof not found")]
    BlockProofNotFound,
    #[error("Block proof link not found")]
    BlockProofLinkNotFound,
    #[error("Requested block is not a key block")]
    NotKeyBlock,
}
