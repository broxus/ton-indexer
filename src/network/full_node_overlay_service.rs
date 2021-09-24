use std::sync::{Arc, Weak};

use anyhow::Result;
use tiny_adnl::utils::*;
use tiny_adnl::{OverlaySubscriber, QueryAnswer, QueryConsumingResult};
use ton_api::ton::{self, TLObject};
use ton_api::{AnyBoxedSerialize, IntoBoxed};

use crate::engine::Engine;
use crate::utils::*;

pub struct FullNodeOverlayService {
    engine: Weak<Engine>,
}

impl FullNodeOverlayService {
    pub fn new(engine: &Arc<Engine>) -> Arc<Self> {
        Arc::new(Self {
            engine: Arc::downgrade(engine),
        })
    }

    fn engine(&self) -> Result<Arc<Engine>, FullNodeOverlayServiceError> {
        self.engine
            .upgrade()
            .ok_or(FullNodeOverlayServiceError::EngineDropped)
    }
}

impl Engine {
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
}

#[async_trait::async_trait]
impl OverlaySubscriber for FullNodeOverlayService {
    async fn try_consume_query(
        &self,
        _local_id: &AdnlNodeIdShort,
        _peer_id: &AdnlNodeIdShort,
        query: TLObject,
    ) -> Result<QueryConsumingResult> {
        //log::info!("Got query: {:?}", query);

        let query = match query.downcast::<ton::rpc::ton_node::GetNextBlockDescription>() {
            Ok(_) => return answer(ton::ton_node::BlockDescription::TonNode_BlockDescriptionEmpty),
            Err(query) => query,
        };

        let query = match query.downcast::<ton::rpc::ton_node::PrepareBlockProof>() {
            Ok(_) => return answer(ton::ton_node::PreparedProof::TonNode_PreparedProofEmpty),
            Err(query) => query,
        };

        let query = match query.downcast::<ton::rpc::ton_node::PrepareKeyBlockProof>() {
            Ok(_) => return answer(ton::ton_node::PreparedProof::TonNode_PreparedProofEmpty),
            Err(query) => query,
        };

        let query = match query.downcast::<ton::rpc::ton_node::PrepareBlock>() {
            Ok(_) => return answer(ton::ton_node::Prepared::TonNode_NotFound),
            Err(query) => query,
        };

        let query = match query.downcast::<ton::rpc::ton_node::PreparePersistentState>() {
            Ok(_) => return answer(ton::ton_node::PreparedState::TonNode_NotFoundState),
            Err(query) => query,
        };

        let query = match query.downcast::<ton::rpc::ton_node::PrepareZeroState>() {
            Ok(_) => return answer(ton::ton_node::PreparedState::TonNode_NotFoundState),
            Err(query) => query,
        };

        let query = match query.downcast::<ton::rpc::ton_node::GetNextKeyBlockIds>() {
            Ok(query) => {
                return self
                    .engine()?
                    .get_next_key_block_ids(query)
                    .await
                    .and_then(answer);
            }
            Err(query) => query,
        };

        let query = match query.downcast::<ton::rpc::ton_node::DownloadNextBlockFull>() {
            Ok(_) => return answer(ton::ton_node::DataFull::TonNode_DataFullEmpty),
            Err(query) => query,
        };

        let query = match query.downcast::<ton::rpc::ton_node::DownloadBlockFull>() {
            Ok(_) => return answer(ton::ton_node::DataFull::TonNode_DataFullEmpty),
            Err(query) => query,
        };

        let query = match query.downcast::<ton::rpc::ton_node::GetArchiveInfo>() {
            Ok(_) => return answer(ton::ton_node::ArchiveInfo::TonNode_ArchiveNotFound),
            Err(query) => query,
        };

        if query
            .downcast::<ton::rpc::ton_node::GetCapabilities>()
            .is_ok()
        {
            //log::warn!("Got capabilities query");
            return answer_boxed(ton::ton_node::capabilities::Capabilities {
                version: 2,
                capabilities: 1,
            });
        };

        Ok(QueryConsumingResult::Consumed(None))
    }
}

fn answer<T: AnyBoxedSerialize>(data: T) -> Result<QueryConsumingResult> {
    Ok(QueryConsumingResult::Consumed(Some(QueryAnswer::Object(
        TLObject::new(data),
    ))))
}

fn answer_boxed<T: IntoBoxed>(data: T) -> Result<QueryConsumingResult>
where
    T::Boxed: AnyBoxedSerialize,
{
    answer(data.into_boxed())
}

#[derive(thiserror::Error, Debug)]
enum FullNodeOverlayServiceError {
    #[error("Engine is already dropped")]
    EngineDropped,
}
