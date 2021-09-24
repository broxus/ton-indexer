use std::sync::{Arc, Weak};

use anyhow::Result;
use tiny_adnl::utils::*;
use tiny_adnl::{OverlaySubscriber, QueryAnswer, QueryConsumingResult};
use ton_api::ton::{self, TLObject};
use ton_api::{AnyBoxedSerialize, IntoBoxed};

use crate::engine::rpc_operations::*;

pub struct FullNodeOverlayService<T> {
    engine: Weak<T>,
}

impl<T> FullNodeOverlayService<T>
where
    T: RpcService,
{
    pub fn new(engine: &Arc<T>) -> Arc<Self> {
        Arc::new(Self {
            engine: Arc::downgrade(engine),
        })
    }

    fn engine(&self) -> Result<Arc<T>, FullNodeOverlayServiceError> {
        self.engine
            .upgrade()
            .ok_or(FullNodeOverlayServiceError::EngineDropped)
    }
}

#[async_trait::async_trait]
impl<T> OverlaySubscriber for FullNodeOverlayService<T>
where
    T: RpcService,
{
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
            Ok(query) => {
                return self
                    .engine()?
                    .download_next_block_full(query)
                    .await
                    .and_then(answer)
            }
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
