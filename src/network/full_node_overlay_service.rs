/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - simplified answer processing
///
use std::future::Future;
use std::sync::{Arc, Weak};

use crate::Engine;
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

    async fn answer<Q, F, R>(
        &self,
        query: TLObject,
        handler: fn(Arc<T>, Q) -> F,
        into_answer: fn(R) -> Result<QueryConsumingResult>,
    ) -> ProcessedQuery
    where
        F: Future<Output = Result<R>>,
        Q: AnyBoxedSerialize,
        R: Send,
    {
        let query = match query.downcast::<Q>() {
            Ok(query) => query,
            Err(query) => return ProcessedQuery::Rejected(query),
        };

        ProcessedQuery::Accepted(match self.engine.upgrade() {
            Some(engine) => handler(engine, query).await.and_then(into_answer),
            None => Err(FullNodeOverlayServiceError::EngineDropped.into()),
        })
    }
}

enum ProcessedQuery {
    Accepted(Result<QueryConsumingResult>),
    Rejected(TLObject),
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
        mut query: TLObject,
    ) -> Result<QueryConsumingResult> {
        //log::info!("Got query: {:?}", query);

        macro_rules! select_query {
            ($($handler:ident => $into_answer:ident),*,) => {
                $(query = match self.answer(query, RpcService::$handler, $into_answer).await {
                    ProcessedQuery::Accepted(result) => return result,
                    ProcessedQuery::Rejected(query) => query,
                });*;
            };
        }

        select_query! {
            get_next_block_description => answer,
            prepare_block_proof => answer,
            prepare_key_block_proof => answer,
            prepare_block => answer,
        };

        query = match query.downcast::<ton::rpc::ton_node::PreparePersistentState>() {
            Ok(_) => return answer(ton::ton_node::PreparedState::TonNode_NotFoundState),
            Err(query) => query,
        };

        query = match query.downcast::<ton::rpc::ton_node::PrepareZeroState>() {
            Ok(_) => return answer(ton::ton_node::PreparedState::TonNode_NotFoundState),
            Err(query) => query,
        };

        select_query! {
            get_next_key_block_ids => answer,
            download_next_block_full => answer,
            download_block_full => answer,
            download_block => answer_raw,
            download_block_proof => answer_raw,
            download_key_block_proof => answer_raw,
            download_block_proof_link => answer_raw,
            download_key_block_proof_link => answer_raw,
        };

        query = match query.downcast::<ton::rpc::ton_node::GetArchiveInfo>() {
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

fn answer_raw(data: Vec<u8>) -> Result<QueryConsumingResult> {
    Ok(QueryConsumingResult::Consumed(Some(QueryAnswer::Raw(data))))
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
