use std::sync::Arc;

use anyhow::Result;
use tiny_adnl::utils::*;
use tiny_adnl::{OverlaySubscriber, QueryAnswer, QueryConsumingResult};
use ton_api::ton::{self, TLObject};
use ton_api::IntoBoxed;

pub struct FullNodeOverlayService;

impl FullNodeOverlayService {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
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
        log::info!("Got query: {:?}", query);

        let _query = match query.downcast::<ton::rpc::ton_node::GetCapabilities>() {
            Ok(_) => {
                log::warn!("Got capabilities query");

                let answer = TLObject::new(
                    ton::ton_node::capabilities::Capabilities {
                        version: 2,
                        capabilities: 1,
                    }
                    .into_boxed(),
                );

                return Ok(QueryConsumingResult::Consumed(Some(QueryAnswer::Object(
                    answer,
                ))));
            }
            Err(query) => query,
        };

        Ok(QueryConsumingResult::Consumed(None))
    }
}
