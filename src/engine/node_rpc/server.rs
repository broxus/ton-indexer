/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - simplified answer processing
///
use std::future::Future;
use std::sync::{Arc, Weak};

use anyhow::Result;
use tiny_adnl::utils::*;
use tiny_adnl::{OverlaySubscriber, QueryAnswer, QueryConsumingResult};
use ton_api::ton::{self, TLObject};
use ton_api::{AnyBoxedSerialize, IntoBoxed};

use crate::db::{BlockConnection, Db, KeyBlocksDirection};
use crate::engine::Engine;
use crate::utils::*;

pub struct NodeRpcServer(Weak<Engine>);

impl NodeRpcServer {
    pub fn new(engine: &Arc<Engine>) -> Arc<Self> {
        Arc::new(Self(Arc::downgrade(engine)))
    }

    async fn answer<'a, Q, F, R>(
        &self,
        query: TLObject,
        handler: fn(QueryHandler, Q) -> F,
        into_answer: fn(R) -> Result<QueryConsumingResult>,
    ) -> ProcessedQuery
    where
        F: Future<Output = Result<R>> + 'a,
        Q: AnyBoxedSerialize,
        R: Send,
    {
        let query = match query.downcast::<Q>() {
            Ok(query) => query,
            Err(query) => return ProcessedQuery::Rejected(query),
        };

        ProcessedQuery::Accepted(match self.0.upgrade() {
            Some(engine) => handler(QueryHandler(engine), query)
                .await
                .and_then(into_answer),
            None => Err(NodeRpcServerError::EngineDropped.into()),
        })
    }
}

enum ProcessedQuery {
    Accepted(Result<QueryConsumingResult>),
    Rejected(TLObject),
}

#[async_trait::async_trait]
impl OverlaySubscriber for NodeRpcServer {
    async fn try_consume_query(
        &self,
        _: &AdnlNodeIdShort,
        _: &AdnlNodeIdShort,
        mut query: TLObject,
    ) -> Result<QueryConsumingResult> {
        //log::info!("Got query: {:?}", query);

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

        macro_rules! select_query {
            ($($handler:ident => $into_answer:ident),*,) => {
                $(query = match self.answer(query, QueryHandler::$handler, $into_answer).await {
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
            prepare_persistent_state => answer,
            prepare_zero_state => answer,
            get_next_key_block_ids => answer,
            download_next_block_full => answer,
            download_block_full => answer,
            download_block => answer_raw,
            download_block_proof => answer_raw,
            download_key_block_proof => answer_raw,
            download_block_proof_link => answer_raw,
            download_key_block_proof_link => answer_raw,
            get_archive_info => answer,
            get_archive_slice => answer_raw,
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

struct QueryHandler(Arc<Engine>);

impl QueryHandler {
    async fn get_next_block_description(
        self,
        query: ton::rpc::ton_node::GetNextBlockDescription,
    ) -> Result<ton::ton_node::BlockDescription> {
        let db = self.0.db.block_connection_storage();
        let prev_block_id = convert_block_id_ext_api2blk(&query.prev_block)?;

        match db.load_connection(&prev_block_id, BlockConnection::Next1) {
            Ok(id) => Ok(ton::ton_node::BlockDescription::TonNode_BlockDescription(
                Box::new(ton::ton_node::blockdescription::BlockDescription {
                    id: convert_block_id_ext_blk2api(&id),
                }),
            )),
            Err(_) => Ok(ton::ton_node::BlockDescription::TonNode_BlockDescriptionEmpty),
        }
    }

    async fn prepare_block_proof(
        self,
        query: ton::rpc::ton_node::PrepareBlockProof,
    ) -> Result<ton::ton_node::PreparedProof> {
        find_block_proof(
            &self.0.db,
            &convert_block_id_ext_api2blk(&query.block)?,
            query.allow_partial.into(),
            false,
        )
    }

    async fn prepare_key_block_proof(
        self,
        query: ton::rpc::ton_node::PrepareKeyBlockProof,
    ) -> Result<ton::ton_node::PreparedProof> {
        find_block_proof(
            &self.0.db,
            &convert_block_id_ext_api2blk(&query.block)?,
            query.allow_partial.into(),
            true,
        )
    }

    async fn prepare_block(
        self,
        query: ton::rpc::ton_node::PrepareBlock,
    ) -> Result<ton::ton_node::Prepared> {
        let block_handle_storage = self.0.db.block_handle_storage();

        let block_id = convert_block_id_ext_api2blk(&query.block)?;
        Ok(match block_handle_storage.load_handle(&block_id)? {
            Some(handle) if handle.meta().has_data() => ton::ton_node::Prepared::TonNode_Prepared,
            _ => ton::ton_node::Prepared::TonNode_NotFound,
        })
    }

    async fn prepare_persistent_state(
        self,
        _: ton::rpc::ton_node::PreparePersistentState,
    ) -> Result<ton::ton_node::PreparedState> {
        // TODO: implement
        Ok(ton::ton_node::PreparedState::TonNode_NotFoundState)
    }

    async fn prepare_zero_state(
        self,
        _: ton::rpc::ton_node::PrepareZeroState,
    ) -> Result<ton::ton_node::PreparedState> {
        // TODO: implement
        Ok(ton::ton_node::PreparedState::TonNode_NotFoundState)
    }

    async fn get_next_key_block_ids(
        self,
        query: ton::rpc::ton_node::GetNextKeyBlockIds,
    ) -> Result<ton::ton_node::KeyBlocks> {
        const NEXT_KEY_BLOCKS_LIMIT: usize = 8;

        let block_handle_storage = self.0.db.block_handle_storage();

        let limit = std::cmp::min(
            query
                .max_size
                .try_into()
                .map_err(|_| NodeRpcServerError::InvalidArgument)?,
            NEXT_KEY_BLOCKS_LIMIT,
        );

        let get_next_key_block_ids = || {
            let start_block_id = convert_block_id_ext_api2blk(&query.block)?;
            if !start_block_id.shard().is_masterchain() {
                return Err(NodeRpcServerError::BlockNotFromMasterChain.into());
            }

            let mut iterator = block_handle_storage
                .key_blocks_iterator(KeyBlocksDirection::ForwardFrom(start_block_id.seq_no))
                .take(limit)
                .peekable();

            if let Some(Ok(id)) = iterator.peek() {
                if id.root_hash != start_block_id.root_hash() {
                    return Err(NodeRpcServerError::InvalidRootHash.into());
                }
                if id.file_hash != start_block_id.file_hash() {
                    return Err(NodeRpcServerError::InvalidFileHash.into());
                }
            }

            let mut ids = Vec::with_capacity(limit);
            while let Some(id) = iterator.next().transpose()? {
                ids.push(convert_block_id_ext_blk2api(&id));
                if ids.len() >= limit {
                    break;
                }
            }

            Result::<_, anyhow::Error>::Ok(ids)
        };

        let (incomplete, error, blocks) = match get_next_key_block_ids() {
            Ok(ids) => (ids.len() < limit, false, ids),
            Err(e) => {
                log::warn!("get_next_key_block_ids failed: {e:?}");
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
        self,
        query: ton::rpc::ton_node::DownloadNextBlockFull,
    ) -> Result<ton::ton_node::DataFull> {
        let block_handle_storage = self.0.db.block_handle_storage();
        let block_connection_storage = self.0.db.block_connection_storage();
        let block_storage = self.0.db.block_storage();

        let prev_block_id = convert_block_id_ext_api2blk(&query.prev_block)?;

        let next_block_id = match block_handle_storage.load_handle(&prev_block_id)? {
            Some(handle) if handle.meta().has_next1() => {
                block_connection_storage.load_connection(&prev_block_id, BlockConnection::Next1)?
            }
            _ => return Ok(ton::ton_node::DataFull::TonNode_DataFullEmpty),
        };

        let mut is_link = false;
        Ok(match block_handle_storage.load_handle(&next_block_id)? {
            Some(handle) if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) => {
                let block = block_storage.load_block_data_raw(&handle).await?;
                let proof = block_storage.load_block_proof_raw(&handle, is_link).await?;

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
        self,
        query: ton::rpc::ton_node::DownloadBlockFull,
    ) -> Result<ton::ton_node::DataFull> {
        let block_handle_storage = self.0.db.block_handle_storage();
        let block_storage = self.0.db.block_storage();

        let block_id = convert_block_id_ext_api2blk(&query.block)?;

        let mut is_link = false;
        Ok(match block_handle_storage.load_handle(&block_id)? {
            Some(handle) if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) => {
                let block = block_storage.load_block_data_raw(&handle).await?;
                let proof = block_storage.load_block_proof_raw(&handle, is_link).await?;

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

    async fn download_block(self, query: ton::rpc::ton_node::DownloadBlock) -> Result<Vec<u8>> {
        let block_id = convert_block_id_ext_api2blk(&query.block)?;
        match self.0.db.block_handle_storage().load_handle(&block_id)? {
            Some(handle) if handle.meta().has_data() => {
                self.0.db.block_storage().load_block_data_raw(&handle).await
            }
            _ => Err(NodeRpcServerError::BlockNotFound.into()),
        }
    }

    async fn download_block_proof(
        self,
        query: ton::rpc::ton_node::DownloadBlockProof,
    ) -> Result<Vec<u8>> {
        load_block_proof(
            &self.0.db,
            &convert_block_id_ext_api2blk(&query.block)?,
            false,
        )
        .await
    }

    async fn download_key_block_proof(
        self,
        query: ton::rpc::ton_node::DownloadKeyBlockProof,
    ) -> Result<Vec<u8>> {
        load_block_proof(
            &self.0.db,
            &convert_block_id_ext_api2blk(&query.block)?,
            false,
        )
        .await
    }

    async fn download_block_proof_link(
        self,
        query: ton::rpc::ton_node::DownloadBlockProofLink,
    ) -> Result<Vec<u8>> {
        load_block_proof(
            &self.0.db,
            &convert_block_id_ext_api2blk(&query.block)?,
            true,
        )
        .await
    }

    async fn download_key_block_proof_link(
        self,
        query: ton::rpc::ton_node::DownloadKeyBlockProofLink,
    ) -> Result<Vec<u8>> {
        load_block_proof(
            &self.0.db,
            &convert_block_id_ext_api2blk(&query.block)?,
            true,
        )
        .await
    }

    async fn get_archive_info(
        self,
        query: ton::rpc::ton_node::GetArchiveInfo,
    ) -> Result<ton::ton_node::ArchiveInfo> {
        let mc_seq_no = query.masterchain_seqno as u32;

        let last_applied_mc_block = self.0.load_last_applied_mc_block_id()?;
        if mc_seq_no > last_applied_mc_block.seq_no {
            return Ok(ton::ton_node::ArchiveInfo::TonNode_ArchiveNotFound);
        }

        let shards_client_mc_block_id = self.0.load_shards_client_mc_block_id()?;
        if mc_seq_no > shards_client_mc_block_id.seq_no {
            return Ok(ton::ton_node::ArchiveInfo::TonNode_ArchiveNotFound);
        }

        Ok(match self.0.db.block_storage().get_archive_id(mc_seq_no) {
            Some(id) => ton::ton_node::ArchiveInfo::TonNode_ArchiveInfo(Box::new(
                ton::ton_node::archiveinfo::ArchiveInfo { id: id as i64 },
            )),
            None => ton::ton_node::ArchiveInfo::TonNode_ArchiveNotFound,
        })
    }

    async fn get_archive_slice(
        self,
        query: ton::rpc::ton_node::GetArchiveSlice,
    ) -> Result<Vec<u8>> {
        Ok(
            match self.0.db.block_storage().get_archive_slice(
                query.archive_id as u32,
                query.offset as usize,
                query.max_size as usize,
            )? {
                Some(data) => data,
                None => return Err(NodeRpcServerError::ArchiveNotFound.into()),
            },
        )
    }
}

fn find_block_proof(
    db: &Db,
    block_id: &ton_block::BlockIdExt,
    allow_partial: bool,
    key_block: bool,
) -> Result<ton::ton_node::PreparedProof> {
    let handle = match db.block_handle_storage().load_handle(block_id)? {
        Some(handle) => handle,
        None => return Ok(ton::ton_node::PreparedProof::TonNode_PreparedProofEmpty),
    };
    let meta = handle.meta();

    if key_block && !meta.is_key_block() {
        return Err(NodeRpcServerError::NotKeyBlock.into());
    }

    if meta.has_proof() {
        Ok(ton::ton_node::PreparedProof::TonNode_PreparedProof)
    } else if allow_partial && meta.has_proof_link() {
        Ok(ton::ton_node::PreparedProof::TonNode_PreparedProofLink)
    } else {
        Ok(ton::ton_node::PreparedProof::TonNode_PreparedProofEmpty)
    }
}

async fn load_block_proof(
    db: &Db,
    block_id: &ton_block::BlockIdExt,
    is_link: bool,
) -> Result<Vec<u8>> {
    let block_handle_storage = db.block_handle_storage();
    let block_storage = db.block_storage();

    match block_handle_storage.load_handle(block_id)? {
        Some(handle)
            if is_link && handle.meta().has_proof_link()
                || !is_link && handle.meta().has_proof() =>
        {
            block_storage.load_block_proof_raw(&handle, is_link).await
        }
        _ if is_link => Err(NodeRpcServerError::BlockProofLinkNotFound.into()),
        _ => Err(NodeRpcServerError::BlockProofNotFound.into()),
    }
}

#[derive(Debug, thiserror::Error)]
enum NodeRpcServerError {
    #[error("Engine is already dropped")]
    EngineDropped,
    #[error("Block was not found")]
    BlockNotFound,
    #[error("Block proof not found")]
    BlockProofNotFound,
    #[error("Block proof link not found")]
    BlockProofLinkNotFound,
    #[error("Requested block is not a key block")]
    NotKeyBlock,
    #[error("Block is not from masterchain")]
    BlockNotFromMasterChain,
    #[error("Invalid argument")]
    InvalidArgument,
    #[error("Invalid root hash")]
    InvalidRootHash,
    #[error("Invalid file hash")]
    InvalidFileHash,
    #[error("Archive not found")]
    ArchiveNotFound,
}
