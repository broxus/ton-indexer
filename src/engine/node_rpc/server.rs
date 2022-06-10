/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
/// - simplified answer processing
///
use std::borrow::Cow;
use std::future::Future;
use std::sync::{Arc, Weak};

use anyhow::Result;
use everscale_network::{QueryConsumingResult, QuerySubscriber, SubscriberContext};
use tl_proto::{TlRead, TlWrite};

use crate::db::{BlockConnection, Db, KeyBlocksDirection};
use crate::engine::Engine;
use crate::proto;

pub struct NodeRpcServer(Weak<Engine>);

impl NodeRpcServer {
    pub fn new(engine: &Arc<Engine>) -> Arc<Self> {
        Arc::new(Self(Arc::downgrade(engine)))
    }

    async fn answer<'a, Q, F, R>(
        &self,
        query: Cow<'a, [u8]>,
        handler: fn(QueryHandler, Q) -> F,
        into_answer: fn(R) -> Vec<u8>,
    ) -> Result<QueryConsumingResult<'a>>
    where
        for<'b> Q: TlRead<'b, Repr = tl_proto::Boxed> + 'static,
        F: Future<Output = Result<R>>,
        R: Send,
    {
        let query = tl_proto::deserialize(&query)?;

        match self.0.upgrade() {
            Some(engine) => handler(QueryHandler(engine), query)
                .await
                .map(|data| QueryConsumingResult::Consumed(Some(into_answer(data)))),
            None => Err(NodeRpcServerError::EngineDropped.into()),
        }
    }
}

#[async_trait::async_trait]
impl QuerySubscriber for NodeRpcServer {
    async fn try_consume_query<'a>(
        &self,
        _: SubscriberContext<'a>,
        constructor: u32,
        query: Cow<'a, [u8]>,
    ) -> Result<QueryConsumingResult<'a>> {
        #[inline(always)]
        fn answer<T: TlWrite<Repr = tl_proto::Boxed>>(data: T) -> Vec<u8> {
            tl_proto::serialize(data)
        }

        #[inline(always)]
        fn answer_raw(data: Vec<u8>) -> Vec<u8> {
            data
        }

        macro_rules! select_query {
            ($($ty:ident => $handler:ident => $into_answer:ident),*, _ => { $($rest:tt)* }) => {
                match constructor {
                    $(
                        proto::$ty::TL_ID => self.answer(query, QueryHandler::$handler, $into_answer).await,
                    )*
                    $($rest)*
                }
            };
        }

        select_query! {
            RpcGetNextBlockDescription => get_next_block_description => answer,
            RpcPrepareBlockProof => prepare_block_proof => answer,
            RpcPrepareKeyBlockProof => prepare_key_block_proof => answer,
            RpcPrepareBlock => prepare_block => answer,
            RpcPreparePersistentState => prepare_persistent_state => answer,
            RpcPrepareZeroState => prepare_zero_state => answer,
            RpcGetNextKeyBlockIds => get_next_key_block_ids => answer,
            RpcDownloadNextBlockFull => download_next_block_full => answer,
            RpcDownloadBlockFull => download_block_full => answer,
            RpcDownloadBlock => download_block => answer_raw,
            RpcDownloadBlockProof => download_block_proof => answer_raw,
            RpcDownloadKeyBlockProof => download_key_block_proof => answer_raw,
            RpcDownloadBlockProofLink => download_block_proof_link => answer_raw,
            RpcDownloadKeyBlockProofLink => download_key_block_proof_link => answer_raw,
            RpcGetArchiveInfo => get_archive_info => answer,
            RpcGetArchiveSlice => get_archive_slice => answer_raw,
            _ => {
                proto::RpcGetCapabilities::TL_ID => {
                    Ok(QueryConsumingResult::Consumed(Some(tl_proto::serialize(
                        proto::Capabilities {
                            version: 2,
                            capabilities: 1,
                        },
                    ))))
                }
                _ => Ok(QueryConsumingResult::Consumed(None))
            }
        }
    }
}

struct QueryHandler(Arc<Engine>);

impl QueryHandler {
    async fn get_next_block_description(
        self,
        query: proto::RpcGetNextBlockDescription,
    ) -> Result<proto::BlockDescription> {
        let db = self.0.db.block_connection_storage();

        match db.load_connection(&query.prev_block, BlockConnection::Next1) {
            Ok(id) => Ok(proto::BlockDescription::Found { id }),
            Err(_) => Ok(proto::BlockDescription::Empty),
        }
    }

    async fn prepare_block_proof(
        self,
        query: proto::RpcPrepareBlockProof,
    ) -> Result<proto::PreparedProof> {
        find_block_proof(&self.0.db, &query.block, query.allow_partial, false)
    }

    async fn prepare_key_block_proof(
        self,
        query: proto::RpcPrepareKeyBlockProof,
    ) -> Result<proto::PreparedProof> {
        find_block_proof(&self.0.db, &query.block, query.allow_partial, true)
    }

    async fn prepare_block(self, query: proto::RpcPrepareBlock) -> Result<proto::Prepared> {
        let block_handle_storage = self.0.db.block_handle_storage();

        Ok(match block_handle_storage.load_handle(&query.block)? {
            Some(handle) if handle.meta().has_data() => proto::Prepared::Found,
            _ => proto::Prepared::NotFound,
        })
    }

    async fn prepare_persistent_state(
        self,
        _: proto::RpcPreparePersistentState,
    ) -> Result<proto::PreparedState> {
        // TODO: implement
        Ok(proto::PreparedState::NotFound)
    }

    async fn prepare_zero_state(
        self,
        _: proto::RpcPrepareZeroState,
    ) -> Result<proto::PreparedState> {
        // TODO: implement
        Ok(proto::PreparedState::NotFound)
    }

    async fn get_next_key_block_ids(
        self,
        query: proto::RpcGetNextKeyBlockIds,
    ) -> Result<proto::KeyBlocks> {
        const NEXT_KEY_BLOCKS_LIMIT: usize = 8;

        let block_handle_storage = self.0.db.block_handle_storage();

        let limit = std::cmp::min(query.max_size as usize, NEXT_KEY_BLOCKS_LIMIT);

        let get_next_key_block_ids = || {
            let start_block_id = &query.block;
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
                ids.push(id);
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

        Ok(proto::KeyBlocks {
            blocks,
            incomplete,
            error,
        })
    }

    async fn download_next_block_full(
        self,
        query: proto::RpcDownloadNextBlockFull,
    ) -> Result<proto::DataFull> {
        let block_handle_storage = self.0.db.block_handle_storage();
        let block_connection_storage = self.0.db.block_connection_storage();
        let block_storage = self.0.db.block_storage();

        let next_block_id = match block_handle_storage.load_handle(&query.prev_block)? {
            Some(handle) if handle.meta().has_next1() => block_connection_storage
                .load_connection(&query.prev_block, BlockConnection::Next1)?,
            _ => return Ok(proto::DataFull::Empty),
        };

        let mut is_link = false;
        Ok(match block_handle_storage.load_handle(&next_block_id)? {
            Some(handle) if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) => {
                let block = block_storage.load_block_data_raw(&handle).await?;
                let proof = block_storage.load_block_proof_raw(&handle, is_link).await?;

                proto::DataFull::Found {
                    block_id: next_block_id,
                    proof: proof.into(),
                    block: block.into(),
                    is_link,
                }
            }
            _ => proto::DataFull::Empty,
        })
    }

    async fn download_block_full(
        self,
        query: proto::RpcDownloadBlockFull,
    ) -> Result<proto::DataFull> {
        let block_handle_storage = self.0.db.block_handle_storage();
        let block_storage = self.0.db.block_storage();

        let mut is_link = false;
        Ok(match block_handle_storage.load_handle(&query.block)? {
            Some(handle) if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) => {
                let block = block_storage.load_block_data_raw(&handle).await?;
                let proof = block_storage.load_block_proof_raw(&handle, is_link).await?;

                proto::DataFull::Found {
                    block_id: query.block,
                    proof: proof.into(),
                    block: block.into(),
                    is_link,
                }
            }
            _ => proto::DataFull::Empty,
        })
    }

    async fn download_block(self, query: proto::RpcDownloadBlock) -> Result<Vec<u8>> {
        match self.0.db.block_handle_storage().load_handle(&query.block)? {
            Some(handle) if handle.meta().has_data() => {
                self.0.db.block_storage().load_block_data_raw(&handle).await
            }
            _ => Err(NodeRpcServerError::BlockNotFound.into()),
        }
    }

    async fn download_block_proof(self, query: proto::RpcDownloadBlockProof) -> Result<Vec<u8>> {
        load_block_proof(&self.0.db, &query.block, false).await
    }

    async fn download_key_block_proof(
        self,
        query: proto::RpcDownloadKeyBlockProof,
    ) -> Result<Vec<u8>> {
        load_block_proof(&self.0.db, &query.block, false).await
    }

    async fn download_block_proof_link(
        self,
        query: proto::RpcDownloadBlockProofLink,
    ) -> Result<Vec<u8>> {
        load_block_proof(&self.0.db, &query.block, true).await
    }

    async fn download_key_block_proof_link(
        self,
        query: proto::RpcDownloadKeyBlockProofLink,
    ) -> Result<Vec<u8>> {
        load_block_proof(&self.0.db, &query.block, true).await
    }

    async fn get_archive_info(self, query: proto::RpcGetArchiveInfo) -> Result<proto::ArchiveInfo> {
        let mc_seq_no = query.masterchain_seqno;

        let last_applied_mc_block = self.0.load_last_applied_mc_block_id()?;
        if mc_seq_no > last_applied_mc_block.seq_no {
            return Ok(proto::ArchiveInfo::NotFound);
        }

        let shards_client_mc_block_id = self.0.load_shards_client_mc_block_id()?;
        if mc_seq_no > shards_client_mc_block_id.seq_no {
            return Ok(proto::ArchiveInfo::NotFound);
        }

        Ok(match self.0.db.block_storage().get_archive_id(mc_seq_no) {
            Some(id) => proto::ArchiveInfo::Found { id: id as u64 },
            None => proto::ArchiveInfo::NotFound,
        })
    }

    async fn get_archive_slice(self, query: proto::RpcGetArchiveSlice) -> Result<Vec<u8>> {
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
) -> Result<proto::PreparedProof> {
    let handle = match db.block_handle_storage().load_handle(block_id)? {
        Some(handle) => handle,
        None => return Ok(proto::PreparedProof::Empty),
    };
    let meta = handle.meta();

    if key_block && !meta.is_key_block() {
        return Err(NodeRpcServerError::NotKeyBlock.into());
    }

    if meta.has_proof() {
        Ok(proto::PreparedProof::Found)
    } else if allow_partial && meta.has_proof_link() {
        Ok(proto::PreparedProof::Link)
    } else {
        Ok(proto::PreparedProof::Empty)
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
    #[error("Invalid root hash")]
    InvalidRootHash,
    #[error("Invalid file hash")]
    InvalidFileHash,
    #[error("Archive not found")]
    ArchiveNotFound,
}
