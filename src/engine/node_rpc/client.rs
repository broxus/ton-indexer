/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::network::{Neighbour, OverlayClient};
use crate::proto;
use crate::utils::*;

#[derive(Clone)]
pub struct NodeRpcClient(pub Arc<OverlayClient>);

impl NodeRpcClient {
    pub fn broadcast_external_message(&self, message: &[u8]) {
        let this = &self.0;

        let broadcast = tl_proto::serialize(proto::ExternalMessageBroadcast { data: message });
        this.broadcast(broadcast, None);
    }

    pub async fn find_persistent_state(
        &self,
        full_state_id: &FullStateId,
    ) -> Result<Option<Arc<Neighbour>>> {
        let this = &self.0;

        let (prepare, neighbour): (proto::PreparedState, _) = this
            .send_adnl_query(
                proto::RpcPreparePersistentState {
                    block: full_state_id.block_id.clone(),
                    masterchain_block: full_state_id.mc_block_id.clone(),
                },
                None,
                Some(TIMEOUT_PREPARE),
                None,
            )
            .await?;

        log::info!("Found state {prepare:?} from peer {}", neighbour.peer_id());

        match prepare {
            proto::PreparedState::Found => Ok(Some(neighbour)),
            proto::PreparedState::NotFound => Ok(None),
        }
    }

    pub async fn download_persistent_state_part(
        &self,
        full_state_id: &FullStateId,
        offset: usize,
        mas_size: usize,
        neighbour: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<Vec<u8>> {
        self.0
            .send_rldp_query_raw(
                neighbour,
                proto::RpcDownloadPersistentStateSlice {
                    block: full_state_id.block_id.clone(),
                    masterchain_block: full_state_id.mc_block_id.clone(),
                    offset: offset as u64,
                    max_size: mas_size as u64,
                },
                attempt,
            )
            .await
    }

    pub async fn download_zero_state(
        &self,
        id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<ShardStateStuff>>> {
        let this = &self.0;

        // Prepare
        let (prepare, neighbour): (proto::PreparedState, _) = this
            .send_adnl_query(
                proto::RpcPrepareZeroState { block: id.clone() },
                None,
                Some(TIMEOUT_PREPARE),
                None,
            )
            .await?;

        // Download
        match prepare {
            proto::PreparedState::Found => {
                let state_bytes = this
                    .send_rldp_query_raw(
                        neighbour,
                        proto::RpcDownloadZeroState { block: id.clone() },
                        0,
                    )
                    .await?;

                Ok(Some(Arc::new(ShardStateStuff::deserialize_zerostate(
                    id.clone(),
                    &state_bytes,
                )?)))
            }
            proto::PreparedState::NotFound => Ok(None),
        }
    }

    pub async fn download_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        is_key_block: bool,
        explicit_neighbour: Option<&Arc<Neighbour>>,
    ) -> Result<Option<BlockProofStuffAug>> {
        let this = &self.0;

        // Prepare
        let (prepare, neighbour): (proto::PreparedProof, _) = if is_key_block {
            this.send_adnl_query(
                proto::RpcPrepareKeyBlockProof {
                    block: block_id.clone(),
                    allow_partial: false,
                },
                None,
                Some(TIMEOUT_PREPARE),
                explicit_neighbour,
            )
            .await?
        } else {
            this.send_adnl_query(
                proto::RpcPrepareBlockProof {
                    block: block_id.clone(),
                    allow_partial: !block_id.shard_id.is_masterchain(),
                },
                None,
                Some(TIMEOUT_PREPARE),
                explicit_neighbour,
            )
            .await?
        };

        // Download
        match prepare {
            proto::PreparedProof::Found if is_key_block => {
                let data = this
                    .send_rldp_query_raw(
                        neighbour,
                        proto::RpcDownloadKeyBlockProof {
                            block: block_id.clone(),
                        },
                        0,
                    )
                    .await?;
                let proof = BlockProofStuff::deserialize(block_id.clone(), &data, false)?;
                Ok(Some(WithArchiveData::new(proof, data)))
            }
            proto::PreparedProof::Found => {
                let data = this
                    .send_rldp_query_raw(
                        neighbour,
                        proto::RpcDownloadBlockProof {
                            block: block_id.clone(),
                        },
                        0,
                    )
                    .await?;
                let proof = BlockProofStuff::deserialize(block_id.clone(), &data, false)?;
                Ok(Some(WithArchiveData::new(proof, data)))
            }
            proto::PreparedProof::Link if is_key_block => {
                let data = this
                    .send_rldp_query_raw(
                        neighbour,
                        proto::RpcDownloadKeyBlockProofLink {
                            block: block_id.clone(),
                        },
                        0,
                    )
                    .await?;
                let proof = BlockProofStuff::deserialize(block_id.clone(), &data, true)?;
                Ok(Some(WithArchiveData::new(proof, data)))
            }
            proto::PreparedProof::Link => {
                let data = this
                    .send_rldp_query_raw(
                        neighbour,
                        proto::RpcDownloadBlockProofLink {
                            block: block_id.clone(),
                        },
                        0,
                    )
                    .await?;
                let proof = BlockProofStuff::deserialize(block_id.clone(), &data, true)?;
                Ok(Some(WithArchiveData::new(proof, data)))
            }
            proto::PreparedProof::Empty => Ok(None),
        }
    }

    pub async fn download_block_full(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuffAug, BlockProofStuffAug)>> {
        let this = &self.0;

        // Prepare
        let (prepare, neighbour): (proto::Prepared, _) = this
            .send_adnl_query(
                proto::RpcPrepareBlock {
                    block: block_id.clone(),
                },
                Some(1),
                None,
                None,
            )
            .await?;

        if prepare == proto::Prepared::NotFound {
            return Ok(None);
        }

        // Download
        let block_data: proto::DataFull = this
            .send_rldp_query(
                proto::RpcDownloadBlockFull {
                    block: block_id.clone(),
                },
                neighbour,
                0,
            )
            .await?;

        match block_data {
            proto::DataFull::Found {
                block_id: foudnd_block_id,
                block: block_data,
                proof: proof_data,
                is_link,
            } => {
                if block_id != &foudnd_block_id {
                    return Err(NodeRpcClientError::ReceivedBlockIdMismatch.into());
                }

                let block = BlockStuff::deserialize_checked(block_id.clone(), &block_data)?;
                let proof = BlockProofStuff::deserialize(block_id.clone(), &proof_data, is_link)?;

                Ok(Some((
                    WithArchiveData::new(block, block_data),
                    WithArchiveData::new(proof, proof_data),
                )))
            }
            proto::DataFull::Empty => {
                log::warn!(
                    "prepareBlock receives Prepared, but DownloadBlockFull receives DataFullEmpty"
                );
                Ok(None)
            }
        }
    }

    pub async fn download_next_block_full(
        &self,
        prev_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuffAug, BlockProofStuffAug)>> {
        const NO_NEIGHBOURS_DELAY: u64 = 1000; // Milliseconds

        let this = &self.0;

        let query = proto::RpcDownloadNextBlockFull {
            prev_block: prev_id.clone(),
        };

        let neighbour = if let Some(neighbour) = this.neighbours().choose_neighbour() {
            neighbour
        } else {
            tokio::time::sleep(Duration::from_millis(NO_NEIGHBOURS_DELAY)).await;
            return Err(NodeRpcClientError::NeighbourNotFound.into());
        };

        // Download
        let data_full: proto::DataFull = this.send_rldp_query(query, neighbour, 0).await?;

        // Parse
        match data_full {
            proto::DataFull::Found {
                block_id,
                block: block_data,
                proof: proof_data,
                is_link,
            } => {
                let block = BlockStuff::deserialize_checked(block_id.clone(), &block_data)?;
                let proof = BlockProofStuff::deserialize(block_id, &proof_data, is_link)?;

                Ok(Some((
                    WithArchiveData::new(block, block_data),
                    WithArchiveData::new(proof, proof_data),
                )))
            }
            proto::DataFull::Empty => Ok(None),
        }
    }

    pub async fn download_next_key_blocks_ids(
        &self,
        block_id: &ton_block::BlockIdExt,
        max_size: u16,
        neighbour: Option<&Arc<Neighbour>>,
    ) -> Result<(Vec<ton_block::BlockIdExt>, Arc<Neighbour>)> {
        let this = &self.0;

        let query = proto::RpcGetNextKeyBlockIds {
            block: block_id.clone(),
            max_size: max_size as u32,
        };

        this.send_adnl_query(query, None, None, neighbour)
            .await
            .and_then(|(key_blocks, neighbour): (proto::KeyBlocks, _)| {
                if !key_blocks.error {
                    Ok((key_blocks.blocks, neighbour))
                } else {
                    Err(NodeRpcClientError::KeyBlocksError.into())
                }
            })
    }

    pub async fn download_archive(
        &self,
        masterchain_seqno: u32,
        neighbour: Option<&Arc<Neighbour>>,
        output: &mut (dyn Write + Send),
    ) -> Result<ArchiveDownloadStatus> {
        const CHUNK_SIZE: u32 = 1 << 21; // 2 MB

        let this = &self.0;

        // Prepare
        let (archive_info, neighbour): (proto::ArchiveInfo, _) = this
            .send_adnl_query(
                proto::RpcGetArchiveInfo { masterchain_seqno },
                Some(1),
                Some(TIMEOUT_ARCHIVE),
                neighbour,
            )
            .await?;

        // Download
        let archive_id = match archive_info {
            proto::ArchiveInfo::Found { id } => id,
            proto::ArchiveInfo::NotFound => {
                return Ok(ArchiveDownloadStatus::NotFound);
            }
        };

        let mut verifier = ArchivePackageVerifier::Start;

        let mut offset = 0;
        let mut part_attempt = 0;
        let mut peer_attempt = 0;
        loop {
            match tokio::time::timeout(
                Duration::from_secs(10),
                this.send_rldp_query_raw(
                    neighbour.clone(),
                    proto::RpcGetArchiveSlice {
                        archive_id,
                        offset,
                        max_size: CHUNK_SIZE,
                    },
                    peer_attempt,
                ),
            )
            .await
            {
                Ok(Ok(chunk)) => {
                    let is_last = chunk.len() < CHUNK_SIZE as usize;

                    verifier
                        .verify(&chunk)
                        .context("Received invalid archive chunk")?;
                    if is_last {
                        verifier.final_check().context("Received invalid archive")?;
                    }

                    output
                        .write_all(&chunk)
                        .context("Failed to write archive chunk")?;

                    if is_last {
                        return Ok(ArchiveDownloadStatus::Downloaded {
                            neighbour,
                            len: chunk.len(),
                        });
                    }

                    offset += chunk.len() as u64;
                    part_attempt = 0;
                }
                Ok(Err(e)) => {
                    peer_attempt += 1;
                    part_attempt += 1;
                    log::error!(
                        "Failed to download archive {}: {e:?}, offset: {offset}, attempt: {part_attempt}",
                        archive_id,
                    );

                    if part_attempt > 2 {
                        return Err(NodeRpcClientError::TooManyFailedAttempts.into());
                    }
                }
                Err(_) => {
                    peer_attempt += 1;
                    part_attempt += 1;
                    if part_attempt > 2 {
                        return Err(NodeRpcClientError::RequestTimeout.into());
                    }
                }
            }
        }
    }

    pub async fn wait_broadcast(&self) -> Result<proto::BlockBroadcast> {
        let info = self.0.wait_for_broadcast().await;
        let broadcast = tl_proto::deserialize(&info.data)?;
        Ok(broadcast)
    }
}

#[derive(Clone)]
pub enum ArchiveDownloadStatus {
    Downloaded {
        neighbour: Arc<Neighbour>,
        len: usize,
    },
    NotFound,
}

const TIMEOUT_PREPARE: u64 = 6000; // Milliseconds
const TIMEOUT_ARCHIVE: u64 = 3000;

#[derive(Debug, thiserror::Error)]
enum NodeRpcClientError {
    #[error("Received block id mismatch")]
    ReceivedBlockIdMismatch,
    #[error("Neighbour not found")]
    NeighbourNotFound,
    #[error("Too many failed attempts")]
    TooManyFailedAttempts,
    #[error("Request timeout")]
    RequestTimeout,
    #[error("Failed to get key blocks")]
    KeyBlocksError,
}
