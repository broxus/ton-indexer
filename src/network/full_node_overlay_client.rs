/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use tiny_adnl::utils::*;
use tiny_adnl::{Neighbour, OverlayClient};
use ton_api::ton::{self, TLObject};
use ton_api::Deserializer;

use crate::utils::*;

#[derive(Clone)]
pub struct FullNodeOverlayClient(pub Arc<OverlayClient>);

impl FullNodeOverlayClient {
    pub async fn broadcast_external_message(&self, message: &[u8]) -> Result<()> {
        let this = &self.0;

        let broadcast = serialize_boxed(ton::ton_node::broadcast::ExternalMessageBroadcast {
            message: ton::ton_node::externalmessage::ExternalMessage {
                data: ton::bytes(message.to_vec()),
            },
        })?;
        this.overlay()
            .broadcast(this.overlay_id(), broadcast, None)?;
        Ok(())
    }

    pub async fn check_persistent_state(
        &self,
        block_id: &ton_block::BlockIdExt,
        masterchain_block_id: &ton_block::BlockIdExt,
        active_peers: &Arc<ActivePeers>,
    ) -> Result<Option<Arc<Neighbour>>> {
        let this = &self.0;

        let (prepare, neighbour): (ton::ton_node::PreparedState, _) = this
            .send_adnl_query(
                TLObject::new(ton::rpc::ton_node::PreparePersistentState {
                    block: convert_block_id_ext_blk2api(block_id),
                    masterchain_block: convert_block_id_ext_blk2api(masterchain_block_id),
                }),
                None,
                Some(TIMEOUT_PREPARE),
                Some(active_peers),
            )
            .await?;

        log::info!("Prepared state: {:?} from {}", prepare, neighbour.peer_id());

        match prepare {
            ton::ton_node::PreparedState::TonNode_PreparedState => Ok(Some(neighbour)),
            ton::ton_node::PreparedState::TonNode_NotFoundState => {
                active_peers.remove(neighbour.peer_id());
                Ok(None)
            }
        }
    }

    pub async fn download_persistent_state_part(
        &self,
        block_id: &ton_block::BlockIdExt,
        masterchain_block_id: &ton_block::BlockIdExt,
        offset: usize,
        mas_size: usize,
        neighbour: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<Vec<u8>> {
        self.0
            .send_rldp_query_raw(
                neighbour,
                &ton::rpc::ton_node::DownloadPersistentStateSlice {
                    block: convert_block_id_ext_blk2api(block_id),
                    masterchain_block: convert_block_id_ext_blk2api(masterchain_block_id),
                    offset: offset as i64,
                    max_size: mas_size as i64,
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
        let (prepare, neighbour): (ton::ton_node::PreparedState, _) = this
            .send_adnl_query(
                TLObject::new(ton::rpc::ton_node::PrepareZeroState {
                    block: convert_block_id_ext_blk2api(id),
                }),
                None,
                Some(TIMEOUT_PREPARE),
                None,
            )
            .await?;

        // Download
        match prepare {
            ton::ton_node::PreparedState::TonNode_PreparedState => {
                let state_bytes = this
                    .send_rldp_query_raw(
                        neighbour,
                        &ton::rpc::ton_node::DownloadZeroState {
                            block: convert_block_id_ext_blk2api(id),
                        },
                        0,
                    )
                    .await?;

                Ok(Some(Arc::new(ShardStateStuff::deserialize_zerostate(
                    id.clone(),
                    &state_bytes,
                )?)))
            }
            ton::ton_node::PreparedState::TonNode_NotFoundState => Ok(None),
        }
    }

    pub async fn download_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        is_link: bool,
        is_key_block: bool,
    ) -> Result<Option<BlockProofStuffAug>> {
        let this = &self.0;

        // Prepare
        let (prepare, neighbour): (ton::ton_node::PreparedProof, _) = if is_key_block {
            this.send_adnl_query(
                ton::rpc::ton_node::PrepareKeyBlockProof {
                    block: convert_block_id_ext_blk2api(block_id),
                    allow_partial: is_link.into(),
                },
                None,
                Some(TIMEOUT_PREPARE),
                None,
            )
            .await?
        } else {
            this.send_adnl_query(
                ton::rpc::ton_node::PrepareBlockProof {
                    block: convert_block_id_ext_blk2api(block_id),
                    allow_partial: Default::default(),
                },
                None,
                Some(TIMEOUT_PREPARE),
                None,
            )
            .await?
        };

        // Download
        match prepare {
            ton::ton_node::PreparedProof::TonNode_PreparedProof if is_key_block => {
                let data = this
                    .send_rldp_query_raw(
                        neighbour,
                        &ton::rpc::ton_node::DownloadKeyBlockProof {
                            block: convert_block_id_ext_blk2api(block_id),
                        },
                        0,
                    )
                    .await?;
                let proof = BlockProofStuff::deserialize(block_id.clone(), &data, false)?;
                Ok(Some(WithArchiveData::new(proof, data)))
            }
            ton::ton_node::PreparedProof::TonNode_PreparedProof => {
                let data = this
                    .send_rldp_query_raw(
                        neighbour,
                        &ton::rpc::ton_node::DownloadBlockProof {
                            block: convert_block_id_ext_blk2api(block_id),
                        },
                        0,
                    )
                    .await?;
                let proof = BlockProofStuff::deserialize(block_id.clone(), &data, false)?;
                Ok(Some(WithArchiveData::new(proof, data)))
            }
            ton::ton_node::PreparedProof::TonNode_PreparedProofLink if is_key_block => {
                let data = this
                    .send_rldp_query_raw(
                        neighbour,
                        &ton::rpc::ton_node::DownloadKeyBlockProofLink {
                            block: convert_block_id_ext_blk2api(block_id),
                        },
                        0,
                    )
                    .await?;
                let proof = BlockProofStuff::deserialize(block_id.clone(), &data, true)?;
                Ok(Some(WithArchiveData::new(proof, data)))
            }
            ton::ton_node::PreparedProof::TonNode_PreparedProofLink => {
                let data = this
                    .send_rldp_query_raw(
                        neighbour,
                        &ton::rpc::ton_node::DownloadBlockProofLink {
                            block: convert_block_id_ext_blk2api(block_id),
                        },
                        0,
                    )
                    .await?;
                let proof = BlockProofStuff::deserialize(block_id.clone(), &data, true)?;
                Ok(Some(WithArchiveData::new(proof, data)))
            }
            ton::ton_node::PreparedProof::TonNode_PreparedProofEmpty => Ok(None),
        }
    }

    pub async fn download_block_full(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuffAug, BlockProofStuffAug)>> {
        let this = &self.0;

        // Prepare
        let (prepare, neighbour): (ton::ton_node::Prepared, _) = this
            .send_adnl_query(
                ton::rpc::ton_node::PrepareBlock {
                    block: convert_block_id_ext_blk2api(block_id),
                },
                Some(1),
                None,
                None,
            )
            .await?;

        // Download
        match prepare {
            ton::ton_node::Prepared::TonNode_NotFound => Ok(None),
            ton::ton_node::Prepared::TonNode_Prepared => {
                let block_data: ton::ton_node::DataFull = this
                    .send_rldp_query(
                        &ton::rpc::ton_node::DownloadBlockFull {
                            block: convert_block_id_ext_blk2api(block_id),
                        },
                        neighbour,
                        0,
                    )
                    .await?;

                match block_data {
                    ton::ton_node::DataFull::TonNode_DataFull(data) => {
                        if !compare_block_ids(block_id, &data.id) {
                            return Err(anyhow!("Received block id mismatch"));
                        }

                        let block =
                            BlockStuff::deserialize_checked(block_id.clone(), &data.block.0)?;
                        let proof = BlockProofStuff::deserialize(
                            block_id.clone(),
                            &data.proof.0,
                            data.is_link.into(),
                        )?;

                        Ok(Some((
                            WithArchiveData::new(block, data.block.0),
                            WithArchiveData::new(proof, data.proof.0),
                        )))
                    }
                    ton::ton_node::DataFull::TonNode_DataFullEmpty => {
                        log::warn!("prepareBlock receives Prepared, but DownloadBlockFull receives DataFullEmpty");
                        Ok(None)
                    }
                }
            }
        }
    }

    pub async fn download_next_block_full(
        &self,
        prev_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuffAug, BlockProofStuffAug)>> {
        const NO_NEIGHBOURS_DELAY: u64 = 1000; // Milliseconds

        let this = &self.0;

        let query = &ton::rpc::ton_node::DownloadNextBlockFull {
            prev_block: convert_block_id_ext_blk2api(prev_id),
        };

        let neighbour = if let Some(neighbour) = this.neighbours().choose_neighbour() {
            neighbour
        } else {
            tokio::time::sleep(Duration::from_millis(NO_NEIGHBOURS_DELAY)).await;
            return Err(anyhow!("neighbour is not found!"));
        };
        log::trace!("USE PEER {}, REQUEST {:?}", neighbour.peer_id(), query);

        // Download
        let data_full: ton::ton_node::DataFull = this.send_rldp_query(query, neighbour, 0).await?;

        // Parse
        match data_full {
            ton::ton_node::DataFull::TonNode_DataFullEmpty => Ok(None),
            ton::ton_node::DataFull::TonNode_DataFull(data_full) => {
                let id = convert_block_id_ext_api2blk(&data_full.id)?;

                let block = BlockStuff::deserialize_checked(id.clone(), &data_full.block.0)?;
                let proof =
                    BlockProofStuff::deserialize(id, &data_full.proof.0, data_full.is_link.into())?;

                Ok(Some((
                    WithArchiveData::new(block, data_full.block.0),
                    WithArchiveData::new(proof, data_full.proof.0),
                )))
            }
        }
    }

    pub async fn download_next_key_blocks_ids(
        &self,
        block_id: &ton_block::BlockIdExt,
        max_size: i32,
    ) -> Result<Vec<ton_block::BlockIdExt>> {
        let this = &self.0;

        let query = TLObject::new(ton::rpc::ton_node::GetNextKeyBlockIds {
            block: convert_block_id_ext_blk2api(block_id),
            max_size,
        });

        this.send_adnl_query(query, None, None, None)
            .await
            .and_then(|(ids, _): (ton::ton_node::KeyBlocks, _)| {
                ids.blocks()
                    .iter()
                    .map(convert_block_id_ext_api2blk)
                    .collect()
            })
    }

    pub async fn download_archive(
        &self,
        masterchain_seqno: u32,
        active_peers: &Arc<ActivePeers>,
    ) -> Result<Option<Vec<u8>>> {
        const CHUNK_SIZE: i32 = 1 << 21; // 2 MB

        let this = &self.0;

        // Prepare
        let (archive_info, neighbour): (ton::ton_node::ArchiveInfo, _) = this
            .send_adnl_query(
                ton::rpc::ton_node::GetArchiveInfo {
                    masterchain_seqno: masterchain_seqno as i32,
                },
                Some(2),
                Some(TIMEOUT_ARCHIVE),
                None,
            )
            .await?;

        // Download
        let info = match archive_info {
            ton::ton_node::ArchiveInfo::TonNode_ArchiveInfo(info) => info,
            ton::ton_node::ArchiveInfo::TonNode_ArchiveNotFound => {
                active_peers.remove(neighbour.peer_id());
                return Ok(None);
            }
        };

        let mut result = Vec::new();
        let mut offset = 0;
        let mut part_attempt = 0;
        let mut peer_attempt = 0;
        loop {
            match tokio::time::timeout(
                Duration::from_secs(10),
                this.send_rldp_query_raw(
                    neighbour.clone(),
                    &ton::rpc::ton_node::GetArchiveSlice {
                        archive_id: info.id,
                        offset,
                        max_size: CHUNK_SIZE,
                    },
                    peer_attempt,
                ),
            )
            .await
            {
                Ok(Ok(mut chunk)) => {
                    let chunk_len = chunk.len() as i32;
                    result.append(&mut chunk);

                    if chunk_len < CHUNK_SIZE {
                        active_peers.remove(neighbour.peer_id());
                        return Ok(Some(result));
                    }

                    offset += chunk_len as i64;
                    part_attempt = 0;
                }
                Ok(Err(e)) => {
                    peer_attempt += 1;
                    part_attempt += 1;
                    log::error!(
                        "Failed to download archive {}: {}, offset: {}, attempt: {}",
                        info.id,
                        e,
                        offset,
                        part_attempt
                    );

                    if part_attempt > 2 {
                        active_peers.remove(neighbour.peer_id());
                        return Err(anyhow!(
                            "Failed to download archive after {} attempts: {}",
                            part_attempt,
                            e
                        ));
                    }
                }
                Err(_) => {
                    peer_attempt += 1;
                    part_attempt += 1;
                    if part_attempt > 2 {
                        active_peers.remove(neighbour.peer_id());
                        return Err(anyhow!(
                            "Failed to download archive after {} attempts: timeout",
                            part_attempt,
                        ));
                    }
                }
            }
        }
    }

    pub async fn wait_broadcast(&self) -> Result<(ton::ton_node::Broadcast, AdnlNodeIdShort)> {
        let this = &self.0;

        loop {
            match this.overlay().wait_for_broadcast(this.overlay_id()).await {
                Ok(info) => {
                    let answer: ton::ton_node::Broadcast =
                        Deserializer::new(&mut info.data.as_slice())
                            .read_boxed()
                            .map_err(|e| anyhow::Error::msg(e.to_string()))?;
                    break Ok((answer, info.from));
                }
                Err(e) => log::error!("broadcast waiting error: {}", e),
            }
        }
    }
}

const TIMEOUT_PREPARE: u64 = 6000; // Milliseconds
const TIMEOUT_ARCHIVE: u64 = 3000;
