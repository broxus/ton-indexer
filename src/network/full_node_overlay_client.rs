use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use dashmap::DashSet;
use tiny_adnl::utils::*;
use tiny_adnl::{Neighbour, OverlayClient};
use ton_api::ton::{self, TLObject};
use ton_api::{Deserializer, IntoBoxed};

use crate::utils::*;

#[async_trait::async_trait]
pub trait FullNodeOverlayClient: Send + Sync {
    async fn broadcast_external_message(&self, message: &[u8]) -> Result<()>;

    async fn check_persistent_state(
        &self,
        block_id: &ton_block::BlockIdExt,
        masterchain_block_id: &ton_block::BlockIdExt,
        active_peers: &Arc<DashSet<AdnlNodeIdShort>>,
    ) -> Result<Option<Arc<Neighbour>>>;

    async fn download_persistent_state_part(
        &self,
        block_id: &ton_block::BlockIdExt,
        masterchain_block_id: &ton_block::BlockIdExt,
        offset: usize,
        mas_size: usize,
        neighbour: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<Vec<u8>>;

    async fn download_zero_state(
        &self,
        id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<ShardStateStuff>>>;

    async fn download_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        is_link: bool,
        is_key_block: bool,
    ) -> Result<Option<BlockProofStuff>>;

    async fn download_block_full(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuff, BlockProofStuff)>>;

    async fn download_next_block_full(
        &self,
        prev_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuff, BlockProofStuff)>>;

    async fn download_next_key_blocks_ids(
        &self,
        block_id: &ton_block::BlockIdExt,
        max_size: i32,
    ) -> Result<Vec<ton_block::BlockIdExt>>;

    async fn download_archive(&self, masterchain_seqno: u32) -> Result<Option<Vec<u8>>>;

    async fn wait_broadcast(&self) -> Result<(ton::ton_node::Broadcast, AdnlNodeIdShort)>;
}

#[async_trait::async_trait]
impl FullNodeOverlayClient for OverlayClient {
    async fn broadcast_external_message(&self, message: &[u8]) -> Result<()> {
        let broadcast = serialize_boxed(ton::ton_node::broadcast::ExternalMessageBroadcast {
            message: ton::ton_node::externalmessage::ExternalMessage {
                data: ton::bytes(message.to_vec()),
            },
        })?;
        self.overlay()
            .broadcast(&self.overlay_id(), &broadcast, None)?;
        Ok(())
    }

    async fn check_persistent_state(
        &self,
        block_id: &ton_block::BlockIdExt,
        masterchain_block_id: &ton_block::BlockIdExt,
        active_peers: &Arc<DashSet<AdnlNodeIdShort>>,
    ) -> Result<Option<Arc<Neighbour>>> {
        let (prepare, neighbour): (ton::ton_node::PreparedState, _) = self
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

    async fn download_persistent_state_part(
        &self,
        block_id: &ton_block::BlockIdExt,
        masterchain_block_id: &ton_block::BlockIdExt,
        offset: usize,
        mas_size: usize,
        neighbour: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<Vec<u8>> {
        self.send_rldp_query_raw(
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

    async fn download_zero_state(
        &self,
        id: &ton_block::BlockIdExt,
    ) -> Result<Option<Arc<ShardStateStuff>>> {
        // Prepare
        let (prepare, neighbour): (ton::ton_node::PreparedState, _) = self
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
                let state_bytes = self
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

    async fn download_block_proof(
        &self,
        block_id: &ton_block::BlockIdExt,
        is_link: bool,
        is_key_block: bool,
    ) -> Result<Option<BlockProofStuff>> {
        // Prepare
        let (prepare, neighbour): (ton::ton_node::PreparedProof, _) = if is_key_block {
            self.send_adnl_query(
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
            self.send_adnl_query(
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
                let data = self
                    .send_rldp_query_raw(
                        neighbour,
                        &ton::rpc::ton_node::DownloadKeyBlockProof {
                            block: convert_block_id_ext_blk2api(block_id),
                        },
                        0,
                    )
                    .await?;
                Ok(Some(BlockProofStuff::deserialize(
                    block_id.clone(),
                    data,
                    false,
                )?))
            }
            ton::ton_node::PreparedProof::TonNode_PreparedProof => {
                let data = self
                    .send_rldp_query_raw(
                        neighbour,
                        &ton::rpc::ton_node::DownloadBlockProof {
                            block: convert_block_id_ext_blk2api(block_id),
                        },
                        0,
                    )
                    .await?;
                Ok(Some(BlockProofStuff::deserialize(
                    block_id.clone(),
                    data,
                    false,
                )?))
            }
            ton::ton_node::PreparedProof::TonNode_PreparedProofLink if is_key_block => {
                let data = self
                    .send_rldp_query_raw(
                        neighbour,
                        &ton::rpc::ton_node::DownloadKeyBlockProofLink {
                            block: convert_block_id_ext_blk2api(block_id),
                        },
                        0,
                    )
                    .await?;
                Ok(Some(BlockProofStuff::deserialize(
                    block_id.clone(),
                    data,
                    true,
                )?))
            }
            ton::ton_node::PreparedProof::TonNode_PreparedProofLink => {
                let data = self
                    .send_rldp_query_raw(
                        neighbour,
                        &ton::rpc::ton_node::DownloadBlockProofLink {
                            block: convert_block_id_ext_blk2api(block_id),
                        },
                        0,
                    )
                    .await?;
                Ok(Some(BlockProofStuff::deserialize(
                    block_id.clone(),
                    data,
                    true,
                )?))
            }
            ton::ton_node::PreparedProof::TonNode_PreparedProofEmpty => Ok(None),
        }
    }

    async fn download_block_full(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuff, BlockProofStuff)>> {
        // Prepare
        let (prepare, neighbour): (ton::ton_node::Prepared, _) = self
            .send_adnl_query(
                ton::rpc::ton_node::PrepareBlock {
                    block: convert_block_id_ext_blk2api(block_id),
                },
                Some(1),
                None,
                None,
            )
            .await?;

        log::info!("Got prepare result: {:?} for {}", prepare, block_id,);

        // Download
        match prepare {
            ton::ton_node::Prepared::TonNode_NotFound => Ok(None),
            ton::ton_node::Prepared::TonNode_Prepared => {
                let block_data: ton::ton_node::DataFull = self
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
                            BlockStuff::deserialize_checked(block_id.clone(), data.block.0)?;
                        let proof = BlockProofStuff::deserialize(
                            block_id.clone(),
                            data.proof.0,
                            data.is_link.into(),
                        )?;
                        Ok(Some((block, proof)))
                    }
                    ton::ton_node::DataFull::TonNode_DataFullEmpty => {
                        log::warn!("prepareBlock receives Prepared, but DownloadBlockFull receives DataFullEmpty");
                        Ok(None)
                    }
                }
            }
        }
    }

    async fn download_next_block_full(
        &self,
        prev_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuff, BlockProofStuff)>> {
        const NO_NEIGHBOURS_DELAY: u64 = 1000; // Milliseconds

        let query = &ton::rpc::ton_node::DownloadNextBlockFull {
            prev_block: convert_block_id_ext_blk2api(prev_id),
        };

        let neighbour = if let Some(neighbour) = self.neighbours().choose_neighbour() {
            neighbour
        } else {
            tokio::time::sleep(Duration::from_millis(NO_NEIGHBOURS_DELAY)).await;
            return Err(anyhow!("neighbour is not found!"));
        };
        log::trace!("USE PEER {}, REQUEST {:?}", neighbour.peer_id(), query);

        // Download
        let data_full: ton::ton_node::DataFull = self.send_rldp_query(query, neighbour, 0).await?;

        // Parse
        match data_full {
            ton::ton_node::DataFull::TonNode_DataFullEmpty => return Ok(None),
            ton::ton_node::DataFull::TonNode_DataFull(data_full) => {
                let id = convert_block_id_ext_api2blk(&data_full.id)?;
                let block = BlockStuff::deserialize_checked(id.clone(), data_full.block.to_vec())?;
                let proof =
                    BlockProofStuff::deserialize(id, data_full.proof.0, data_full.is_link.into())?;
                Ok(Some((block, proof)))
            }
        }
    }

    async fn download_next_key_blocks_ids(
        &self,
        block_id: &ton_block::BlockIdExt,
        max_size: i32,
    ) -> Result<Vec<ton_block::BlockIdExt>> {
        let query = TLObject::new(ton::rpc::ton_node::GetNextKeyBlockIds {
            block: convert_block_id_ext_blk2api(block_id),
            max_size,
        });

        self.send_adnl_query(query, None, None, None)
            .await
            .and_then(|(ids, _): (ton::ton_node::KeyBlocks, _)| {
                ids.blocks()
                    .iter()
                    .map(convert_block_id_ext_api2blk)
                    .collect()
            })
    }

    async fn download_archive(&self, masterchain_seqno: u32) -> Result<Option<Vec<u8>>> {
        const CHUNK_SIZE: i32 = 1 << 21; // 2 MB

        // Prepare
        let (archive_info, neighbour): (ton::ton_node::ArchiveInfo, _) = self
            .send_adnl_query(
                ton::rpc::ton_node::GetArchiveInfo {
                    masterchain_seqno: masterchain_seqno as i32,
                },
                None,
                Some(TIMEOUT_PREPARE),
                None,
            )
            .await?;

        // Download
        let info = match archive_info {
            ton::ton_node::ArchiveInfo::TonNode_ArchiveInfo(info) => info,
            ton::ton_node::ArchiveInfo::TonNode_ArchiveNotFound => return Ok(None),
        };

        let mut result = Vec::new();
        let mut offset = 0;
        let mut part_attempt = 0;
        let mut peer_attempt = 0;
        loop {
            match self
                .send_rldp_query_raw(
                    neighbour.clone(),
                    &ton::rpc::ton_node::GetArchiveSlice {
                        archive_id: info.id,
                        offset,
                        max_size: CHUNK_SIZE,
                    },
                    peer_attempt,
                )
                .await
            {
                Ok(mut chunk) => {
                    let chunk_len = chunk.len() as i32;
                    result.append(&mut chunk);

                    if chunk_len < CHUNK_SIZE {
                        return Ok(Some(result));
                    }

                    offset += chunk_len as i64;
                    part_attempt = 0;
                }
                Err(e) => {
                    peer_attempt += 1;
                    part_attempt += 1;
                    log::error!(
                        "Failed to download archive {}: {}, offset: {}, attempt: {}",
                        info.id,
                        e,
                        offset,
                        part_attempt
                    );

                    if part_attempt > 10 {
                        return Err(anyhow!(
                            "Failed to download archive after {} attempts: {}",
                            part_attempt,
                            e
                        ));
                    }
                }
            }
        }
    }

    async fn wait_broadcast(&self) -> Result<(ton::ton_node::Broadcast, AdnlNodeIdShort)> {
        loop {
            match self.overlay().wait_for_broadcast(self.overlay_id()).await {
                Ok(info) => {
                    let answer: ton::ton_node::Broadcast =
                        Deserializer::new(&mut std::io::Cursor::new(info.data))
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
