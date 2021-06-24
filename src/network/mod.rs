use std::convert::{TryFrom, TryInto};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use dashmap::{DashMap, DashSet};
use tiny_adnl::utils::*;
use tiny_adnl::*;
use ton_api::ton::rpc;
use ton_api::ton::{self, TLObject};
use ton_api::{BoxedDeserialize, BoxedSerialize, Deserializer};

use crate::block::{convert_block_id_ext_api2blk, convert_block_id_ext_blk2api, BlockStuff};
use crate::config::Config;

pub struct NodeNetwork {
    adnl: Arc<AdnlNode>,
    dht: Arc<DhtNode>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    masterchain_overlay_short_id: OverlayIdShort,
    masterchain_overlay_id: OverlayIdFull,
    overlays: Arc<DashMap<OverlayIdShort, Arc<OverlayClient>>>,
    overlay_awaiters: OperationsPool<OverlayIdShort, Arc<dyn FullNodeOverlayClient>>,
}

impl NodeNetwork {
    pub const TAG_DHT_KEY: usize = 1;
    pub const TAG_OVERLAY_KEY: usize = 2;

    const PERIOD_CHECK_OVERLAY_NODES: u64 = 1; // seconds
    const PERIOD_STORE_IP_ADDRESS: u64 = 500; // seconds
    const PERIOD_UPDATE_PEERS: u64 = 5; // seconds

    pub async fn new(mut config: Config) -> Result<Arc<Self>> {
        let masterchain_zero_state_id = config.zero_state;

        let adnl = AdnlNode::with_config(config.adnl.take().unwrap().try_into()?);
        let dht = DhtNode::with_adnl_node(adnl.clone(), Self::TAG_DHT_KEY)?;

        let overlay = OverlayNode::with_adnl_node_and_zero_state(
            adnl.clone(),
            masterchain_zero_state_id.file_hash.as_slice().try_into()?,
            Self::TAG_DHT_KEY,
        )?;
        let rldp = RldpNode::with_adnl_node(adnl.clone(), vec![overlay.clone()]);

        for peer in config.global_config.get_dht_nodes_configs()? {
            dht.add_peer(peer)?;
        }

        let masterchain_shard_id = ton_block::ShardIdent::with_tagged_prefix(
            masterchain_zero_state_id.workchain,
            masterchain_zero_state_id.shard as u64,
        )
        .map_err(|e| anyhow!("Failed to create masterchain shard id: {}", e))?;

        let masterchain_overlay_id = overlay.compute_overlay_id(
            masterchain_shard_id.workchain_id(),
            masterchain_shard_id.shard_prefix_with_tag() as i64,
        )?;
        let masterchain_overlay_short_id = masterchain_overlay_id.compute_short_id()?;

        let dht_key = adnl.key_by_tag(Self::TAG_DHT_KEY)?;
        NodeNetwork::periodic_store_ip_addr(&dht, &dht_key, None);

        let overlay_key = adnl.key_by_tag(Self::TAG_OVERLAY_KEY)?;
        NodeNetwork::periodic_store_ip_addr(&dht, &overlay_key, None);

        let node_network = Arc::new(NodeNetwork {
            adnl,
            dht,
            overlay,
            rldp,
            masterchain_overlay_short_id,
            masterchain_overlay_id,
            overlays: Arc::new(Default::default()),
            overlay_awaiters: OperationsPool::new("overlay_operations"),
        });

        Ok(node_network)
    }

    pub async fn start(self: &Arc<Self>) -> Result<Arc<dyn FullNodeOverlayClient>> {
        self.adnl
            .start(vec![
                self.dht.clone(),
                self.overlay.clone(),
                self.rldp.clone(),
            ])
            .await?;

        let overlay = self
            .get_overlay(
                self.masterchain_overlay_id,
                self.masterchain_overlay_short_id,
            )
            .await?;

        Ok(overlay)
    }

    pub fn add_masterchain_subscriber(&self, consumer: Arc<dyn OverlaySubscriber>) {
        self.overlay
            .add_subscriber(self.masterchain_overlay_short_id, consumer);
    }

    async fn get_overlay(
        self: &Arc<Self>,
        overlay_full_id: OverlayIdFull,
        overlay_id: OverlayIdShort,
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {
        loop {
            if let Some(overlay) = self.overlays.get(&overlay_id) {
                return Ok(overlay.value().clone() as Arc<dyn FullNodeOverlayClient>);
            }

            let overlay_opt = self
                .overlay_awaiters
                .do_or_wait(
                    &overlay_id,
                    None,
                    self.get_overlay_worker(overlay_full_id, overlay_id),
                )
                .await?;

            if let Some(overlay) = overlay_opt {
                return Ok(overlay);
            }
        }
    }

    async fn get_overlay_worker(
        self: &Arc<Self>,
        overlay_full_id: OverlayIdFull,
        overlay_id: OverlayIdShort,
    ) -> Result<Arc<dyn FullNodeOverlayClient>> {
        self.overlay.add_public_overlay(&overlay_id)?;
        let node = self.overlay.get_signed_node(&overlay_id)?;

        NodeNetwork::periodic_store_overlay_node(self.dht.clone(), overlay_full_id, node);

        let peers = self.update_overlay_peers(&overlay_id, &mut None).await?;
        if peers.is_empty() {
            log::warn!("No nodes were found in overlay {}", &overlay_id);
        }

        let neighbours = Neighbours::new(&self.dht, &self.overlay, &overlay_id, &peers);

        let overlay_client = Arc::new(OverlayClient::new(
            self.overlay.clone(),
            self.rldp.clone(),
            neighbours.clone(),
            overlay_id,
        ));

        neighbours.start_pinging_neighbours();
        neighbours.start_reloading_neighbours();
        neighbours.start_searching_peers();

        self.start_updating_peers(&overlay_client);

        NodeNetwork::process_overlay_peers(&neighbours, &self.dht, &self.overlay, &overlay_id);

        let result = self
            .overlays
            .entry(overlay_id)
            .or_insert(overlay_client)
            .clone();

        Ok(result as Arc<dyn FullNodeOverlayClient>)
    }

    async fn update_overlay_peers(
        &self,
        overlay_id: &OverlayIdShort,
        external_iter: &mut Option<ExternalDhtIter>,
    ) -> Result<Vec<AdnlNodeIdShort>> {
        log::info!("Overlay {} node search in progress...", overlay_id);
        let nodes = self
            .dht
            .find_overlay_nodes(overlay_id, external_iter)
            .await?;
        log::trace!("Found overlay nodes ({}):", nodes.len());

        let mut result = Vec::new();
        for (ip, node) in nodes.into_iter() {
            log::trace!("Node: {:?}, address: {}", node, ip);
            if let Some(peer) = self.overlay.add_public_peer(overlay_id, ip, &node)? {
                result.push(peer);
            }
        }
        Ok(result)
    }

    async fn update_peers(
        &self,
        overlay_client: &Arc<OverlayClient>,
        iter: &mut Option<ExternalDhtIter>,
    ) -> Result<()> {
        let mut peers = self
            .update_overlay_peers(overlay_client.overlay_id(), iter)
            .await?;
        for peer_id in peers {
            overlay_client.neighbours().add(peer_id);
        }
        Ok(())
    }

    fn start_updating_peers(self: &Arc<Self>, overlay_client: &Arc<OverlayClient>) {
        let network = self.clone();
        let overlay_client = overlay_client.clone();

        tokio::spawn(async move {
            let mut iter = None;
            loop {
                log::trace!("find overlay nodes by dht...");
                if let Err(e) = network.update_peers(&overlay_client, &mut iter).await {
                    log::warn!("Error find overlay nodes by dht: {}", e);
                }
                if overlay_client.neighbours().len() >= MAX_NEIGHBOURS {
                    log::trace!("finish find overlay nodes.");
                    return;
                }
                tokio::time::sleep(Duration::from_secs(Self::PERIOD_UPDATE_PEERS)).await;
            }
        });
    }

    async fn add_overlay_peers(
        neighbours: &Arc<Neighbours>,
        dht: &Arc<DhtNode>,
        overlay: &Arc<OverlayNode>,
        overlay_id: &OverlayIdShort,
    ) -> Result<()> {
        let peers = overlay
            .wait_for_peers(&overlay_id)
            .await
            .map_err(|e| anyhow!("Failed to wait for peers: {}", e))?;

        for peer in peers.iter() {
            let peer_full_id = AdnlNodeIdFull::try_from(&peer.id)?;
            let peer_id = peer_full_id.compute_short_id()?;

            if neighbours.contains_overlay_peer(&peer_id) {
                continue;
            }

            let ip = dht.find_address(&peer_id).await?.0;

            overlay.add_public_peer(overlay_id, ip, peer)?;
            neighbours.add_overlay_peer(peer_id);

            log::trace!(
                "add_overlay_peers: add overlay peer {:?}, address: {}",
                peer,
                ip
            );
        }
        Ok(())
    }

    fn periodic_store_ip_addr(
        dht: &Arc<DhtNode>,
        node_key: &Arc<StoredAdnlNodeKey>,
        validator_keys: Option<Arc<DashMap<AdnlNodeIdShort, usize>>>,
    ) {
        let dht = dht.clone();
        let node_key = node_key.clone();

        tokio::spawn(async move {
            loop {
                if let Err(e) = dht.store_ip_address(&node_key).await {
                    log::warn!("store ip address is ERROR: {}", e)
                }

                tokio::time::sleep(Duration::from_secs(Self::PERIOD_STORE_IP_ADDRESS)).await;

                if let Some(actual_validator_adnl_keys) = &validator_keys {
                    if !actual_validator_adnl_keys.contains_key(node_key.id()) {
                        break;
                    }
                }
            }
        });
    }

    fn periodic_store_overlay_node(
        dht: Arc<DhtNode>,
        overlay_full_id: OverlayIdFull,
        overlay_node: ton::overlay::node::Node,
    ) {
        tokio::spawn(async move {
            let overlay_node = overlay_node;
            loop {
                let result = dht
                    .store_overlay_node(&overlay_full_id, &overlay_node)
                    .await;
                log::info!("overlay_store status: {:?}", result);
                tokio::time::sleep(Duration::from_secs(Self::PERIOD_STORE_IP_ADDRESS)).await;
            }
        });
    }

    fn process_overlay_peers(
        neighbours: &Arc<Neighbours>,
        dht: &Arc<DhtNode>,
        overlay: &Arc<OverlayNode>,
        overlay_id: &OverlayIdShort,
    ) {
        let neighbours = neighbours.clone();
        let dht = dht.clone();
        let overlay = overlay.clone();
        let overlay_id = *overlay_id;

        tokio::spawn(async move {
            loop {
                if let Err(e) =
                    Self::add_overlay_peers(&neighbours, &dht, &overlay, &overlay_id).await
                {
                    log::warn!("add_overlay_peers: {}", e);
                };
                tokio::time::sleep(Duration::from_secs(Self::PERIOD_CHECK_OVERLAY_NODES)).await;
            }
        });
    }
}

#[async_trait::async_trait]
pub trait FullNodeOverlayClient: Send + Sync {
    async fn download_zero_state(
        &self,
        id: &ton_block::BlockIdExt,
    ) -> Result<Option<ton_block::BlockIdExt>>;

    async fn download_next_block_full(
        &self,
        prev_id: &ton_block::BlockIdExt,
    ) -> Result<Option<BlockStuff>>;

    async fn download_next_key_blocks_ids(
        &self,
        block_id: &ton_block::BlockIdExt,
        max_size: i32,
    ) -> Result<Vec<ton_block::BlockIdExt>>;

    async fn wait_broadcast(&self) -> Result<(ton::ton_node::Broadcast, AdnlNodeIdShort)>;
}

#[async_trait::async_trait]
impl FullNodeOverlayClient for OverlayClient {
    async fn download_zero_state(
        &self,
        id: &ton_block::BlockIdExt,
    ) -> Result<Option<ton_block::BlockIdExt>> {
        // Prepare
        let (prepare, _good_peer): (ton::ton_node::PreparedState, _) = self
            .send_adnl_query(
                TLObject::new(rpc::ton_node::PrepareZeroState {
                    block: convert_block_id_ext_blk2api(id),
                }),
                None,
                Some(PREPARE_TIMEOUT),
                None,
            )
            .await?;

        log::info!("Got prepared state: {:?}", prepare);
        Ok(Some(id.clone()))
    }

    async fn download_next_block_full(
        &self,
        prev_id: &ton_block::BlockIdExt,
    ) -> Result<Option<BlockStuff>> {
        const NO_NEIGHBOURS_DELAY: u64 = 1000; // Milliseconds

        let query = &rpc::ton_node::DownloadNextBlockFull {
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
                let block = BlockStuff::deserialize_checked(id, data_full.block.to_vec())?;
                Ok(Some(block))
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

const PREPARE_TIMEOUT: u64 = 6000; // Milliseconds
