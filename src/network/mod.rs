mod awaiters_pool;
mod neighbours;

use std::convert::{TryFrom, TryInto};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use dashmap::{DashMap, DashSet};
use tiny_adnl::utils::*;
use tiny_adnl::{AdnlNode, AdnlNodeConfig, DhtNode, ExternalDhtIterator, OverlayNode, RldpNode};
use ton_api::ton::rpc;
use ton_api::ton::{self, TLObject};
use ton_api::{BoxedDeserialize, BoxedSerialize, Deserializer};

use crate::block::{convert_block_id_ext_api2blk, convert_block_id_ext_blk2api, BlockStuff};
use crate::config::Config;
use crate::network::awaiters_pool::AwaitersPool;
use crate::network::neighbours::{Neighbour, Neighbours};

pub struct NodeNetwork {
    adnl: Arc<AdnlNode>,
    dht: Arc<DhtNode>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    masterchain_overlay_short_id: OverlayIdShort,
    masterchain_overlay_id: OverlayIdFull,
    overlays: Arc<DashMap<OverlayIdShort, Arc<NodeClientOverlay>>>,
    overlay_awaiters: AwaitersPool<OverlayIdShort, Arc<dyn FullNodeOverlayClient>>,
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
            overlay_awaiters: AwaitersPool::new("overlay_awaiters"),
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
            .clone()
            .get_overlay(
                self.masterchain_overlay_id,
                self.masterchain_overlay_short_id,
            )
            .await?;

        Ok(overlay)
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
                    self.clone().get_overlay_worker(overlay_full_id, overlay_id),
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
        use dashmap::mapref::entry::Entry;

        self.overlay.add_public_overlay(&overlay_id)?;
        let node = self.overlay.get_signed_node(&overlay_id)?;

        NodeNetwork::periodic_store_overlay_node(self.dht.clone(), overlay_full_id, node);

        let peers = self.update_overlay_peers(&overlay_id, &mut None).await?;
        if peers.is_empty() {
            log::warn!("No nodes were found in overlay {}", &overlay_id);
        }

        let neighbours = Neighbours::new(&peers, &self.dht, &self.overlay, overlay_id)?;
        let peers = Arc::new(neighbours);

        let client_overlay = Arc::new(NodeClientOverlay::new(
            overlay_id,
            self.overlay.clone(),
            self.rldp.clone(),
            peers.clone(),
        ));

        peers.start_ping();
        peers.start_reload();
        peers.start_rnd_peers_process();

        self.start_update_peers(&client_overlay);

        NodeNetwork::process_overlay_peers(&peers, &self.dht, &self.overlay, &overlay_id);

        let result = self
            .overlays
            .entry(overlay_id)
            .or_insert(client_overlay)
            .clone();

        Ok(result as Arc<dyn FullNodeOverlayClient>)
    }

    async fn update_overlay_peers(
        &self,
        overlay_id: &OverlayIdShort,
        external_iter: &mut Option<ExternalDhtIterator>,
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
        client_overlay: &Arc<NodeClientOverlay>,
        iter: &mut Option<ExternalDhtIterator>,
    ) -> Result<()> {
        let mut peers = self
            .update_overlay_peers(client_overlay.overlay_id(), iter)
            .await?;
        while let Some(peer) = peers.pop() {
            client_overlay.peers().add(peer)?;
        }
        Ok(())
    }

    fn start_update_peers(self: &Arc<Self>, client_overlay: &Arc<NodeClientOverlay>) {
        let network = self.clone();
        let client_overlay = client_overlay.clone();

        tokio::spawn(async move {
            let mut iter = None;
            loop {
                log::trace!("find overlay nodes by dht...");
                if let Err(e) = network.update_peers(&client_overlay, &mut iter).await {
                    log::warn!("Error find overlay nodes by dht: {}", e);
                }
                if client_overlay.peers().count() >= neighbours::MAX_NEIGHBOURS {
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
                let result =
                    DhtNode::store_overlay_node(&dht, &overlay_full_id, &overlay_node).await;
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
    async fn download_next_block_full(
        &self,
        prev_id: &ton_block::BlockIdExt,
    ) -> Result<Option<BlockStuff>>;
}

#[async_trait::async_trait]
impl FullNodeOverlayClient for NodeClientOverlay {
    async fn download_next_block_full(
        &self,
        prev_id: &ton_block::BlockIdExt,
    ) -> Result<Option<BlockStuff>> {
        let request = &rpc::ton_node::DownloadNextBlockFull {
            prev_block: convert_block_id_ext_blk2api(prev_id),
        };

        let peer = if let Some(p) = self.peers.choose_neighbour()? {
            p
        } else {
            tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_NO_NEIGHBOURS)).await;
            return Err(anyhow!("neighbour is not found!"));
        };
        log::trace!("USE PEER {}, REQUEST {:?}", peer.id(), request);

        // Download
        let data_full: ton::ton_node::DataFull =
            self.send_rldp_query_typed(request, peer, 0).await?;

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
}

#[derive(Clone)]
pub struct NodeClientOverlay {
    overlay_id: OverlayIdShort,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    peers: Arc<Neighbours>,
}

impl NodeClientOverlay {
    const ADNL_ATTEMPTS: u32 = 50;
    const TIMEOUT_PREPARE: u64 = 6000; // Milliseconds
    const TIMEOUT_DELTA: u64 = 50; // Milliseconds
    const TIMEOUT_NO_NEIGHBOURS: u64 = 1000; // Milliseconds

    pub fn new(
        overlay_id: OverlayIdShort,
        overlay: Arc<OverlayNode>,
        rldp: Arc<RldpNode>,
        peers: Arc<Neighbours>,
    ) -> Self {
        Self {
            overlay_id,
            overlay,
            rldp,
            peers,
        }
    }

    pub fn overlay_id(&self) -> &OverlayIdShort {
        &self.overlay_id
    }

    pub fn overlay(&self) -> &Arc<OverlayNode> {
        &self.overlay
    }

    pub fn peers(&self) -> &Arc<Neighbours> {
        &self.peers
    }

    async fn send_adnl_query_to_peer<R, D>(
        &self,
        peer: &Arc<Neighbour>,
        data: &TLObject,
        timeout: Option<u64>,
    ) -> Result<Option<D>>
    where
        R: ton_api::AnyBoxedSerialize,
        D: ton_api::AnyBoxedSerialize,
    {
        let request_str = if log::log_enabled!(log::Level::Trace) {
            format!("ADNL {}", std::any::type_name::<R>())
        } else {
            String::default()
        };
        log::trace!("USE PEER {}, {}", peer.id(), request_str);

        let now = Instant::now();
        let timeout = timeout.or_else(|| Some(compute_timeout(peer.roundtrip_adnl())));
        let answer = self
            .overlay
            .query(&self.overlay_id, peer.id(), &data, timeout)
            .await?;

        let roundtrip = now.elapsed().as_millis() as u64;

        if let Some(answer) = answer {
            match answer.downcast::<D>() {
                Ok(answer) => {
                    peer.query_success(roundtrip, false);
                    return Ok(Some(answer));
                }
                Err(obj) => {
                    log::warn!("Wrong answer {:?} to {:?} from {}", obj, data, peer.id())
                }
            }
        } else {
            log::warn!("No reply to {:?} from {}", data, peer.id())
        }

        self.peers
            .update_neighbour_stats(peer.id(), roundtrip, false, false, true)?;
        Ok(None)
    }

    // use this function if request size and answer size < 768 bytes (send query via ADNL)
    async fn send_adnl_query<R, D>(
        &self,
        request: R,
        attempts: Option<u32>,
        timeout: Option<u64>,
        active_peers: Option<&Arc<DashSet<AdnlNodeIdShort>>>,
    ) -> Result<(D, Arc<Neighbour>)>
    where
        R: ton_api::AnyBoxedSerialize,
        D: ton_api::AnyBoxedSerialize,
    {
        let data = TLObject::new(request);
        let attempts = attempts.unwrap_or(Self::ADNL_ATTEMPTS);

        for _ in 0..attempts {
            let peer = if let Some(p) = self.peers.choose_neighbour()? {
                p
            } else {
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_NO_NEIGHBOURS)).await;
                anyhow::bail!("Neighbour not found")
            };

            if let Some(active_peers) = &active_peers {
                active_peers.insert(peer.id().clone());
            }

            match self
                .send_adnl_query_to_peer::<R, D>(&peer, &data, timeout)
                .await
            {
                Err(e) => {
                    if let Some(active_peers) = &active_peers {
                        active_peers.remove(peer.id());
                    }
                    return Err(e);
                }
                Ok(Some(answer)) => return Ok((answer, peer)),
                Ok(None) => {
                    if let Some(active_peers) = &active_peers {
                        active_peers.remove(peer.id());
                    }
                }
            }
        }

        Err(anyhow!(
            "Cannot send query {:?} in {} attempts",
            data,
            attempts
        ))
    }

    async fn send_rldp_query_raw<T>(
        &self,
        request: &T,
        peer: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<Vec<u8>>
    where
        T: BoxedSerialize + std::fmt::Debug,
    {
        let (answer, peer, roundtrip) = self.send_rldp_query(request, peer, attempt).await?;
        peer.query_success(roundtrip, true);
        Ok(answer)
    }

    async fn send_rldp_query_typed<T, D>(
        &self,
        request: &T,
        peer: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<D>
    where
        T: BoxedSerialize + std::fmt::Debug,
        D: BoxedDeserialize,
    {
        let (answer, peer, roundtrip) = self.send_rldp_query(request, peer, attempt).await?;
        match Deserializer::new(&mut std::io::Cursor::new(answer)).read_boxed() {
            Ok(data) => {
                peer.query_success(roundtrip, true);
                Ok(data)
            }
            Err(e) => {
                self.peers
                    .update_neighbour_stats(peer.id(), roundtrip, false, true, true)?;
                Err(anyhow!(e))
            }
        }
    }

    async fn send_rldp_query<T>(
        &self,
        request: &T,
        peer: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<(Vec<u8>, Arc<Neighbour>, u64)>
    where
        T: BoxedSerialize + std::fmt::Debug,
    {
        let mut query = self
            .overlay
            .get_query_prefix(&self.overlay_id)
            .map_err(|e| anyhow!("Failed to get query prefix: {}", e))?;

        serialize_append(&mut query, request)
            .map_err(|e| anyhow!("Failed to serialize query: {}", e))?;
        let data = Arc::new(query);

        let request_str = if log::log_enabled!(log::Level::Trace) {
            std::any::type_name::<T>().to_string()
        } else {
            String::default()
        };

        log::trace!("USE PEER {}, {}", peer.id(), request_str);

        let (answer, roundtrip) = self
            .overlay
            .query_via_rldp(
                &self.overlay_id,
                peer.id(),
                &data,
                &self.rldp,
                Some(10 * 1024 * 1024),
                peer.roundtrip_rldp()
                    .map(|t| t + attempt as u64 * Self::TIMEOUT_DELTA),
            )
            .await
            .map_err(|e| anyhow!("RLDP query failed: {}", e))?;

        if let Some(answer) = answer {
            Ok((answer, peer, roundtrip))
        } else {
            self.peers
                .update_neighbour_stats(peer.id(), roundtrip, false, true, true)?;
            Err(anyhow!(
                "No RLDP answer to {:?} from {}",
                request,
                peer.id()
            ))
        }
    }
}

pub fn compute_timeout(roundtrip: Option<u64>) -> u64 {
    let timeout = roundtrip.unwrap_or(AdnlNode::MAX_QUERY_TIMEOUT);
    if timeout < AdnlNode::MIN_QUERY_TIMEOUT {
        AdnlNode::MIN_QUERY_TIMEOUT
    } else {
        timeout
    }
}
