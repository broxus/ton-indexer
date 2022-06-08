/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use std::convert::TryFrom;
use std::net::SocketAddrV4;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_network::adnl::{AdnlNode, AdnlNodeMetrics, AdnlNodeOptions, Keystore};
use everscale_network::dht::{DhtNode, DhtNodeMetrics, DhtNodeOptions};
use everscale_network::network::{Neighbours, NeighboursMetrics, NeighboursOptions, OverlayClient};
use everscale_network::overlay::{
    OverlayNode, OverlayShard, OverlayShardMetrics, OverlayShardOptions,
};
use everscale_network::proto;
use everscale_network::rldp::{RldpNode, RldpNodeMetrics, RldpNodeOptions};
use everscale_network::utils::{
    AdnlNodeIdFull, AdnlNodeIdShort, OverlayIdFull, OverlayIdShort, StoredAdnlNodeKey,
};
use everscale_network::{NetworkBuilder, QuerySubscriber};
use global_config::*;
use tokio_util::sync::CancellationToken;
use ton_types::FxDashMap;

pub struct NodeNetwork {
    adnl: Arc<AdnlNode>,
    dht: Arc<DhtNode>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    neighbours_options: NeighboursOptions,
    overlay_shard_options: OverlayShardOptions,
    overlays: Arc<FxDashMap<OverlayIdShort, Arc<OverlayClient>>>,
    working_state: Arc<WorkingState>,
}

impl Drop for NodeNetwork {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl NodeNetwork {
    pub const TAG_DHT_KEY: usize = 1;
    pub const TAG_OVERLAY_KEY: usize = 2;

    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        socket_addr: SocketAddrV4,
        keystore: Keystore,
        adnl_options: AdnlNodeOptions,
        rldp_options: RldpNodeOptions,
        dht_options: DhtNodeOptions,
        neighbours_options: NeighboursOptions,
        overlay_shard_options: OverlayShardOptions,
        global_config: GlobalConfig,
    ) -> Result<Arc<Self>> {
        let working_state = Arc::new(WorkingState::new());

        let masterchain_zero_state_id = global_config.zero_state;

        let (adnl, dht, rldp, overlay) =
            NetworkBuilder::with_adnl(socket_addr, keystore, adnl_options)
                .with_dht(Self::TAG_DHT_KEY, dht_options)
                .with_rldp(rldp_options)
                .with_overlay(
                    masterchain_zero_state_id.file_hash.into(),
                    Self::TAG_OVERLAY_KEY,
                )
                .build()?;

        for peer in global_config.dht_nodes {
            dht.add_dht_peer(peer)?;
        }

        let dht_key = adnl.key_by_tag(Self::TAG_DHT_KEY)?.clone();
        log::info!("DHT adnl id: {}", dht_key.id());
        start_broadcasting_our_ip(working_state.clone(), dht.clone(), dht_key);

        let overlay_key = adnl.key_by_tag(Self::TAG_OVERLAY_KEY)?.clone();
        log::info!("Overlay adnl id: {}", overlay_key.id());
        start_broadcasting_our_ip(working_state.clone(), dht.clone(), overlay_key);

        let node_network = Arc::new(NodeNetwork {
            adnl,
            dht,
            overlay,
            rldp,
            neighbours_options,
            overlay_shard_options,
            overlays: Arc::new(Default::default()),
            working_state,
        });

        Ok(node_network)
    }

    pub fn metrics(&self) -> NetworkMetrics {
        NetworkMetrics {
            adnl: self.adnl.metrics(),
            dht: self.dht.metrics(),
            rldp: self.rldp.metrics(),
        }
    }

    pub fn neighbour_metrics(
        &self,
    ) -> impl Iterator<Item = (OverlayIdShort, NeighboursMetrics)> + '_ {
        self.overlays
            .iter()
            .map(|item| (*item.key(), item.neighbours().metrics()))
    }

    pub fn overlay_metrics(
        &self,
    ) -> impl Iterator<Item = (OverlayIdShort, OverlayShardMetrics)> + '_ {
        self.overlay.metrics()
    }

    pub fn start(self: &Arc<Self>) -> Result<()> {
        self.adnl.start()
    }

    pub fn shutdown(&self) {
        self.adnl.shutdown();
        self.working_state.shutdown();
    }

    pub fn compute_overlay_id(&self, workchain: i32) -> (OverlayIdFull, OverlayIdShort) {
        let full_id = self.overlay.compute_overlay_id(workchain);
        let short_id = full_id.compute_short_id();
        (full_id, short_id)
    }

    pub fn add_subscriber(&self, workchain: i32, subscriber: Arc<dyn QuerySubscriber>) {
        let (_, overlay_id) = self.compute_overlay_id(workchain);
        self.overlay.add_overlay_subscriber(overlay_id, subscriber);
    }

    pub async fn create_overlay_client(
        self: &Arc<Self>,
        workchain: i32,
    ) -> Result<Arc<OverlayClient>> {
        let (overlay_full_id, overlay_id) = self.compute_overlay_id(workchain);

        let (shard, _) = self
            .overlay
            .add_public_overlay(&overlay_id, self.overlay_shard_options);
        let node = shard.sign_local_node();

        start_broadcasting_our_node(
            self.working_state.clone(),
            self.dht.clone(),
            overlay_full_id,
            node,
        );

        let peers = self.update_overlay_peers(&shard).await?;
        if peers.is_empty() {
            log::warn!("No nodes were found in overlay {overlay_id}");
        }

        let neighbours = Neighbours::new(&self.dht, &shard, &peers, self.neighbours_options);

        let overlay_client = Arc::new(OverlayClient::new(
            self.rldp.clone(),
            neighbours.clone(),
            shard,
        ));

        neighbours.start_pinging_neighbours();
        neighbours.start_reloading_neighbours();
        neighbours.start_searching_peers();

        self.start_updating_peers(&overlay_client);

        start_processing_peers(self.working_state.clone(), neighbours, self.dht.clone());

        let result = self
            .overlays
            .entry(overlay_id)
            .or_insert(overlay_client)
            .clone();

        Ok(result)
    }

    fn start_updating_peers(self: &Arc<Self>, overlay_client: &Arc<OverlayClient>) {
        const PEER_UPDATE_INTERVAL: u64 = 5; // Seconds

        let interval = Duration::from_secs(PEER_UPDATE_INTERVAL);

        let network = self.clone();
        let overlay_client = overlay_client.clone();
        let working_state = self.working_state.clone();

        tokio::spawn(async move {
            while working_state.is_working() {
                log::trace!("find overlay nodes by dht...");

                if let Err(e) = network.update_peers(&overlay_client).await {
                    log::warn!("Failed to find overlay nodes by dht: {e:?}");
                }

                if overlay_client.neighbours().len() >= network.neighbours_options.max_neighbours {
                    log::trace!("Finish searching for overlay nodes");
                    return;
                }

                if working_state.wait_or_complete(interval).await {
                    log::warn!("Stopped updating peers");
                    return;
                }
            }
        });
    }

    async fn update_peers(&self, overlay_client: &OverlayClient) -> Result<()> {
        let peers = self
            .update_overlay_peers(overlay_client.overlay_shard())
            .await?;
        for peer_id in peers {
            overlay_client.neighbours().add(peer_id);
        }
        Ok(())
    }

    async fn update_overlay_peers(
        &self,
        overlay_shard: &OverlayShard,
    ) -> Result<Vec<AdnlNodeIdShort>> {
        log::info!("Overlay {} node search in progress...", overlay_shard.id());
        let nodes = self.dht.find_overlay_nodes(overlay_shard.id()).await?;
        log::trace!("Found overlay nodes ({})", nodes.len());

        overlay_shard.add_public_peers(
            &self.adnl,
            nodes
                .iter()
                .map(|(addr, node)| (*addr, node.as_equivalent_ref())),
        )
    }
}

#[derive(Debug, Copy, Clone)]
pub struct NetworkMetrics {
    pub adnl: AdnlNodeMetrics,
    pub dht: DhtNodeMetrics,
    pub rldp: RldpNodeMetrics,
}

struct WorkingState {
    working: AtomicBool,
    cancellation_token: CancellationToken,
}

impl WorkingState {
    fn new() -> Self {
        Self {
            working: AtomicBool::new(true),
            cancellation_token: Default::default(),
        }
    }

    fn shutdown(&self) {
        self.working.store(false, Ordering::Release);
        self.cancellation_token.cancel();
    }

    fn is_working(&self) -> bool {
        self.working.load(Ordering::Acquire)
    }

    /// Returns true if stopped
    async fn wait_or_complete(&self, duration: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(duration) => false,
            _ = self.cancellation_token.cancelled() => true,
        }
    }
}

fn start_broadcasting_our_ip(
    working_state: Arc<WorkingState>,
    dht: Arc<DhtNode>,
    key: Arc<StoredAdnlNodeKey>,
) {
    const IP_BROADCASTING_INTERVAL: u64 = 500; // Seconds
    let interval = Duration::from_secs(IP_BROADCASTING_INTERVAL);
    let ip = dht.adnl().socket_addr();

    tokio::spawn(async move {
        while working_state.is_working() {
            if let Err(e) = dht.store_ip_address(&key, ip).await {
                log::warn!("Failed to store ip address in DHT: {e:?}")
            }

            if working_state.wait_or_complete(interval).await {
                break;
            }
        }
        log::warn!("Stopped broadcasting our ip");
    });
}

fn start_broadcasting_our_node(
    working_state: Arc<WorkingState>,
    dht: Arc<DhtNode>,
    overlay_full_id: OverlayIdFull,
    overlay_node: proto::overlay::NodeOwned,
) {
    const NODE_BROADCASTING_INTERVAL: u64 = 500; // Seconds
    let interval = Duration::from_secs(NODE_BROADCASTING_INTERVAL);

    tokio::spawn(async move {
        while working_state.is_working() {
            let result = dht
                .store_overlay_node(&overlay_full_id, overlay_node.as_equivalent_ref())
                .await;

            log::info!("overlay_store status: {result:?}");

            if working_state.wait_or_complete(interval).await {
                break;
            }
        }
        log::warn!("Stopped broadcasting our node");
    });
}

fn start_processing_peers(
    working_state: Arc<WorkingState>,
    neighbours: Arc<Neighbours>,
    dht: Arc<DhtNode>,
) {
    const PEERS_PROCESSING_INTERVAL: u64 = 1; // Seconds
    let interval = Duration::from_secs(PEERS_PROCESSING_INTERVAL);

    tokio::spawn(async move {
        while working_state.is_working() {
            if let Err(e) = process_overlay_peers(&neighbours, &dht).await {
                log::warn!("Failed to process overlay peers: {e:?}");
            };

            if working_state.wait_or_complete(interval).await {
                break;
            }
        }
        log::warn!("Stopped processing peers");
    });
}

async fn process_overlay_peers(neighbours: &Neighbours, dht: &Arc<DhtNode>) -> Result<()> {
    let overlay_shard = neighbours.overlay_shard();
    let peers = overlay_shard.take_new_peers();

    for peer in peers.into_values() {
        let peer_id = match AdnlNodeIdFull::try_from(peer.id.as_equivalent_ref())
            .map(|full_id| full_id.compute_short_id())
        {
            Ok(peer_id) if !neighbours.contains_overlay_peer(&peer_id) => peer_id,
            Ok(_) => continue,
            Err(e) => {
                log::warn!("Invalid peer id: {e:?}");
                continue;
            }
        };

        let ip = match dht.find_address(&peer_id).await {
            Ok((ip, _)) => ip,
            Err(e) => {
                log::warn!("Failed to find peer address: {e:?}");
                continue;
            }
        };

        log::trace!("add_overlay_peers: add overlay peer {peer:?}, address: {ip}");

        neighbours
            .overlay_shard()
            .add_public_peer(dht.adnl(), ip, peer.as_equivalent_ref())?;
        neighbours.add_overlay_peer(peer_id);
    }

    Ok(())
}
