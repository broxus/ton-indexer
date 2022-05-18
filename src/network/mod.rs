/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use std::convert::TryFrom;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use global_config::*;
use tiny_adnl::utils::*;
use tiny_adnl::*;
use tokio_util::sync::CancellationToken;
use ton_api::ton;

pub use self::full_node_overlay_client::*;
pub use self::full_node_overlay_service::*;

mod full_node_overlay_client;
mod full_node_overlay_service;

pub struct NodeNetwork {
    adnl: Arc<AdnlNode>,
    dht: Arc<DhtNode>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    neighbours_options: NeighboursOptions,
    overlay_shard_options: OverlayShardOptions,
    overlays: Arc<FxDashMap<OverlayIdShort, Arc<OverlayClient>>>,
    overlay_awaiters: OperationsPool<OverlayIdShort, FullNodeOverlayClient>,
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
        ip_address: AdnlAddressUdp,
        keystore: AdnlKeystore,
        adnl_options: AdnlNodeOptions,
        rldp_options: RldpNodeOptions,
        dht_options: DhtNodeOptions,
        neighbours_options: NeighboursOptions,
        overlay_shard_options: OverlayShardOptions,
        global_config: GlobalConfig,
    ) -> Result<Arc<Self>> {
        let working_state = Arc::new(WorkingState::new());

        let masterchain_zero_state_id = global_config.zero_state;

        let adnl = AdnlNode::new(ip_address, keystore, adnl_options, None);
        let dht = DhtNode::new(adnl.clone(), Self::TAG_DHT_KEY, dht_options)?;

        let overlay = OverlayNode::new(
            adnl.clone(),
            masterchain_zero_state_id.file_hash.into(),
            Self::TAG_DHT_KEY,
        )?;
        let rldp = RldpNode::new(adnl.clone(), vec![overlay.clone()], rldp_options);

        for peer in global_config.dht_nodes {
            dht.add_peer(peer)?;
        }

        let dht_key = adnl.key_by_tag(Self::TAG_DHT_KEY)?;
        log::info!("DHT adnl id: {}", dht_key.id());
        start_broadcasting_our_ip(working_state.clone(), dht.clone(), dht_key);

        let overlay_key = adnl.key_by_tag(Self::TAG_OVERLAY_KEY)?;
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
            overlay_awaiters: OperationsPool::new("overlay_operations"),
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

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        self.adnl
            .start(vec![
                self.dht.clone(),
                self.overlay.clone(),
                self.rldp.clone(),
            ])
            .await
    }

    pub fn shutdown(&self) {
        self.adnl.shutdown();
        self.working_state.shutdown();
    }

    pub fn add_subscriber(
        &self,
        overlay_id: OverlayIdShort,
        subscriber: Arc<dyn OverlaySubscriber>,
    ) {
        self.overlay.add_subscriber(overlay_id, subscriber);
    }

    pub fn compute_overlay_id(&self, workchain: i32) -> (OverlayIdFull, OverlayIdShort) {
        let full_id = self
            .overlay
            .compute_overlay_id(workchain, ton_block::SHARD_FULL as i64);
        let short_id = full_id.compute_short_id();
        (full_id, short_id)
    }

    pub async fn get_overlay(
        self: &Arc<Self>,
        overlay_full_id: OverlayIdFull,
        overlay_id: OverlayIdShort,
    ) -> Result<FullNodeOverlayClient> {
        loop {
            if let Some(overlay) = self.overlays.get(&overlay_id) {
                return Ok(FullNodeOverlayClient(overlay.value().clone()));
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
    ) -> Result<FullNodeOverlayClient> {
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

        let peers = self.update_overlay_peers(&shard, &mut None).await?;
        if peers.is_empty() {
            log::warn!("No nodes were found in overlay {}", &overlay_id);
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

        Ok(FullNodeOverlayClient(result))
    }

    fn start_updating_peers(self: &Arc<Self>, overlay_client: &Arc<OverlayClient>) {
        const PEER_UPDATE_INTERVAL: u64 = 5; // Seconds

        let interval = Duration::from_secs(PEER_UPDATE_INTERVAL);

        let network = self.clone();
        let overlay_client = overlay_client.clone();
        let working_state = self.working_state.clone();

        tokio::spawn(async move {
            let mut iter = None;
            while working_state.is_working() {
                log::trace!("find overlay nodes by dht...");

                if let Err(e) = network.update_peers(&overlay_client, &mut iter).await {
                    log::warn!("Error find overlay nodes by dht: {}", e);
                }

                if overlay_client.neighbours().len() >= network.neighbours_options.max_neighbours {
                    log::trace!("finish find overlay nodes.");
                    return;
                }

                if working_state.wait_or_complete(interval).await {
                    log::warn!("Stopped updating peers");
                    return;
                }
            }
        });
    }

    async fn update_peers(
        &self,
        overlay_client: &OverlayClient,
        iter: &mut Option<ExternalDhtIter>,
    ) -> Result<()> {
        let peers = self
            .update_overlay_peers(overlay_client.overlay_shard(), iter)
            .await?;
        for peer_id in peers {
            overlay_client.neighbours().add(peer_id);
        }
        Ok(())
    }

    async fn update_overlay_peers(
        &self,
        overlay_shard: &OverlayShard,
        external_iter: &mut Option<ExternalDhtIter>,
    ) -> Result<Vec<AdnlNodeIdShort>> {
        log::info!("Overlay {} node search in progress...", overlay_shard.id());
        let nodes = self
            .dht
            .find_overlay_nodes(overlay_shard.id(), external_iter)
            .await?;
        log::trace!("Found overlay nodes ({})", nodes.len());

        overlay_shard.add_public_peers(nodes)
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

    tokio::spawn(async move {
        while working_state.is_working() {
            if let Err(e) = dht.store_ip_address(&key).await {
                log::warn!("store ip address is ERROR: {}", e)
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
    overlay_node: ton::overlay::node::Node,
) {
    const NODE_BROADCASTING_INTERVAL: u64 = 500; // Seconds
    let interval = Duration::from_secs(NODE_BROADCASTING_INTERVAL);

    tokio::spawn(async move {
        while working_state.is_working() {
            let result = dht
                .store_overlay_node(&overlay_full_id, &overlay_node)
                .await;

            log::info!("overlay_store status: {:?}", result);

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
                log::warn!("add_overlay_peers: {}", e);
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
        let peer_id =
            match AdnlNodeIdFull::try_from(&peer.id).map(|full_id| full_id.compute_short_id()) {
                Ok(peer_id) if !neighbours.contains_overlay_peer(&peer_id) => peer_id,
                Ok(_) => continue,
                Err(e) => {
                    log::warn!("Invalid peer id: {}", e);
                    continue;
                }
            };

        let ip = match dht.find_address(&peer_id).await {
            Ok((ip, _)) => ip,
            Err(e) => {
                log::warn!("Failed to find peer address: {}", e);
                continue;
            }
        };

        log::trace!(
            "add_overlay_peers: add overlay peer {:?}, address: {}",
            peer,
            ip
        );

        neighbours.overlay_shard().add_public_peer(ip, peer)?;
        neighbours.add_overlay_peer(peer_id);
    }

    Ok(())
}
