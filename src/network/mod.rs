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
use everscale_network::*;
use global_config::*;
use metrics::Label;
use tokio_util::sync::CancellationToken;

pub use self::neighbour::Neighbour;
use self::neighbours::Neighbours;
pub use self::neighbours::NeighboursOptions;
pub use self::overlay_client::OverlayClient;
use crate::utils::FastDashMap;

use crate::{set_metrics, set_metrics_with_label};

mod neighbour;
mod neighbours;
mod neighbours_cache;
mod overlay_client;

pub struct NodeNetwork {
    adnl: Arc<adnl::Node>,
    dht: Arc<dht::Node>,
    overlay: Arc<overlay::Node>,
    rldp: Arc<rldp::Node>,
    neighbours_options: NeighboursOptions,
    overlay_shard_options: overlay::OverlayOptions,
    overlays: Arc<FastDashMap<overlay::IdShort, Arc<OverlayClient>>>,
    zero_state_file_hash: [u8; 32],
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
        keystore: adnl::Keystore,
        adnl_options: adnl::NodeOptions,
        rldp_options: rldp::NodeOptions,
        dht_options: dht::NodeOptions,
        neighbours_options: NeighboursOptions,
        overlay_shard_options: overlay::OverlayOptions,
        global_config: GlobalConfig,
    ) -> Result<Arc<Self>> {
        let working_state = Arc::new(WorkingState::new());

        let (adnl, dht, rldp, overlay) =
            NetworkBuilder::with_adnl(socket_addr, keystore, adnl_options)
                .with_dht(Self::TAG_DHT_KEY, dht_options)
                .with_rldp(rldp_options)
                .with_overlay(Self::TAG_OVERLAY_KEY)
                .build()?;

        for peer in global_config.dht_nodes {
            dht.add_dht_peer(peer)?;
        }

        dht.find_more_dht_nodes().await?;

        let dht_key = adnl.key_by_tag(Self::TAG_DHT_KEY)?.clone();
        tracing::info!(local_id = %dht_key.id(), "created DHT node");
        start_broadcasting_our_ip(working_state.clone(), dht.clone(), dht_key);

        let overlay_key = adnl.key_by_tag(Self::TAG_OVERLAY_KEY)?.clone();
        tracing::info!(local_id = %overlay_key.id(), "created overlay node");
        start_broadcasting_our_ip(working_state.clone(), dht.clone(), overlay_key);

        let node_network = Arc::new(NodeNetwork {
            adnl,
            dht,
            overlay,
            rldp,
            neighbours_options,
            overlay_shard_options,
            overlays: Arc::new(Default::default()),
            zero_state_file_hash: *global_config.zero_state.file_hash.as_array(),
            working_state,
        });

        {
            let node_network = Arc::downgrade(&node_network);
            tokio::spawn(async move {
                while let Some(node_network) = node_network.upgrade() {
                    node_network.metrics_update_step().await;
                }
            });
        }

        Ok(node_network)
    }
    async fn metrics_update_step(&self) {
        const OVERLAY_ID: &str = "overlay_id";

        let adnl_metrics = self.adnl.metrics();
        let dht_metrics = self.dht.metrics();
        let rldp_metrics = self.rldp.metrics();

        set_metrics!(
            // ADNL Metrics
            "network_adnl_peer_count" => adnl_metrics.peer_count,
            "network_adnl_channels_by_id_len" => adnl_metrics.channels_by_id_len,
            "network_adnl_channels_by_peers_len" => adnl_metrics.channels_by_peers_len,
            "network_adnl_incoming_transfers_len" => adnl_metrics.incoming_transfers_len,
            "network_adnl_query_count" => adnl_metrics.query_count,
            // DHT Metrics
            "network_dht_peers_cache_len" => dht_metrics.known_peers_len,
            "network_dht_bucket_peer_count" => dht_metrics.bucket_peer_count,
            "network_dht_storage_len" => dht_metrics.storage_len,
            "network_dht_storage_total_size" => dht_metrics.storage_total_size,
            // RLDP Metrics
            "network_rldp_peer_count" => rldp_metrics.peer_count,
            "network_rldp_transfers_cache_len" => rldp_metrics.transfers_cache_len,
        );
        for (overlay_id, overlay_metrics) in self.overlay_metrics() {
            let overlay_id = base64::encode(overlay_id.as_slice());
            let label = Label::new(OVERLAY_ID, overlay_id);

            set_metrics_with_label!(label.clone();
                "overlay_owned_broadcasts_len" => overlay_metrics.owned_broadcasts_len,
                "overlay_finished_broadcasts_len" => overlay_metrics.finished_broadcasts_len,
                "overlay_node_count" => overlay_metrics.node_count,
                "overlay_known_peers_len" => overlay_metrics.known_peers,
                "overlay_neighbours" => overlay_metrics.neighbours,
                "overlay_received_broadcasts_data_len" => overlay_metrics.received_broadcasts_data_len,
                "overlay_received_broadcasts_barrier_count" => overlay_metrics.received_broadcasts_barrier_count
            );
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    pub fn adnl(&self) -> &Arc<adnl::Node> {
        &self.adnl
    }

    pub fn rldp(&self) -> &Arc<rldp::Node> {
        &self.rldp
    }

    pub fn overlay(&self) -> &Arc<overlay::Node> {
        &self.overlay
    }

    pub fn dht(&self) -> &Arc<dht::Node> {
        &self.dht
    }

    pub fn overlay_metrics(
        &self,
    ) -> impl Iterator<Item = (overlay::IdShort, overlay::OverlayMetrics)> + '_ {
        self.overlay.metrics()
    }

    pub fn shutdown(&self) {
        self.adnl.shutdown();
        self.working_state.shutdown();
    }

    pub fn compute_overlay_id(&self, workchain: i32) -> (overlay::IdFull, overlay::IdShort) {
        let full_id = overlay::IdFull::for_workchain_overlay(workchain, &self.zero_state_file_hash);
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
            tracing::warn!(%overlay_id, "no nodes found");
        }

        let neighbours = Neighbours::new(&self.dht, &shard, &peers, self.neighbours_options);

        let overlay_client = Arc::new(OverlayClient::new(
            self.rldp.clone(),
            shard,
            neighbours.clone(),
        ));

        neighbours.start_pinging_neighbours();
        neighbours.start_reloading_neighbours();
        neighbours.start_exchanging_peers();

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
                tracing::trace!("searching for overlay nodes in DHT");

                if let Err(e) = network.update_peers(&overlay_client).await {
                    tracing::warn!("failed to find overlay nodes in DHT: {e:?}");
                }

                if overlay_client.neighbours().len()
                    >= network.neighbours_options.max_neighbours as usize
                {
                    tracing::trace!("finished searching for overlay nodes in DHT");
                    return;
                }

                if working_state.wait_or_complete(interval).await {
                    tracing::warn!("stopped updating peers");
                    return;
                }
            }
        });
    }

    async fn update_peers(&self, overlay_client: &OverlayClient) -> Result<()> {
        let peers = self.update_overlay_peers(overlay_client.overlay()).await?;
        for peer_id in peers {
            overlay_client.neighbours().add(peer_id);
        }
        Ok(())
    }

    async fn update_overlay_peers(
        &self,
        overlay: &overlay::Overlay,
    ) -> Result<Vec<adnl::NodeIdShort>> {
        tracing::info!(overlay_id = %overlay.id(), "searching for overlay nodes");
        let nodes = self.dht.find_overlay_nodes(overlay.id()).await?;
        tracing::trace!(node_count = nodes.len(), "found overlay nodes");

        overlay.add_public_peers(
            &self.adnl,
            nodes
                .iter()
                .map(|(addr, node)| (*addr, node.as_equivalent_ref())),
        )
    }
}

#[derive(Debug, Copy, Clone)]
pub struct NetworkMetrics {
    pub adnl: adnl::NodeMetrics,
    pub dht: dht::NodeMetrics,
    pub rldp: rldp::NodeMetrics,
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
    dht: Arc<dht::Node>,
    key: Arc<adnl::Key>,
) {
    const IP_BROADCASTING_INTERVAL: u64 = 500; // Seconds
    let interval = Duration::from_secs(IP_BROADCASTING_INTERVAL);
    let addr = dht.adnl().socket_addr();

    tokio::spawn(async move {
        while working_state.is_working() {
            if let Err(e) = dht.store_address(&key, addr).await {
                tracing::warn!("failed to store address in DHT: {e:?}")
            }

            if working_state.wait_or_complete(interval).await {
                break;
            }
        }
        tracing::warn!("stopped broadcasting our ip");
    });
}

fn start_broadcasting_our_node(
    working_state: Arc<WorkingState>,
    dht: Arc<dht::Node>,
    overlay_full_id: overlay::IdFull,
    overlay_node: proto::overlay::NodeOwned,
) {
    const NODE_BROADCASTING_INTERVAL: u64 = 500; // Seconds
    let interval = Duration::from_secs(NODE_BROADCASTING_INTERVAL);

    tokio::spawn(async move {
        while working_state.is_working() {
            let result = dht
                .store_overlay_node(&overlay_full_id, overlay_node.as_equivalent_ref())
                .await;

            tracing::info!("overlay_store status: {result:?}");

            if working_state.wait_or_complete(interval).await {
                break;
            }
        }
        tracing::warn!("stopped broadcasting our node");
    });
}

fn start_processing_peers(
    working_state: Arc<WorkingState>,
    neighbours: Arc<Neighbours>,
    dht: Arc<dht::Node>,
) {
    const PEERS_PROCESSING_INTERVAL: u64 = 1; // Seconds
    let interval = Duration::from_secs(PEERS_PROCESSING_INTERVAL);

    tokio::spawn(async move {
        while working_state.is_working() {
            if let Err(e) = process_overlay_peers(&neighbours, &dht).await {
                tracing::warn!("failed to process overlay peers: {e:?}");
            };

            if working_state.wait_or_complete(interval).await {
                break;
            }
        }
        tracing::warn!("stopped processing peers");
    });
}

async fn process_overlay_peers(neighbours: &Neighbours, dht: &Arc<dht::Node>) -> Result<()> {
    let overlay_shard = neighbours.overlay();
    let peers = overlay_shard.take_new_peers();

    for peer in peers.into_values() {
        let peer_id = match adnl::NodeIdFull::try_from(peer.id.as_equivalent_ref())
            .map(|full_id| full_id.compute_short_id())
        {
            Ok(peer_id) if !neighbours.contains_overlay_peer(&peer_id) => peer_id,
            Ok(_) => continue,
            Err(e) => {
                tracing::warn!("invalid peer id: {e:?}");
                continue;
            }
        };

        let addr = match dht.find_address(&peer_id).await {
            Ok((ip, _)) => ip,
            Err(e) => {
                tracing::debug!("failed to find peer address: {e:?}");
                continue;
            }
        };

        neighbours
            .overlay()
            .add_public_peer(dht.adnl(), addr, peer.as_equivalent_ref())?;
        neighbours.add_overlay_peer(peer_id);
    }

    Ok(())
}
