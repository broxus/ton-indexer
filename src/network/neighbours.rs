use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use everscale_network::{adnl, dht, overlay};
use tokio::sync::Semaphore;

use super::neighbour::*;
use super::neighbours_cache::*;
use crate::utils::FastDashSet;

pub struct Neighbours {
    dht: Arc<dht::Node>,
    overlay: Arc<overlay::Overlay>,
    options: NeighboursOptions,

    cache: Arc<NeighboursCache>,
    overlay_peers: FastDashSet<adnl::NodeIdShort>,

    failed_attempts: AtomicU64,
    all_attempts: AtomicU64,

    start: Instant,
}

#[derive(Debug, Copy, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct NeighboursOptions {
    /// Default: 16
    pub max_neighbours: u32,
    /// Default: 10
    pub reloading_min_interval_sec: u32,
    /// Default: 30
    pub reloading_max_interval_sec: u32,
    /// Default: 500
    pub ping_interval_ms: u64,
    /// Default: 1000
    pub search_interval_ms: u64,
    /// Default: 10
    pub ping_min_timeout_ms: u64,
    /// Default: 1000
    pub ping_max_timeout_ms: u64,
    /// Default: 2000
    pub default_rldp_roundtrip_ms: u64,
    /// Default: 6
    pub max_ping_tasks: usize,
    /// Default: 6
    pub max_exchange_tasks: usize,
}

impl Default for NeighboursOptions {
    fn default() -> Self {
        Self {
            max_neighbours: 16,
            reloading_min_interval_sec: 10,
            reloading_max_interval_sec: 30,
            ping_interval_ms: 500,
            search_interval_ms: 1000,
            ping_min_timeout_ms: 10,
            ping_max_timeout_ms: 1000,
            default_rldp_roundtrip_ms: 2000,
            max_ping_tasks: 6,
            max_exchange_tasks: 6,
        }
    }
}

impl Neighbours {
    pub fn new(
        dht: &Arc<dht::Node>,
        overlay: &Arc<overlay::Overlay>,
        initial_peers: &[adnl::NodeIdShort],
        options: NeighboursOptions,
    ) -> Arc<Self> {
        let cache = Arc::new(NeighboursCache::new(
            initial_peers,
            options.max_neighbours,
            NeighbourOptions::from(&options),
        ));

        let this = Arc::new(Self {
            dht: dht.clone(),
            overlay: overlay.clone(),
            options,
            cache,
            overlay_peers: Default::default(),
            failed_attempts: Default::default(),
            all_attempts: Default::default(),
            start: Instant::now(),
        });

        {
            // Neighbours metrics update loop
            const UPDATE_INTERVAL: Duration = Duration::from_secs(5);

            let this = Arc::downgrade(&this);
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(UPDATE_INTERVAL);
                loop {
                    interval.tick().await;
                    let Some(this) = this.upgrade() else {
                        break;
                    };
                    this.update_metrics();
                }
            });
        }

        this
    }

    fn update_metrics(&self) {
        crate::set_metrics! {
            "neighbours_peers_count" => self.len() as i64,
        }
    }

    pub fn options(&self) -> &NeighboursOptions {
        &self.options
    }

    pub fn overlay(&self) -> &Arc<overlay::Overlay> {
        &self.overlay
    }

    /// Starts background process of sending ping queries to all neighbours
    pub fn start_pinging_neighbours(self: &Arc<Self>) {
        let interval = Duration::from_millis(self.options.ping_interval_ms);

        let neighbours = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let neighbours = match neighbours.upgrade() {
                    Some(neighbours) => neighbours,
                    None => return,
                };

                if let Err(e) = neighbours.ping_neighbours().await {
                    tracing::warn!("failed to ping neighbours: {e}");
                    tokio::time::sleep(interval).await;
                }
            }
        });
    }

    /// Starts background process of updating neighbours cache
    pub fn start_reloading_neighbours(self: &Arc<Self>) {
        use rand::distributions::Distribution;

        let neighbours = Arc::downgrade(self);

        let (min_ms, max_ms) = ordered_boundaries(
            self.options.reloading_min_interval_sec,
            self.options.reloading_max_interval_sec,
        );
        let distribution = rand::distributions::Uniform::new(min_ms, max_ms);

        tokio::spawn(async move {
            loop {
                let sleep_duration = distribution.sample(&mut rand::thread_rng()) as u64;
                tokio::time::sleep(Duration::from_secs(sleep_duration)).await;

                let neighbours = match neighbours.upgrade() {
                    Some(neighbours) => neighbours,
                    None => return,
                };

                if let Err(e) = neighbours.reload_neighbours() {
                    tracing::warn!("failed to reload neighbours: {e}");
                }
            }
        });
    }

    /// Starts background process of broadcasting random peers to all neighbours
    pub fn start_exchanging_peers(self: &Arc<Self>) {
        let interval = Duration::from_millis(self.options.search_interval_ms);

        let neighbours = Arc::downgrade(self);
        let adnl = self.dht.adnl().clone();

        let semaphore = Arc::new(Semaphore::new(self.options.max_exchange_tasks));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                let neighbours = match neighbours.upgrade() {
                    Some(neighbours) => neighbours,
                    None => return,
                };

                let mut index = 0;
                while let Some(peer_id) = neighbours.cache.get_peer_id(index) {
                    index += 1;

                    let new_peers = match neighbours
                        .overlay
                        .exchange_random_peers_ext(&adnl, &peer_id, None, &neighbours.overlay_peers)
                        .await
                    {
                        Ok(Some(new_peers)) if !new_peers.is_empty() => new_peers,
                        _ => continue,
                    };

                    let permit = semaphore.clone().acquire_owned().await.ok();
                    let neighbours = neighbours.clone();
                    tokio::spawn(async move {
                        for peer_id in new_peers.into_iter() {
                            match neighbours.dht.find_address(&peer_id).await {
                                Ok((addr, _)) => {
                                    tracing::debug!(%peer_id, %addr, "found overlay peer");
                                    neighbours.add_overlay_peer(peer_id);
                                }
                                Err(e) => {
                                    tracing::debug!("failed to find overlay peer address: {e}");
                                }
                            }
                        }
                        drop(permit);
                    });
                }
            }
        });
    }

    /// Returns neighbours cache len
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn add(&self, peer_id: adnl::NodeIdShort) -> bool {
        self.cache.insert(peer_id)
    }

    pub fn contains_overlay_peer(&self, peer_id: &adnl::NodeIdShort) -> bool {
        self.overlay_peers.contains(peer_id)
    }

    pub fn add_overlay_peer(&self, peer_id: adnl::NodeIdShort) {
        self.overlay_peers.insert(peer_id);
    }

    pub fn choose_neighbour(&self) -> Option<Arc<Neighbour>> {
        self.cache
            .choose_neighbour(&mut rand::thread_rng(), self.average_failures())
    }

    pub fn average_failures(&self) -> f64 {
        self.failed_attempts.load(Ordering::Acquire) as f64
            / std::cmp::max(self.all_attempts.load(Ordering::Acquire), 1) as f64
    }

    pub fn update_neighbour_stats(
        &self,
        peer_id: &adnl::NodeIdShort,
        roundtrip: u64,
        success: bool,
        is_rldp: bool,
        update_attempts: bool,
    ) {
        let neighbour = match self.cache.get(peer_id) {
            Some(neighbour) => neighbour,
            None => return,
        };

        neighbour.update_stats(roundtrip, success, is_rldp, update_attempts);
        if update_attempts {
            self.all_attempts.fetch_add(1, Ordering::Release);
            if !success {
                self.failed_attempts.fetch_add(1, Ordering::Release);
            }
        }
    }

    async fn ping_neighbours(self: &Arc<Self>) -> Result<()> {
        let neighbour_count = self.cache.len();
        if neighbour_count == 0 {
            return Err(NeighboursError::NoPeersInOverlay(*self.overlay.id()).into());
        } else {
            tracing::trace!(
                overlay_id = %self.overlay.id(),
                %neighbour_count,
                "pinging neighbours in overlay",
            )
        }

        let max_tasks = std::cmp::min(neighbour_count, self.options.max_ping_tasks);
        let semaphore = Arc::new(Semaphore::new(max_tasks));
        loop {
            let neighbour = match self.cache.get_next_for_ping(&self.start) {
                Some(neighbour) => neighbour,
                None => {
                    tracing::trace!("no neighbours to ping");
                    tokio::time::sleep(Duration::from_millis(self.options.ping_min_timeout_ms))
                        .await;
                    continue;
                }
            };

            let ms_since_last_ping = self.elapsed().saturating_sub(neighbour.last_ping());
            let additional_sleep = if ms_since_last_ping < self.options.ping_max_timeout_ms {
                self.options
                    .ping_max_timeout_ms
                    .saturating_sub(ms_since_last_ping)
            } else {
                self.options.ping_min_timeout_ms
            };
            tokio::time::sleep(Duration::from_millis(additional_sleep)).await;

            let guard = semaphore.clone().acquire_owned().await?;
            let neighbours = self.clone();
            tokio::spawn(async move {
                if let Err(e) = neighbours.update_capabilities(neighbour).await {
                    tracing::debug!("failed to ping peer: {e}");
                }
                // Explicitly release acquired semaphore
                drop(guard);
            });
        }
    }

    fn reload_neighbours(&self) -> Result<()> {
        tracing::trace!(overlay_id = %self.overlay.id(), "started reloading neighbours");

        let peers = adnl::PeersSet::with_capacity(self.options.max_neighbours * 2 + 1);
        self.overlay
            .write_cached_peers(self.options.max_neighbours * 2, &peers);
        self.process_neighbours(peers)?;

        tracing::trace!(overlay_id = %self.overlay.id(), "finished reloading neighbours");
        Ok(())
    }

    async fn update_capabilities(self: &Arc<Self>, neighbour: Arc<Neighbour>) -> Result<()> {
        tracing::trace!(
            overlay_id = %self.overlay.id(),
            peer_id = %neighbour.peer_id(),
            "quering capabilities",
        );

        let timeout = self
            .dht
            .adnl()
            .compute_query_timeout(neighbour.roundtrip_adnl());

        let now = Instant::now();
        neighbour.set_last_ping(self.elapsed());

        let query = crate::proto::RpcGetCapabilities;
        match self
            .overlay
            .adnl_query(self.dht.adnl(), neighbour.peer_id(), query, Some(timeout))
            .await
        {
            Ok(Some(answer)) => {
                let capabilities = tl_proto::deserialize(&answer)?;
                tracing::debug!(
                    peer_id = %neighbour.peer_id(),
                    overlay_id = %self.overlay.id(),
                    ?capabilities,
                    "got capabilities",
                );

                let roundtrip = now.elapsed().as_millis() as u64;
                self.update_neighbour_stats(neighbour.peer_id(), roundtrip, true, false, false);

                if let Some(neighbour) = self.cache.get(neighbour.peer_id()) {
                    neighbour.update_proto_version(capabilities);
                }

                Ok(())
            }
            _ => Err(NeighboursError::NoCapabilitiesReceived(*neighbour.peer_id()).into()),
        }
    }

    fn process_neighbours(&self, peers: adnl::PeersSet) -> Result<()> {
        let mut cache = self.cache.write();

        let mut rng = rand::thread_rng();
        for peer_id in peers {
            if cache.contains(&peer_id) {
                continue;
            }

            let (hint, unreliable_peer) = cache.insert_or_replace_unreliable(&mut rng, peer_id);
            if let Some(unreliable_peer) = unreliable_peer {
                self.overlay.remove_public_peer(&unreliable_peer);
                self.overlay_peers.remove(&unreliable_peer);
            }

            if hint == NeighboursCacheHint::DefinitelyFull {
                break;
            }
        }

        Ok(())
    }

    fn elapsed(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

#[derive(Debug, Copy, Clone)]
pub struct NeighboursMetrics {
    pub peer_search_task_count: usize,
}

fn ordered_boundaries<T>(min: T, max: T) -> (T, T)
where
    T: Ord,
{
    if min > max {
        (max, min)
    } else {
        (min, max)
    }
}

#[derive(thiserror::Error, Debug)]
enum NeighboursError {
    #[error("No peers in overlay {}", .0)]
    NoPeersInOverlay(overlay::IdShort),
    #[error("No capabilities received for {}", .0)]
    NoCapabilitiesReceived(adnl::NodeIdShort),
}
