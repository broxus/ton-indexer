use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use everscale_network::utils::PackedSocketAddr;
use everscale_network::{adnl, overlay, rldp};
use tl_proto::{TlRead, TlWrite};

use super::neighbour::Neighbour;
use super::neighbours::Neighbours;

pub struct OverlayClient {
    rldp: Arc<rldp::Node>,
    overlay_shard: Arc<overlay::Shard>,
    neighbours: Arc<Neighbours>,
}

impl OverlayClient {
    pub fn new(
        rldp: Arc<rldp::Node>,
        overlay_shard: Arc<overlay::Shard>,
        neighbours: Arc<Neighbours>,
    ) -> Self {
        Self {
            rldp,
            overlay_shard,
            neighbours,
        }
    }

    pub fn overlay_shard(&self) -> &Arc<overlay::Shard> {
        &self.overlay_shard
    }

    pub fn neighbours(&self) -> &Arc<Neighbours> {
        &self.neighbours
    }

    pub fn resolve_ip(&self, neighbour: &Neighbour) -> Option<PackedSocketAddr> {
        self.rldp
            .adnl()
            .get_peer_ip(self.overlay_shard.overlay_key().id(), neighbour.peer_id())
    }

    #[track_caller]
    pub async fn send_rldp_query<Q, A>(
        &self,
        query: Q,
        neighbour: Arc<Neighbour>,
        attempt: u32,
    ) -> Result<A>
    where
        Q: TlWrite,
        for<'a> A: TlRead<'a, Repr = tl_proto::Boxed> + 'static,
    {
        let (answer, neighbour, roundtrip) = self
            .send_rldp_query_to_neighbour(neighbour, query, attempt)
            .await?;
        match tl_proto::deserialize(&answer) {
            Ok(answer) => {
                neighbour.query_succeeded(roundtrip, true);
                Ok(answer)
            }
            Err(e) => {
                self.neighbours.update_neighbour_stats(
                    neighbour.peer_id(),
                    roundtrip,
                    false,
                    true,
                    true,
                );
                Err(e.into())
            }
        }
    }

    pub async fn send_rldp_query_raw<Q>(
        &self,
        neighbour: Arc<Neighbour>,
        query: Q,
        attempt: u32,
    ) -> Result<Vec<u8>>
    where
        Q: TlWrite,
    {
        let (answer, neighbour, roundtrip) = self
            .send_rldp_query_to_neighbour(neighbour, query, attempt)
            .await?;
        neighbour.query_succeeded(roundtrip, true);
        Ok(answer)
    }

    pub async fn send_adnl_query<Q, A>(
        &self,
        query: Q,
        attempts: Option<u32>,
        timeout: Option<u64>,
        explicit_neighbour: Option<&Arc<Neighbour>>,
    ) -> Result<(A, Arc<Neighbour>)>
    where
        Q: TlWrite,
        for<'a> A: TlRead<'a, Repr = tl_proto::Boxed> + 'static,
    {
        const NO_NEIGHBOURS_DELAY: u64 = 1000; // Milliseconds

        let query = tl_proto::serialize(query);
        let query = tl_proto::RawBytes::<tl_proto::Boxed>::new(&query);

        let attempts = attempts.unwrap_or(DEFAULT_ADNL_ATTEMPTS);

        for _ in 0..attempts {
            let neighbour = match explicit_neighbour {
                Some(neighbour) => neighbour.clone(),
                None => match self.neighbours.choose_neighbour() {
                    Some(neighbour) => neighbour,
                    None => {
                        tokio::time::sleep(Duration::from_millis(NO_NEIGHBOURS_DELAY)).await;
                        return Err(OverlayClientError::NeNeighboursFound.into());
                    }
                },
            };

            if let Some(answer) = self
                .send_adnl_query_to_neighbour::<_, A>(&neighbour, query, timeout)
                .await?
            {
                return Ok((answer, neighbour));
            }
        }

        Err(OverlayClientError::AdnlQueryFailed(attempts).into())
    }

    pub fn broadcast(
        &self,
        data: Vec<u8>,
        source: Option<&Arc<adnl::Key>>,
    ) -> overlay::OutgoingBroadcastInfo {
        self.overlay_shard.broadcast(
            self.rldp.adnl(),
            data,
            source,
            overlay::BroadcastTarget::RandomNeighbours,
        )
    }

    pub async fn wait_for_broadcast(&self) -> overlay::IncomingBroadcastInfo {
        self.overlay_shard.wait_for_broadcast().await
    }

    async fn send_adnl_query_to_neighbour<Q, A>(
        &self,
        neighbour: &Neighbour,
        query: Q,
        timeout: Option<u64>,
    ) -> Result<Option<A>>
    where
        Q: TlWrite,
        for<'a> A: TlRead<'a, Repr = tl_proto::Boxed> + 'static,
    {
        let adnl = self.rldp.adnl();
        let timeout =
            timeout.or_else(|| Some(adnl.compute_query_timeout(neighbour.roundtrip_adnl())));
        let peer_id = neighbour.peer_id();

        let now = Instant::now();
        let answer = self
            .overlay_shard
            .adnl_query(adnl, peer_id, query, timeout)
            .await?;
        let roundtrip = now.elapsed().as_millis() as u64;

        match answer.map(|answer| tl_proto::deserialize::<A>(&answer)) {
            Some(Ok(answer)) => {
                neighbour.query_succeeded(roundtrip, false);
                return Ok(Some(answer));
            }
            Some(Err(e)) => {
                log::warn!(
                    "Invalid answer from {peer_id} ({}): {e:?}",
                    ResolvedIp(self.resolve_ip(neighbour))
                );
            }
            None => {
                log::warn!(
                    "No reply from {peer_id} ({})",
                    ResolvedIp(self.resolve_ip(neighbour))
                );
            }
        }

        self.neighbours
            .update_neighbour_stats(peer_id, roundtrip, false, false, true);
        Ok(None)
    }

    async fn send_rldp_query_to_neighbour<T>(
        &self,
        neighbour: Arc<Neighbour>,
        query: T,
        attempt: u32,
    ) -> Result<(Vec<u8>, Arc<Neighbour>, u64)>
    where
        T: TlWrite,
    {
        const ATTEMPT_INTERVAL: u64 = 50; // Milliseconds

        let prefix = self.overlay_shard.query_prefix();
        let mut query_data = Vec::with_capacity(prefix.len() + query.max_size_hint());
        query_data.extend_from_slice(prefix);
        query.write_to(&mut query_data);

        let (answer, roundtrip) = self
            .overlay_shard
            .rldp_query(
                &self.rldp,
                neighbour.peer_id(),
                query_data,
                neighbour
                    .roundtrip_rldp()
                    .map(|roundtrip| roundtrip + attempt as u64 * ATTEMPT_INTERVAL),
            )
            .await?;

        match answer {
            Some(answer) => Ok((answer, neighbour, roundtrip)),
            None => {
                self.neighbours.update_neighbour_stats(
                    neighbour.peer_id(),
                    roundtrip,
                    false,
                    true,
                    true,
                );
                Err(OverlayClientError::NoRldpQueryAnswer(*neighbour.peer_id()).into())
            }
        }
    }
}

const DEFAULT_ADNL_ATTEMPTS: u32 = 50;

struct ResolvedIp(Option<PackedSocketAddr>);

impl std::fmt::Display for ResolvedIp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Some(ip) => ip.fmt(f),
            None => f.write_str("unknown"),
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum OverlayClientError {
    #[error("No neighbours found")]
    NeNeighboursFound,
    #[error("Failed to send adnl query in {} attempts", .0)]
    AdnlQueryFailed(u32),
    #[error("No RLDP query answer from {}", .0)]
    NoRldpQueryAnswer(adnl::NodeIdShort),
}
