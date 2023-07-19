use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use everscale_network::adnl;
use rand::Rng;

use super::NeighboursOptions;
use crate::proto;

pub struct Neighbour {
    peer_id: adnl::NodeIdShort,

    last_ping: AtomicU64,

    proto_version: AtomicU32,
    capabilities: AtomicU64,

    roundtrip_adnl: AtomicU64,
    roundtrip_rldp: AtomicU64,

    all_attempts: AtomicU64,
    failed_attempts: AtomicU64,
    penalty_points: AtomicU32,
    active_check: AtomicBool,
    unreliability: AtomicU32,
}

#[derive(Default, Copy, Clone)]
pub struct NeighbourOptions {
    pub default_rldp_roundtrip_ms: u64,
}

impl From<&NeighboursOptions> for NeighbourOptions {
    fn from(value: &NeighboursOptions) -> Self {
        Self {
            default_rldp_roundtrip_ms: value.default_rldp_roundtrip_ms,
        }
    }
}

impl Neighbour {
    pub(crate) fn new(peer_id: adnl::NodeIdShort, options: NeighbourOptions) -> Self {
        Self {
            peer_id,
            last_ping: Default::default(),
            proto_version: Default::default(),
            capabilities: Default::default(),
            roundtrip_adnl: Default::default(),
            roundtrip_rldp: AtomicU64::new(options.default_rldp_roundtrip_ms),
            all_attempts: Default::default(),
            failed_attempts: Default::default(),
            penalty_points: Default::default(),
            active_check: Default::default(),
            unreliability: Default::default(),
        }
    }

    pub fn try_select<R: Rng>(
        &self,
        rng: &mut R,
        total_weight: &mut u64,
        average_failures: f64,
    ) -> bool {
        let mut unreliability = self.unreliability.load(Ordering::Acquire);
        let penalty_points = self.penalty_points.load(Ordering::Acquire);
        let proto_version = self.proto_version.load(Ordering::Acquire);
        let capabilities = self.capabilities.load(Ordering::Acquire);
        let failures = self.failed_attempts.load(Ordering::Acquire) as f64
            / std::cmp::max(self.all_attempts.load(Ordering::Acquire), 1) as f64;

        if proto_version < PROTO_VERSION {
            unreliability += 4;
        } else if proto_version == PROTO_VERSION && capabilities < PROTO_CAPABILITIES {
            unreliability += 2;
        }

        if unreliability > FAIL_UNRELIABILITY {
            return false;
        }

        if failures > average_failures * 1.2 {
            if penalty_points > 0 {
                let _ =
                    self.penalty_points
                        .fetch_update(Ordering::Release, Ordering::Relaxed, |x| {
                            if x > 0 {
                                Some(x - 1)
                            } else {
                                None
                            }
                        });
                return false;
            }

            self.active_check.store(true, Ordering::Release);
        }

        let weight = (1 << (FAIL_UNRELIABILITY - unreliability)) as u64;
        *total_weight += weight;

        rng.gen_range(0..*total_weight) < weight
    }

    pub fn peer_id(&self) -> &adnl::NodeIdShort {
        &self.peer_id
    }

    pub fn last_ping(&self) -> u64 {
        self.last_ping.load(Ordering::Acquire)
    }

    pub fn set_last_ping(&self, elapsed: u64) {
        self.last_ping.store(elapsed, Ordering::Release)
    }

    pub fn update_proto_version(&self, data: proto::Capabilities) {
        self.proto_version.store(data.version, Ordering::Release);
        self.capabilities
            .store(data.capabilities, Ordering::Release);
    }

    pub fn update_stats(
        &self,
        roundtrip: u64,
        success: bool,
        is_rldp: bool,
        update_attempts: bool,
    ) {
        const PENALTY_POINTS: u32 = 100;

        if success {
            self.query_succeeded(roundtrip, is_rldp);
        } else {
            self.query_failed(roundtrip, is_rldp);
        }

        if update_attempts {
            self.all_attempts.fetch_add(1, Ordering::Release);
            if !success {
                self.failed_attempts.fetch_add(1, Ordering::Release);
            }

            if self.active_check.load(Ordering::Acquire) {
                if !success {
                    self.penalty_points
                        .fetch_add(PENALTY_POINTS, Ordering::Release);
                }
                self.active_check.store(false, Ordering::Release);
            }
        }
    }

    pub fn query_succeeded(&self, roundtrip: u64, is_rldp: bool) {
        loop {
            let old_unreliability = self.unreliability.load(Ordering::Acquire);
            if old_unreliability > 0 {
                let new_unreliability = old_unreliability - 1;
                if self
                    .unreliability
                    .compare_exchange(
                        old_unreliability,
                        new_unreliability,
                        Ordering::Release,
                        Ordering::Relaxed,
                    )
                    .is_err()
                {
                    continue;
                }
            }
            break;
        }
        if is_rldp {
            self.update_roundtrip_rldp(roundtrip)
        } else {
            self.update_roundtrip_adnl(roundtrip)
        }
    }

    pub fn query_failed(&self, roundtrip: u64, is_rldp: bool) {
        self.unreliability.fetch_add(1, Ordering::Release);
        if is_rldp {
            self.update_roundtrip_rldp(roundtrip)
        } else {
            self.update_roundtrip_adnl(roundtrip)
        }
    }

    pub fn roundtrip_adnl(&self) -> Option<u64> {
        fetch_roundtrip(&self.roundtrip_adnl)
    }

    pub fn roundtrip_rldp(&self) -> Option<u64> {
        fetch_roundtrip(&self.roundtrip_rldp)
    }

    pub fn update_roundtrip_adnl(&self, roundtrip: u64) {
        set_roundtrip(&self.roundtrip_adnl, roundtrip)
    }

    pub fn update_roundtrip_rldp(&self, roundtrip: u64) {
        set_roundtrip(&self.roundtrip_rldp, roundtrip)
    }

    pub fn unreliability(&self) -> u32 {
        self.unreliability.load(Ordering::Acquire)
    }
}

fn fetch_roundtrip(storage: &AtomicU64) -> Option<u64> {
    let roundtrip = storage.load(Ordering::Acquire);
    if roundtrip == 0 {
        None
    } else {
        Some(roundtrip)
    }
}

fn set_roundtrip(storage: &AtomicU64, roundtrip: u64) {
    let roundtrip_old = storage.load(Ordering::Acquire);
    let roundtrip = if roundtrip_old > 0 {
        (roundtrip_old + roundtrip) / 2
    } else {
        roundtrip
    };
    storage.store(roundtrip, Ordering::Release);
}

const PROTO_VERSION: u32 = 2;
const PROTO_CAPABILITIES: u64 = 1;
const FAIL_UNRELIABILITY: u32 = 10;
