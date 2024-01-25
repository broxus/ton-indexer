pub use crate::config::*;
pub use crate::db::RocksdbStats;
pub use crate::engine::{
    BlockBroadcastCounters, Engine, EngineStatus, ProcessBlockContext, ProcessBlocksEdgeContext,
    Subscriber,
};
pub use crate::network::{NeighboursOptions, NodeNetwork};
pub use crate::storage::{BriefBlockMeta, ShardStateStorageMetrics, ShardStatesGcStatus};

#[cfg(feature = "archive-uploader")]
pub use archive_uploader;
pub use global_config::*;

mod config;
mod db;
mod engine;
mod network;
mod proto;
mod storage;
pub mod utils;

pub mod alloc {
    use broxus_util::alloc::set_jemalloc_param;
    pub use broxus_util::alloc::{allocator, Allocator};

    /// Configures jemalloc
    ///
    /// # Safety
    /// Jemalloc must be set as global allocator
    pub unsafe fn apply_config() {
        tracing::debug!("applying jemalloc config");

        set_jemalloc_param("opt.abort_conf", true);
        set_jemalloc_param("opt.lg_extent_max_active_fit", 2_usize);
        set_jemalloc_param("opt.narenas", 2_u32);
        set_jemalloc_param("opt.lg_tcache_max", 10_usize);
        set_jemalloc_param("opt.muzzy_decay_ms", 100_isize);
        set_jemalloc_param("opt.dirty_decay_ms", 100_isize);

        tracing::debug!("applied jemalloc config");
    }
}
