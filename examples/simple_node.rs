use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use argh::FromArgs;
use broxus_util::now;
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

use ton_indexer::{utils::*, ShardStatesGcStatus};
use ton_indexer::{Engine, GlobalConfig, NodeConfig, ProcessBlockContext};

#[global_allocator]
static GLOBAL: ton_indexer::alloc::Allocator = ton_indexer::alloc::allocator();

#[derive(Debug, FromArgs)]
#[argh(description = "")]
pub struct App {
    /// path to config
    #[argh(option, short = 'c', default = "String::from(\"config.yaml\")")]
    pub config: String,

    /// path to the global config with zerostate and static dht nodes
    #[argh(option, default = "String::from(\"ton-global.config.json\")")]
    pub global_config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let logger = tracing_subscriber::fmt().with_env_filter(
        EnvFilter::builder()
            .with_default_directive(tracing::Level::INFO.into())
            .from_env_lossy(),
    );

    logger.init();

    let any_signal = broxus_util::any_signal(broxus_util::TERMINATION_SIGNALS);

    let run = run(argh::from_env());

    tokio::select! {
        result = run => result,
        signal = any_signal => {
            if let Ok(signal) = signal {
                tracing::warn!(?signal, "received termination signal, flushing state...");
            }
            // NOTE: engine future is safely dropped here so rocksdb method
            // `rocksdb_close` is called in DB object destructor
            Ok(())
        }
    }
}

async fn run(app: App) -> Result<()> {
    // SAFETY: global allocator is set to jemalloc
    unsafe { ton_indexer::alloc::apply_config() };

    let mut config: Config = broxus_util::read_config(app.config)?;
    config
        .indexer
        .ip_address
        .set_ip(broxus_util::resolve_public_ip(None).await?);

    let global_config = read_global_config(app.global_config)?;

    let engine = Engine::new(
        config.indexer,
        global_config,
        Arc::new(LoggerSubscriber::default()),
    )
    .await?;

    tokio::spawn({
        struct CacheStatsWriter<'a>(&'a CacheStats);

        impl std::fmt::Debug for CacheStatsWriter<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let stats = self.0;
                f.debug_struct("CacheStats")
                    .field("hits", &stats.hits)
                    .field("misses", &stats.misses)
                    .field("requests", &stats.requests)
                    .field("occupied", &stats.occupied)
                    .field("hits_ratio", &stats.hits_ratio)
                    .field("size", &bytesize::ByteSize::b(stats.size_bytes))
                    .finish()
            }
        }

        struct GcStatsWriter<'a>(&'a ShardStatesGcStatus);

        impl std::fmt::Debug for GcStatsWriter<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let stats = self.0;
                let current = stats.current_seqno.load(Ordering::Acquire);
                let progress = if stats.end_seqno > stats.start_seqno {
                    (current.saturating_sub(stats.start_seqno) as f64) * 100.0
                        / ((stats.end_seqno - stats.start_seqno) as f64)
                } else {
                    100.0
                };

                f.debug_struct("GcStats")
                    .field("current_shard", &stats.current_shard)
                    .field("start_seqno", &stats.start_seqno)
                    .field("end_seqno", &stats.end_seqno)
                    .field("current_seqno", &current)
                    .field("progress", &progress)
                    .finish()
            }
        }

        const INTERVAL: Duration = Duration::from_secs(30);

        let engine = engine.clone();

        async move {
            let mut interval = tokio::time::interval(INTERVAL);
            loop {
                interval.tick().await;

                let stats = engine.internal_metrics().cells_cache_stats;
                tracing::info!("cache stats: {:#?}", CacheStatsWriter(&stats));

                let stats = engine.get_db_metrics().shard_state_storage.gc_status;
                if let Some(stats) = stats {
                    tracing::info!("gc stats: {:#?}", GcStatsWriter(&stats));
                }
            }
        }
    });

    engine.start().await?;

    futures_util::future::pending().await
}

#[derive(Default)]
struct LoggerSubscriber {
    counter: AtomicUsize,
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for LoggerSubscriber {
    async fn process_block(&self, ctx: ProcessBlockContext<'_>) -> Result<()> {
        if ctx.id().is_masterchain() {
            return Ok(());
        }

        if self.counter.fetch_add(1, Ordering::Relaxed) % 500 != 0 {
            return Ok(());
        }

        let created_at = ctx.meta().gen_utime() as i64;

        ctx.block().read_info()?;
        ctx.block().read_value_flow()?;

        tracing::info!(time_diff = (now() as i64 - created_at));

        // Use to test heavy load:
        //
        // use ton_block::{DepthBalanceInfo, Deserializable, ShardAccount};
        // use ton_types::HashmapType;
        // if let Some(state) = ctx.shard_state() {
        //     let state = state.read_accounts()?;
        //     state
        //         .iterate_slices(|ref mut key, ref mut value| {
        //             ton_types::UInt256::construct_from(key)?;
        //             DepthBalanceInfo::construct_from(value)?;
        //             let shard_acc = ShardAccount::construct_from(value)?;
        //             let _acc = shard_acc.read_account()?;

        //             Ok(true)
        //         })
        //         .ok();
        // }

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Config {
    indexer: NodeConfig,
}

fn read_global_config<T>(path: T) -> Result<GlobalConfig>
where
    T: AsRef<Path>,
{
    let file = std::fs::File::open(path)?;
    let reader = std::io::BufReader::new(file);
    let config = serde_json::from_reader(reader)?;
    Ok(config)
}
