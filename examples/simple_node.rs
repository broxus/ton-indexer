use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;
use argh::FromArgs;
use everscale_network::utils::now;
use serde::{Deserialize, Serialize};
use ton_block::{DepthBalanceInfo, Deserializable, ShardAccount};
use ton_types::{HashmapType, UInt256};

use ton_indexer::utils::*;
use ton_indexer::{Engine, GlobalConfig, NodeConfig, ProcessBlockContext};

#[global_allocator]
static GLOBAL: ton_indexer::alloc::Allocator = ton_indexer::alloc::allocator();

#[derive(Debug, PartialEq, FromArgs)]
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
async fn main() {
    if let Err(e) = run(argh::from_env()).await {
        eprintln!("Fatal error: {e:?}");
        std::process::exit(1);
    }
}

async fn run(app: App) -> Result<()> {
    let mut config = read_config(app.config)?;

    let ip_address = config
        .ip_address
        .resolve(config.indexer.ip_address.port())
        .await?;
    config.indexer.ip_address = ip_address;

    let global_config = read_global_config(app.global_config)?;
    init_logger(&config.logger_settings)?;

    let subscribers =
        vec![Arc::new(LoggerSubscriber::default()) as Arc<dyn ton_indexer::Subscriber>];

    let engine = Engine::new(config.indexer, global_config, subscribers).await?;
    engine.start().await?;

    futures::future::pending().await
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

        log::info!("TIME_DIFF: {}", now() as i64 - created_at);

        if let Some(state) = ctx.shard_state() {
            let state = state.read_accounts()?;
            state
                .iterate_slices(|ref mut key, ref mut value| {
                    UInt256::construct_from(key)?;
                    DepthBalanceInfo::construct_from(value)?;
                    let shard_acc = ShardAccount::construct_from(value)?;
                    let _acc = shard_acc.read_account()?;

                    Ok(true)
                })
                .ok();
        }

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct Config {
    #[serde(default = "default_ip_address")]
    ip_address: ip_resolver::PublicIp,
    indexer: NodeConfig,
    logger_settings: serde_yaml::Value,
}

fn default_ip_address() -> ip_resolver::PublicIp {
    ip_resolver::PublicIp::Public
}

fn read_config<T>(path: T) -> Result<Config>
where
    T: AsRef<Path>,
{
    let mut config = config::Config::new();
    config.merge(config::File::from(path.as_ref()).format(config::FileFormat::Yaml))?;
    config.merge(config::Environment::new())?;

    let config: Config = config.try_into()?;
    Ok(config)
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

fn init_logger(config: &serde_yaml::Value) -> Result<()> {
    let config = serde_yaml::from_value(config.clone())?;
    log4rs::config::init_raw_config(config)?;
    Ok(())
}
