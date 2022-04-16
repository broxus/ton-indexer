use std::net::SocketAddrV4;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use argh::FromArgs;
use serde::{Deserialize, Serialize};
use tiny_adnl::utils::*;
use ton_block::{DepthBalanceInfo, Deserializable, ShardAccount};
use ton_types::{HashmapType, UInt256};

use ton_indexer::utils::*;
use ton_indexer::*;

#[global_allocator]
static GLOBAL: ton_indexer::alloc::Allocator = ton_indexer::alloc::allocator();

#[derive(Debug, PartialEq, FromArgs)]
#[argh(description = "")]
pub struct Arguments {
    /// generate default config
    #[argh(option)]
    pub gen_config: Option<String>,

    /// path to config
    #[argh(option, short = 'c')]
    pub config: Option<String>,

    /// path to the global config with zerostate and static dht nodes
    #[argh(option)]
    pub global_config: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Arguments = argh::from_env();
    human_panic::setup_panic!();

    profl::init("metrics")?;

    // countme::enable(true);
    // tokio::spawn(async {
    //     loop {
    //         let counts = countme::get::<ton_types::Cell>();
    //         log::warn!(
    //             "Cell: Live: {}. Total: {}, Max: {}",
    //             counts.live,
    //             counts.total,
    //             counts.max_live
    //         );
    //         let counts = countme::get::<StorageCell>();
    //         log::warn!(
    //             "StorageCell: Live: {}. Total: {}, Max: {}",
    //             counts.live,
    //             counts.total,
    //             counts.max_live
    //         );
    //
    //         tokio::time::sleep(Duration::from_secs(10)).await;
    //     }
    // });

    match (args.gen_config, args.config, args.global_config) {
        (Some(new_config_path), _, _) => generate_config(new_config_path)
            .await
            .context("Application startup")?,
        (_, Some(config), Some(global_config)) => {
            let config = read_config(config)?;
            let global_config = read_global_config(global_config)?;
            init_logger(&config.logger_settings)?;

            if let Err(e) = start(config.indexer, global_config).await {
                eprintln!("{e:?}");
                std::process::exit(1);
            }
        }
        _ => {
            eprintln!("unknown parameters");
            std::process::exit(1);
        }
    }

    Ok(())
}

async fn start(node_config: NodeConfig, global_config: GlobalConfig) -> Result<()> {
    let subscribers =
        vec![Arc::new(LoggerSubscriber::default()) as Arc<dyn ton_indexer::Subscriber>];

    let engine = Engine::new(node_config, global_config, subscribers).await?;
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
    indexer: ton_indexer::NodeConfig,

    #[serde(default = "default_logger_settings")]
    pub logger_settings: serde_yaml::Value,
}

impl Config {
    async fn generate() -> Result<Self> {
        const DEFAULT_PORT: u16 = 30303;

        let ip_address = match public_ip::addr_v4().await {
            Some(ip) => SocketAddrV4::new(ip, DEFAULT_PORT),
            None => anyhow::bail!("External ip not found"),
        };

        let indexer = ton_indexer::NodeConfig {
            ip_address,
            ..Default::default()
        };

        Ok(Self {
            indexer,
            logger_settings: default_logger_settings(),
        })
    }
}

fn default_logger_settings() -> serde_yaml::Value {
    const DEFAULT_LOG4RS_SETTINGS: &str = r##"
    appenders:
      stdout:
        kind: console
        encoder:
          pattern: "{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} {M} = {m} {n}"
    root:
      level: error
      appenders:
        - stdout
    loggers:
      ton_indexer:
        level: debug
        appenders:
          - stdout
        additive: false
    "##;
    serde_yaml::from_str(DEFAULT_LOG4RS_SETTINGS).unwrap()
}

async fn generate_config<T>(path: T) -> Result<()>
where
    T: AsRef<Path>,
{
    use std::io::Write;

    let mut file = std::fs::File::create(path)?;
    let config = Config::generate().await?;
    file.write_all(serde_yaml::to_string(&config)?.as_bytes())?;
    Ok(())
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

fn read_global_config<T>(path: T) -> Result<ton_indexer::GlobalConfig>
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
