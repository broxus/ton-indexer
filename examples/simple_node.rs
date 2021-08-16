use std::net::{IpAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::{Clap, IntoApp};
use serde::{Deserialize, Serialize};
use tiny_adnl::utils::*;

use ton_indexer::utils::*;
use ton_indexer::*;

#[derive(Clone, Debug, Clap)]
pub struct Arguments {
    /// Generate default config
    #[clap(long)]
    pub gen_config: Option<PathBuf>,

    /// Path to config
    #[clap(short, long, conflicts_with = "gen-config", requires = "global-config")]
    pub config: Option<PathBuf>,

    /// Path to the global config with zerostate and static dht nodes
    #[clap(long)]
    pub global_config: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Arguments = Arguments::parse();

    match (args.gen_config, args.config, args.global_config) {
        (Some(new_config_path), _, _) => generate_config(new_config_path)
            .await
            .context("Application startup")?,
        (_, Some(config), Some(global_config)) => {
            let config = read_config(config)?;
            let global_config = read_global_config(global_config)?;
            init_logger(&config.logger_settings)?;

            if let Err(e) = start(config.indexer, global_config).await {
                eprintln!("{:?}", e);
                std::process::exit(1);
            }
        }
        _ => Arguments::into_app().print_help()?,
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
    async fn process_block(
        &self,
        block: &BlockStuff,
        _block_proof: Option<&BlockProofStuff>,
        _shard_state: &ShardStateStuff,
    ) -> Result<()> {
        if block.id().is_masterchain() {
            return Ok(());
        }

        if self.counter.fetch_add(1, Ordering::Relaxed) % 10 != 0 {
            return Ok(());
        }

        let info = block.block().info.read_struct()?;
        log::info!("TIME_DIFF: {}", now() - info.gen_utime().0 as i32);

        Ok(())
    }

    async fn process_shard_state(&self, _shard_state: &ShardStateStuff) -> Result<()> {
        //log::info!("FOUND SHARD STATE {}", shard_state.block_id());
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

        let ip = external_ip::ConsensusBuilder::new()
            .add_sources(external_ip::get_http_sources::<external_ip::Sources>())
            .build()
            .get_consensus()
            .await;

        let ip_address = match ip {
            Some(IpAddr::V4(ip)) => SocketAddrV4::new(ip, DEFAULT_PORT),
            Some(_) => anyhow::bail!("IPv6 not supported"),
            None => anyhow::bail!("External ip not found"),
        };

        let indexer = ton_indexer::NodeConfig {
            ip_address,
            keys: Vec::new(),
            sled_db_path: PathBuf::new().join("db/sled"),
            file_db_path: PathBuf::new().join("db/file"),
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
    T: AsRef<std::path::Path>,
{
    use std::io::Write;

    let mut file = std::fs::File::create(path)?;
    let config = Config::generate().await?;
    file.write_all(serde_yaml::to_string(&config)?.as_bytes())?;
    Ok(())
}

fn read_config(path: PathBuf) -> Result<Config> {
    let mut config = config::Config::new();
    config.merge(config::File::from(path).format(config::FileFormat::Yaml))?;
    config.merge(config::Environment::new())?;

    let config: Config = config.try_into()?;
    Ok(config)
}

fn read_global_config(path: PathBuf) -> Result<ton_indexer::GlobalConfig> {
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
