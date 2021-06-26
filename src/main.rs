use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Clap, IntoApp};
use serde::{Deserialize, Serialize};

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

            if let Err(e) = ton_indexer_lib::start(config.indexer, global_config).await {
                eprintln!("{:?}", e);
                std::process::exit(1);
            }
        }
        _ => Arguments::into_app().print_help()?,
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Config {
    indexer: ton_indexer_lib::NodeConfig,

    #[serde(default = "default_logger_settings")]
    pub logger_settings: serde_yaml::Value,
}

impl Config {
    async fn generate() -> Result<Self> {
        Ok(Self {
            indexer: ton_indexer_lib::NodeConfig::generate().await?,
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
          pattern: "{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} {M} {f}:{L} = {m} {n}"
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

fn read_global_config(path: PathBuf) -> Result<ton_indexer_lib::GlobalConfig> {
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
