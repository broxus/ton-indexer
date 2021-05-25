mod config;

use anyhow::Result;

pub use crate::config::Config;

pub async fn start(_config: config::Config) -> Result<()> {
    // TODO
    Ok(())
}
