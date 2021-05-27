mod block;
mod config;
mod network;
mod utils;

use anyhow::Result;

pub use crate::config::Config;
use crate::network::NodeNetwork;

pub async fn start(config: Config) -> Result<()> {
    let node_network = NodeNetwork::new(config).await?;
    node_network.start().await?;

    // TODO
    futures::future::pending().await
}
