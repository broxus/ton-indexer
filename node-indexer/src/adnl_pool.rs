use anyhow::Error;
use bb8::PooledConnection;
use std::sync::Arc;

use std::sync::atomic::Ordering;
use std::time::Duration;
use tiny_adnl::{AdnlTcpClient, AdnlTcpClientConfig};

pub struct AdnlManageConnection {
    config: AdnlTcpClientConfig,
}

impl AdnlManageConnection {
    pub fn new(config: AdnlTcpClientConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl bb8::ManageConnection for AdnlManageConnection {
    type Connection = Arc<AdnlTcpClient>;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        log::trace!("Establishing adnl connection...");
        let client = AdnlTcpClient::connect(self.config.clone())
            .await
            .map_err(|e| {
                log::error!("Failed getting new adnl connection: {:?}", e);
                Error::msg(e.to_string())
            })?;

        log::info!("Established adnl connection");

        Ok(client)
    }

    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        let result = conn
            .ping(Duration::from_secs(10))
            .await
            .map_err(|e| e.context("Connection is invalid"));
        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                log::warn!("Adnl connection is invalid: {}", e);
                Err(e)
            }
        }
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        let res = conn.has_broken.load(Ordering::Acquire);
        if res {
            log::error!("Connection has broken");
        }
        res
    }
}
