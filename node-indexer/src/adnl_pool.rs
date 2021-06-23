use anyhow::Error;
use bb8::PooledConnection;
use std::sync::Arc;

use std::sync::atomic::Ordering;
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
                log::error!("Connection error: {:?}", e);
                Error::msg(e.to_string())
            })?;

        log::info!("Established adnl connection");

        Ok(client)
    }

    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        conn.ping(10).await?;
        Ok(())
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        let res = conn.has_broken.load(Ordering::Acquire);
        if res {
            log::error!("Connection has broken");
        }
        res
    }
}
