use adnl::client::{AdnlClient, AdnlClientConfig};
use anyhow::Error;
use bb8::PooledConnection;
use std::ops::DerefMut;

pub struct AdnlManageConnection {
    config: AdnlClientConfig,
}

impl AdnlManageConnection {
    pub fn new(config: AdnlClientConfig) -> Self {
        Self { config }
    }
}

#[async_trait::async_trait]
impl bb8::ManageConnection for AdnlManageConnection {
    type Connection = AdnlClient;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        log::trace!("Establishing adnl connection...");

        let connection = AdnlClient::connect(&self.config).await.map_err(|e| {
            log::error!("Connection error: {:?}", e);
            Error::msg(e.to_string())
        })?;

        log::trace!("Established adnl connection");

        Ok(connection)
    }

    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        log::trace!("Check if connection is valid...");

        // conn.deref_mut().ping().await.map(|_| ()).map_err(|e| {
        //     log::error!("Ping error: {:?}", e);
        //     Error::msg(e.to_string())
        // })?;

        log::trace!("Connection is valid");

        Ok(())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
