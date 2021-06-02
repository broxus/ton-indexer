use anyhow::Error;
use bb8::PooledConnection;
use std::sync::Arc;

use crate::adnl::{AdnlClient, AdnlClientConfig};

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
    type Connection = Arc<AdnlClient>;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        log::trace!("Establishing adnl connection...");
        let client = AdnlClient::connect(self.config.clone())
            .await
            .map_err(|e| {
                log::error!("Connection error: {:?}", e);
                Error::msg(e.to_string())
            })?;

        log::info!("Established adnl connection");

        Ok(client)
    }

    async fn is_valid(&self, _conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}
