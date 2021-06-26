use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use super::db::*;
use crate::network::*;

#[async_trait::async_trait]
pub trait Downloader: Send + Sync {
    type Item;

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>>;
}

pub struct DownloadContext<'a, T> {
    pub name: &'a str,
    pub block_id: &'a ton_block::BlockIdExt,
    pub max_attempts: Option<u32>,
    pub timeouts: Option<DownloaderTimeouts>,

    pub client: Arc<dyn FullNodeOverlayClient>,
    pub db: &'a dyn Db,

    pub downloader: Arc<dyn Downloader<Item = T>>,
}

impl<'a, T> DownloadContext<'a, T> {
    pub async fn download(&mut self) -> Result<T> {
        let mut attempt = 1;
        loop {
            match self.downloader.try_download(self).await {
                Ok(Some(result)) => break Ok(result),
                Ok(None) => log::info!("Got no data for {}", self.name),
                Err(e) => log::info!("Error in {}: {}", self.name, e),
            }

            attempt += 1;
            if matches!(self.max_attempts, Some(max_attempts) if attempt > max_attempts) {
                return Err(DownloaderError::AttemptsExceeded.into());
            }

            if let Some(timeouts) = &mut self.timeouts {
                let sleep_duration = Duration::from_millis(timeouts.update());
                tokio::time::sleep(sleep_duration).await;
            }
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct DownloaderTimeouts {
    /// Milliseconds
    pub initial: u64,
    /// Milliseconds
    pub max: u64,

    pub multiplier: u64,
}

impl DownloaderTimeouts {
    fn update(&mut self) -> u64 {
        self.initial = std::cmp::min(self.max, self.initial * self.multiplier / 10);
        self.initial
    }
}

#[derive(thiserror::Error, Debug)]
enum DownloaderError {
    #[error("Number of attempts exceeded")]
    AttemptsExceeded,
}
