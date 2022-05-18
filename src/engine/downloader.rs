/// This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
///
/// Changes:
/// - replaced old `failure` crate with `anyhow`
///
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;

use crate::db::*;
use crate::network::*;
use crate::utils::*;

impl<'a, T> DownloadContext<'a, T> {
    async fn load_full_block(
        &self,
        block_id: &ton_block::BlockIdExt,
    ) -> Result<Option<(BlockStuffAug, BlockProofStuffAug)>> {
        match self.db.block_handle_storage().load_handle(block_id)? {
            Some(handle) => {
                let mut is_link = false;
                if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) {
                    let block = self.db.block_storage().load_block_data(&handle).await?;
                    let block_proof = self
                        .db
                        .block_storage()
                        .load_block_proof(&handle, is_link)
                        .await?;
                    Ok(Some((
                        BlockStuffAug::loaded(block),
                        BlockProofStuffAug::loaded(block_proof),
                    )))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}

pub struct BlockDownloader;

#[async_trait::async_trait]
impl Downloader for BlockDownloader {
    type Item = (BlockStuffAug, BlockProofStuffAug);

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        if let Some(full_block) = context.load_full_block(context.block_id).await? {
            return Ok(Some(full_block));
        }

        context.client.download_block_full(context.block_id).await
    }
}

pub struct BlockProofDownloader {
    pub is_link: bool,
    pub is_key_block: bool,
}

#[async_trait::async_trait]
impl Downloader for BlockProofDownloader {
    type Item = BlockProofStuffAug;

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        if let Some(handle) = context
            .db
            .block_handle_storage()
            .load_handle(context.block_id)?
        {
            let mut is_link = false;
            if handle.has_proof_or_link(&mut is_link) {
                let proof = context
                    .db
                    .block_storage()
                    .load_block_proof(&handle, is_link)
                    .await?;
                return Ok(Some(Self::Item::loaded(proof)));
            }
        }

        context
            .client
            .download_block_proof(
                context.block_id,
                self.is_link,
                self.is_key_block,
                context.explicit_neighbour,
            )
            .await
    }
}

pub struct NextBlockDownloader;

#[async_trait::async_trait]
impl Downloader for NextBlockDownloader {
    type Item = (BlockStuffAug, BlockProofStuffAug);

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        if let Some(prev_handle) = context
            .db
            .block_handle_storage()
            .load_handle(context.block_id)?
        {
            if prev_handle.meta().has_next1() {
                let next_block_id = context
                    .db
                    .block_connection_storage()
                    .load_connection(context.block_id, BlockConnection::Next1)?;

                if let Some(full_block) = context.load_full_block(&next_block_id).await? {
                    return Ok(Some(full_block));
                }
            }
        }

        context
            .client
            .download_next_block_full(context.block_id)
            .await
    }
}

pub struct ZeroStateDownloader;

#[async_trait::async_trait]
impl Downloader for ZeroStateDownloader {
    type Item = Arc<ShardStateStuff>;

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        context.client.download_zero_state(context.block_id).await
    }
}

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

    pub client: &'a FullNodeOverlayClient,
    pub db: &'a Db,

    pub downloader: Arc<dyn Downloader<Item = T>>,
    pub explicit_neighbour: Option<&'a Arc<tiny_adnl::Neighbour>>,
}

impl<'a, T> DownloadContext<'a, T> {
    pub fn with_explicit_neighbour(
        mut self,
        explicit_neighbour: Option<&'a Arc<tiny_adnl::Neighbour>>,
    ) -> Self {
        self.explicit_neighbour = explicit_neighbour;
        self
    }

    pub async fn download(&mut self) -> Result<T> {
        let mut attempt = 1;
        loop {
            match self.downloader.try_download(self).await {
                Ok(Some(result)) => break Ok(result),
                Ok(None) => log::debug!("Got no data for {}", self.name),
                Err(e) => {
                    self.explicit_neighbour = None;
                    log::debug!("Error in {}: {}", self.name, e)
                }
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

#[derive(Debug, Copy, Clone)]
pub struct DownloaderTimeouts {
    /// Milliseconds
    pub initial: u64,
    /// Milliseconds
    pub max: u64,

    pub multiplier: f64,
}

impl DownloaderTimeouts {
    fn update(&mut self) -> u64 {
        self.initial = std::cmp::min(self.max, (self.initial as f64 * self.multiplier) as u64);
        self.initial
    }
}

#[derive(thiserror::Error, Debug)]
enum DownloaderError {
    #[error("Number of attempts exceeded")]
    AttemptsExceeded,
}
