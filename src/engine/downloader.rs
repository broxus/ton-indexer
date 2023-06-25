//! This file is a modified copy of the file from https://github.com/tonlabs/ton-labs-node
//!
//! Changes:
//! - replaced old `failure` crate with `anyhow`
//!

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use everscale_types::models::*;

use crate::engine::NodeRpcClient;
use crate::network::Neighbour;
use crate::storage::*;
use crate::utils::*;

impl<'a, T> DownloadContext<'a, T> {
    async fn load_full_block(
        &self,
        block_id: &BlockId,
    ) -> Result<Option<(BlockStuffAug, BlockProofStuffAug)>> {
        let block_handle_storage = self.storage.block_handle_storage();
        let block_storage = self.storage.block_storage();

        match block_handle_storage.load_handle(block_id)? {
            Some(handle) => {
                let mut is_link = false;
                if handle.meta().has_data() && handle.has_proof_or_link(&mut is_link) {
                    let block = block_storage.load_block_data(&handle).await?;
                    let block_proof = block_storage.load_block_proof(&handle, is_link).await?;
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
    pub is_key_block: bool,
}

#[async_trait::async_trait]
impl Downloader for BlockProofDownloader {
    type Item = BlockProofStuffAug;

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        let block_handle_storage = context.storage.block_handle_storage();
        let block_storage = context.storage.block_storage();

        if let Some(handle) = block_handle_storage.load_handle(context.block_id)? {
            let mut is_link = false;
            if handle.has_proof_or_link(&mut is_link) {
                let proof = block_storage.load_block_proof(&handle, is_link).await?;
                return Ok(Some(Self::Item::loaded(proof)));
            }
        }

        context
            .client
            .download_block_proof(
                context.block_id,
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
        let block_handle_storage = context.storage.block_handle_storage();
        let block_connection_storage = context.storage.block_connection_storage();

        if let Some(prev_handle) = block_handle_storage.load_handle(context.block_id)? {
            if prev_handle.meta().has_next1() {
                let next_block_id = block_connection_storage
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
    pub block_id: &'a BlockId,
    pub max_attempts: Option<u32>,
    pub timeouts: Option<DownloaderTimeouts>,

    pub client: &'a NodeRpcClient,
    pub storage: &'a Storage,

    pub downloader: Arc<dyn Downloader<Item = T>>,
    pub explicit_neighbour: Option<&'a Arc<Neighbour>>,

    pub counters: Option<&'a DownloaderCounters>,
}

impl<'a, T> DownloadContext<'a, T> {
    pub fn with_explicit_neighbour(
        mut self,
        explicit_neighbour: Option<&'a Arc<Neighbour>>,
    ) -> Self {
        self.explicit_neighbour = explicit_neighbour;
        self
    }

    pub async fn download(&mut self) -> Result<T> {
        let mut attempt = 1;
        loop {
            let res = self.downloader.try_download(self).await;
            if let Some(counters) = &self.counters {
                counters.total.fetch_add(1, Ordering::Relaxed);
            }

            match res {
                Ok(Some(result)) => break Ok(result),
                Ok(None) => {
                    if let Some(counters) = &self.counters {
                        counters.timeouts.fetch_add(1, Ordering::Relaxed);
                    }
                    tracing::debug!("got no data for {}", self.name)
                }
                Err(e) => {
                    if let Some(counters) = &self.counters {
                        counters.errors.fetch_add(1, Ordering::Relaxed);
                    }
                    self.explicit_neighbour = None;
                    tracing::debug!("error in {}: {e:?}", self.name)
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

#[derive(Debug, Default)]
pub struct DownloaderCounters {
    total: AtomicU64,
    errors: AtomicU64,
    timeouts: AtomicU64,
}

#[derive(thiserror::Error, Debug)]
enum DownloaderError {
    #[error("Number of attempts exceeded")]
    AttemptsExceeded,
}
