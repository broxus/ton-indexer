use std::sync::Arc;

use anyhow::Result;

use self::db::*;
use self::downloader::*;
use crate::config::*;
use crate::utils::*;

mod db;
mod downloader;

pub struct Engine {
    db: Arc<dyn Db>,
}
//
// impl Engine {
//     pub async fn new(config: Config) -> Result<Arc<Self>> {
//         let db = Arc::new(InMemoryDb {});
//     }
// }

struct BlockDownloader;

#[async_trait::async_trait]
impl Downloader for BlockDownloader {
    type Item = (BlockStuff, BlockProofStuff);

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        context.client.download_block_full(context.block_id).await
    }
}

struct BlockProofDownloader {
    is_link: bool,
    is_key_block: bool,
}

#[async_trait::async_trait]
impl Downloader for BlockProofDownloader {
    type Item = BlockProofStuff;

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        context
            .client
            .download_block_proof(context.block_id, self.is_link, self.is_key_block)
            .await
    }
}

struct NextBlockDownloader;

#[async_trait::async_trait]
impl Downloader for NextBlockDownloader {
    type Item = (BlockStuff, BlockProofStuff);

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        context
            .client
            .download_next_block_full(context.block_id)
            .await
    }
}

struct ZeroStateDownloader;

#[async_trait::async_trait]
impl Downloader for ZeroStateDownloader {
    type Item = (ShardStateStuff, Vec<u8>);

    async fn try_download(
        &self,
        context: &DownloadContext<'_, Self::Item>,
    ) -> Result<Option<Self::Item>> {
        context.client.download_zero_state(context.block_id).await
    }
}
