use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::Result;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use futures_util::{Stream, StreamExt, TryFutureExt};
use pin_project_lite::pin_project;
use rusoto_s3::{HeadBucketRequest, S3Client, S3};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveDownloaderConfig {
    /// Name of the endpoint (e.g. `"eu-east-2"`)
    pub name: String,

    /// Endpoint to be used. For instance, `"https://s3.my-provider.net"` or just
    /// `"s3.my-provider.net"` (default scheme is https).
    pub endpoint: String,

    /// The bucket name
    pub bucket: String,

    /// Retry interval in case of failure (Default: 1000)
    #[serde(default = "default_retry_interval_ms")]
    pub retry_interval_ms: u64,

    /// Retry count (Default: 10)
    #[serde(default = "default_retry_count")]
    pub retry_count: usize,

    /// AWS API access credentials
    #[serde(default)]
    pub credentials: Option<AwsCredentials>,
}

fn default_retry_interval_ms() -> u64 {
    1000
}

fn default_retry_count() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsCredentials {
    /// Access key id
    pub access_key: String,
    /// Secret access key
    pub secret_key: String,
    /// Session token
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Clone)]
pub struct ArchiveDownloader(Arc<SharedState>);

impl ArchiveDownloader {
    pub async fn new(config: ArchiveDownloaderConfig) -> Result<Self> {
        let region = rusoto_signature::Region::Custom {
            name: config.name,
            endpoint: config.endpoint,
        };

        let credentials = rusoto_credential::StaticProvider::from(match config.credentials {
            Some(credentials) => rusoto_credential::AwsCredentials::new(
                credentials.access_key,
                credentials.secret_key,
                credentials.token,
                None,
            ),
            None => rusoto_credential::AwsCredentials::default(),
        });

        let client = rusoto_core::HttpClient::new()?;
        let s3_client = S3Client::new_with(client, credentials, region);

        // Check if bucket exists and client works
        // TODO: change this request to the publicly available one
        s3_client
            .head_bucket(HeadBucketRequest {
                bucket: config.bucket.clone(),
                expected_bucket_owner: None,
            })
            .await?;

        Ok(Self(Arc::new(SharedState {
            s3_client,
            bucket: config.bucket,
            parallel_downloads: 10,
            retry_count: config.retry_count,
            retry_interval: Duration::from_millis(config.retry_interval_ms),
        })))
    }

    pub fn archives_stream(&self) -> impl Stream<Item = Result<(String, Vec<u8>)>> + Unpin {
        let download_archives = SharedState::download_archives;

        let fetch_keys_fut = self.0.clone().fetch_archives_list(None);
        let objects = download_archives(self.0.clone(), Default::default());

        ArchivesStream {
            shared_state: self.0.clone(),
            continuation_token: None,
            download_archives,
            fetch_keys_fut: Some(fetch_keys_fut.boxed()),
            objects,
            has_more_keys: true,
        }
    }
}

pin_project! {
    struct ArchivesStream<MS, S> {
        shared_state: Arc<SharedState>,
        continuation_token: Option<String>,
        download_archives: MS,
        #[pin]
        fetch_keys_fut: Option<BoxFuture<'static, Result<rusoto_s3::ListObjectsV2Output>>>,
        #[pin]
        objects: S,
        has_more_keys: bool,
    }
}

impl<MS, S> Stream for ArchivesStream<MS, S>
where
    MS: Fn(Arc<SharedState>, Vec<String>) -> S,
    S: Stream<Item = Result<(String, Vec<u8>)>> + Unpin,
{
    type Item = Result<(String, Vec<u8>)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            break match this.objects.as_mut().poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(Some(item)),
                Poll::Ready(None) => {
                    match this.fetch_keys_fut.as_mut().as_pin_mut() {
                        Some(fut) => {
                            let objects = match fut.poll(cx) {
                                Poll::Ready(Ok(result)) => {
                                    this.fetch_keys_fut.take();
                                    result
                                }
                                Poll::Ready(Err(e)) => {
                                    this.fetch_keys_fut.take();
                                    return Poll::Ready(Some(Err(e)));
                                }
                                Poll::Pending => return Poll::Pending,
                            };

                            *this.has_more_keys = objects.is_truncated.unwrap_or_default();
                            *this.continuation_token = objects.next_continuation_token;

                            let objects = objects
                                .contents
                                .unwrap_or_default()
                                .into_iter()
                                .flat_map(|item| item.key)
                                .collect();

                            *this.objects =
                                (this.download_archives)(this.shared_state.clone(), objects);

                            // poll the new stream immediately
                            continue;
                        }
                        None if *this.has_more_keys => {
                            *this.fetch_keys_fut = Some(
                                this.shared_state
                                    .clone()
                                    .fetch_archives_list(this.continuation_token.clone())
                                    .boxed(),
                            );

                            // poll the new future immediately
                            continue;
                        }
                        None => Poll::Ready(None),
                    }
                }
                Poll::Pending => Poll::Pending,
            };
        }
    }
}

struct SharedState {
    s3_client: S3Client,
    bucket: String,
    parallel_downloads: usize,
    retry_count: usize,
    retry_interval: Duration,
}

impl SharedState {
    async fn fetch_archives_list(
        self: Arc<Self>,
        continuation_token: Option<String>,
    ) -> Result<rusoto_s3::ListObjectsV2Output> {
        let mut attempt = 0;
        loop {
            let req = rusoto_s3::ListObjectsV2Request {
                bucket: self.bucket.clone(),
                continuation_token: continuation_token.clone(),
                ..Default::default()
            };

            match self.s3_client.list_objects_v2(req).await {
                Ok(res) => return Ok(res),
                Err(e) if attempt < self.retry_count => {
                    log::info!("Failed to get archives list (attempt {attempt}). {e:?}");
                    attempt += 1;
                    tokio::time::sleep(self.retry_interval).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn download_archives(
        self: Arc<Self>,
        keys: Vec<String>,
    ) -> impl Stream<Item = Result<(String, Vec<u8>)>> + Unpin {
        let parallel_downloads = self.parallel_downloads;

        futures_util::stream::iter(keys)
            .map(move |key| self.clone().download_archive(key))
            .buffered(parallel_downloads)
    }

    async fn download_archive(self: Arc<Self>, key: String) -> Result<(String, Vec<u8>)> {
        let mut attempt = 0;
        loop {
            let req = rusoto_s3::GetObjectRequest {
                bucket: self.bucket.clone(),
                key: key.clone(),
                ..Default::default()
            };

            let res = self
                .s3_client
                .get_object(req)
                .and_then(|object| async move {
                    let mut res = Vec::new();
                    let mut body_stream = object
                        .body
                        .unwrap_or_else(|| Vec::new().into())
                        .into_async_read();
                    body_stream.read_to_end(&mut res).await?;
                    Ok(res)
                })
                .await;

            match res {
                Ok(body) => return Ok((key, body)),
                Err(e) if attempt < self.retry_count => {
                    log::info!("Failed to get archive {key} (attempt {attempt}). {e:?}");
                    attempt += 1;
                    tokio::time::sleep(self.retry_interval).await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[ignore]
    #[tokio::test]
    async fn test_downloader() {
        let downloader = ArchiveDownloader::new(ArchiveDownloaderConfig {
            name: "".to_owned(),
            endpoint: "http://127.0.0.1:9000".to_owned(),
            bucket: "archives3".to_owned(),
            retry_interval_ms: 100,
            retry_count: 4,
            credentials: Some(AwsCredentials {
                access_key: "indexer".to_owned(),
                secret_key: "testtest".to_owned(),
                token: None,
            }),
        })
        .await
        .unwrap();

        let mut stream = downloader.archives_stream();
        while let Some(item) = stream.next().await {
            let (id, archive) = item.unwrap();
            println!("ARCHIVE {id}. LEN: {}", archive.len());
        }
    }
}
