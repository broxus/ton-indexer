use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use rusoto_s3::{HeadBucketRequest, PutObjectRequest, S3Client, S3};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveUploaderConfig {
    /// Name of the endpoint (e.g. `"eu-east-2"`)
    pub name: String,

    /// Endpoint to be used. For instance, `"https://s3.my-provider.net"` or just
    /// `"s3.my-provider.net"` (default scheme is https).
    pub endpoint: String,

    /// The bucket name
    pub bucket: String,

    /// Archive prefix before its id (Default: empty)
    #[serde(default)]
    pub archive_key_prefix: String,

    /// Interval of polling for new archives (Default: 600)
    #[serde(default = "default_archives_search_interval_sec")]
    pub archives_search_interval_sec: u64,

    /// Retry interval in case of failure (Default: 1000)
    #[serde(default = "default_retry_interval_ms")]
    pub retry_interval_ms: u64,

    /// AWS API access credentials
    #[serde(default)]
    pub credentials: Option<AwsCredentials>,
}

fn default_archives_search_interval_sec() -> u64 {
    600
}

fn default_retry_interval_ms() -> u64 {
    1000
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
pub struct ArchiveUploader(Arc<SharedState>);

impl ArchiveUploader {
    pub async fn new(config: ArchiveUploaderConfig) -> Result<Self> {
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
        s3_client
            .head_bucket(HeadBucketRequest {
                bucket: config.bucket.clone(),
                expected_bucket_owner: None,
            })
            .await?;

        Ok(ArchiveUploader(Arc::new(SharedState {
            s3_client,
            bucket: config.bucket,
            archive_key_prefix: config.archive_key_prefix,
            retry_interval: Duration::from_millis(config.retry_interval_ms),
        })))
    }

    /// Prepares a new archive to upload
    pub fn prepare_upload(&self, archive_id: u32, archive_data: Vec<u8>) -> PreparedArchiveUpload {
        let content_md5 = Some(md5::compute(&archive_data)).map(|x| base64::encode(&x.as_slice()));
        let content_length = Some(archive_data.len() as i64);

        let body = bytes::Bytes::from(archive_data);

        PreparedArchiveUpload {
            state: self.0.clone(),
            archive_id,
            key: format!("{}{archive_id:09}", self.0.archive_key_prefix),
            content_md5,
            content_length,
            body,
        }
    }

    /// Uploads an archive
    pub async fn upload(&self, archive_id: u32, archive_data: Vec<u8>) {
        let archive = self.prepare_upload(archive_id, archive_data);
        loop {
            match archive.try_upload().await {
                Ok(()) => return,
                Err(e) => {
                    tracing::error!(
                        archive_id = archive.archive_id,
                        "failed to upload archive: {e:?}"
                    );
                    tokio::time::sleep(archive.retry_interval()).await;
                }
            }
        }
    }
}

pub struct PreparedArchiveUpload {
    state: Arc<SharedState>,
    archive_id: u32,
    key: String,
    content_md5: Option<String>,
    content_length: Option<i64>,
    body: Bytes,
}

impl PreparedArchiveUpload {
    pub fn archive_id(&self) -> u32 {
        self.archive_id
    }

    pub fn entry_key(&self) -> &str {
        &self.key
    }

    pub fn entry_body(&self) -> &Bytes {
        &self.body
    }

    pub fn retry_interval(&self) -> Duration {
        self.state.retry_interval
    }

    pub async fn try_upload(&self) -> Result<()> {
        let body = Ok(self.body.clone());
        let body = rusoto_core::ByteStream::new(futures_util::stream::once(async move { body }));

        let request = PutObjectRequest {
            bucket: self.state.bucket.clone(),
            key: self.key.clone(),
            content_md5: self.content_md5.clone(),
            content_length: self.content_length,
            body: Some(body),
            ..Default::default()
        };

        self.state.s3_client.put_object(request).await?;

        Ok(())
    }
}

struct SharedState {
    s3_client: S3Client,
    bucket: String,
    archive_key_prefix: String,
    retry_interval: Duration,
}
