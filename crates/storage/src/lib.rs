//! Object storage abstractions and OpenDAL-backed implementations.

use async_trait::async_trait;
use bytes::Bytes;
use opendal::Operator;
use opendal::layers::RetryLayer;
use opendal::services::{Memory, S3};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

pub const DEFAULT_WRITE_CHUNK_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_WRITE_CONCURRENCY: usize = 4;
pub const MIN_S3_MULTIPART_CHUNK_BYTES: usize = 5 * 1024 * 1024;
const S3_RETRY_MAX_TIMES: usize = 3;
const S3_RETRY_MIN_DELAY: Duration = Duration::from_millis(50);
const S3_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object storage operation failed: {0}")]
    Backend(#[from] opendal::Error),
    #[error("object {0} already exists")]
    AlreadyExists(String),
    #[error("invalid object storage configuration: {0}")]
    InvalidConfig(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub last_modified_ms: Option<i64>,
    pub etag: Option<String>,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put_immutable(&self, key: &str, value: Bytes) -> Result<ObjectMetadata, StorageError>;
    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError>;
    async fn head(&self, key: &str) -> Result<ObjectMetadata, StorageError>;
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, StorageError>;
    async fn delete(&self, key: &str) -> Result<(), StorageError>;
    async fn check(&self) -> Result<(), StorageError>;
}

#[derive(Clone)]
pub struct OpenDalObjectStore {
    operator: Operator,
    written: Arc<Mutex<std::collections::HashSet<String>>>,
    write_chunk_bytes: usize,
    write_concurrency: usize,
    reserve_multipart: bool,
}

impl OpenDalObjectStore {
    pub fn memory() -> Result<Self, StorageError> {
        let operator = Operator::new(Memory::default())?;
        Ok(Self {
            operator,
            written: Arc::new(Mutex::new(Default::default())),
            write_chunk_bytes: DEFAULT_WRITE_CHUNK_BYTES,
            write_concurrency: DEFAULT_WRITE_CONCURRENCY,
            reserve_multipart: false,
        })
    }

    pub fn s3(config: S3Config) -> Result<Self, StorageError> {
        config.validate()?;
        let mut builder = S3::default()
            .bucket(&config.bucket)
            .region(&config.region)
            .root(&config.root);

        if let Some(endpoint) = config.endpoint {
            builder = builder.endpoint(&endpoint);
        }
        if let Some(access_key_id) = config.access_key_id {
            builder = builder.access_key_id(&access_key_id);
        }
        if let Some(secret_access_key) = config.secret_access_key {
            builder = builder.secret_access_key(&secret_access_key);
        }

        let operator = Operator::new(builder)?.layer(
            RetryLayer::new()
                .with_jitter()
                .with_min_delay(S3_RETRY_MIN_DELAY)
                .with_max_delay(S3_RETRY_MAX_DELAY)
                .with_max_times(S3_RETRY_MAX_TIMES),
        );
        Ok(Self {
            operator,
            written: Arc::new(Mutex::new(Default::default())),
            write_chunk_bytes: config.write_chunk_bytes,
            write_concurrency: config.write_concurrency,
            reserve_multipart: true,
        })
    }

    async fn write_immutable(
        &self,
        key: &str,
        value: Bytes,
    ) -> Result<opendal::Metadata, StorageError> {
        if self.reserve_multipart && value.len() > self.write_chunk_bytes {
            self.operator
                .write_with(key, Bytes::new())
                .if_not_exists(true)
                .await
                .map_err(|error| immutable_write_error(key, error))?;

            // The conditional single PUT above is the cross-Agent reservation. Do not delete it
            // after an ambiguous multipart failure: the metadata GC owns safe orphan cleanup.
            return self
                .operator
                .write_with(key, value)
                .chunk(self.write_chunk_bytes)
                .concurrent(self.write_concurrency)
                .await
                .map_err(StorageError::Backend);
        }

        self.operator
            .write_with(key, value)
            .if_not_exists(true)
            .await
            .map_err(|error| immutable_write_error(key, error))
    }
}

fn immutable_write_error(key: &str, error: opendal::Error) -> StorageError {
    if matches!(
        error.kind(),
        opendal::ErrorKind::AlreadyExists | opendal::ErrorKind::ConditionNotMatch
    ) {
        StorageError::AlreadyExists(key.to_owned())
    } else {
        StorageError::Backend(error)
    }
}

#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub root: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub write_chunk_bytes: usize,
    pub write_concurrency: usize,
}

impl S3Config {
    fn validate(&self) -> Result<(), StorageError> {
        if self.write_chunk_bytes < MIN_S3_MULTIPART_CHUNK_BYTES {
            return Err(StorageError::InvalidConfig(format!(
                "write chunk size must be at least {MIN_S3_MULTIPART_CHUNK_BYTES} bytes"
            )));
        }
        if self.write_concurrency == 0 {
            return Err(StorageError::InvalidConfig(
                "write concurrency must be positive".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            root: String::new(),
            region: "us-east-1".to_owned(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            write_chunk_bytes: DEFAULT_WRITE_CHUNK_BYTES,
            write_concurrency: DEFAULT_WRITE_CONCURRENCY,
        }
    }
}

#[async_trait]
impl ObjectStore for OpenDalObjectStore {
    async fn put_immutable(&self, key: &str, value: Bytes) -> Result<ObjectMetadata, StorageError> {
        let reserved = self
            .written
            .lock()
            .expect("object key lock is not poisoned")
            .insert(key.to_owned());
        if !reserved {
            return Err(StorageError::AlreadyExists(key.to_owned()));
        }
        match self.write_immutable(key, value).await {
            Ok(metadata) => Ok(ObjectMetadata {
                key: key.to_owned(),
                size: metadata.content_length(),
                last_modified_ms: metadata
                    .last_modified()
                    .map(|timestamp| timestamp.into_inner().as_millisecond()),
                etag: metadata.etag().map(ToOwned::to_owned),
            }),
            Err(error) => {
                self.written
                    .lock()
                    .expect("object key lock is not poisoned")
                    .remove(key);
                Err(error)
            }
        }
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        let reader = self.operator.reader(key).await?;
        Ok(reader.read(range).await?.to_bytes())
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, StorageError> {
        let metadata = self.operator.stat(key).await?;
        Ok(ObjectMetadata {
            key: key.to_owned(),
            size: metadata.content_length(),
            last_modified_ms: metadata
                .last_modified()
                .map(|timestamp| timestamp.into_inner().as_millisecond()),
            etag: metadata.etag().map(ToOwned::to_owned),
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, StorageError> {
        let entries = self.operator.list(prefix).await?;
        let mut result = Vec::with_capacity(entries.len());
        for entry in entries {
            let key = entry.path().to_owned();
            let size = entry.metadata().content_length();
            let last_modified_ms = entry
                .metadata()
                .last_modified()
                .map(|timestamp| timestamp.into_inner().as_millisecond());
            result.push(ObjectMetadata {
                key,
                size,
                last_modified_ms,
                etag: entry.metadata().etag().map(ToOwned::to_owned),
            });
        }
        Ok(result)
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.operator.delete(key).await?;
        Ok(())
    }

    async fn check(&self) -> Result<(), StorageError> {
        self.operator.check().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_store_supports_immutable_range_reads() {
        let store = OpenDalObjectStore::memory().unwrap();
        let metadata = store
            .put_immutable("objects/a", Bytes::from_static(b"abcdef"))
            .await
            .unwrap();
        assert_eq!(metadata.size, 6);
        assert_eq!(store.get_range("objects/a", 1..4).await.unwrap(), "bcd");
        assert!(matches!(
            store
                .put_immutable("objects/a", Bytes::from_static(b"other"))
                .await,
            Err(StorageError::AlreadyExists(_))
        ));
    }

    #[test]
    fn s3_multipart_options_are_validated() {
        assert!(matches!(
            OpenDalObjectStore::s3(S3Config {
                write_chunk_bytes: MIN_S3_MULTIPART_CHUNK_BYTES - 1,
                ..S3Config::default()
            }),
            Err(StorageError::InvalidConfig(_))
        ));
        assert!(matches!(
            OpenDalObjectStore::s3(S3Config {
                write_concurrency: 0,
                ..S3Config::default()
            }),
            Err(StorageError::InvalidConfig(_))
        ));
    }
}
