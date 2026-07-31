use crate::Metrics;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use rutomq_storage::{ObjectMetadata, ObjectStore, ObjectStream, StorageError};
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

pub(crate) struct ObservedObjectStore {
    inner: Arc<dyn ObjectStore>,
    metrics: Arc<Metrics>,
}

impl ObservedObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, metrics: Arc<Metrics>) -> Self {
        Self { inner, metrics }
    }

    fn record<T>(
        &self,
        operation: &str,
        started: Instant,
        bytes: u64,
        result: &Result<T, StorageError>,
    ) {
        self.metrics
            .object_store_requests
            .with_label_values(&[operation])
            .inc();
        self.metrics
            .object_store_duration
            .with_label_values(&[operation])
            .observe(started.elapsed().as_secs_f64());
        if result.is_err() {
            self.metrics
                .object_store_errors
                .with_label_values(&[operation])
                .inc();
        } else if bytes > 0 {
            self.metrics
                .object_store_bytes
                .with_label_values(&[operation])
                .inc_by(bytes);
        }
    }
}

#[async_trait]
impl ObjectStore for ObservedObjectStore {
    async fn put_immutable(&self, key: &str, value: Bytes) -> Result<ObjectMetadata, StorageError> {
        let bytes = u64::try_from(value.len()).unwrap_or(u64::MAX);
        let started = Instant::now();
        let result = self.inner.put_immutable(key, value).await;
        self.record("put", started, bytes, &result);
        result
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        let started = Instant::now();
        let result = self.inner.get_range(key, range).await;
        let bytes = result
            .as_ref()
            .map(|value| u64::try_from(value.len()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        self.record("get", started, bytes, &result);
        result
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata, StorageError> {
        let started = Instant::now();
        let result = self.inner.head(key).await;
        self.record("head", started, 0, &result);
        result
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, StorageError> {
        let started = Instant::now();
        let result = self.inner.list(prefix).await;
        self.record("list", started, 0, &result);
        result
    }

    async fn list_stream(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> Result<ObjectStream, StorageError> {
        let started = Instant::now();
        let result = self.inner.list_stream(prefix, start_after).await;
        self.record("list", started, 0, &result);
        let metrics = self.metrics.clone();
        Ok(Box::pin(result?.inspect(move |entry| {
            if entry.is_err() {
                metrics
                    .object_store_errors
                    .with_label_values(&["list"])
                    .inc();
            }
        })))
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        let started = Instant::now();
        let result = self.inner.delete(key).await;
        self.record("delete", started, 0, &result);
        result
    }

    async fn check(&self) -> Result<(), StorageError> {
        let started = Instant::now();
        let result = self.inner.check().await;
        self.record("check", started, 0, &result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rutomq_storage::OpenDalObjectStore;

    #[tokio::test]
    async fn records_success_bytes_latency_and_errors_by_operation() {
        let metrics = Arc::new(Metrics::new().unwrap());
        let store = ObservedObjectStore::new(
            Arc::new(OpenDalObjectStore::memory().unwrap()),
            metrics.clone(),
        );
        store
            .put_immutable("object", Bytes::from_static(b"abcdef"))
            .await
            .unwrap();
        assert_eq!(store.get_range("object", 1..4).await.unwrap(), "bcd");
        assert!(
            store
                .put_immutable("object", Bytes::from_static(b"duplicate"))
                .await
                .is_err()
        );

        assert_eq!(
            metrics
                .object_store_requests
                .with_label_values(&["put"])
                .get(),
            2
        );
        assert_eq!(
            metrics
                .object_store_errors
                .with_label_values(&["put"])
                .get(),
            1
        );
        assert_eq!(
            metrics.object_store_bytes.with_label_values(&["put"]).get(),
            6
        );
        assert_eq!(
            metrics.object_store_bytes.with_label_values(&["get"]).get(),
            3
        );
        assert_eq!(
            metrics
                .object_store_duration
                .with_label_values(&["get"])
                .get_sample_count(),
            1
        );
    }
}
