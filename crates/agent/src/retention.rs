use crate::health::Metrics;
use chrono::Utc;
use rutomq_control::{MetadataStore, PartitionKey, RetentionPage};
use rutomq_storage::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

const MAX_PARTITIONS_PER_SWEEP: usize = 64;
const MAX_SPANS_PER_SWEEP: usize = 1_000;
const MAX_OBJECTS_PER_SWEEP: usize = 1_000;

pub fn spawn(
    metadata: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
    interval: Duration,
    object_delete_grace: Duration,
    metrics: Arc<Metrics>,
) {
    tokio::spawn(async move {
        let interval = if interval.is_zero() {
            Duration::from_millis(1)
        } else {
            interval
        };
        let mut partition_cursor = None;
        let mut object_cursor = None;
        loop {
            tokio::time::sleep(interval).await;
            sweep_page(
                &metadata,
                &objects,
                object_delete_grace.as_millis() as i64,
                &metrics,
                &mut partition_cursor,
                &mut object_cursor,
            )
            .await;
        }
    });
}

async fn sweep_page(
    metadata: &Arc<dyn MetadataStore>,
    objects: &Arc<dyn ObjectStore>,
    object_delete_grace_ms: i64,
    metrics: &Metrics,
    partition_cursor: &mut Option<PartitionKey>,
    object_cursor: &mut Option<String>,
) {
    let result = match metadata
        .apply_retention_page(
            Utc::now().timestamp_millis(),
            object_delete_grace_ms,
            RetentionPage {
                start_after_partition: partition_cursor.as_ref(),
                start_after_object: object_cursor.as_deref(),
                max_partitions: MAX_PARTITIONS_PER_SWEEP,
                max_spans: MAX_SPANS_PER_SWEEP,
                max_objects: MAX_OBJECTS_PER_SWEEP,
            },
        )
        .await
    {
        Ok(result) => result,
        Err(error) => {
            metrics.retention_errors.inc();
            warn!(%error, "retention metadata sweep failed");
            return;
        }
    };
    *partition_cursor = result.next_partition.clone();
    *object_cursor = result.next_object.clone();
    metrics.retention_removed_spans.inc_by(result.removed_spans);
    for object_key in result.deletable_objects {
        match objects.delete(&object_key).await {
            Ok(()) => match metadata.complete_object_deletion(&object_key).await {
                Ok(true) => metrics.retention_deleted_objects.inc(),
                Ok(false) => {}
                Err(error) => {
                    metrics.retention_errors.inc();
                    warn!(%object_key, %error, "retained object metadata completion failed");
                }
            },
            Err(error) => {
                metrics.retention_errors.inc();
                warn!(%object_key, %error, "retained object deletion failed");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use rutomq_control::{BatchDraft, MemoryMetadataStore, ObjectRef, PartitionKey, TopicConfig};
    use rutomq_storage::{ObjectMetadata, ObjectStream, OpenDalObjectStore, StorageError};
    use std::ops::Range;

    async fn sweep(
        metadata: &Arc<dyn MetadataStore>,
        objects: &Arc<dyn ObjectStore>,
        object_delete_grace_ms: i64,
        metrics: &Metrics,
    ) {
        sweep_page(
            metadata,
            objects,
            object_delete_grace_ms,
            metrics,
            &mut None,
            &mut None,
        )
        .await;
    }

    #[derive(Clone)]
    struct DeleteFailingStore {
        inner: OpenDalObjectStore,
    }

    #[async_trait]
    impl ObjectStore for DeleteFailingStore {
        async fn put_immutable(
            &self,
            key: &str,
            value: Bytes,
        ) -> Result<ObjectMetadata, StorageError> {
            self.inner.put_immutable(key, value).await
        }

        async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
            self.inner.get_range(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMetadata, StorageError> {
            self.inner.head(key).await
        }

        async fn list_stream(
            &self,
            prefix: &str,
            start_after: Option<&str>,
        ) -> Result<ObjectStream, StorageError> {
            self.inner.list_stream(prefix, start_after).await
        }

        async fn delete(&self, _key: &str) -> Result<(), StorageError> {
            Err(StorageError::InvalidConfig(
                "injected delete failure".to_owned(),
            ))
        }

        async fn check(&self) -> Result<(), StorageError> {
            self.inner.check().await
        }
    }

    #[tokio::test]
    async fn replacement_worker_retries_persisted_object_deletion() {
        let metadata = Arc::new(MemoryMetadataStore::new());
        metadata
            .create_topic_with_config(
                "retention-retry",
                1,
                TopicConfig {
                    retention_ms: 0,
                    file_delete_delay_ms: 0,
                    ..TopicConfig::default()
                },
            )
            .await
            .unwrap();
        let inner = OpenDalObjectStore::memory().unwrap();
        let key = "objects/retention-retry";
        inner
            .put_immutable(key, Bytes::from_static(b"data"))
            .await
            .unwrap();
        metadata
            .stage_object(ObjectRef {
                key: key.to_owned(),
                size: 4,
            })
            .await
            .unwrap();
        metadata
            .commit_object(
                ObjectRef {
                    key: key.to_owned(),
                    size: 4,
                },
                vec![BatchDraft {
                    partition: PartitionKey::new("retention-retry", 0),
                    byte_start: 0,
                    byte_end: 4,
                    record_count: 1,
                    timestamp_ms: 0,
                    checksum: None,
                    producer: None,
                    transactional_id: None,
                    verify_transaction_partition: true,
                }],
            )
            .await
            .unwrap();

        let metadata_store: Arc<dyn MetadataStore> = metadata.clone();
        let failed_worker: Arc<dyn ObjectStore> = Arc::new(DeleteFailingStore {
            inner: inner.clone(),
        });
        let metrics = Metrics::new().unwrap();
        sweep(&metadata_store, &failed_worker, 0, &metrics).await;

        assert!(metadata.object_committed(key).await.unwrap());
        assert!(inner.head(key).await.is_ok());
        assert_eq!(metrics.retention_errors.get(), 1);
        assert_eq!(metrics.retention_deleted_objects.get(), 0);

        let replacement_worker: Arc<dyn ObjectStore> = Arc::new(inner.clone());
        sweep(&metadata_store, &replacement_worker, 0, &metrics).await;

        assert!(!metadata.object_committed(key).await.unwrap());
        assert!(inner.list("objects/").await.unwrap().is_empty());
        assert_eq!(metrics.retention_deleted_objects.get(), 1);
    }
}
