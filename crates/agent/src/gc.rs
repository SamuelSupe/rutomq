use crate::batcher::PendingObjects;
use crate::health::Metrics;
use chrono::Utc;
use futures_util::StreamExt;
use rutomq_control::MetadataStore;
use rutomq_storage::{ObjectMetadata, ObjectStore};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{debug, warn};

const MAX_INTENTS_PER_SWEEP: i64 = 1_000;
const MAX_OBJECTS_PER_SWEEP: usize = 1_000;

pub fn spawn(
    metadata: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
    cluster_id: String,
    pending: PendingObjects,
    interval: Duration,
    grace: Duration,
    metrics: Arc<Metrics>,
) {
    tokio::spawn(async move {
        let prefix = format!("data/{cluster_id}/");
        let interval = interval.max(Duration::from_millis(1));
        let mut object_cursor = None;
        loop {
            collect_page(
                &metadata,
                &objects,
                &prefix,
                &pending,
                stale_before_ms(Utc::now().timestamp_millis(), grace),
                &metrics,
                &mut object_cursor,
            )
            .await;
            sleep(interval).await;
        }
    });
}

async fn collect_page(
    metadata: &Arc<dyn MetadataStore>,
    objects: &Arc<dyn ObjectStore>,
    prefix: &str,
    pending: &PendingObjects,
    stale_before_ms: i64,
    metrics: &Metrics,
    object_cursor: &mut Option<String>,
) {
    let claimed = match metadata
        .claim_stale_objects(stale_before_ms, MAX_INTENTS_PER_SWEEP)
        .await
    {
        Ok(keys) => keys.into_iter().collect::<HashSet<_>>(),
        Err(error) => {
            metrics.orphan_gc_errors.inc();
            debug!(%error, prefix, "orphan upload-intent claim failed");
            return;
        }
    };
    for key in &claimed {
        if is_pending(pending, key) {
            continue;
        }
        delete_claimed(metadata, objects, key, metrics).await;
    }

    let mut entries = match objects.list_stream(prefix, object_cursor.as_deref()).await {
        Ok(entries) => entries,
        Err(error) => {
            metrics.orphan_gc_errors.inc();
            debug!(%error, prefix, "orphan object listing failed");
            return;
        }
    };
    let mut scanned = 0;
    while scanned < MAX_OBJECTS_PER_SWEEP {
        let entry = match entries.next().await {
            Some(Ok(entry)) => entry,
            Some(Err(error)) => {
                metrics.orphan_gc_errors.inc();
                debug!(%error, prefix, "orphan object listing failed");
                return;
            }
            None => {
                *object_cursor = None;
                return;
            }
        };
        *object_cursor = Some(entry.key.clone());
        scanned += 1;
        if claimed.contains(&entry.key) || is_pending(pending, &entry.key) {
            continue;
        }
        match metadata.object_committed(&entry.key).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => {
                metrics.orphan_gc_errors.inc();
                debug!(key = %entry.key, %error, "orphan object metadata lookup failed");
                continue;
            }
        }
        match metadata.object_staged(&entry.key).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => {
                metrics.orphan_gc_errors.inc();
                debug!(key = %entry.key, %error, "orphan upload-intent lookup failed");
                continue;
            }
        }
        if object_is_old_enough(objects, &entry, stale_before_ms).await {
            delete_untracked(objects, &entry.key, metrics).await;
        }
    }
}

fn stale_before_ms(now_ms: i64, grace: Duration) -> i64 {
    let grace_ms = i64::try_from(grace.as_millis()).unwrap_or(i64::MAX);
    now_ms.saturating_sub(grace_ms)
}

#[cfg(test)]
async fn collect(
    metadata: &Arc<dyn MetadataStore>,
    objects: &Arc<dyn ObjectStore>,
    prefix: &str,
    pending: &PendingObjects,
    now_ms: i64,
    grace: Duration,
    metrics: &Metrics,
) {
    collect_page(
        metadata,
        objects,
        prefix,
        pending,
        stale_before_ms(now_ms, grace),
        metrics,
        &mut None,
    )
    .await;
}

fn is_pending(pending: &PendingObjects, key: &str) -> bool {
    pending
        .lock()
        .expect("pending object lock is not poisoned")
        .contains(key)
}

async fn object_is_old_enough(
    objects: &Arc<dyn ObjectStore>,
    entry: &ObjectMetadata,
    stale_before_ms: i64,
) -> bool {
    if let Some(last_modified_ms) = entry.last_modified_ms {
        return last_modified_ms <= stale_before_ms;
    }
    objects
        .head(&entry.key)
        .await
        .ok()
        .and_then(|metadata| metadata.last_modified_ms)
        .is_some_and(|last_modified_ms| last_modified_ms <= stale_before_ms)
}

async fn delete_claimed(
    metadata: &Arc<dyn MetadataStore>,
    objects: &Arc<dyn ObjectStore>,
    key: &str,
    metrics: &Metrics,
) {
    match objects.delete(key).await {
        Ok(()) => match metadata.complete_stale_object_deletion(key).await {
            Ok(true) => metrics.orphan_gc_deleted.inc(),
            Ok(false) => {}
            Err(error) => {
                metrics.orphan_gc_errors.inc();
                warn!(key, %error, "orphan upload-intent completion failed");
            }
        },
        Err(error) => {
            metrics.orphan_gc_errors.inc();
            warn!(key, %error, "orphan object deletion failed");
        }
    }
}

async fn delete_untracked(objects: &Arc<dyn ObjectStore>, key: &str, metrics: &Metrics) {
    match objects.delete(key).await {
        Ok(()) => metrics.orphan_gc_deleted.inc(),
        Err(error) => {
            metrics.orphan_gc_errors.inc();
            warn!(key, %error, "orphan object deletion failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use rutomq_control::{BatchDraft, MemoryMetadataStore, MetadataStore, ObjectRef, PartitionKey};
    use rutomq_storage::{
        ObjectMetadata, ObjectStore, ObjectStream, OpenDalObjectStore, StorageError,
    };
    use std::collections::HashSet;
    use std::ops::Range;
    use std::sync::Mutex;

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
    async fn removes_unreferenced_objects_but_keeps_committed_objects() {
        let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
        metadata.create_topic("events", 1).await.unwrap();
        let objects: Arc<dyn ObjectStore> = Arc::new(OpenDalObjectStore::memory().unwrap());
        metadata
            .stage_object(ObjectRef {
                key: "data/test/orphan".to_owned(),
                size: 6,
            })
            .await
            .unwrap();
        objects
            .put_immutable("data/test/orphan", Bytes::from_static(b"orphan"))
            .await
            .unwrap();
        objects
            .put_immutable("data/test/live", Bytes::from_static(b"live"))
            .await
            .unwrap();
        metadata
            .commit_object(
                ObjectRef {
                    key: "data/test/live".into(),
                    size: 4,
                },
                vec![BatchDraft {
                    partition: PartitionKey::new("events", 0),
                    byte_start: 0,
                    byte_end: 4,
                    record_count: 1,
                    timestamp_ms: 1,
                    checksum: None,
                    producer: None,
                    transactional_id: None,
                    verify_transaction_partition: true,
                }],
            )
            .await
            .unwrap();
        let metrics = Metrics::new().unwrap();
        let pending = Arc::new(Mutex::new(HashSet::new()));
        collect(
            &metadata,
            &objects,
            "data/test/",
            &pending,
            Utc::now().timestamp_millis().saturating_add(1),
            Duration::ZERO,
            &metrics,
        )
        .await;
        let entries = objects.list("data/test/").await.unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.key.as_str())
                .collect::<Vec<_>>(),
            ["data/test/live"]
        );
        assert_eq!(metrics.orphan_gc_deleted.get(), 1);
    }

    #[tokio::test]
    async fn upload_intent_prevents_cross_agent_orphan_deletion() {
        let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
        let objects: Arc<dyn ObjectStore> = Arc::new(OpenDalObjectStore::memory().unwrap());
        let key = "data/test/staged";
        metadata
            .stage_object(ObjectRef {
                key: key.to_owned(),
                size: 6,
            })
            .await
            .unwrap();
        objects
            .put_immutable(key, Bytes::from_static(b"staged"))
            .await
            .unwrap();
        let metrics = Metrics::new().unwrap();
        let pending = Arc::new(Mutex::new(HashSet::new()));
        collect(
            &metadata,
            &objects,
            "data/test/",
            &pending,
            Utc::now().timestamp_millis(),
            Duration::from_secs(60),
            &metrics,
        )
        .await;
        assert!(objects.head(key).await.is_ok());
        assert!(metadata.object_staged(key).await.unwrap());
        assert_eq!(metrics.orphan_gc_deleted.get(), 0);
    }

    #[tokio::test]
    async fn expired_upload_intent_is_claimed_before_object_deletion() {
        let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
        let objects: Arc<dyn ObjectStore> = Arc::new(OpenDalObjectStore::memory().unwrap());
        let key = "data/test/expired";
        metadata
            .stage_object(ObjectRef {
                key: key.to_owned(),
                size: 7,
            })
            .await
            .unwrap();
        objects
            .put_immutable(key, Bytes::from_static(b"expired"))
            .await
            .unwrap();
        let metrics = Metrics::new().unwrap();
        let pending = Arc::new(Mutex::new(HashSet::new()));
        collect(
            &metadata,
            &objects,
            "data/test/",
            &pending,
            Utc::now().timestamp_millis().saturating_add(1),
            Duration::ZERO,
            &metrics,
        )
        .await;
        assert!(objects.head(key).await.is_err());
        assert!(!metadata.object_staged(key).await.unwrap());
        assert_eq!(metrics.orphan_gc_deleted.get(), 1);
    }

    #[tokio::test]
    async fn replacement_worker_retries_persisted_orphan_claim() {
        let metadata = Arc::new(MemoryMetadataStore::new());
        metadata.create_topic("events", 1).await.unwrap();
        let inner = OpenDalObjectStore::memory().unwrap();
        let key = "data/test/retry";
        metadata
            .stage_object(ObjectRef {
                key: key.to_owned(),
                size: 5,
            })
            .await
            .unwrap();
        inner
            .put_immutable(key, Bytes::from_static(b"retry"))
            .await
            .unwrap();

        let metadata_store: Arc<dyn MetadataStore> = metadata.clone();
        let failed_worker: Arc<dyn ObjectStore> = Arc::new(DeleteFailingStore {
            inner: inner.clone(),
        });
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let metrics = Metrics::new().unwrap();
        collect(
            &metadata_store,
            &failed_worker,
            "data/test/",
            &pending,
            Utc::now().timestamp_millis().saturating_add(1),
            Duration::ZERO,
            &metrics,
        )
        .await;

        assert!(metadata.object_staged(key).await.unwrap());
        assert!(inner.head(key).await.is_ok());
        assert_eq!(metrics.orphan_gc_errors.get(), 1);
        assert_eq!(metrics.orphan_gc_deleted.get(), 0);
        assert!(
            metadata
                .commit_object(
                    ObjectRef {
                        key: key.to_owned(),
                        size: 5,
                    },
                    vec![BatchDraft {
                        partition: PartitionKey::new("events", 0),
                        byte_start: 0,
                        byte_end: 5,
                        record_count: 1,
                        timestamp_ms: 1,
                        checksum: None,
                        producer: None,
                        transactional_id: None,
                        verify_transaction_partition: true,
                    }],
                )
                .await
                .is_err()
        );

        let replacement_worker: Arc<dyn ObjectStore> = Arc::new(inner.clone());
        collect(
            &metadata_store,
            &replacement_worker,
            "data/test/",
            &pending,
            Utc::now().timestamp_millis().saturating_add(2),
            Duration::ZERO,
            &metrics,
        )
        .await;

        assert!(!metadata.object_staged(key).await.unwrap());
        assert!(inner.head(key).await.is_err());
        assert_eq!(metrics.orphan_gc_deleted.get(), 1);

        collect(
            &metadata_store,
            &replacement_worker,
            "data/test/",
            &pending,
            Utc::now().timestamp_millis().saturating_add(3),
            Duration::ZERO,
            &metrics,
        )
        .await;
        assert_eq!(metrics.orphan_gc_deleted.get(), 1);
    }

    #[tokio::test]
    async fn replacement_worker_completes_claim_after_physical_delete() {
        let metadata = Arc::new(MemoryMetadataStore::new());
        let inner = OpenDalObjectStore::memory().unwrap();
        let key = "data/test/deleted-before-completion";
        metadata
            .stage_object(ObjectRef {
                key: key.to_owned(),
                size: 7,
            })
            .await
            .unwrap();
        inner
            .put_immutable(key, Bytes::from_static(b"deleted"))
            .await
            .unwrap();
        assert!(
            metadata
                .claim_stale_objects(Utc::now().timestamp_millis().saturating_add(1), 1)
                .await
                .unwrap()
                .iter()
                .any(|claimed| claimed == key)
        );
        inner.delete(key).await.unwrap();
        assert!(metadata.object_staged(key).await.unwrap());

        let metadata_store: Arc<dyn MetadataStore> = metadata.clone();
        let objects: Arc<dyn ObjectStore> = Arc::new(inner);
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let metrics = Metrics::new().unwrap();
        collect(
            &metadata_store,
            &objects,
            "data/test/",
            &pending,
            Utc::now().timestamp_millis().saturating_add(2),
            Duration::ZERO,
            &metrics,
        )
        .await;

        assert!(!metadata.object_staged(key).await.unwrap());
        assert_eq!(metrics.orphan_gc_deleted.get(), 1);
    }

    #[tokio::test]
    async fn object_discovery_is_bounded_and_resumes_from_its_cursor() {
        let metadata: Arc<dyn MetadataStore> = Arc::new(MemoryMetadataStore::new());
        let store = OpenDalObjectStore::memory().unwrap();
        for index in 0..=MAX_OBJECTS_PER_SWEEP {
            store
                .put_immutable(
                    &format!("data/test/{index:04}"),
                    Bytes::from_static(b"orphan"),
                )
                .await
                .unwrap();
        }
        let objects: Arc<dyn ObjectStore> = Arc::new(store.clone());
        let pending = Arc::new(Mutex::new(HashSet::new()));
        let metrics = Metrics::new().unwrap();
        let mut cursor = None;
        let now_ms = Utc::now().timestamp_millis().saturating_add(1_000);

        collect_page(
            &metadata,
            &objects,
            "data/test/",
            &pending,
            stale_before_ms(now_ms, Duration::ZERO),
            &metrics,
            &mut cursor,
        )
        .await;
        assert_eq!(cursor.as_deref(), Some("data/test/0999"));
        assert_eq!(store.list("data/test/").await.unwrap().len(), 1_001);

        collect_page(
            &metadata,
            &objects,
            "data/test/",
            &pending,
            stale_before_ms(now_ms, Duration::ZERO),
            &metrics,
            &mut cursor,
        )
        .await;
        assert!(cursor.is_none());
        assert_eq!(store.list("data/test/").await.unwrap().len(), 1_001);
    }
}
