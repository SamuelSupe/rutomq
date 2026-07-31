use super::tests::{decode_response, request_frame, sample_records};
use super::*;
use crate::records::encode_records;
use async_trait::async_trait;
use bytes::Bytes;
use kafka_protocol::messages::FetchResponse;
use kafka_protocol::messages::ProduceResponse;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use rutomq_control::MemoryMetadataStore;
use rutomq_protocol::records::{Record, TimestampType};
use rutomq_storage::{
    MIN_S3_MULTIPART_CHUNK_BYTES, ObjectMetadata, ObjectStore, ObjectStream, OpenDalObjectStore,
    S3Config, StorageError,
};
use std::ops::Range;

#[derive(Clone)]
struct CorruptingStore {
    inner: OpenDalObjectStore,
}

#[async_trait]
impl ObjectStore for CorruptingStore {
    async fn put_immutable(&self, key: &str, value: Bytes) -> Result<ObjectMetadata, StorageError> {
        self.inner.put_immutable(key, value).await
    }

    async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes, StorageError> {
        let mut value = self.inner.get_range(key, range).await?.to_vec();
        if let Some(first) = value.first_mut() {
            *first ^= 1;
        }
        Ok(value.into())
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

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.inner.delete(key).await
    }

    async fn check(&self) -> Result<(), StorageError> {
        self.inner.check().await
    }
}

#[tokio::test]
async fn repeated_fetch_uses_one_immutable_range_get() {
    let metrics = Arc::new(Metrics::new().unwrap());
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata.create_topic("cache", 1).await.unwrap();
    let broker = Broker::new(
        metadata,
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig {
            flush_interval: Duration::from_millis(1),
            fetch_cache_bytes: 1024 * 1024,
            ..AgentConfig::default()
        },
        metrics.clone(),
    );
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("cache"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(sample_records())),
                ]),
        ]);
    broker
        .handle_request(request_frame(ApiKey::Produce, 3, 1, &produce))
        .await
        .unwrap();
    let fetch = FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name("cache"))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1024 * 1024),
                ]),
        ]);
    for correlation_id in [2, 3] {
        broker
            .handle_request(request_frame(ApiKey::Fetch, 4, correlation_id, &fetch))
            .await
            .unwrap();
    }

    assert_eq!(metrics.fetch_cache_misses.get(), 1);
    assert_eq!(metrics.fetch_cache_hits.get(), 1);
    assert_eq!(
        metrics
            .object_store_requests
            .with_label_values(&["get"])
            .get(),
        1
    );
    assert!(metrics.fetch_cache_bytes.get() > 0);
}

#[tokio::test]
async fn corrupted_range_uses_versioned_storage_error_and_is_not_cached() {
    let metrics = Arc::new(Metrics::new().unwrap());
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata.create_topic("corrupt", 1).await.unwrap();
    let broker = Broker::new(
        metadata,
        Arc::new(CorruptingStore {
            inner: OpenDalObjectStore::memory().unwrap(),
        }),
        AgentConfig {
            flush_interval: Duration::from_millis(1),
            fetch_cache_bytes: 1024 * 1024,
            ..AgentConfig::default()
        },
        metrics.clone(),
    );
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("corrupt"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(sample_records())),
                ]),
        ]);
    broker
        .handle_request(request_frame(ApiKey::Produce, 3, 1, &produce))
        .await
        .unwrap();
    let fetch = FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name("corrupt"))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1024 * 1024),
                ]),
        ]);
    for (correlation_id, version, error_code) in [
        (2, 4, crate::kafka_error::NOT_LEADER_OR_FOLLOWER),
        (3, 5, crate::kafka_error::NOT_LEADER_OR_FOLLOWER),
        (4, 6, crate::kafka_error::KAFKA_STORAGE_ERROR),
    ] {
        let frame = broker
            .handle_request(request_frame(
                ApiKey::Fetch,
                version,
                correlation_id,
                &fetch,
            ))
            .await
            .unwrap();
        let response: FetchResponse = decode_response(ApiKey::Fetch, version, frame);
        assert_eq!(response.responses[0].partitions[0].error_code, error_code);
    }
    assert_eq!(metrics.object_integrity_failures.get(), 3);
    assert_eq!(metrics.fetch_cache_bytes.get(), 0);
}

#[tokio::test]
async fn large_produce_uses_multipart_and_fetches_exact_ranges() {
    let Ok(endpoint) = std::env::var("RUTOMQ_TEST_S3_ENDPOINT") else {
        return;
    };
    let suffix = Uuid::new_v4().simple().to_string();
    let cluster_id = format!("multipart-{suffix}");
    let store = OpenDalObjectStore::s3(S3Config {
        bucket: test_env_or("RUTOMQ_TEST_S3_BUCKET", "rutomq"),
        root: format!("agent-{suffix}"),
        endpoint: Some(endpoint),
        access_key_id: Some(test_env_or("RUTOMQ_TEST_S3_ACCESS_KEY_ID", "minioadmin")),
        secret_access_key: Some(test_env_or(
            "RUTOMQ_TEST_S3_SECRET_ACCESS_KEY",
            "minioadmin",
        )),
        write_chunk_bytes: MIN_S3_MULTIPART_CHUNK_BYTES,
        write_concurrency: 2,
        ..S3Config::default()
    })
    .unwrap();
    let inspector = store.clone();
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata.create_topic("multipart", 12).await.unwrap();
    let broker = Broker::new(
        metadata,
        Arc::new(store),
        AgentConfig {
            cluster_id: cluster_id.clone(),
            flush_interval: Duration::from_millis(1),
            max_batch_bytes: 16 * 1024 * 1024,
            max_fetch_bytes: 16 * 1024 * 1024,
            fetch_cache_bytes: 0,
            ..AgentConfig::default()
        },
        Arc::new(Metrics::new().unwrap()),
    );
    let records = large_record_batch();
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("multipart"))
                .with_partition_data(
                    (0..12)
                        .map(|partition| {
                            PartitionProduceData::default()
                                .with_index(partition)
                                .with_records(Some(records.clone()))
                        })
                        .collect(),
                ),
        ]);
    let frame = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 1, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, frame);
    assert!(
        response.responses[0]
            .partition_responses
            .iter()
            .all(|partition| partition.error_code == 0)
    );

    let prefix = format!("data/{cluster_id}/");
    let objects = inspector.list(&prefix).await.unwrap();
    assert_eq!(objects.len(), 1);
    assert!(objects[0].size > (MIN_S3_MULTIPART_CHUNK_BYTES * 2) as u64);
    assert!(
        inspector
            .head(&objects[0].key)
            .await
            .unwrap()
            .etag
            .as_deref()
            .is_some_and(|etag| etag.contains('-'))
    );

    let fetch = FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(16 * 1024 * 1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name("multipart"))
                .with_partitions(
                    (0..12)
                        .map(|partition| {
                            FetchPartition::default()
                                .with_partition(partition)
                                .with_fetch_offset(0)
                                .with_partition_max_bytes(1024 * 1024)
                        })
                        .collect(),
                ),
        ]);
    let frame = broker
        .handle_request(request_frame(ApiKey::Fetch, 4, 2, &fetch))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 4, frame);
    assert_eq!(response.responses[0].partitions.len(), 12);
    assert!(response.responses[0].partitions.iter().all(|partition| {
        partition.error_code == 0
            && partition
                .records
                .as_ref()
                .is_some_and(|records| !records.is_empty())
    }));

    inspector.delete(&objects[0].key).await.unwrap();
}

fn large_record_batch() -> Bytes {
    encode_records(&[Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 1,
        key: Some(Bytes::from_static(b"multipart")),
        value: Some(Bytes::from(vec![7; 900 * 1024])),
        headers: Vec::new(),
    }])
    .unwrap()
}

fn test_env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}
