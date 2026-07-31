//! PostgreSQL-backed metadata and an in-memory implementation for tests.

mod acls;
mod assignment_interval;
mod classic_group_barrier;
#[cfg(test)]
mod classic_group_barrier_tests;
#[cfg(test)]
mod classic_group_tests;
mod client_metrics;
mod client_quotas;
mod compaction;
mod consumer_assignor;
mod consumer_groups;
mod delegation_tokens;
mod features;
#[cfg(test)]
mod group_heartbeat_recovery_tests;
mod groups;
mod member_epoch;
mod observability;
mod postgres_acls;
mod postgres_broker_configs;
mod postgres_classic_group_members;
mod postgres_classic_group_store;
mod postgres_classic_groups;
mod postgres_classic_join_barrier;
mod postgres_client_metrics;
mod postgres_client_quotas;
mod postgres_compaction;
mod postgres_consumer_groups;
mod postgres_delegation_tokens;
mod postgres_features;
mod postgres_group_admin;
mod postgres_group_configs;
mod postgres_log_dirs;
mod postgres_objects;
mod postgres_observability;
mod postgres_offsets;
mod postgres_producers;
mod postgres_retention;
mod postgres_scram;
mod postgres_share_groups;
mod postgres_share_offsets;
mod postgres_share_records;
mod postgres_share_state;
mod postgres_streams_groups;
mod postgres_topics;
mod postgres_transactions;
mod retention;
mod scram_credentials;
mod share_groups;
mod share_records;
mod share_state;
mod span_integrity;
mod streams_group_assignment;
#[cfg(test)]
mod streams_group_tests;
mod streams_group_types;
mod streams_groups;
mod streams_topology;
mod streams_topology_partitions;
mod topic_names;
#[cfg(test)]
mod transaction_v2_tests;
mod transactions;

pub use acls::{
    AclFilter, AclOperation, AclPatternFilter, AclPatternType, AclPermission, AclResourceType,
    AclRule,
};
pub use client_metrics::{
    CLIENT_ID, CLIENT_INSTANCE_ID, CLIENT_METRICS_DEFAULT_INTERVAL_MS, CLIENT_METRICS_INTERVAL_MS,
    CLIENT_METRICS_MATCH, CLIENT_METRICS_MAX_INTERVAL_MS, CLIENT_METRICS_METRICS,
    CLIENT_METRICS_MIN_INTERVAL_MS, CLIENT_SOFTWARE_NAME, CLIENT_SOFTWARE_VERSION,
    CLIENT_SOURCE_ADDRESS, CLIENT_SOURCE_PORT, ClientMetricConfigAlteration,
    ClientMetricSubscription,
};
pub use client_quotas::{
    CLIENT_ID_ENTITY, CONNECTION_CREATION_RATE, CONSUMER_BYTE_RATE, CONTROLLER_MUTATION_RATE,
    ClientQuota, ClientQuotaAlteration, ClientQuotaEntity, IP_ENTITY, PRODUCER_BYTE_RATE,
    REQUEST_PERCENTAGE, USER_ENTITY,
};
pub use compaction::{
    CompactedObject, CompactedSpanDraft, CompactionPlan, CompactionSourceSpan,
    CompactionTransactionState,
};
pub use consumer_groups::{
    ConsumerGroupDescription, ConsumerGroupHeartbeat, ConsumerGroupHeartbeatResult,
    ConsumerGroupMemberDescription, ConsumerOwnedTopicPartitions, ConsumerTopicAssignment,
};
pub use delegation_tokens::DelegationToken;
pub use features::{
    CONSUMER_GROUP_VERSION, FeatureLevelUpdate, FeatureMetadata, FeatureUpgradeType,
    GROUP_VERSION_FEATURE, KAFKA_4_0_IV0, KAFKA_4_0_IV1, KAFKA_4_0_IV2, KAFKA_4_0_IV3,
    KAFKA_4_2_IV0, KAFKA_4_2_IV1, KAFKA_4_3_IV0, METADATA_VERSION_FEATURE, SHARE_GROUP_VERSION,
    SHARE_VERSION_FEATURE, STREAMS_GROUP_VERSION, STREAMS_VERSION_FEATURE, SUPPORTED_FEATURES,
    SupportedFeature, TRANSACTION_VERSION_2, TRANSACTION_VERSION_FEATURE,
};
pub use groups::{
    ClassicGroupDescription, ClassicGroupMemberDescription, GroupAssignment, GroupMember,
    GroupMemberIdentity, GroupProtocol, GroupSummary, JoinGroupResult, LeaveGroupMemberError,
    LeaveGroupMemberResult,
};
pub use observability::{ConsumerLag, PartitionRetentionSize, TransactionStateCounts};
pub use retention::{RetentionPage, RetentionResult, TopicConfig};
pub use scram_credentials::{ScramCredential, ScramCredentialAlteration};
pub use share_groups::{
    SHARE_GROUP_ASSIGNOR, ShareGroupDescription, ShareGroupHeartbeat, ShareGroupHeartbeatResult,
    ShareGroupMemberDescription, ShareTopicAssignment,
};
pub use share_records::{
    ShareAcknowledgeRecords, ShareAcknowledgementBatch, ShareAcknowledgementType,
    ShareAcquireRequest, ShareAcquiredRecord, ShareAutoOffsetReset, ShareFetchSession,
    ShareFetchSessionUpdate, ShareOffsetDeleteResult, ShareOffsetUpdate, ShareOffsetUpdateResult,
    SharePartitionOffset, SharePartitionState, ShareSessionPartition,
};
pub use share_state::{
    ACKNOWLEDGED_DELIVERY_STATE, ARCHIVED_DELIVERY_STATE, AVAILABLE_DELIVERY_STATE,
    ShareStateBatch, ShareStateInitialization, ShareStateKey, ShareStateRead, ShareStateSnapshot,
    ShareStateSummary, ShareStateWrite,
};
pub use span_integrity::{
    CURRENT_OBJECT_FORMAT_VERSION, LEGACY_OBJECT_FORMAT_VERSION, SpanChecksum, SpanIntegrity,
};
pub use streams_groups::{
    STREAMS_STATUS_ASSIGNMENT_DELAYED, STREAMS_STATUS_INCORRECTLY_PARTITIONED_TOPICS,
    STREAMS_STATUS_MISSING_INTERNAL_TOPICS, STREAMS_STATUS_MISSING_SOURCE_TOPICS,
    STREAMS_STATUS_SHUTDOWN_APPLICATION, STREAMS_STATUS_STALE_TOPOLOGY, StreamsCopartitionGroup,
    StreamsEndpoint, StreamsEndpointPartitions, StreamsGroupDescription, StreamsGroupHeartbeat,
    StreamsGroupHeartbeatResult, StreamsGroupMemberDescription, StreamsGroupStatus,
    StreamsInternalTopic, StreamsKeyValue, StreamsSubtopology, StreamsTaskAssignment,
    StreamsTaskId, StreamsTaskOffset, StreamsTopicPartitions, StreamsTopology,
};
pub use streams_topology::{streams_internal_topic_requirements, streams_topology_topic_names};
pub use transactions::{
    FetchIsolation, ProducerBatch, ProducerInitialization, ProducerSession, TransactionDescription,
    TransactionFilter, TransactionState, TransactionStatus, increment_producer_sequence,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use uuid::Uuid;

const PRODUCER_BATCH_HISTORY_LIMIT: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AssignmentProtocol {
    Consumer,
    Share,
    Streams,
}

impl AssignmentProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Consumer => "consumer",
            Self::Share => "share",
            Self::Streams => "streams",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupAssignmentTask {
    pub protocol: AssignmentProtocol,
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub assignment_timestamp: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupAssignmentCompletion {
    Published,
    Unchanged,
    Stale,
    GroupNotFound,
}

impl GroupAssignmentCompletion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::Unchanged => "unchanged",
            Self::Stale => "stale",
            Self::GroupNotFound => "group_not_found",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupHeartbeatOutcome<T> {
    pub result: T,
    pub assignment_task: Option<GroupAssignmentTask>,
}

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("topic {0} was not found")]
    TopicNotFound(String),
    #[error("topic {0} already exists")]
    TopicAlreadyExists(String),
    #[error("{0}")]
    InvalidTopic(String),
    #[error(
        "requested partition count {requested} for topic {topic} must be greater than current count {current}"
    )]
    InvalidPartitionCount {
        topic: String,
        current: i32,
        requested: i32,
    },
    #[error(
        "offset {offset} is outside the retained range {start}..={end} for partition {partition:?}"
    )]
    OffsetOutOfRange {
        partition: PartitionKey,
        offset: i64,
        start: i64,
        end: i64,
    },
    #[error("partition {topic}-{partition} was not found")]
    PartitionNotFound { topic: String, partition: i32 },
    #[error("invalid metadata request: {0}")]
    InvalidRequest(String),
    #[error("invalid regular expression: {0}")]
    InvalidRegularExpression(String),
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
    #[error("invalid feature update: {0}")]
    InvalidUpdateVersion(String),
    #[error("client metrics subscription {0} was not found")]
    ClientMetricSubscriptionNotFound(String),
    #[error("consumer group {0} was not found")]
    GroupNotFound(String),
    #[error("group {0} uses another protocol")]
    GroupProtocolMismatch(String),
    #[error("consumer group {0} is not empty")]
    NonEmptyGroup(String),
    #[error("group {0} has reached its maximum size")]
    GroupMaxSizeReached(String),
    #[error("consumer group member {member} was not found in {group}")]
    GroupMemberNotFound { group: String, member: String },
    #[error("consumer group member must rejoin with allocated member id {member_id}")]
    MemberIdRequired { member_id: String },
    #[error("consumer group {0} has no protocol supported by every member")]
    InconsistentGroupProtocol(String),
    #[error("consumer group {0} is preparing a new generation")]
    RebalanceInProgress(String),
    #[error("consumer group instance id {instance_id} in {group} is owned by another member")]
    FencedInstanceId { group: String, instance_id: String },
    #[error("consumer group member {member} in {group} expected epoch {expected}, got {actual}")]
    FencedMemberEpoch {
        group: String,
        member: String,
        expected: i32,
        actual: i32,
    },
    #[error("share session for member {member} in group {group} was not found")]
    ShareSessionNotFound { group: String, member: String },
    #[error(
        "share session for member {member} in group {group} expected epoch {expected}, got {actual}"
    )]
    InvalidShareSessionEpoch {
        group: String,
        member: String,
        expected: i32,
        actual: i32,
    },
    #[error("share record offset {0} is not in an acknowledgeable state")]
    InvalidShareRecordState(i64),
    #[error("share state leader epoch is fenced: current {current}, requested {requested}")]
    FencedShareLeaderEpoch { current: i32, requested: i32 },
    #[error("share state epoch is fenced: current {current}, requested {requested}")]
    FencedShareStateEpoch { current: i32, requested: i32 },
    #[error("consumer group assignor {0} is not supported")]
    UnsupportedConsumerAssignor(String),
    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(String),
    #[error(
        "consumer group instance id {instance_id} in {group} is still owned by member {member}"
    )]
    UnreleasedInstanceId {
        group: String,
        instance_id: String,
        member: String,
    },
    #[error("consumer group {group} expected generation {expected}, got {actual}")]
    IllegalGeneration {
        group: String,
        expected: i32,
        actual: i32,
    },
    #[error("producer {0} is unknown")]
    UnknownProducer(i64),
    #[error(
        "producer {producer_id} epoch is fenced: expected {expected_epoch}, got {actual_epoch}"
    )]
    ProducerFenced {
        producer_id: i64,
        expected_epoch: i16,
        actual_epoch: i16,
    },
    #[error(
        "transaction coordinator for producer {producer_id} is fenced: current epoch {current_epoch}, requested {requested_epoch}"
    )]
    TransactionCoordinatorFenced {
        producer_id: i64,
        current_epoch: i32,
        requested_epoch: i32,
    },
    #[error(
        "producer {producer_id} sequence is out of order for {partition:?}: expected {expected}, got {actual}"
    )]
    OutOfOrderSequence {
        producer_id: i64,
        partition: PartitionKey,
        expected: i32,
        actual: i32,
    },
    #[error("transactional id {0} was not found")]
    TransactionNotFound(String),
    #[error("invalid transaction state: {0}")]
    InvalidTransactionState(String),
    #[error("invalid transaction timeout {0}ms")]
    InvalidTransactionTimeout(i32),
    #[error("delegation token was not found")]
    DelegationTokenNotFound,
    #[error("delegation token owner or renewer does not match the requester")]
    DelegationTokenOwnerMismatch,
    #[error("delegation token has expired")]
    DelegationTokenExpired,
    #[error("metadata database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("metadata migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
}

pub fn validate_topic_name(name: &str) -> Result<(), ControlError> {
    topic_names::validate(name)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PartitionKey {
    pub topic: String,
    pub partition: i32,
}

impl PartitionKey {
    pub fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub id: Uuid,
    pub name: String,
    pub partitions: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectRef {
    pub key: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchDraft {
    pub partition: PartitionKey,
    pub byte_start: u64,
    pub byte_end: u64,
    pub record_count: i32,
    pub timestamp_ms: i64,
    pub checksum: Option<SpanChecksum>,
    pub producer: Option<ProducerBatch>,
    pub transactional_id: Option<String>,
    #[serde(default = "verification_enabled_by_default")]
    pub verify_transaction_partition: bool,
}

const fn verification_enabled_by_default() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSpan {
    pub partition: PartitionKey,
    pub object_key: String,
    pub byte_start: u64,
    pub byte_end: u64,
    pub base_offset: i64,
    pub last_offset: i64,
    pub record_count: i32,
    pub timestamp_ms: i64,
    pub integrity: SpanIntegrity,
    pub producer: Option<ProducerBatch>,
    pub transaction_id: Option<Uuid>,
    pub offsets_preserved: bool,
}

#[derive(Debug, Clone)]
pub struct PartitionFetch {
    pub spans: Vec<StoredSpan>,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionWatermarks {
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub log_start_offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetCommit {
    pub partition: PartitionKey,
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
    pub retention_time_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedOffset {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
    pub commit_timestamp_ms: i64,
    pub expire_timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveProducer {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub last_sequence: i32,
    pub last_timestamp: i64,
    pub current_transaction_start_offset: i64,
}

#[async_trait]
pub trait MetadataStore: Send + Sync {
    async fn validate_topic_creation(&self, name: &str) -> Result<(), ControlError> {
        topic_names::validate(name)?;
        let topics = self.topics(None).await?;
        if topics.iter().any(|topic| topic.name == name) {
            return Err(ControlError::TopicAlreadyExists(name.to_owned()));
        }
        if let Some(existing) =
            topic_names::collision(name, topics.iter().map(|topic| topic.name.as_str()))
        {
            return Err(topic_names::collision_error(name, existing));
        }
        Ok(())
    }

    async fn create_topic(&self, name: &str, partitions: i32) -> Result<TopicInfo, ControlError>;
    async fn create_topic_with_config(
        &self,
        name: &str,
        partitions: i32,
        config: TopicConfig,
    ) -> Result<TopicInfo, ControlError>;
    async fn create_partitions(
        &self,
        name: &str,
        new_count: i32,
    ) -> Result<TopicInfo, ControlError>;
    async fn delete_topic(&self, name: &str) -> Result<(), ControlError>;
    async fn delete_topic_by_id(&self, id: Uuid) -> Result<Option<TopicInfo>, ControlError>;
    async fn topic(&self, name: &str) -> Result<Option<TopicInfo>, ControlError>;
    async fn topic_by_id(&self, id: Uuid) -> Result<Option<TopicInfo>, ControlError>;
    async fn topics(&self, names: Option<&[String]>) -> Result<Vec<TopicInfo>, ControlError>;
    async fn topic_config(&self, name: &str) -> Result<TopicConfig, ControlError>;
    async fn set_topic_config(&self, name: &str, config: TopicConfig) -> Result<(), ControlError>;
    async fn stage_object(&self, object: ObjectRef) -> Result<(), ControlError>;
    async fn commit_object(
        &self,
        object: ObjectRef,
        batches: Vec<BatchDraft>,
    ) -> Result<Vec<StoredSpan>, ControlError>;
    async fn fetch(
        &self,
        partition: &PartitionKey,
        offset: i64,
        max_bytes: usize,
        isolation: FetchIsolation,
    ) -> Result<PartitionFetch, ControlError>;
    async fn partition_watermarks(
        &self,
        partition: &PartitionKey,
    ) -> Result<PartitionWatermarks, ControlError>;
    async fn list_offset(
        &self,
        partition: &PartitionKey,
        timestamp_ms: i64,
    ) -> Result<i64, ControlError>;
    async fn partition_size(&self, partition: &PartitionKey) -> Result<i64, ControlError>;
    async fn partition_retention_sizes(
        &self,
        limit: usize,
    ) -> Result<Vec<PartitionRetentionSize>, ControlError>;
    async fn describe_producers(
        &self,
        partition: &PartitionKey,
    ) -> Result<Vec<ActiveProducer>, ControlError>;
    async fn expire_producer_sequences(
        &self,
        now_ms: i64,
        expiration_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError>;
    async fn expire_consumer_offsets(
        &self,
        now_ms: i64,
        retention_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError>;
    async fn delete_records(
        &self,
        partition: &PartitionKey,
        before_offset: i64,
    ) -> Result<i64, ControlError>;
    async fn commit_offsets(
        &self,
        group_id: &str,
        offsets: Vec<OffsetCommit>,
    ) -> Result<(), ControlError>;
    #[allow(clippy::too_many_arguments)]
    async fn commit_member_offsets(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        generation_or_epoch: i32,
        _api_version: i16,
        offsets: Vec<OffsetCommit>,
    ) -> Result<Vec<bool>, ControlError> {
        let count = offsets.len();
        self.validate_group_member(group_id, member_id, group_instance_id, generation_or_epoch)
            .await?;
        self.commit_offsets(group_id, offsets).await?;
        Ok(vec![true; count])
    }
    async fn fetch_offsets(
        &self,
        group_id: &str,
        partitions: &[PartitionKey],
    ) -> Result<HashMap<PartitionKey, CommittedOffset>, ControlError>;
    async fn consumer_lags(&self, limit: usize) -> Result<Vec<ConsumerLag>, ControlError>;
    async fn delete_offsets(
        &self,
        group_id: &str,
        partitions: &[PartitionKey],
    ) -> Result<HashSet<PartitionKey>, ControlError>;
    #[allow(clippy::too_many_arguments)]
    async fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: &[(String, Vec<u8>)],
        client: (&str, &str, &[String], i32),
        api_version: i16,
    ) -> Result<JoinGroupResult, ControlError>;
    #[allow(clippy::too_many_arguments)]
    async fn begin_join_group(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: &[(String, Vec<u8>)],
        client: (&str, &str, &[String], i32),
        rebalance_timeout_ms: i32,
        initial_rebalance_delay_ms: i32,
        max_size: i32,
        api_version: i16,
    ) -> Result<JoinGroupResult, ControlError>;
    async fn poll_join_group(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        rebalance_id: Uuid,
        api_version: i16,
    ) -> Result<JoinGroupResult, ControlError>;
    async fn sync_group(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        assignments: Vec<GroupAssignment>,
    ) -> Result<Vec<u8>, ControlError>;
    async fn heartbeat_group(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<(), ControlError>;
    async fn leave_group(
        &self,
        group_id: &str,
        members: &[GroupMemberIdentity],
    ) -> Result<Vec<LeaveGroupMemberResult>, ControlError>;
    async fn consumer_group_heartbeat(
        &self,
        heartbeat: ConsumerGroupHeartbeat,
    ) -> Result<ConsumerGroupHeartbeatResult, ControlError>;
    async fn consumer_group_heartbeat_deferred(
        &self,
        heartbeat: ConsumerGroupHeartbeat,
    ) -> Result<GroupHeartbeatOutcome<ConsumerGroupHeartbeatResult>, ControlError>;
    async fn describe_consumer_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, ConsumerGroupDescription>, ControlError>;
    async fn streams_group_heartbeat(
        &self,
        heartbeat: StreamsGroupHeartbeat,
    ) -> Result<StreamsGroupHeartbeatResult, ControlError>;
    async fn streams_group_heartbeat_deferred(
        &self,
        heartbeat: StreamsGroupHeartbeat,
    ) -> Result<GroupHeartbeatOutcome<StreamsGroupHeartbeatResult>, ControlError>;
    async fn describe_streams_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, StreamsGroupDescription>, ControlError>;
    async fn share_group_heartbeat(
        &self,
        heartbeat: ShareGroupHeartbeat,
    ) -> Result<ShareGroupHeartbeatResult, ControlError>;
    async fn share_group_heartbeat_deferred(
        &self,
        heartbeat: ShareGroupHeartbeat,
    ) -> Result<GroupHeartbeatOutcome<ShareGroupHeartbeatResult>, ControlError>;
    async fn complete_group_assignment(
        &self,
        task: GroupAssignmentTask,
    ) -> Result<GroupAssignmentCompletion, ControlError>;
    async fn describe_share_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, ShareGroupDescription>, ControlError>;
    async fn update_share_fetch_session(
        &self,
        update: ShareFetchSessionUpdate,
    ) -> Result<ShareFetchSession, ControlError>;
    async fn existing_share_partition_state(
        &self,
        group_id: &str,
        member_id: &str,
        partition: &ShareSessionPartition,
    ) -> Result<Option<SharePartitionState>, ControlError>;
    async fn share_partition_state(
        &self,
        group_id: &str,
        member_id: &str,
        partition: &ShareSessionPartition,
        reset: ShareAutoOffsetReset,
    ) -> Result<SharePartitionState, ControlError>;
    async fn describe_share_group_offsets(
        &self,
        group_id: &str,
        partitions: Option<&[PartitionKey]>,
    ) -> Result<Vec<SharePartitionOffset>, ControlError>;
    async fn alter_share_group_offsets(
        &self,
        group_id: &str,
        updates: &[ShareOffsetUpdate],
    ) -> Result<Vec<ShareOffsetUpdateResult>, ControlError>;
    async fn delete_share_group_offsets(
        &self,
        group_id: &str,
        topics: &[String],
    ) -> Result<Vec<ShareOffsetDeleteResult>, ControlError>;
    async fn acquire_share_records(
        &self,
        request: ShareAcquireRequest,
    ) -> Result<Vec<ShareAcquiredRecord>, ControlError>;
    async fn acknowledge_share_records(
        &self,
        request: ShareAcknowledgeRecords,
    ) -> Result<(), ControlError>;
    async fn initialize_share_group_state(
        &self,
        initialization: ShareStateInitialization,
    ) -> Result<(), ControlError>;
    async fn read_share_group_state(
        &self,
        read: ShareStateRead,
    ) -> Result<ShareStateSnapshot, ControlError>;
    async fn write_share_group_state(&self, write: ShareStateWrite) -> Result<(), ControlError>;
    async fn delete_share_group_state(&self, key: &ShareStateKey) -> Result<(), ControlError>;
    async fn summarize_share_group_state(
        &self,
        key: &ShareStateKey,
    ) -> Result<Option<ShareStateSummary>, ControlError>;
    async fn list_groups(&self) -> Result<Vec<GroupSummary>, ControlError>;
    async fn list_groups_limited(&self, limit: usize) -> Result<Vec<GroupSummary>, ControlError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut groups = self.list_groups().await?;
        groups.sort_by(|left, right| left.group_id.cmp(&right.group_id));
        groups.truncate(limit);
        Ok(groups)
    }
    async fn describe_classic_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, ClassicGroupDescription>, ControlError>;
    async fn delete_group(&self, group_id: &str) -> Result<(), ControlError>;
    async fn validate_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        generation_or_epoch: i32,
    ) -> Result<(), ControlError>;
    async fn init_producer(
        &self,
        transactional_id: Option<&str>,
        transaction_timeout_ms: i32,
        current: Option<ProducerSession>,
    ) -> Result<ProducerSession, ControlError> {
        Ok(self
            .init_producer_with_options(
                transactional_id,
                transaction_timeout_ms,
                current,
                false,
                false,
            )
            .await?
            .producer)
    }
    async fn init_producer_with_options(
        &self,
        transactional_id: Option<&str>,
        transaction_timeout_ms: i32,
        current: Option<ProducerSession>,
        enable_2_pc: bool,
        keep_prepared_txn: bool,
    ) -> Result<ProducerInitialization, ControlError>;
    async fn add_partitions_to_transaction(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        partitions: &[PartitionKey],
        verify_only: bool,
    ) -> Result<(), ControlError>;
    async fn add_offsets_to_transaction(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        group_id: &str,
    ) -> Result<(), ControlError>;
    async fn commit_transaction_offsets(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        group_id: &str,
        offsets: Vec<OffsetCommit>,
    ) -> Result<(), ControlError>;
    #[allow(clippy::too_many_arguments)]
    async fn commit_transaction_member_offsets(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        generation_or_epoch: i32,
        add_group: bool,
        offsets: Vec<OffsetCommit>,
    ) -> Result<(), ControlError> {
        let anonymous_transactional_commit =
            generation_or_epoch == -1 && member_id.is_empty() && group_instance_id.is_none();
        if !anonymous_transactional_commit {
            self.validate_group_member(group_id, member_id, group_instance_id, generation_or_epoch)
                .await?;
        }
        if add_group {
            self.add_offsets_to_transaction(transactional_id, producer, group_id)
                .await?;
        }
        self.commit_transaction_offsets(transactional_id, producer, group_id, offsets)
            .await
    }
    async fn end_transaction(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        committed: bool,
    ) -> Result<(), ControlError>;
    async fn end_transaction_with_epoch_bump(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        committed: bool,
    ) -> Result<ProducerSession, ControlError>;
    async fn write_transaction_marker(
        &self,
        producer: ProducerSession,
        partitions: &[PartitionKey],
        committed: bool,
        coordinator_epoch: i32,
        transaction_version: i8,
    ) -> Result<(), ControlError>;
    async fn describe_transactions(
        &self,
        transactional_ids: &[String],
    ) -> Result<HashMap<String, TransactionDescription>, ControlError>;
    async fn list_transactions(
        &self,
        filter: &TransactionFilter,
    ) -> Result<Vec<TransactionDescription>, ControlError>;
    async fn transaction_state_counts(&self) -> Result<TransactionStateCounts, ControlError>;
    async fn expire_transactional_ids(
        &self,
        now_ms: i64,
        expiration_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError>;
    async fn create_acl(&self, rule: AclRule) -> Result<(), ControlError>;
    async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclRule>, ControlError>;
    async fn delete_acls(&self, filters: &[AclFilter]) -> Result<Vec<Vec<AclRule>>, ControlError>;
    async fn scram_credentials(
        &self,
        users: Option<&[String]>,
    ) -> Result<Vec<ScramCredential>, ControlError>;
    async fn alter_scram_credentials(
        &self,
        alterations: Vec<ScramCredentialAlteration>,
    ) -> Result<HashSet<String>, ControlError>;
    async fn client_quotas(&self) -> Result<Vec<ClientQuota>, ControlError>;
    async fn alter_client_quotas(
        &self,
        alterations: Vec<ClientQuotaAlteration>,
    ) -> Result<(), ControlError>;
    async fn client_metric_subscriptions(
        &self,
    ) -> Result<Vec<ClientMetricSubscription>, ControlError>;
    async fn client_metric_subscription(
        &self,
        name: &str,
    ) -> Result<Option<ClientMetricSubscription>, ControlError>;
    async fn alter_client_metric_subscription(
        &self,
        alteration: ClientMetricConfigAlteration,
        validate_only: bool,
    ) -> Result<(), ControlError>;
    async fn group_config(&self, group_id: &str) -> Result<BTreeMap<String, String>, ControlError>;
    async fn group_config_ids(&self) -> Result<Vec<String>, ControlError>;
    async fn alter_group_config(
        &self,
        group_id: &str,
        changes: BTreeMap<String, Option<String>>,
        validate_only: bool,
    ) -> Result<(), ControlError>;
    async fn broker_config(&self) -> Result<BTreeMap<String, String>, ControlError>;
    async fn alter_broker_config(
        &self,
        changes: BTreeMap<String, Option<String>>,
        validate_only: bool,
    ) -> Result<(), ControlError>;
    async fn features(&self) -> Result<FeatureMetadata, ControlError>;
    async fn update_features(
        &self,
        updates: Vec<FeatureLevelUpdate>,
        validate_only: bool,
    ) -> Result<FeatureMetadata, ControlError>;
    async fn create_delegation_token(&self, token: DelegationToken) -> Result<(), ControlError>;
    async fn delegation_token_by_id(
        &self,
        token_id: &str,
        now_ms: i64,
    ) -> Result<Option<DelegationToken>, ControlError>;
    async fn delegation_tokens(&self, now_ms: i64) -> Result<Vec<DelegationToken>, ControlError>;
    async fn renew_delegation_token(
        &self,
        hmac: &[u8],
        principal: &str,
        now_ms: i64,
        requested_period_ms: i64,
        default_period_ms: i64,
    ) -> Result<i64, ControlError>;
    async fn expire_delegation_token(
        &self,
        hmac: &[u8],
        principal: &str,
        now_ms: i64,
        expiry_period_ms: i64,
    ) -> Result<i64, ControlError>;
    async fn delete_expired_delegation_tokens(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError>;
    async fn authorize(
        &self,
        principal: &str,
        host: &str,
        resource_type: AclResourceType,
        resource_name: &str,
        operation: AclOperation,
        allow_if_no_acl: bool,
    ) -> Result<bool, ControlError>;
    async fn authorize_by_resource_type(
        &self,
        principal: &str,
        host: &str,
        resource_type: AclResourceType,
        operation: AclOperation,
        allow_if_no_acl: bool,
    ) -> Result<bool, ControlError>;
    async fn apply_retention(
        &self,
        now_ms: i64,
        object_delete_grace_ms: i64,
    ) -> Result<RetentionResult, ControlError> {
        self.apply_retention_page(
            now_ms,
            object_delete_grace_ms,
            RetentionPage {
                start_after_partition: None,
                start_after_object: None,
                max_partitions: usize::MAX,
                max_spans: usize::MAX,
                max_objects: usize::MAX,
            },
        )
        .await
    }
    async fn apply_retention_page(
        &self,
        now_ms: i64,
        object_delete_grace_ms: i64,
        page: RetentionPage<'_>,
    ) -> Result<RetentionResult, ControlError>;
    async fn complete_object_deletion(&self, key: &str) -> Result<bool, ControlError>;
    async fn claim_compaction(
        &self,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<CompactionPlan>, ControlError>;
    async fn commit_compaction(
        &self,
        plan: &CompactionPlan,
        objects: Vec<CompactedObject>,
        recheck_at_ms: Option<i64>,
        now_ms: i64,
    ) -> Result<bool, ControlError>;
    async fn release_compaction(
        &self,
        partition: &PartitionKey,
        lease_id: Uuid,
    ) -> Result<(), ControlError>;
    async fn abort_expired_transactions(&self) -> Result<u64, ControlError>;
    async fn abort_expired_transactions_batch(&self, limit: usize) -> Result<u64, ControlError> {
        if limit == 0 {
            return Ok(0);
        }
        self.abort_expired_transactions().await
    }
    async fn claim_stale_objects(
        &self,
        before_ms: i64,
        limit: i64,
    ) -> Result<Vec<String>, ControlError>;
    async fn complete_stale_object_deletion(&self, key: &str) -> Result<bool, ControlError>;
    async fn object_committed(&self, key: &str) -> Result<bool, ControlError>;
    async fn object_staged(&self, key: &str) -> Result<bool, ControlError>;
    async fn check(&self) -> Result<(), ControlError>;
}

#[derive(Clone)]
pub struct MemoryMetadataStore {
    state: Arc<RwLock<MemoryState>>,
    #[cfg(feature = "test-support")]
    authorization_failure: Arc<std::sync::atomic::AtomicI8>,
}

#[derive(Clone, Default)]
struct MemoryState {
    topics: HashMap<String, TopicInfo>,
    topic_configs: HashMap<String, TopicConfig>,
    partitions: HashMap<PartitionKey, MemoryPartition>,
    objects: HashMap<String, ObjectRef>,
    staged_objects: HashMap<String, i64>,
    orphan_gc_claims: HashMap<String, i64>,
    unreferenced_objects: HashMap<String, i64>,
    object_delete_after: HashMap<String, i64>,
    offsets: HashMap<(String, PartitionKey), CommittedOffset>,
    groups: HashMap<String, MemoryGroup>,
    pending_group_members: HashMap<(String, String), DateTime<Utc>>,
    consumer_groups: HashMap<String, consumer_groups::ConsumerGroupState>,
    streams_groups: HashMap<String, streams_groups::StreamsGroupState>,
    share_groups: HashMap<String, share_groups::ShareGroupState>,
    share_records: share_records::MemoryShareStore,
    next_producer_id: i64,
    producers: HashMap<i64, MemoryProducer>,
    transactional_producers: HashMap<String, i64>,
    transactions: HashMap<Uuid, MemoryTransaction>,
    acl_rules: Vec<AclRule>,
    scram_credentials: HashMap<(String, i8), ScramCredential>,
    client_quotas: HashMap<ClientQuotaEntity, BTreeMap<String, f64>>,
    client_metric_subscriptions: BTreeMap<String, ClientMetricSubscription>,
    group_configs: HashMap<String, BTreeMap<String, String>>,
    broker_configs: BTreeMap<String, String>,
    features: FeatureMetadata,
    delegation_tokens: HashMap<String, DelegationToken>,
}

fn ensure_classic_group_is_empty_for_consumer(
    state: &MemoryState,
    group_id: &str,
    now: DateTime<Utc>,
) -> Result<(), ControlError> {
    let Some(mut group) = state.groups.get(group_id).cloned() else {
        return Ok(());
    };
    group.remove_expired_members(now);
    if group.members.is_empty() {
        Ok(())
    } else {
        Err(ControlError::GroupProtocolMismatch(group_id.to_owned()))
    }
}

fn empty_consumer_group_for_classic(
    state: &MemoryState,
    group_id: &str,
    now: DateTime<Utc>,
) -> Result<bool, ControlError> {
    let Some(mut group) = state.consumer_groups.get(group_id).cloned() else {
        return Ok(false);
    };
    consumer_groups::expire_members(&mut group, now);
    if group.members.is_empty() {
        Ok(true)
    } else {
        Err(ControlError::GroupProtocolMismatch(group_id.to_owned()))
    }
}

fn commit_memory_offsets(
    state: &mut MemoryState,
    group_id: &str,
    offsets: Vec<OffsetCommit>,
) -> Result<(), ControlError> {
    if group_id.is_empty() {
        return Err(ControlError::InvalidRequest(
            "group id must not be empty".to_owned(),
        ));
    }
    for offset in &offsets {
        if offset.offset < 0 {
            return Err(ControlError::InvalidRequest(
                "committed offset must not be negative".to_owned(),
            ));
        }
        if !state.partitions.contains_key(&offset.partition) {
            return Err(ControlError::PartitionNotFound {
                topic: offset.partition.topic.clone(),
                partition: offset.partition.partition,
            });
        }
    }
    let commit_timestamp_ms = Utc::now().timestamp_millis();
    for offset in offsets {
        let expire_timestamp_ms =
            offset_expire_timestamp(commit_timestamp_ms, offset.retention_time_ms)?;
        state.offsets.insert(
            (group_id.to_owned(), offset.partition),
            CommittedOffset {
                offset: offset.offset,
                leader_epoch: offset.leader_epoch,
                metadata: offset.metadata,
                commit_timestamp_ms,
                expire_timestamp_ms,
            },
        );
    }
    Ok(())
}

fn offset_expire_timestamp(
    commit_timestamp_ms: i64,
    retention_time_ms: Option<i64>,
) -> Result<Option<i64>, ControlError> {
    let Some(retention_time_ms) = retention_time_ms else {
        return Ok(None);
    };
    if retention_time_ms < 0 {
        return Err(ControlError::InvalidRequest(
            "offset retention time must be non-negative".to_owned(),
        ));
    }
    commit_timestamp_ms
        .checked_add(retention_time_ms)
        .map(Some)
        .ok_or_else(|| ControlError::InvalidRequest("offset retention time overflow".to_owned()))
}

fn memory_offset_has_pending_transaction(
    state: &MemoryState,
    group_id: &str,
    partition: &PartitionKey,
) -> bool {
    state.transactions.values().any(|transaction| {
        transaction.status == TransactionStatus::Ongoing
            && transaction
                .offsets
                .contains_key(&(group_id.to_owned(), partition.clone()))
    })
}

fn member_session_active(last_heartbeat: DateTime<Utc>, timeout_ms: i32, now_ms: i64) -> bool {
    last_heartbeat
        .timestamp_millis()
        .saturating_add(i64::from(timeout_ms))
        > now_ms
}

fn consumer_member_subscribes(member: &consumer_groups::ConsumerMemberState, topic: &str) -> bool {
    member
        .subscribed_topic_names
        .iter()
        .any(|name| name == topic)
        || member
            .subscribed_topic_regex
            .as_deref()
            .is_some_and(|pattern| {
                regex::Regex::new(&format!("^(?:{pattern})$"))
                    .is_ok_and(|regex| regex.is_match(topic))
            })
}

fn streams_group_subscribes(group: &streams_groups::StreamsGroupState, topic: &str) -> bool {
    group.topology.subtopologies.iter().any(|subtopology| {
        subtopology.source_topics.iter().any(|name| name == topic)
            || subtopology
                .repartition_source_topics
                .iter()
                .any(|source| source.name == topic)
            || subtopology.source_topic_regex.iter().any(|pattern| {
                regex::Regex::new(&format!("^(?:{pattern})$"))
                    .is_ok_and(|regex| regex.is_match(topic))
            })
    })
}

fn prepare_memory_classic_offset_expiration(state: &mut MemoryState, now_ms: i64) {
    for group in state.groups.values_mut() {
        let active = group.members.values().any(|member| {
            (group.rebalance_pending && member.joined_rebalance_id == group.rebalance_id)
                || member_session_active(member.last_heartbeat, member.session_timeout_ms, now_ms)
        });
        if active {
            group.empty_since = None;
        } else if group.empty_since.is_none() {
            let empty_since_ms = group
                .members
                .values()
                .map(|member| {
                    member
                        .last_heartbeat
                        .timestamp_millis()
                        .saturating_add(i64::from(member.session_timeout_ms))
                })
                .max()
                .unwrap_or(now_ms)
                .min(now_ms);
            group.empty_since = DateTime::from_timestamp_millis(empty_since_ms);
        }
    }
}

fn memory_default_offset_expired(
    state: &MemoryState,
    group_id: &str,
    partition: &PartitionKey,
    offset: &CommittedOffset,
    now_ms: i64,
    retention_ms: i64,
) -> bool {
    let committed_long_enough = now_ms.saturating_sub(offset.commit_timestamp_ms) >= retention_ms;
    if let Some(group) = state.groups.get(group_id) {
        let active_members = group
            .members
            .values()
            .filter(|member| {
                (group.rebalance_pending && member.joined_rebalance_id == group.rebalance_id)
                    || member_session_active(
                        member.last_heartbeat,
                        member.session_timeout_ms,
                        now_ms,
                    )
            })
            .collect::<Vec<_>>();
        if active_members.is_empty() {
            return group.empty_since.is_some_and(|empty_since| {
                now_ms.saturating_sub(empty_since.timestamp_millis()) >= retention_ms
            });
        }
        let stable = !group.rebalance_pending
            && active_members
                .iter()
                .all(|member| group.assignments.contains_key(&member.member_id));
        return stable
            && group.protocol_type == "consumer"
            && committed_long_enough
            && !active_members.iter().any(|member| {
                member
                    .subscribed_topics
                    .iter()
                    .any(|topic| topic == &partition.topic)
            });
    }
    if let Some(group) = state.consumer_groups.get(group_id) {
        return committed_long_enough
            && !group.members.values().any(|member| {
                (member.member_epoch == -2
                    || member_session_active(
                        member.last_heartbeat,
                        member.session_timeout_ms,
                        now_ms,
                    ))
                    && consumer_member_subscribes(member, &partition.topic)
            });
    }
    if let Some(group) = state.streams_groups.get(group_id) {
        let active = group.members.values().any(|member| {
            member_session_active(member.last_heartbeat, member.session_timeout_ms, now_ms)
        });
        return committed_long_enough
            && !(active && streams_group_subscribes(group, &partition.topic));
    }
    committed_long_enough
}

fn expire_memory_consumer_offsets(
    state: &mut MemoryState,
    now_ms: i64,
    retention_ms: i64,
    limit: usize,
) -> Result<u64, ControlError> {
    if retention_ms < 0 {
        return Err(ControlError::InvalidRequest(
            "consumer offset retention must be non-negative".to_owned(),
        ));
    }
    if limit == 0 {
        return Ok(0);
    }
    prepare_memory_classic_offset_expiration(state, now_ms);
    let mut candidates = state
        .offsets
        .iter()
        .filter_map(|((group_id, partition), offset)| {
            let expired = offset
                .expire_timestamp_ms
                .is_some_and(|expires_at| now_ms >= expires_at)
                || (offset.expire_timestamp_ms.is_none()
                    && memory_default_offset_expired(
                        state,
                        group_id,
                        partition,
                        offset,
                        now_ms,
                        retention_ms,
                    ));
            (expired && !memory_offset_has_pending_transaction(state, group_id, partition))
                .then(|| (group_id.clone(), partition.clone()))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        (&left.0, &left.1.topic, left.1.partition).cmp(&(
            &right.0,
            &right.1.topic,
            right.1.partition,
        ))
    });
    candidates.truncate(limit);
    let affected_groups = candidates
        .iter()
        .map(|(group_id, _)| group_id.clone())
        .collect::<HashSet<_>>();
    for key in &candidates {
        state.offsets.remove(key);
    }
    for group_id in affected_groups {
        let has_offsets = state
            .offsets
            .keys()
            .any(|(stored_group, _)| stored_group == &group_id);
        let has_pending = state.transactions.values().any(|transaction| {
            transaction.status == TransactionStatus::Ongoing
                && transaction
                    .offsets
                    .keys()
                    .any(|(stored_group, _)| stored_group == &group_id)
        });
        if has_offsets || has_pending {
            continue;
        }
        if state
            .groups
            .get(&group_id)
            .is_some_and(|group| group.empty_since.is_some())
        {
            state.groups.remove(&group_id);
        }
        if state
            .consumer_groups
            .get(&group_id)
            .is_some_and(|group| group.members.is_empty())
        {
            state.consumer_groups.remove(&group_id);
        }
        if state
            .streams_groups
            .get(&group_id)
            .is_some_and(|group| group.members.is_empty())
        {
            state.streams_groups.remove(&group_id);
        }
    }
    Ok(candidates.len() as u64)
}

fn validate_memory_group_member(
    state: &mut MemoryState,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
    generation_or_epoch: i32,
) -> Result<(), ControlError> {
    if member_id.is_empty() && generation_or_epoch < 0 {
        return Ok(());
    }
    if let Some(group) = state.consumer_groups.get(group_id) {
        return consumer_groups::validate_member(group, member_id, generation_or_epoch);
    }
    if state.streams_groups.contains_key(group_id) {
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        let group = state
            .streams_groups
            .get_mut(group_id)
            .expect("checked streams group exists");
        streams_groups::expire_and_describe(group, &topics, Utc::now())?;
        return streams_groups::validate_member(group, member_id, generation_or_epoch);
    }
    let group = state
        .groups
        .get_mut(group_id)
        .ok_or_else(|| ControlError::GroupNotFound(group_id.to_owned()))?;
    if group.rebalance_pending {
        return Err(ControlError::RebalanceInProgress(group_id.to_owned()));
    }
    if group.remove_expired_members(Utc::now()) {
        group.begin_rebalance();
    }
    if group.generation_id != generation_or_epoch {
        return Err(ControlError::IllegalGeneration {
            group: group_id.to_owned(),
            expected: group.generation_id,
            actual: generation_or_epoch,
        });
    }
    group.validate_member_identity(group_id, member_id, group_instance_id)
}

fn commit_memory_transaction_offsets(
    state: &mut MemoryState,
    transactional_id: &str,
    producer: ProducerSession,
    group_id: &str,
    add_group: bool,
    offsets: Vec<OffsetCommit>,
) -> Result<(), ControlError> {
    for offset in &offsets {
        if offset.offset < 0 {
            return Err(ControlError::InvalidRequest(
                "committed offset must not be negative".to_owned(),
            ));
        }
        if !state.partitions.contains_key(&offset.partition) {
            return Err(ControlError::PartitionNotFound {
                topic: offset.partition.topic.clone(),
                partition: offset.partition.partition,
            });
        }
    }
    let transaction_id =
        memory_transaction_id(state, transactional_id, producer, add_group, false)?;
    let transaction = state
        .transactions
        .get_mut(&transaction_id)
        .expect("current transaction exists");
    if add_group {
        transaction.groups.insert(group_id.to_owned());
    }
    if !transaction.groups.contains(group_id) {
        return Err(ControlError::InvalidTransactionState(format!(
            "group {group_id} was not added to the transaction"
        )));
    }
    let commit_timestamp_ms = Utc::now().timestamp_millis();
    for offset in offsets {
        let expire_timestamp_ms =
            offset_expire_timestamp(commit_timestamp_ms, offset.retention_time_ms)?;
        transaction.offsets.insert(
            (group_id.to_owned(), offset.partition),
            CommittedOffset {
                offset: offset.offset,
                leader_epoch: offset.leader_epoch,
                metadata: offset.metadata,
                commit_timestamp_ms,
                expire_timestamp_ms,
            },
        );
    }
    touch_memory_producer(state, producer.producer_id);
    Ok(())
}

fn memory_share_member_partitions(
    state: &MemoryState,
    group_id: &str,
    member_id: &str,
) -> Result<HashSet<ShareSessionPartition>, ControlError> {
    let group = state
        .share_groups
        .get(group_id)
        .ok_or_else(|| ControlError::GroupNotFound(group_id.to_owned()))?;
    let member = group
        .members
        .get(member_id)
        .ok_or_else(|| ControlError::GroupMemberNotFound {
            group: group_id.to_owned(),
            member: member_id.to_owned(),
        })?;
    Ok(member
        .assignment
        .iter()
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .copied()
                .map(move |partition| ShareSessionPartition {
                    topic_id: topic.topic_id,
                    partition,
                })
        })
        .collect())
}

fn validate_memory_share_state_key(
    state: &MemoryState,
    key: &ShareStateKey,
) -> Result<(), ControlError> {
    share_state::validate_key(key)?;
    let topic = state
        .topics
        .values()
        .find(|topic| topic.id == key.topic_id)
        .ok_or_else(|| ControlError::TopicNotFound(key.topic_id.to_string()))?;
    if key.partition >= topic.partitions {
        return Err(ControlError::PartitionNotFound {
            topic: topic.name.clone(),
            partition: key.partition,
        });
    }
    Ok(())
}

#[derive(Clone)]
struct MemoryPartition {
    next_offset: i64,
    log_start_offset: i64,
    compaction_last_offset: i64,
    compaction_recheck_at_ms: Option<i64>,
    compaction_lease: Option<(Uuid, i64)>,
    spans: Vec<StoredSpan>,
}

fn memory_partition_watermarks(state: &MemoryState, log: &MemoryPartition) -> PartitionWatermarks {
    let last_stable_offset = log
        .spans
        .iter()
        .filter_map(|span| {
            let transaction_id = span.transaction_id?;
            let transaction = state.transactions.get(&transaction_id)?;
            (transaction.status == TransactionStatus::Ongoing).then_some(span.base_offset)
        })
        .min()
        .unwrap_or(log.next_offset);
    PartitionWatermarks {
        high_watermark: log.next_offset,
        last_stable_offset,
        log_start_offset: log.log_start_offset,
    }
}

impl Default for MemoryPartition {
    fn default() -> Self {
        Self {
            next_offset: 0,
            log_start_offset: 0,
            compaction_last_offset: -1,
            compaction_recheck_at_ms: None,
            compaction_lease: None,
            spans: Vec::new(),
        }
    }
}

#[derive(Clone, Default)]
struct MemoryGroup {
    generation_id: i32,
    protocol_type: String,
    protocol_name: String,
    leader: String,
    members: HashMap<String, GroupMember>,
    assignments: HashMap<String, Vec<u8>>,
    rebalance_id: Option<Uuid>,
    rebalance_pending: bool,
    rebalance_started_at: Option<DateTime<Utc>>,
    rebalance_deadline: Option<DateTime<Utc>>,
    initial_rebalance_deadline: Option<DateTime<Utc>>,
    empty_since: Option<DateTime<Utc>>,
}

impl MemoryGroup {
    fn remove_expired_members(&mut self, now: DateTime<Utc>) -> bool {
        let was_non_empty = !self.members.is_empty();
        let expired = self
            .members
            .iter()
            .filter(|(_, member)| {
                !(self.rebalance_pending && member.joined_rebalance_id == self.rebalance_id)
                    && member.last_heartbeat
                        + chrono::Duration::milliseconds(i64::from(member.session_timeout_ms))
                        <= now
            })
            .map(|(member_id, _)| member_id.clone())
            .collect::<Vec<_>>();
        for member_id in &expired {
            self.members.remove(member_id);
            self.assignments.remove(member_id);
        }
        if self.members.is_empty() && was_non_empty {
            self.empty_since = Some(now);
        } else if !self.members.is_empty() {
            self.empty_since = None;
        }
        !expired.is_empty()
    }

    fn begin_rebalance(&mut self) {
        self.generation_id += 1;
        self.assignments.clear();
        if !self.members.contains_key(&self.leader) {
            self.leader = self.members.keys().min().cloned().unwrap_or_default();
        }
    }

    fn member_id_for_instance(&self, group_instance_id: &str) -> Option<String> {
        self.members
            .values()
            .find(|member| member.group_instance_id.as_deref() == Some(group_instance_id))
            .map(|member| member.member_id.clone())
    }

    fn validate_member_identity(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<(), ControlError> {
        if let Some(group_instance_id) = group_instance_id {
            let expected = self
                .member_id_for_instance(group_instance_id)
                .ok_or_else(|| ControlError::GroupMemberNotFound {
                    group: group_id.to_owned(),
                    member: member_id.to_owned(),
                })?;
            if expected != member_id {
                return Err(ControlError::FencedInstanceId {
                    group: group_id.to_owned(),
                    instance_id: group_instance_id.to_owned(),
                });
            }
        }
        if !self.members.contains_key(member_id) {
            return Err(ControlError::GroupMemberNotFound {
                group: group_id.to_owned(),
                member: member_id.to_owned(),
            });
        }
        Ok(())
    }

    fn apply_protocol(&mut self, protocol_name: &str) {
        self.protocol_name = protocol_name.to_owned();
        for member in self.members.values_mut() {
            groups::select_member_protocol(member, protocol_name);
        }
    }
}

#[derive(Clone)]
struct MemoryProducer {
    epoch: i16,
    transactional_id: Option<String>,
    transaction_timeout_ms: i32,
    two_phase_commit: bool,
    current_transaction_id: Option<Uuid>,
    last_transaction_update_ms: i64,
    sequences: HashMap<PartitionKey, MemoryProducerSequence>,
}

#[derive(Clone)]
struct MemoryProducerSequence {
    epoch: i16,
    last_sequence: i32,
    last_timestamp: i64,
    history_start_offset: i64,
}

#[derive(Clone)]
struct MemoryTransaction {
    transactional_id: String,
    producer: ProducerSession,
    status: TransactionStatus,
    partitions: HashSet<PartitionKey>,
    groups: HashSet<String>,
    offsets: HashMap<(String, PartitionKey), CommittedOffset>,
    started_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    marker_producer_epoch: Option<i16>,
    marker_coordinator_epoch: Option<i32>,
    marker_transaction_version: Option<i8>,
}

impl MemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn set_authorization_failure(&self, enabled: bool) {
        self.authorization_failure.store(
            if enabled { -1 } else { 0 },
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn set_authorization_failure_for(&self, resource_type: Option<AclResourceType>) {
        self.authorization_failure.store(
            resource_type.map_or(0, AclResourceType::code),
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

impl Default for MemoryMetadataStore {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(MemoryState::default())),
            #[cfg(feature = "test-support")]
            authorization_failure: Arc::new(std::sync::atomic::AtomicI8::new(0)),
        }
    }
}

#[async_trait]
impl MetadataStore for MemoryMetadataStore {
    async fn validate_topic_creation(&self, name: &str) -> Result<(), ControlError> {
        topic_names::validate(name)?;
        let state = self.state.read().await;
        if state.topics.contains_key(name) {
            return Err(ControlError::TopicAlreadyExists(name.to_owned()));
        }
        if let Some(existing) =
            topic_names::collision(name, state.topics.keys().map(String::as_str))
        {
            return Err(topic_names::collision_error(name, existing));
        }
        Ok(())
    }

    async fn create_topic(&self, name: &str, partitions: i32) -> Result<TopicInfo, ControlError> {
        self.create_topic_with_config(name, partitions, TopicConfig::default())
            .await
    }

    async fn create_topic_with_config(
        &self,
        name: &str,
        partitions: i32,
        config: TopicConfig,
    ) -> Result<TopicInfo, ControlError> {
        if partitions <= 0 {
            return Err(ControlError::InvalidRequest(
                "partitions must be positive".to_owned(),
            ));
        }
        topic_names::validate(name)?;
        config.validate()?;
        let mut state = self.state.write().await;
        if state.topics.contains_key(name) {
            return Err(ControlError::TopicAlreadyExists(name.to_owned()));
        }
        if let Some(existing) =
            topic_names::collision(name, state.topics.keys().map(String::as_str))
        {
            return Err(topic_names::collision_error(name, existing));
        }
        let info = TopicInfo {
            id: Uuid::new_v4(),
            name: name.to_owned(),
            partitions,
        };
        state.topics.insert(name.to_owned(), info.clone());
        state.topic_configs.insert(name.to_owned(), config);
        for partition in 0..partitions {
            state.partitions.insert(
                PartitionKey::new(name, partition),
                MemoryPartition::default(),
            );
        }
        Ok(info)
    }

    async fn create_partitions(
        &self,
        name: &str,
        new_count: i32,
    ) -> Result<TopicInfo, ControlError> {
        let mut state = self.state.write().await;
        let current = state
            .topics
            .get(name)
            .ok_or_else(|| ControlError::TopicNotFound(name.to_owned()))?
            .partitions;
        if new_count <= current {
            return Err(ControlError::InvalidPartitionCount {
                topic: name.to_owned(),
                current,
                requested: new_count,
            });
        }
        for partition in current..new_count {
            state.partitions.insert(
                PartitionKey::new(name, partition),
                MemoryPartition::default(),
            );
        }
        let topic = state.topics.get_mut(name).expect("topic was checked");
        topic.partitions = new_count;
        Ok(topic.clone())
    }

    async fn delete_topic(&self, name: &str) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        let info = state
            .topics
            .get(name)
            .cloned()
            .ok_or_else(|| ControlError::TopicNotFound(name.to_owned()))?;
        delete_memory_topic(&mut state, &info);
        Ok(())
    }

    async fn delete_topic_by_id(&self, id: Uuid) -> Result<Option<TopicInfo>, ControlError> {
        let mut state = self.state.write().await;
        let Some(info) = state.topics.values().find(|topic| topic.id == id).cloned() else {
            return Ok(None);
        };
        delete_memory_topic(&mut state, &info);
        Ok(Some(info))
    }

    async fn topic(&self, name: &str) -> Result<Option<TopicInfo>, ControlError> {
        Ok(self.state.read().await.topics.get(name).cloned())
    }

    async fn topic_by_id(&self, id: Uuid) -> Result<Option<TopicInfo>, ControlError> {
        Ok(self
            .state
            .read()
            .await
            .topics
            .values()
            .find(|topic| topic.id == id)
            .cloned())
    }

    async fn topics(&self, names: Option<&[String]>) -> Result<Vec<TopicInfo>, ControlError> {
        let state = self.state.read().await;
        let mut result = state
            .topics
            .values()
            .filter(|topic| names.is_none_or(|names| names.iter().any(|name| name == &topic.name)))
            .cloned()
            .collect::<Vec<_>>();
        result.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(result)
    }

    async fn topic_config(&self, name: &str) -> Result<TopicConfig, ControlError> {
        self.state
            .read()
            .await
            .topic_configs
            .get(name)
            .cloned()
            .ok_or_else(|| ControlError::TopicNotFound(name.to_owned()))
    }

    async fn set_topic_config(&self, name: &str, config: TopicConfig) -> Result<(), ControlError> {
        config.validate()?;
        let mut state = self.state.write().await;
        let stored = state
            .topic_configs
            .get_mut(name)
            .ok_or_else(|| ControlError::TopicNotFound(name.to_owned()))?;
        *stored = config;
        Ok(())
    }

    async fn stage_object(&self, object: ObjectRef) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        if state.objects.contains_key(&object.key)
            || state.staged_objects.contains_key(&object.key)
            || state.orphan_gc_claims.contains_key(&object.key)
        {
            return Err(ControlError::InvalidRequest(format!(
                "object {} is already staged or committed",
                object.key
            )));
        }
        state
            .staged_objects
            .insert(object.key, Utc::now().timestamp_millis());
        Ok(())
    }

    async fn commit_object(
        &self,
        object: ObjectRef,
        batches: Vec<BatchDraft>,
    ) -> Result<Vec<StoredSpan>, ControlError> {
        let mut state = self.state.write().await;
        if state.orphan_gc_claims.contains_key(&object.key) {
            return Err(ControlError::InvalidRequest(format!(
                "object {} was claimed for orphan deletion",
                object.key
            )));
        }
        let mut staged = state.clone();
        let committed = commit_memory_object(&mut staged, object, batches)?;
        for span in &committed {
            staged.staged_objects.remove(&span.object_key);
        }
        *state = staged;
        Ok(committed)
    }

    async fn fetch(
        &self,
        partition: &PartitionKey,
        offset: i64,
        max_bytes: usize,
        isolation: FetchIsolation,
    ) -> Result<PartitionFetch, ControlError> {
        let state = self.state.read().await;
        let log =
            state
                .partitions
                .get(partition)
                .ok_or_else(|| ControlError::PartitionNotFound {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                })?;
        let watermarks = memory_partition_watermarks(&state, log);
        if offset < watermarks.log_start_offset || offset > watermarks.high_watermark {
            return Err(ControlError::OffsetOutOfRange {
                partition: partition.clone(),
                offset,
                start: watermarks.log_start_offset,
                end: watermarks.high_watermark,
            });
        }
        let mut bytes = 0usize;
        let mut spans = Vec::new();
        for span in &log.spans {
            if span.last_offset < offset {
                continue;
            }
            if isolation == FetchIsolation::ReadCommitted {
                if span.base_offset >= watermarks.last_stable_offset {
                    break;
                }
                if let Some(transaction_id) = span.transaction_id {
                    let visible =
                        state
                            .transactions
                            .get(&transaction_id)
                            .is_some_and(|transaction| {
                                transaction.status == TransactionStatus::Committed
                            });
                    if !visible {
                        continue;
                    }
                }
            }
            let span_size = (span.byte_end - span.byte_start) as usize;
            if !spans.is_empty() && bytes + span_size > max_bytes {
                break;
            }
            bytes += span_size;
            spans.push(span.clone());
        }
        Ok(PartitionFetch {
            spans,
            high_watermark: watermarks.high_watermark,
            last_stable_offset: watermarks.last_stable_offset,
            log_start_offset: watermarks.log_start_offset,
        })
    }

    async fn partition_watermarks(
        &self,
        partition: &PartitionKey,
    ) -> Result<PartitionWatermarks, ControlError> {
        let state = self.state.read().await;
        let log =
            state
                .partitions
                .get(partition)
                .ok_or_else(|| ControlError::PartitionNotFound {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                })?;
        Ok(memory_partition_watermarks(&state, log))
    }

    async fn list_offset(
        &self,
        partition: &PartitionKey,
        timestamp_ms: i64,
    ) -> Result<i64, ControlError> {
        let state = self.state.read().await;
        let log =
            state
                .partitions
                .get(partition)
                .ok_or_else(|| ControlError::PartitionNotFound {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                })?;
        if timestamp_ms == -2 {
            return Ok(log.log_start_offset);
        }
        if timestamp_ms == -1 {
            return Ok(log.next_offset);
        }
        Ok(log
            .spans
            .iter()
            .find(|span| span.timestamp_ms >= timestamp_ms)
            .map(|span| span.base_offset)
            .unwrap_or(log.next_offset))
    }

    async fn partition_size(&self, partition: &PartitionKey) -> Result<i64, ControlError> {
        let state = self.state.read().await;
        let log =
            state
                .partitions
                .get(partition)
                .ok_or_else(|| ControlError::PartitionNotFound {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                })?;
        Ok(log.spans.iter().fold(0i64, |total, span| {
            let size =
                i64::try_from(span.byte_end.saturating_sub(span.byte_start)).unwrap_or(i64::MAX);
            total.saturating_add(size)
        }))
    }

    async fn partition_retention_sizes(
        &self,
        limit: usize,
    ) -> Result<Vec<PartitionRetentionSize>, ControlError> {
        let state = self.state.read().await;
        let mut observations = state
            .partitions
            .iter()
            .filter_map(|(partition, log)| {
                let config = state.topic_configs.get(&partition.topic)?;
                let size_bytes = log.spans.iter().fold(0i64, |total, span| {
                    let size = i64::try_from(span.byte_end.saturating_sub(span.byte_start))
                        .unwrap_or(i64::MAX);
                    total.saturating_add(size)
                });
                Some(PartitionRetentionSize {
                    partition: partition.clone(),
                    size_bytes,
                    retention_bytes: config.retention_bytes,
                })
            })
            .collect::<Vec<_>>();
        observations.sort_by(|left, right| {
            (&left.partition.topic, left.partition.partition)
                .cmp(&(&right.partition.topic, right.partition.partition))
        });
        observations.truncate(limit);
        Ok(observations)
    }

    async fn describe_producers(
        &self,
        partition: &PartitionKey,
    ) -> Result<Vec<ActiveProducer>, ControlError> {
        let state = self.state.read().await;
        let log =
            state
                .partitions
                .get(partition)
                .ok_or_else(|| ControlError::PartitionNotFound {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                })?;
        let mut active = state
            .producers
            .iter()
            .filter_map(|(producer_id, producer)| {
                let sequence = producer.sequences.get(partition)?;
                let transaction_start = producer
                    .current_transaction_id
                    .filter(|transaction_id| {
                        state
                            .transactions
                            .get(transaction_id)
                            .is_some_and(|transaction| {
                                transaction.status == TransactionStatus::Ongoing
                            })
                    })
                    .and_then(|transaction_id| {
                        log.spans
                            .iter()
                            .filter(|span| {
                                span.transaction_id == Some(transaction_id)
                                    && span
                                        .producer
                                        .is_some_and(|batch| batch.producer_id == *producer_id)
                            })
                            .map(|span| span.base_offset)
                            .min()
                    })
                    .unwrap_or(-1);
                Some(ActiveProducer {
                    producer_id: *producer_id,
                    producer_epoch: sequence.epoch,
                    last_sequence: sequence.last_sequence,
                    last_timestamp: sequence.last_timestamp,
                    current_transaction_start_offset: transaction_start,
                })
            })
            .collect::<Vec<_>>();
        active.sort_unstable_by_key(|producer| producer.producer_id);
        Ok(active)
    }

    async fn expire_producer_sequences(
        &self,
        now_ms: i64,
        expiration_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        if expiration_ms <= 0 {
            return Err(ControlError::InvalidRequest(
                "producer id expiration must be positive".to_owned(),
            ));
        }
        if limit == 0 {
            return Ok(0);
        }
        let cutoff_ms = now_ms.saturating_sub(expiration_ms);
        let mut state = self.state.write().await;
        let mut candidates = Vec::new();
        for (producer_id, producer) in &state.producers {
            for (partition, sequence) in &producer.sequences {
                if sequence.last_timestamp > cutoff_ms {
                    continue;
                }
                let has_pending_transaction = state.partitions.get(partition).is_some_and(|log| {
                    log.spans.iter().any(|span| {
                        span.producer
                            .is_some_and(|batch| batch.producer_id == *producer_id)
                            && span.transaction_id.is_some_and(|transaction_id| {
                                state
                                    .transactions
                                    .get(&transaction_id)
                                    .is_some_and(|transaction| {
                                        transaction.status == TransactionStatus::Ongoing
                                    })
                            })
                    })
                });
                if !has_pending_transaction {
                    candidates.push((sequence.last_timestamp, *producer_id, partition.clone()));
                }
            }
        }
        candidates.sort_unstable_by(|left, right| {
            (&left.0, &left.1, &left.2.topic, &left.2.partition).cmp(&(
                &right.0,
                &right.1,
                &right.2.topic,
                &right.2.partition,
            ))
        });
        candidates.truncate(limit);
        let mut expired = 0u64;
        for (_, producer_id, partition) in candidates {
            if state
                .producers
                .get_mut(&producer_id)
                .is_some_and(|producer| producer.sequences.remove(&partition).is_some())
            {
                expired += 1;
            }
        }
        Ok(expired)
    }

    async fn expire_consumer_offsets(
        &self,
        now_ms: i64,
        retention_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        let mut state = self.state.write().await;
        expire_memory_consumer_offsets(&mut state, now_ms, retention_ms, limit)
    }

    async fn delete_records(
        &self,
        partition: &PartitionKey,
        before_offset: i64,
    ) -> Result<i64, ControlError> {
        let mut state = self.state.write().await;
        let delete_delay_ms = state
            .topic_configs
            .get(&partition.topic)
            .cloned()
            .unwrap_or_default()
            .file_delete_delay_ms;
        let pending_transaction_start = state.partitions.get(partition).and_then(|log| {
            log.spans
                .iter()
                .filter(|span| {
                    span.transaction_id.is_some_and(|transaction_id| {
                        state
                            .transactions
                            .get(&transaction_id)
                            .is_some_and(|transaction| {
                                transaction.status == TransactionStatus::Ongoing
                            })
                    })
                })
                .map(|span| span.base_offset)
                .min()
        });
        let (log_start_offset, object_keys) = {
            let log = state.partitions.get_mut(partition).ok_or_else(|| {
                ControlError::PartitionNotFound {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                }
            })?;
            let target = if before_offset == -1 {
                log.next_offset
            } else {
                before_offset
            };
            if target < 0 || target > log.next_offset {
                return Err(ControlError::OffsetOutOfRange {
                    partition: partition.clone(),
                    offset: before_offset,
                    start: log.log_start_offset,
                    end: log.next_offset,
                });
            }
            let target = pending_transaction_start.map_or(target, |pending| target.min(pending));
            log.log_start_offset = log.log_start_offset.max(target);
            let log_start_offset = log.log_start_offset;
            let object_keys = log
                .spans
                .iter()
                .filter(|span| span.last_offset < log_start_offset)
                .map(|span| span.object_key.clone())
                .collect::<HashSet<_>>();
            log.spans
                .retain(|span| span.last_offset >= log_start_offset);
            (log_start_offset, object_keys)
        };
        reconcile_memory_producer_sequences(&mut state, partition);
        defer_memory_object_delete(
            &mut state,
            object_keys,
            Utc::now().timestamp_millis(),
            delete_delay_ms,
        );
        Ok(log_start_offset)
    }

    async fn commit_offsets(
        &self,
        group_id: &str,
        offsets: Vec<OffsetCommit>,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        commit_memory_offsets(&mut state, group_id, offsets)
    }

    async fn commit_member_offsets(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        generation_or_epoch: i32,
        api_version: i16,
        offsets: Vec<OffsetCommit>,
    ) -> Result<Vec<bool>, ControlError> {
        let mut state = self.state.write().await;
        if let Some(group) = state.consumer_groups.get(group_id) {
            if api_version < 9 {
                return Err(ControlError::UnsupportedVersion(
                    "consumer protocol members require OffsetCommit version 9 or newer".to_owned(),
                ));
            }
            let partitions = offsets
                .iter()
                .map(|offset| offset.partition.clone())
                .collect::<Vec<_>>();
            let validity = consumer_groups::validate_offset_commit(
                group,
                member_id,
                generation_or_epoch,
                &partitions,
            )?;
            let accepted = offsets
                .into_iter()
                .zip(&validity)
                .filter_map(|(offset, accepted)| accepted.then_some(offset))
                .collect();
            commit_memory_offsets(&mut state, group_id, accepted)?;
            return Ok(validity);
        }
        let count = offsets.len();
        validate_memory_group_member(
            &mut state,
            group_id,
            member_id,
            group_instance_id,
            generation_or_epoch,
        )?;
        commit_memory_offsets(&mut state, group_id, offsets)?;
        Ok(vec![true; count])
    }

    async fn fetch_offsets(
        &self,
        group_id: &str,
        partitions: &[PartitionKey],
    ) -> Result<HashMap<PartitionKey, CommittedOffset>, ControlError> {
        let state = self.state.read().await;
        for partition in partitions {
            if !state.partitions.contains_key(partition) {
                return Err(ControlError::PartitionNotFound {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                });
            }
        }
        Ok(partitions
            .iter()
            .filter_map(|partition| {
                state
                    .offsets
                    .get(&(group_id.to_owned(), partition.clone()))
                    .cloned()
                    .map(|offset| (partition.clone(), offset))
            })
            .collect())
    }

    async fn consumer_lags(&self, limit: usize) -> Result<Vec<ConsumerLag>, ControlError> {
        let state = self.state.read().await;
        let mut lags = state
            .offsets
            .iter()
            .filter_map(|((group_id, partition), committed)| {
                let high_watermark = state.partitions.get(partition)?.next_offset;
                Some(ConsumerLag {
                    group_id: group_id.clone(),
                    partition: partition.clone(),
                    committed_offset: committed.offset,
                    high_watermark,
                    lag: (high_watermark - committed.offset).max(0),
                })
            })
            .collect::<Vec<_>>();
        lags.sort_by(|left, right| {
            (
                &left.group_id,
                &left.partition.topic,
                left.partition.partition,
            )
                .cmp(&(
                    &right.group_id,
                    &right.partition.topic,
                    right.partition.partition,
                ))
        });
        lags.truncate(limit);
        Ok(lags)
    }

    async fn delete_offsets(
        &self,
        group_id: &str,
        partitions: &[PartitionKey],
    ) -> Result<HashSet<PartitionKey>, ControlError> {
        let mut state = self.state.write().await;
        let exists = state.groups.contains_key(group_id)
            || state.consumer_groups.contains_key(group_id)
            || state
                .offsets
                .keys()
                .any(|(stored_group, _)| stored_group == group_id);
        if !exists {
            return Err(ControlError::GroupNotFound(group_id.to_owned()));
        }
        for partition in partitions {
            if !state.partitions.contains_key(partition) {
                return Err(ControlError::PartitionNotFound {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                });
            }
        }
        let classic_topics = state
            .groups
            .get(group_id)
            .into_iter()
            .flat_map(|group| group.members.values())
            .flat_map(|member| member.subscribed_topics.iter())
            .cloned()
            .collect::<HashSet<_>>();
        let consumer = state.consumer_groups.get(group_id);
        let blocked = partitions
            .iter()
            .filter(|partition| {
                classic_topics.contains(&partition.topic)
                    || consumer.is_some_and(|group| {
                        group.members.values().any(|member| {
                            member.subscribed_topic_names.contains(&partition.topic)
                                || member
                                    .subscribed_topic_regex
                                    .as_ref()
                                    .is_some_and(|pattern| {
                                        regex::Regex::new(pattern)
                                            .is_ok_and(|regex| regex.is_match(&partition.topic))
                                    })
                        })
                    })
            })
            .cloned()
            .collect::<HashSet<_>>();
        for partition in partitions {
            if !blocked.contains(partition) {
                state
                    .offsets
                    .remove(&(group_id.to_owned(), partition.clone()));
            }
        }
        Ok(blocked)
    }

    async fn join_group(
        &self,
        group_id: &str,
        requested_member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: &[(String, Vec<u8>)],
        client: (&str, &str, &[String], i32),
        api_version: i16,
    ) -> Result<JoinGroupResult, ControlError> {
        let (client_id, client_host, subscribed_topics, session_timeout_ms) = client;
        if group_id.is_empty()
            || protocol_type.is_empty()
            || protocols.is_empty()
            || session_timeout_ms <= 0
        {
            return Err(ControlError::InvalidRequest(
                "group id, protocol type, protocols, and session timeout are required".to_owned(),
            ));
        }
        let mut state = self.state.write().await;
        if state.share_groups.contains_key(group_id) || state.streams_groups.contains_key(group_id)
        {
            return Err(ControlError::GroupProtocolMismatch(group_id.to_owned()));
        }
        let now = Utc::now();
        let replace_consumer_group = empty_consumer_group_for_classic(&state, group_id, now)?;
        state
            .pending_group_members
            .retain(|(pending_group, _), expires_at| {
                pending_group != group_id || *expires_at > now
            });
        if let Some(group) = state.groups.get_mut(group_id)
            && group.remove_expired_members(now)
        {
            if group.members.is_empty() {
                group.protocol_type.clear();
                group.protocol_name.clear();
                group.leader.clear();
            } else {
                group.begin_rebalance();
            }
        }

        let incoming_protocols = groups::protocols(protocols);
        if let Some(group) = state.groups.get(group_id) {
            if !group.protocol_type.is_empty() && group.protocol_type != protocol_type {
                return Err(ControlError::InconsistentGroupProtocol(group_id.to_owned()));
            }
            let mut protocol_sets = group
                .members
                .values()
                .map(|member| member.protocols.as_slice())
                .collect::<Vec<_>>();
            protocol_sets.push(incoming_protocols.as_slice());
            if groups::select_protocol(&protocol_sets).is_none() {
                return Err(ControlError::InconsistentGroupProtocol(group_id.to_owned()));
            }
        }

        if group_instance_id.is_none() && requested_member_id.is_empty() && api_version >= 4 {
            if replace_consumer_group {
                state.consumer_groups.remove(group_id);
            }
            let member_id = format!("{client_id}-{}", Uuid::new_v4());
            state.pending_group_members.insert(
                (group_id.to_owned(), member_id.clone()),
                now + chrono::Duration::milliseconds(i64::from(session_timeout_ms)),
            );
            return Err(ControlError::MemberIdRequired { member_id });
        }

        let mapped_static_member = group_instance_id.and_then(|instance_id| {
            state
                .groups
                .get(group_id)
                .and_then(|group| group.member_id_for_instance(instance_id))
        });
        let existing_requested_member = !requested_member_id.is_empty()
            && state
                .groups
                .get(group_id)
                .is_some_and(|group| group.members.contains_key(requested_member_id));
        let (member_id, replaced_member_id) = match (
            group_instance_id,
            requested_member_id.is_empty(),
            mapped_static_member,
        ) {
            (Some(_), true, existing) => (format!("{client_id}-{}", Uuid::new_v4()), existing),
            (Some(instance_id), false, Some(existing)) if existing != requested_member_id => {
                return Err(ControlError::FencedInstanceId {
                    group: group_id.to_owned(),
                    instance_id: instance_id.to_owned(),
                });
            }
            (Some(_), false, Some(existing)) => (existing, None),
            (Some(_), false, None) => {
                return Err(ControlError::GroupMemberNotFound {
                    group: group_id.to_owned(),
                    member: requested_member_id.to_owned(),
                });
            }
            (None, true, _) => (format!("{client_id}-{}", Uuid::new_v4()), None),
            (None, false, _) if existing_requested_member => (requested_member_id.to_owned(), None),
            (None, false, _) => {
                let pending = state
                    .pending_group_members
                    .remove(&(group_id.to_owned(), requested_member_id.to_owned()));
                if pending.is_none() {
                    return Err(ControlError::GroupMemberNotFound {
                        group: group_id.to_owned(),
                        member: requested_member_id.to_owned(),
                    });
                }
                (requested_member_id.to_owned(), None)
            }
        };

        if replace_consumer_group {
            state.consumer_groups.remove(group_id);
        }
        let group = state.groups.entry(group_id.to_owned()).or_default();
        group.empty_since = None;
        let previous_member = replaced_member_id
            .as_deref()
            .or(existing_requested_member.then_some(member_id.as_str()))
            .and_then(|member_id| group.members.get(member_id))
            .cloned();
        let mut protocol_sets = group
            .members
            .iter()
            .filter(|(existing_member_id, _)| {
                replaced_member_id.as_deref() != Some(existing_member_id.as_str())
                    && existing_member_id.as_str() != member_id
            })
            .map(|(_, member)| member.protocols.as_slice())
            .collect::<Vec<_>>();
        protocol_sets.push(incoming_protocols.as_slice());
        let protocol_name = groups::select_protocol(&protocol_sets)
            .ok_or_else(|| ControlError::InconsistentGroupProtocol(group_id.to_owned()))?;
        let metadata = incoming_protocols
            .iter()
            .find(|protocol| protocol.name == protocol_name)
            .map(|protocol| protocol.metadata.clone())
            .expect("selected protocol is offered by the joining member");
        let protocol_changed =
            !group.protocol_name.is_empty() && group.protocol_name != protocol_name;
        let member_changed = previous_member.as_ref().is_some_and(|member| {
            !member
                .protocols
                .iter()
                .map(|protocol| &protocol.name)
                .eq(incoming_protocols.iter().map(|protocol| &protocol.name))
                || member.subscribed_topics != subscribed_topics
        });
        let new_member = previous_member.is_none();
        let identity_replaced = replaced_member_id.is_some();
        let old_leader = identity_replaced
            .then(|| group.leader.clone())
            .filter(|leader| replaced_member_id.as_deref() == Some(leader.as_str()));

        if let Some(replaced_member_id) = &replaced_member_id {
            group.members.remove(replaced_member_id);
            if let Some(assignment) = group.assignments.remove(replaced_member_id) {
                group.assignments.insert(member_id.clone(), assignment);
            }
            if group.leader == *replaced_member_id {
                group.leader.clone_from(&member_id);
            }
        }
        group.protocol_type = protocol_type.to_owned();
        group.members.insert(
            member_id.clone(),
            GroupMember {
                member_id: member_id.clone(),
                group_instance_id: group_instance_id.map(str::to_owned),
                protocols: incoming_protocols,
                protocol_name: protocol_name.clone(),
                metadata,
                subscribed_topics: subscribed_topics.to_vec(),
                client_id: client_id.to_owned(),
                client_host: client_host.to_owned(),
                rebalance_timeout_ms: session_timeout_ms,
                session_timeout_ms,
                last_heartbeat: now,
                joined_rebalance_id: None,
            },
        );
        group.apply_protocol(&protocol_name);
        let rebalance = (new_member && !identity_replaced) || member_changed || protocol_changed;
        if rebalance {
            group.begin_rebalance();
        } else if group.leader.is_empty() {
            group.leader.clone_from(&member_id);
        }
        let mut members = group.members.values().cloned().collect::<Vec<_>>();
        members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
        Ok(JoinGroupResult {
            generation_id: group.generation_id,
            protocol_type: group.protocol_type.clone(),
            protocol_name,
            leader: old_leader
                .filter(|_| api_version < 9 && !rebalance)
                .unwrap_or_else(|| group.leader.clone()),
            skip_assignment: api_version >= 9
                && identity_replaced
                && !rebalance
                && group.leader == member_id,
            member_id,
            members,
            pending_rebalance: None,
            retry_after_ms: 0,
        })
    }

    async fn begin_join_group(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: &[(String, Vec<u8>)],
        client: (&str, &str, &[String], i32),
        rebalance_timeout_ms: i32,
        initial_rebalance_delay_ms: i32,
        max_size: i32,
        api_version: i16,
    ) -> Result<JoinGroupResult, ControlError> {
        classic_group_barrier::begin_memory(
            self,
            group_id,
            member_id,
            group_instance_id,
            protocol_type,
            protocols,
            client,
            rebalance_timeout_ms,
            initial_rebalance_delay_ms,
            max_size,
            api_version,
        )
        .await
    }

    async fn poll_join_group(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        rebalance_id: Uuid,
        api_version: i16,
    ) -> Result<JoinGroupResult, ControlError> {
        classic_group_barrier::poll_memory(
            self,
            group_id,
            member_id,
            group_instance_id,
            rebalance_id,
            api_version,
        )
        .await
    }

    async fn sync_group(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        assignments: Vec<GroupAssignment>,
    ) -> Result<Vec<u8>, ControlError> {
        let mut state = self.state.write().await;
        let group = state
            .groups
            .get_mut(group_id)
            .ok_or_else(|| ControlError::GroupNotFound(group_id.to_owned()))?;
        if group.rebalance_pending {
            return Err(ControlError::RebalanceInProgress(group_id.to_owned()));
        }
        if group.remove_expired_members(Utc::now()) {
            group.begin_rebalance();
        }
        if group.generation_id != generation_id {
            return Err(ControlError::IllegalGeneration {
                group: group_id.to_owned(),
                expected: group.generation_id,
                actual: generation_id,
            });
        }
        group.validate_member_identity(group_id, member_id, group_instance_id)?;
        for assignment in assignments {
            if group.members.contains_key(&assignment.member_id) {
                group
                    .assignments
                    .insert(assignment.member_id, assignment.assignment);
            }
        }
        Ok(group
            .assignments
            .get(member_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn heartbeat_group(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        let group = state
            .groups
            .get_mut(group_id)
            .ok_or_else(|| ControlError::GroupNotFound(group_id.to_owned()))?;
        if group.rebalance_pending {
            return Err(ControlError::RebalanceInProgress(group_id.to_owned()));
        }
        if group.remove_expired_members(Utc::now()) {
            group.begin_rebalance();
        }
        if group.generation_id != generation_id {
            return Err(ControlError::IllegalGeneration {
                group: group_id.to_owned(),
                expected: group.generation_id,
                actual: generation_id,
            });
        }
        group.validate_member_identity(group_id, member_id, group_instance_id)?;
        let member = group
            .members
            .get_mut(member_id)
            .expect("validated member exists");
        member.last_heartbeat = Utc::now();
        Ok(())
    }

    async fn leave_group(
        &self,
        group_id: &str,
        members: &[GroupMemberIdentity],
    ) -> Result<Vec<LeaveGroupMemberResult>, ControlError> {
        let mut state = self.state.write().await;
        let group = state
            .groups
            .get_mut(group_id)
            .ok_or_else(|| ControlError::GroupNotFound(group_id.to_owned()))?;
        let mut removed = BTreeMap::<String, ()>::new();
        let mut results = Vec::with_capacity(members.len());
        for identity in members {
            let resolved_member = match identity.group_instance_id.as_deref() {
                Some(instance_id) => match group.member_id_for_instance(instance_id) {
                    None => Err(LeaveGroupMemberError::UnknownMemberId),
                    Some(member_id)
                        if !identity.member_id.is_empty() && identity.member_id != member_id =>
                    {
                        Err(LeaveGroupMemberError::FencedInstanceId)
                    }
                    Some(member_id) => Ok(member_id),
                },
                None if identity.member_id.is_empty() => {
                    Err(LeaveGroupMemberError::UnknownMemberId)
                }
                None if group.members.contains_key(&identity.member_id) => {
                    Ok(identity.member_id.clone())
                }
                None => Err(LeaveGroupMemberError::UnknownMemberId),
            };
            let error = match resolved_member {
                Ok(member_id) => {
                    removed.insert(member_id, ());
                    None
                }
                Err(error) => Some(error),
            };
            results.push(LeaveGroupMemberResult {
                identity: identity.clone(),
                error,
            });
        }
        for member_id in removed.keys() {
            group.members.remove(member_id);
            group.assignments.remove(member_id);
        }
        let empty = group.members.is_empty();
        if empty && !removed.is_empty() {
            group.generation_id = group.generation_id.saturating_add(1);
            group.protocol_name.clear();
            group.leader.clear();
            group.assignments.clear();
            group.empty_since = Some(Utc::now());
        } else if !removed.is_empty() {
            if group.rebalance_pending {
                classic_group_barrier::finish_memory_after_membership_change(group);
            } else {
                let protocol_sets = group
                    .members
                    .values()
                    .map(|member| member.protocols.as_slice())
                    .collect::<Vec<_>>();
                let protocol_name = groups::select_protocol(&protocol_sets)
                    .expect("remaining members retain at least one common protocol");
                group.apply_protocol(&protocol_name);
                group.begin_rebalance();
            }
        }
        Ok(results)
    }

    async fn consumer_group_heartbeat(
        &self,
        heartbeat: ConsumerGroupHeartbeat,
    ) -> Result<ConsumerGroupHeartbeatResult, ControlError> {
        let mut state = self.state.write().await;
        if state.share_groups.contains_key(&heartbeat.group_id)
            || state.streams_groups.contains_key(&heartbeat.group_id)
        {
            return Err(ControlError::GroupProtocolMismatch(
                heartbeat.group_id.clone(),
            ));
        }
        let now = Utc::now();
        ensure_classic_group_is_empty_for_consumer(&state, &heartbeat.group_id, now)?;
        let group_id = heartbeat.group_id.clone();
        let current = state.consumer_groups.get(&group_id).cloned();
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        let (group, result) = consumer_groups::heartbeat(current, heartbeat, &topics, now)?;
        state.groups.remove(&group_id);
        state
            .pending_group_members
            .retain(|(pending_group, _), _| pending_group != &group_id);
        state.consumer_groups.insert(group_id, group);
        Ok(result)
    }

    async fn consumer_group_heartbeat_deferred(
        &self,
        heartbeat: ConsumerGroupHeartbeat,
    ) -> Result<GroupHeartbeatOutcome<ConsumerGroupHeartbeatResult>, ControlError> {
        let mut state = self.state.write().await;
        if state.share_groups.contains_key(&heartbeat.group_id)
            || state.streams_groups.contains_key(&heartbeat.group_id)
        {
            return Err(ControlError::GroupProtocolMismatch(
                heartbeat.group_id.clone(),
            ));
        }
        let now = Utc::now();
        ensure_classic_group_is_empty_for_consumer(&state, &heartbeat.group_id, now)?;
        let group_id = heartbeat.group_id.clone();
        let current = state.consumer_groups.get(&group_id).cloned();
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        let (group, result, assignment_task) =
            consumer_groups::heartbeat_deferred(current, heartbeat, &topics, now)?;
        state.groups.remove(&group_id);
        state
            .pending_group_members
            .retain(|(pending_group, _), _| pending_group != &group_id);
        state.consumer_groups.insert(group_id, group);
        Ok(GroupHeartbeatOutcome {
            result,
            assignment_task,
        })
    }

    async fn describe_consumer_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, ConsumerGroupDescription>, ControlError> {
        let state = self.state.read().await;
        Ok(group_ids
            .iter()
            .filter_map(|group_id| {
                state
                    .consumer_groups
                    .get(group_id)
                    .map(|group| (group_id.clone(), consumer_groups::describe(group)))
            })
            .collect())
    }

    async fn streams_group_heartbeat(
        &self,
        heartbeat: StreamsGroupHeartbeat,
    ) -> Result<StreamsGroupHeartbeatResult, ControlError> {
        let mut state = self.state.write().await;
        if state.groups.contains_key(&heartbeat.group_id)
            || state.consumer_groups.contains_key(&heartbeat.group_id)
            || state.share_groups.contains_key(&heartbeat.group_id)
        {
            return Err(ControlError::GroupProtocolMismatch(
                heartbeat.group_id.clone(),
            ));
        }
        let group_id = heartbeat.group_id.clone();
        let current = state.streams_groups.get(&group_id).cloned();
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        let (group, result) = streams_groups::heartbeat(current, heartbeat, &topics, Utc::now())?;
        state.streams_groups.insert(group_id, group);
        Ok(result)
    }

    async fn streams_group_heartbeat_deferred(
        &self,
        heartbeat: StreamsGroupHeartbeat,
    ) -> Result<GroupHeartbeatOutcome<StreamsGroupHeartbeatResult>, ControlError> {
        let mut state = self.state.write().await;
        if state.groups.contains_key(&heartbeat.group_id)
            || state.consumer_groups.contains_key(&heartbeat.group_id)
            || state.share_groups.contains_key(&heartbeat.group_id)
        {
            return Err(ControlError::GroupProtocolMismatch(
                heartbeat.group_id.clone(),
            ));
        }
        let group_id = heartbeat.group_id.clone();
        let current = state.streams_groups.get(&group_id).cloned();
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        let (group, result, assignment_task) =
            streams_groups::heartbeat_deferred(current, heartbeat, &topics, Utc::now())?;
        state.streams_groups.insert(group_id, group);
        Ok(GroupHeartbeatOutcome {
            result,
            assignment_task,
        })
    }

    async fn describe_streams_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, StreamsGroupDescription>, ControlError> {
        let mut state = self.state.write().await;
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        let mut descriptions = HashMap::new();
        for group_id in group_ids {
            if let Some(group) = state.streams_groups.get_mut(group_id) {
                let (_, description) =
                    streams_groups::expire_and_describe(group, &topics, Utc::now())?;
                descriptions.insert(group_id.clone(), description);
            }
        }
        Ok(descriptions)
    }

    async fn share_group_heartbeat(
        &self,
        heartbeat: ShareGroupHeartbeat,
    ) -> Result<ShareGroupHeartbeatResult, ControlError> {
        let mut state = self.state.write().await;
        if state.groups.contains_key(&heartbeat.group_id)
            || state.consumer_groups.contains_key(&heartbeat.group_id)
            || state.streams_groups.contains_key(&heartbeat.group_id)
        {
            return Err(ControlError::GroupProtocolMismatch(
                heartbeat.group_id.clone(),
            ));
        }
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        let group_id = heartbeat.group_id.clone();
        let group = state
            .share_groups
            .entry(group_id.clone())
            .or_insert_with(|| share_groups::ShareGroupState::new(group_id));
        share_groups::apply_heartbeat(group, heartbeat, &topics, Utc::now())
    }

    async fn share_group_heartbeat_deferred(
        &self,
        heartbeat: ShareGroupHeartbeat,
    ) -> Result<GroupHeartbeatOutcome<ShareGroupHeartbeatResult>, ControlError> {
        let mut state = self.state.write().await;
        if state.groups.contains_key(&heartbeat.group_id)
            || state.consumer_groups.contains_key(&heartbeat.group_id)
            || state.streams_groups.contains_key(&heartbeat.group_id)
        {
            return Err(ControlError::GroupProtocolMismatch(
                heartbeat.group_id.clone(),
            ));
        }
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        let group_id = heartbeat.group_id.clone();
        let group = state
            .share_groups
            .entry(group_id.clone())
            .or_insert_with(|| share_groups::ShareGroupState::new(group_id));
        let (result, assignment_task) =
            share_groups::apply_heartbeat_deferred(group, heartbeat, &topics, Utc::now())?;
        Ok(GroupHeartbeatOutcome {
            result,
            assignment_task,
        })
    }

    async fn complete_group_assignment(
        &self,
        task: GroupAssignmentTask,
    ) -> Result<GroupAssignmentCompletion, ControlError> {
        let mut state = self.state.write().await;
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        match task.protocol {
            AssignmentProtocol::Consumer => {
                let Some(group) = state.consumer_groups.get_mut(&task.group_id) else {
                    return Ok(GroupAssignmentCompletion::GroupNotFound);
                };
                consumer_groups::complete_assignment(group, &topics, &task, Utc::now())
            }
            AssignmentProtocol::Share => {
                let Some(group) = state.share_groups.get_mut(&task.group_id) else {
                    return Ok(GroupAssignmentCompletion::GroupNotFound);
                };
                Ok(share_groups::complete_assignment(
                    group,
                    &topics,
                    &task,
                    Utc::now(),
                ))
            }
            AssignmentProtocol::Streams => {
                let Some(group) = state.streams_groups.get_mut(&task.group_id) else {
                    return Ok(GroupAssignmentCompletion::GroupNotFound);
                };
                streams_groups::complete_assignment(group, &topics, &task, Utc::now())
            }
        }
    }

    async fn describe_share_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, ShareGroupDescription>, ControlError> {
        let mut state = self.state.write().await;
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        let mut descriptions = HashMap::new();
        for group_id in group_ids {
            if let Some(group) = state.share_groups.get_mut(group_id) {
                share_groups::expire_members(group, &topics, Utc::now());
                descriptions.insert(group_id.clone(), group.description());
            }
        }
        Ok(descriptions)
    }

    async fn update_share_fetch_session(
        &self,
        update: ShareFetchSessionUpdate,
    ) -> Result<ShareFetchSession, ControlError> {
        let mut state = self.state.write().await;
        let assigned = memory_share_member_partitions(&state, &update.group_id, &update.member_id)?;
        state.share_records.update_session(update, &assigned)
    }

    async fn existing_share_partition_state(
        &self,
        group_id: &str,
        member_id: &str,
        partition: &ShareSessionPartition,
    ) -> Result<Option<SharePartitionState>, ControlError> {
        let mut state = self.state.write().await;
        let assigned = memory_share_member_partitions(&state, group_id, member_id)?;
        if !assigned.contains(partition) {
            return Err(ControlError::InvalidRequest(
                "share partition is not assigned to the member".to_owned(),
            ));
        }
        let topic_name = state
            .topics
            .values()
            .find(|topic| topic.id == partition.topic_id)
            .map(|topic| topic.name.clone())
            .ok_or_else(|| ControlError::TopicNotFound(partition.topic_id.to_string()))?;
        let log_start_offset = state
            .partitions
            .get(&PartitionKey::new(topic_name, partition.partition))
            .ok_or_else(|| ControlError::PartitionNotFound {
                topic: partition.topic_id.to_string(),
                partition: partition.partition,
            })?
            .log_start_offset;
        Ok(state.share_records.existing_partition_state(
            group_id,
            partition.topic_id,
            partition.partition,
            log_start_offset,
        ))
    }

    async fn share_partition_state(
        &self,
        group_id: &str,
        member_id: &str,
        partition: &ShareSessionPartition,
        reset: ShareAutoOffsetReset,
    ) -> Result<SharePartitionState, ControlError> {
        let mut state = self.state.write().await;
        let assigned = memory_share_member_partitions(&state, group_id, member_id)?;
        if !assigned.contains(partition) {
            return Err(ControlError::InvalidRequest(
                "share partition is not assigned to the member".to_owned(),
            ));
        }
        let topic_name = state
            .topics
            .values()
            .find(|topic| topic.id == partition.topic_id)
            .map(|topic| topic.name.clone())
            .ok_or_else(|| ControlError::TopicNotFound(partition.topic_id.to_string()))?;
        let log = state
            .partitions
            .get(&PartitionKey::new(topic_name, partition.partition))
            .ok_or_else(|| ControlError::PartitionNotFound {
                topic: partition.topic_id.to_string(),
                partition: partition.partition,
            })?;
        let (log_start_offset, high_watermark) = (log.log_start_offset, log.next_offset);
        Ok(state.share_records.partition_state(
            group_id,
            partition.topic_id,
            partition.partition,
            log_start_offset,
            high_watermark,
            reset,
        ))
    }

    async fn describe_share_group_offsets(
        &self,
        group_id: &str,
        partitions: Option<&[PartitionKey]>,
    ) -> Result<Vec<SharePartitionOffset>, ControlError> {
        let state = self.state.read().await;
        if !state.share_groups.contains_key(group_id) {
            return Err(ControlError::GroupNotFound(group_id.to_owned()));
        }
        let partitions = match partitions {
            Some(partitions) => partitions.to_vec(),
            None => state
                .share_records
                .offsets(group_id)
                .into_iter()
                .filter_map(|(topic_id, partition, _)| {
                    state
                        .topics
                        .values()
                        .find(|topic| topic.id == topic_id)
                        .map(|topic| PartitionKey::new(&topic.name, partition))
                })
                .collect(),
        };
        partitions
            .into_iter()
            .map(|partition| {
                let topic = state
                    .topics
                    .get(&partition.topic)
                    .ok_or_else(|| ControlError::TopicNotFound(partition.topic.clone()))?;
                let log = state.partitions.get(&partition).ok_or_else(|| {
                    ControlError::PartitionNotFound {
                        topic: partition.topic.clone(),
                        partition: partition.partition,
                    }
                })?;
                let start_offset = state
                    .share_records
                    .offset(group_id, topic.id, partition.partition)
                    .map_or(-1, |offset| offset.start_offset.max(log.log_start_offset));
                let delivery_complete_count = if start_offset < 0 {
                    0
                } else {
                    state.share_records.delivery_complete_count(
                        group_id,
                        topic.id,
                        partition.partition,
                        start_offset,
                        log.next_offset,
                    )
                };
                Ok(SharePartitionOffset {
                    partition,
                    topic_id: topic.id,
                    start_offset,
                    leader_epoch: 0,
                    high_watermark: log.next_offset,
                    delivery_complete_count,
                })
            })
            .collect()
    }

    async fn alter_share_group_offsets(
        &self,
        group_id: &str,
        updates: &[ShareOffsetUpdate],
    ) -> Result<Vec<ShareOffsetUpdateResult>, ControlError> {
        share_records::validate_offset_updates(group_id, updates)?;
        let mut state = self.state.write().await;
        if state.groups.contains_key(group_id)
            || state.consumer_groups.contains_key(group_id)
            || state.streams_groups.contains_key(group_id)
        {
            return Err(ControlError::GroupProtocolMismatch(group_id.to_owned()));
        }
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        if let Some(group) = state.share_groups.get_mut(group_id) {
            share_groups::expire_members(group, &topics, Utc::now());
        }
        if state
            .share_groups
            .get(group_id)
            .is_some_and(|group| !group.members.is_empty())
        {
            return Err(ControlError::NonEmptyGroup(group_id.to_owned()));
        }
        state
            .share_groups
            .entry(group_id.to_owned())
            .or_insert_with(|| share_groups::ShareGroupState::new(group_id));
        let mut results = Vec::with_capacity(updates.len());
        for update in updates {
            let topic = state.topics.get(&update.partition.topic).cloned();
            let valid = topic.as_ref().is_some_and(|topic| {
                update.partition.partition >= 0 && update.partition.partition < topic.partitions
            });
            if valid {
                state.share_records.reset_offset(
                    group_id,
                    topic.as_ref().expect("validated topic").id,
                    update.partition.partition,
                    update.start_offset,
                );
            }
            results.push(ShareOffsetUpdateResult {
                partition: update.partition.clone(),
                topic_id: topic.map(|topic| topic.id),
                updated: valid,
            });
        }
        Ok(results)
    }

    async fn delete_share_group_offsets(
        &self,
        group_id: &str,
        topics: &[String],
    ) -> Result<Vec<ShareOffsetDeleteResult>, ControlError> {
        share_records::validate_offset_topics(group_id, topics)?;
        let mut state = self.state.write().await;
        let topic_metadata = state.topics.values().cloned().collect::<Vec<_>>();
        if let Some(group) = state.share_groups.get_mut(group_id) {
            share_groups::expire_members(group, &topic_metadata, Utc::now());
        }
        let group = state
            .share_groups
            .get(group_id)
            .ok_or_else(|| ControlError::GroupNotFound(group_id.to_owned()))?;
        if !group.members.is_empty() {
            return Err(ControlError::NonEmptyGroup(group_id.to_owned()));
        }
        let mut results = Vec::with_capacity(topics.len());
        for topic_name in topics {
            let topic_id = state.topics.get(topic_name).map(|topic| topic.id);
            let deleted = topic_id.is_some_and(|topic_id| {
                state.share_records.delete_topic_offsets(group_id, topic_id)
            });
            results.push(ShareOffsetDeleteResult {
                topic: topic_name.clone(),
                topic_id,
                deleted,
            });
        }
        Ok(results)
    }

    async fn acquire_share_records(
        &self,
        request: ShareAcquireRequest,
    ) -> Result<Vec<ShareAcquiredRecord>, ControlError> {
        let mut state = self.state.write().await;
        let assigned =
            memory_share_member_partitions(&state, &request.group_id, &request.member_id)?;
        let partition = ShareSessionPartition {
            topic_id: request.topic_id,
            partition: request.partition,
        };
        if !assigned.contains(&partition) {
            return Err(ControlError::InvalidRequest(
                "share partition is not assigned to the member".to_owned(),
            ));
        }
        state.share_records.acquire(request, Utc::now())
    }

    async fn acknowledge_share_records(
        &self,
        request: ShareAcknowledgeRecords,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        let assigned =
            memory_share_member_partitions(&state, &request.group_id, &request.member_id)?;
        let partition = ShareSessionPartition {
            topic_id: request.topic_id,
            partition: request.partition,
        };
        if !assigned.contains(&partition) {
            return Err(ControlError::InvalidRequest(
                "share partition is not assigned to the member".to_owned(),
            ));
        }
        state.share_records.acknowledge(request, Utc::now())
    }

    async fn initialize_share_group_state(
        &self,
        initialization: ShareStateInitialization,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        validate_memory_share_state_key(&state, &initialization.key)?;
        state.share_records.initialize_state(initialization)
    }

    async fn read_share_group_state(
        &self,
        read: ShareStateRead,
    ) -> Result<ShareStateSnapshot, ControlError> {
        let mut state = self.state.write().await;
        validate_memory_share_state_key(&state, &read.key)?;
        state.share_records.read_state(read)
    }

    async fn write_share_group_state(&self, write: ShareStateWrite) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        validate_memory_share_state_key(&state, &write.key)?;
        state.share_records.write_state(write)
    }

    async fn delete_share_group_state(&self, key: &ShareStateKey) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        validate_memory_share_state_key(&state, key)?;
        state.share_records.delete_state(key)
    }

    async fn summarize_share_group_state(
        &self,
        key: &ShareStateKey,
    ) -> Result<Option<ShareStateSummary>, ControlError> {
        let state = self.state.read().await;
        validate_memory_share_state_key(&state, key)?;
        state.share_records.summarize_state(key)
    }

    async fn list_groups(&self) -> Result<Vec<GroupSummary>, ControlError> {
        let mut state = self.state.write().await;
        let mut groups = HashMap::new();
        for (group_id, _) in state.offsets.keys() {
            groups.insert(
                group_id.clone(),
                GroupSummary {
                    group_id: group_id.clone(),
                    protocol_type: String::new(),
                    state: "Empty".to_owned(),
                    group_type: "Classic".to_owned(),
                },
            );
        }
        for (group_id, group) in &state.groups {
            let assignment_count = group
                .members
                .keys()
                .filter(|member_id| group.assignments.contains_key(*member_id))
                .count();
            groups.insert(
                group_id.clone(),
                GroupSummary {
                    group_id: group_id.clone(),
                    protocol_type: group.protocol_type.clone(),
                    state: groups::classic_group_state(
                        group.members.len(),
                        assignment_count,
                        group.rebalance_pending,
                    )
                    .to_owned(),
                    group_type: "Classic".to_owned(),
                },
            );
        }
        for (group_id, group) in &state.consumer_groups {
            let description = consumer_groups::describe(group);
            groups.insert(
                group_id.clone(),
                GroupSummary {
                    group_id: group_id.clone(),
                    protocol_type: "consumer".to_owned(),
                    state: description.state,
                    group_type: "Consumer".to_owned(),
                },
            );
        }
        let topics = state.topics.values().cloned().collect::<Vec<_>>();
        for (group_id, group) in &mut state.streams_groups {
            let (_, description) = streams_groups::expire_and_describe(group, &topics, Utc::now())?;
            groups.insert(
                group_id.clone(),
                GroupSummary {
                    group_id: group_id.clone(),
                    protocol_type: "streams".to_owned(),
                    state: description.state,
                    group_type: "Streams".to_owned(),
                },
            );
        }
        for (group_id, group) in &state.share_groups {
            let description = group.description();
            groups.insert(
                group_id.clone(),
                GroupSummary {
                    group_id: group_id.clone(),
                    protocol_type: "share".to_owned(),
                    state: description.state,
                    group_type: "Share".to_owned(),
                },
            );
        }
        let mut groups = groups.into_values().collect::<Vec<_>>();
        groups.sort_by(|left, right| left.group_id.cmp(&right.group_id));
        Ok(groups)
    }

    async fn describe_classic_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, ClassicGroupDescription>, ControlError> {
        let state = self.state.read().await;
        let mut descriptions = HashMap::new();
        for group_id in group_ids {
            if let Some(group) = state.groups.get(group_id) {
                let assignment_count = group
                    .members
                    .keys()
                    .filter(|member_id| group.assignments.contains_key(*member_id))
                    .count();
                let mut members = group
                    .members
                    .values()
                    .map(|member| ClassicGroupMemberDescription {
                        member_id: member.member_id.clone(),
                        group_instance_id: member.group_instance_id.clone(),
                        client_id: member.client_id.clone(),
                        client_host: member.client_host.clone(),
                        member_metadata: member.metadata.clone(),
                        member_assignment: group
                            .assignments
                            .get(&member.member_id)
                            .cloned()
                            .unwrap_or_default(),
                    })
                    .collect::<Vec<_>>();
                members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
                descriptions.insert(
                    group_id.clone(),
                    ClassicGroupDescription {
                        group_id: group_id.clone(),
                        state: groups::classic_group_state(
                            group.members.len(),
                            assignment_count,
                            group.rebalance_pending,
                        )
                        .to_owned(),
                        generation_id: group.generation_id,
                        protocol_type: group.protocol_type.clone(),
                        protocol_data: group.protocol_name.clone(),
                        members,
                    },
                );
            } else if state
                .offsets
                .keys()
                .any(|(offset_group_id, _)| offset_group_id == group_id)
            {
                descriptions.insert(
                    group_id.clone(),
                    ClassicGroupDescription {
                        group_id: group_id.clone(),
                        state: "Empty".to_owned(),
                        generation_id: 0,
                        protocol_type: String::new(),
                        protocol_data: String::new(),
                        members: Vec::new(),
                    },
                );
            }
        }
        Ok(descriptions)
    }

    async fn delete_group(&self, group_id: &str) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        let active = state
            .groups
            .get(group_id)
            .is_some_and(|group| !group.members.is_empty())
            || state
                .consumer_groups
                .get(group_id)
                .is_some_and(|group| !group.members.is_empty())
            || state
                .streams_groups
                .get(group_id)
                .is_some_and(|group| !group.members.is_empty())
            || state
                .share_groups
                .get(group_id)
                .is_some_and(|group| !group.members.is_empty());
        if active {
            return Err(ControlError::NonEmptyGroup(group_id.to_owned()));
        }
        let had_offsets = state
            .offsets
            .keys()
            .any(|(offset_group_id, _)| offset_group_id == group_id);
        let existed = state.groups.remove(group_id).is_some()
            || state.consumer_groups.remove(group_id).is_some()
            || state.streams_groups.remove(group_id).is_some()
            || state.share_groups.remove(group_id).is_some()
            || had_offsets;
        if !existed {
            return Err(ControlError::GroupNotFound(group_id.to_owned()));
        }
        state
            .offsets
            .retain(|(offset_group_id, _), _| offset_group_id != group_id);
        state.share_records.delete_group(group_id);
        Ok(())
    }

    async fn validate_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        generation_or_epoch: i32,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        validate_memory_group_member(
            &mut state,
            group_id,
            member_id,
            group_instance_id,
            generation_or_epoch,
        )
    }

    async fn init_producer_with_options(
        &self,
        transactional_id: Option<&str>,
        transaction_timeout_ms: i32,
        current: Option<ProducerSession>,
        enable_2_pc: bool,
        keep_prepared_txn: bool,
    ) -> Result<ProducerInitialization, ControlError> {
        let transaction_timeout_ms =
            effective_transaction_timeout(transactional_id, transaction_timeout_ms)?;
        validate_two_phase_options(transactional_id, enable_2_pc, keep_prepared_txn)?;
        let mut state = self.state.write().await;
        if let Some(transactional_id) = transactional_id {
            if let Some(producer_id) = state.transactional_producers.get(transactional_id).copied()
            {
                let (producer_epoch, previous_transaction) = {
                    let producer = state
                        .producers
                        .get(&producer_id)
                        .expect("transactional producer index is consistent");
                    (producer.epoch, producer.current_transaction_id)
                };
                validate_current_producer(producer_id, producer_epoch, current)?;
                let ongoing_transaction = if keep_prepared_txn {
                    preserved_memory_transaction(&state, previous_transaction)?
                } else {
                    if let Some(transaction_id) = previous_transaction
                        && let Some(transaction) = state.transactions.get_mut(&transaction_id)
                    {
                        transaction.status = TransactionStatus::Aborted;
                    }
                    None
                };
                if enable_2_pc
                    && let Some(transaction_id) = previous_transaction
                    && let Some(transaction) = state.transactions.get_mut(&transaction_id)
                    && transaction.status == TransactionStatus::Ongoing
                {
                    transaction.expires_at = None;
                }
                let session = bump_memory_transactional_producer(
                    &mut state,
                    transactional_id,
                    producer_id,
                    transaction_timeout_ms,
                    enable_2_pc,
                    keep_prepared_txn.then_some(previous_transaction).flatten(),
                )?;
                return Ok(ProducerInitialization {
                    producer: session,
                    ongoing_transaction,
                });
            }
            if current.is_some() {
                return Err(ControlError::TransactionNotFound(
                    transactional_id.to_owned(),
                ));
            }
            let session = allocate_memory_producer(
                &mut state,
                Some(transactional_id.to_owned()),
                transaction_timeout_ms,
                enable_2_pc,
            );
            state
                .transactional_producers
                .insert(transactional_id.to_owned(), session.producer_id);
            return Ok(ProducerInitialization {
                producer: session,
                ongoing_transaction: None,
            });
        }

        if let Some(current) = current {
            let producer = state
                .producers
                .get_mut(&current.producer_id)
                .ok_or(ControlError::UnknownProducer(current.producer_id))?;
            if producer.transactional_id.is_some() {
                return Err(ControlError::UnknownProducer(current.producer_id));
            }
            if producer.epoch != current.producer_epoch {
                return Err(ControlError::ProducerFenced {
                    producer_id: current.producer_id,
                    expected_epoch: producer.epoch,
                    actual_epoch: current.producer_epoch,
                });
            }
            if let Some(next_epoch) = producer.epoch.checked_add(1) {
                producer.epoch = next_epoch;
                producer.sequences.clear();
                return Ok(ProducerInitialization {
                    producer: ProducerSession {
                        producer_id: current.producer_id,
                        producer_epoch: next_epoch,
                    },
                    ongoing_transaction: None,
                });
            }
            let session = allocate_memory_producer(&mut state, None, transaction_timeout_ms, false);
            return Ok(ProducerInitialization {
                producer: session,
                ongoing_transaction: None,
            });
        }
        let session = allocate_memory_producer(&mut state, None, transaction_timeout_ms, false);
        Ok(ProducerInitialization {
            producer: session,
            ongoing_transaction: None,
        })
    }

    async fn add_partitions_to_transaction(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        partitions: &[PartitionKey],
        verify_only: bool,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        for partition in partitions {
            if !state.partitions.contains_key(partition) {
                return Err(ControlError::PartitionNotFound {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                });
            }
        }
        if verify_only {
            validate_memory_transactional_producer(&state, transactional_id, producer)?;
            return Ok(());
        }
        let transaction_id =
            memory_transaction_id(&mut state, transactional_id, producer, true, false)?;
        let transaction = state
            .transactions
            .get_mut(&transaction_id)
            .expect("current transaction exists");
        transaction.partitions.extend(partitions.iter().cloned());
        touch_memory_producer(&mut state, producer.producer_id);
        Ok(())
    }

    async fn add_offsets_to_transaction(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        group_id: &str,
    ) -> Result<(), ControlError> {
        if group_id.is_empty() {
            return Err(ControlError::InvalidRequest(
                "group id must not be empty".to_owned(),
            ));
        }
        let mut state = self.state.write().await;
        let transaction_id =
            memory_transaction_id(&mut state, transactional_id, producer, true, false)?;
        state
            .transactions
            .get_mut(&transaction_id)
            .expect("current transaction exists")
            .groups
            .insert(group_id.to_owned());
        touch_memory_producer(&mut state, producer.producer_id);
        Ok(())
    }

    async fn commit_transaction_offsets(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        group_id: &str,
        offsets: Vec<OffsetCommit>,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        commit_memory_transaction_offsets(
            &mut state,
            transactional_id,
            producer,
            group_id,
            false,
            offsets,
        )
    }

    async fn commit_transaction_member_offsets(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        generation_or_epoch: i32,
        add_group: bool,
        offsets: Vec<OffsetCommit>,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        let partitions = offsets
            .iter()
            .map(|offset| offset.partition.clone())
            .collect::<Vec<_>>();
        if let Some(group) = state.consumer_groups.get(group_id) {
            consumer_groups::validate_transaction_offset_commit(
                group,
                member_id,
                group_instance_id,
                generation_or_epoch,
                &partitions,
            )?;
        } else {
            let anonymous_transactional_commit =
                generation_or_epoch == -1 && member_id.is_empty() && group_instance_id.is_none();
            if !anonymous_transactional_commit {
                validate_memory_group_member(
                    &mut state,
                    group_id,
                    member_id,
                    group_instance_id,
                    generation_or_epoch,
                )?;
            }
        }
        commit_memory_transaction_offsets(
            &mut state,
            transactional_id,
            producer,
            group_id,
            add_group,
            offsets,
        )
    }

    async fn end_transaction(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        committed: bool,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        finish_memory_transaction(&mut state, transactional_id, producer, committed, false)?;
        Ok(())
    }

    async fn end_transaction_with_epoch_bump(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        committed: bool,
    ) -> Result<ProducerSession, ControlError> {
        let mut state = self.state.write().await;
        finish_memory_transaction(&mut state, transactional_id, producer, committed, true)
    }

    async fn write_transaction_marker(
        &self,
        producer: ProducerSession,
        partitions: &[PartitionKey],
        committed: bool,
        coordinator_epoch: i32,
        transaction_version: i8,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        write_memory_transaction_marker(
            &mut state,
            producer,
            partitions,
            committed,
            coordinator_epoch,
            transaction_version,
        )
    }

    async fn describe_transactions(
        &self,
        transactional_ids: &[String],
    ) -> Result<HashMap<String, TransactionDescription>, ControlError> {
        let state = self.state.read().await;
        Ok(transactional_ids
            .iter()
            .filter_map(|transactional_id| {
                describe_memory_transaction(&state, transactional_id)
                    .map(|description| (transactional_id.clone(), description))
            })
            .collect())
    }

    async fn list_transactions(
        &self,
        filter: &TransactionFilter,
    ) -> Result<Vec<TransactionDescription>, ControlError> {
        let state = self.state.read().await;
        let descriptions = state
            .transactional_producers
            .keys()
            .filter_map(|transactional_id| describe_memory_transaction(&state, transactional_id))
            .collect();
        filter_transaction_descriptions(descriptions, filter, Utc::now().timestamp_millis())
    }

    async fn transaction_state_counts(&self) -> Result<TransactionStateCounts, ControlError> {
        let state = self.state.read().await;
        let mut counts = TransactionStateCounts::default();
        for transactional_id in state.transactional_producers.keys() {
            if let Some(description) = describe_memory_transaction(&state, transactional_id) {
                counts.record(description.state);
            }
        }
        Ok(counts)
    }

    async fn expire_transactional_ids(
        &self,
        now_ms: i64,
        expiration_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        if expiration_ms <= 0 {
            return Err(ControlError::InvalidRequest(
                "transactional id expiration must be positive".to_owned(),
            ));
        }
        if limit == 0 {
            return Ok(0);
        }
        let cutoff_ms = now_ms.saturating_sub(expiration_ms);
        let mut state = self.state.write().await;
        let mut candidates = state
            .transactional_producers
            .iter()
            .filter_map(|(transactional_id, producer_id)| {
                let producer = state
                    .producers
                    .get(producer_id)
                    .expect("transactional producer index is consistent");
                let ongoing = state.transactions.values().any(|transaction| {
                    transaction.producer.producer_id == *producer_id
                        && transaction.status == TransactionStatus::Ongoing
                });
                (producer.last_transaction_update_ms <= cutoff_ms && !ongoing).then_some((
                    producer.last_transaction_update_ms,
                    *producer_id,
                    transactional_id.clone(),
                ))
            })
            .collect::<Vec<_>>();
        candidates.sort();
        candidates.truncate(limit);
        for (_, producer_id, transactional_id) in &candidates {
            state.transactional_producers.remove(transactional_id);
            let producer = state
                .producers
                .get_mut(producer_id)
                .expect("transactional producer index is consistent");
            producer.transactional_id = None;
            producer.current_transaction_id = None;
            producer.two_phase_commit = false;
            producer.last_transaction_update_ms = now_ms;
        }
        Ok(candidates.len() as u64)
    }

    async fn create_acl(&self, rule: AclRule) -> Result<(), ControlError> {
        rule.validate()?;
        let mut state = self.state.write().await;
        if !state.acl_rules.contains(&rule) {
            state.acl_rules.push(rule);
        }
        Ok(())
    }

    async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclRule>, ControlError> {
        let mut rules = self
            .state
            .read()
            .await
            .acl_rules
            .iter()
            .filter(|rule| filter.matches(rule))
            .cloned()
            .collect::<Vec<_>>();
        rules.sort_by(|left, right| {
            (
                left.resource_type.code(),
                &left.resource_name,
                left.pattern_type.code(),
                &left.principal,
                &left.host,
                left.operation.code(),
                left.permission.code(),
            )
                .cmp(&(
                    right.resource_type.code(),
                    &right.resource_name,
                    right.pattern_type.code(),
                    &right.principal,
                    &right.host,
                    right.operation.code(),
                    right.permission.code(),
                ))
        });
        Ok(rules)
    }

    async fn delete_acls(&self, filters: &[AclFilter]) -> Result<Vec<Vec<AclRule>>, ControlError> {
        let mut state = self.state.write().await;
        let mut results = Vec::with_capacity(filters.len());
        for filter in filters {
            let mut deleted = Vec::new();
            state.acl_rules.retain(|rule| {
                if filter.matches(rule) {
                    deleted.push(rule.clone());
                    false
                } else {
                    true
                }
            });
            results.push(deleted);
        }
        Ok(results)
    }

    async fn scram_credentials(
        &self,
        users: Option<&[String]>,
    ) -> Result<Vec<ScramCredential>, ControlError> {
        let users = users.map(|users| users.iter().collect::<HashSet<_>>());
        let mut credentials = self
            .state
            .read()
            .await
            .scram_credentials
            .values()
            .filter(|credential| {
                users
                    .as_ref()
                    .is_none_or(|users| users.contains(&credential.user))
            })
            .cloned()
            .collect::<Vec<_>>();
        credentials.sort_by(|left, right| {
            (&left.user, left.mechanism).cmp(&(&right.user, right.mechanism))
        });
        Ok(credentials)
    }

    async fn alter_scram_credentials(
        &self,
        alterations: Vec<ScramCredentialAlteration>,
    ) -> Result<HashSet<String>, ControlError> {
        let mut state = self.state.write().await;
        let mut missing = HashSet::new();
        for alteration in alterations {
            match alteration {
                ScramCredentialAlteration::Upsert(credential) => {
                    state
                        .scram_credentials
                        .insert((credential.user.clone(), credential.mechanism), credential);
                }
                ScramCredentialAlteration::Delete { user, mechanism } => {
                    if state
                        .scram_credentials
                        .remove(&(user.clone(), mechanism))
                        .is_none()
                    {
                        missing.insert(user);
                    }
                }
            }
        }
        Ok(missing)
    }

    async fn client_quotas(&self) -> Result<Vec<ClientQuota>, ControlError> {
        let mut quotas = self
            .state
            .read()
            .await
            .client_quotas
            .iter()
            .map(|(entity, values)| ClientQuota {
                entity: entity.clone(),
                values: values.clone(),
            })
            .collect::<Vec<_>>();
        quotas.sort_by(|left, right| left.entity.cmp(&right.entity));
        Ok(quotas)
    }

    async fn alter_client_quotas(
        &self,
        alterations: Vec<ClientQuotaAlteration>,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        for alteration in alterations {
            let values = state
                .client_quotas
                .entry(alteration.entity.clone())
                .or_default();
            for (key, value) in alteration.ops {
                if let Some(value) = value {
                    values.insert(key, value);
                } else {
                    values.remove(&key);
                }
            }
            if values.is_empty() {
                state.client_quotas.remove(&alteration.entity);
            }
        }
        Ok(())
    }

    async fn client_metric_subscriptions(
        &self,
    ) -> Result<Vec<ClientMetricSubscription>, ControlError> {
        Ok(self
            .state
            .read()
            .await
            .client_metric_subscriptions
            .values()
            .cloned()
            .collect())
    }

    async fn client_metric_subscription(
        &self,
        name: &str,
    ) -> Result<Option<ClientMetricSubscription>, ControlError> {
        Ok(self
            .state
            .read()
            .await
            .client_metric_subscriptions
            .get(name)
            .cloned())
    }

    async fn alter_client_metric_subscription(
        &self,
        alteration: ClientMetricConfigAlteration,
        validate_only: bool,
    ) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        let current = state
            .client_metric_subscriptions
            .get(&alteration.name)
            .cloned();
        let proposed = client_metrics::apply_alteration(current, &alteration)?;
        if validate_only {
            return Ok(());
        }
        match proposed {
            Some(subscription) => {
                state
                    .client_metric_subscriptions
                    .insert(alteration.name, subscription);
            }
            None => {
                state.client_metric_subscriptions.remove(&alteration.name);
            }
        }
        Ok(())
    }

    async fn group_config(&self, group_id: &str) -> Result<BTreeMap<String, String>, ControlError> {
        Ok(self
            .state
            .read()
            .await
            .group_configs
            .get(group_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn group_config_ids(&self) -> Result<Vec<String>, ControlError> {
        let mut ids = self
            .state
            .read()
            .await
            .group_configs
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    async fn alter_group_config(
        &self,
        group_id: &str,
        changes: BTreeMap<String, Option<String>>,
        validate_only: bool,
    ) -> Result<(), ControlError> {
        if validate_only {
            return Ok(());
        }
        let mut state = self.state.write().await;
        let config = state.group_configs.entry(group_id.to_owned()).or_default();
        for (key, value) in changes {
            if let Some(value) = value {
                config.insert(key, value);
            } else {
                config.remove(&key);
            }
        }
        if config.is_empty() {
            state.group_configs.remove(group_id);
        }
        Ok(())
    }

    async fn broker_config(&self) -> Result<BTreeMap<String, String>, ControlError> {
        Ok(self.state.read().await.broker_configs.clone())
    }

    async fn alter_broker_config(
        &self,
        changes: BTreeMap<String, Option<String>>,
        validate_only: bool,
    ) -> Result<(), ControlError> {
        if validate_only {
            return Ok(());
        }
        let mut state = self.state.write().await;
        for (key, value) in changes {
            if let Some(value) = value {
                state.broker_configs.insert(key, value);
            } else {
                state.broker_configs.remove(&key);
            }
        }
        Ok(())
    }

    async fn features(&self) -> Result<FeatureMetadata, ControlError> {
        Ok(self.state.read().await.features.clone())
    }

    async fn update_features(
        &self,
        updates: Vec<FeatureLevelUpdate>,
        validate_only: bool,
    ) -> Result<FeatureMetadata, ControlError> {
        let mut state = self.state.write().await;
        let proposed = features::apply_updates(&state.features.finalized, &updates)?;
        if !validate_only && proposed != state.features.finalized {
            state.features.finalized = proposed;
            state.features.epoch += 1;
        }
        Ok(state.features.clone())
    }

    async fn create_delegation_token(&self, token: DelegationToken) -> Result<(), ControlError> {
        let mut state = self.state.write().await;
        if state.delegation_tokens.contains_key(&token.token_id)
            || state
                .delegation_tokens
                .values()
                .any(|existing| existing.hmac == token.hmac)
        {
            return Err(ControlError::InvalidRequest(
                "delegation token id or HMAC already exists".to_owned(),
            ));
        }
        state
            .delegation_tokens
            .insert(token.token_id.clone(), token);
        Ok(())
    }

    async fn delegation_token_by_id(
        &self,
        token_id: &str,
        now_ms: i64,
    ) -> Result<Option<DelegationToken>, ControlError> {
        Ok(self
            .state
            .read()
            .await
            .delegation_tokens
            .get(token_id)
            .filter(|token| !token.is_expired(now_ms))
            .cloned())
    }

    async fn delegation_tokens(&self, now_ms: i64) -> Result<Vec<DelegationToken>, ControlError> {
        let mut tokens = self
            .state
            .read()
            .await
            .delegation_tokens
            .values()
            .filter(|token| !token.is_expired(now_ms))
            .cloned()
            .collect::<Vec<_>>();
        tokens.sort_by(|left, right| left.token_id.cmp(&right.token_id));
        Ok(tokens)
    }

    async fn renew_delegation_token(
        &self,
        hmac: &[u8],
        principal: &str,
        now_ms: i64,
        requested_period_ms: i64,
        default_period_ms: i64,
    ) -> Result<i64, ControlError> {
        let mut state = self.state.write().await;
        let token = state
            .delegation_tokens
            .values_mut()
            .find(|token| token.hmac == hmac)
            .ok_or(ControlError::DelegationTokenNotFound)?;
        if token.is_expired(now_ms) {
            return Err(ControlError::DelegationTokenExpired);
        }
        if !token.owner_or_renewer(principal) {
            return Err(ControlError::DelegationTokenOwnerMismatch);
        }
        let period_ms = if requested_period_ms > 0 {
            requested_period_ms.min(default_period_ms)
        } else {
            default_period_ms
        };
        token.expiry_timestamp_ms = token.max_timestamp_ms.min(now_ms.saturating_add(period_ms));
        Ok(token.expiry_timestamp_ms)
    }

    async fn expire_delegation_token(
        &self,
        hmac: &[u8],
        principal: &str,
        now_ms: i64,
        expiry_period_ms: i64,
    ) -> Result<i64, ControlError> {
        let mut state = self.state.write().await;
        let token_id = state
            .delegation_tokens
            .values()
            .find(|token| token.hmac == hmac)
            .map(|token| token.token_id.clone())
            .ok_or(ControlError::DelegationTokenNotFound)?;
        let token = state
            .delegation_tokens
            .get_mut(&token_id)
            .expect("delegation token found above");
        if !token.owner_or_renewer(principal) {
            return Err(ControlError::DelegationTokenOwnerMismatch);
        }
        if expiry_period_ms < 0 {
            state.delegation_tokens.remove(&token_id);
            return Ok(now_ms);
        }
        if token.is_expired(now_ms) {
            return Err(ControlError::DelegationTokenExpired);
        }
        token.expiry_timestamp_ms = token
            .max_timestamp_ms
            .min(now_ms.saturating_add(expiry_period_ms));
        Ok(token.expiry_timestamp_ms)
    }

    async fn delete_expired_delegation_tokens(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        let mut state = self.state.write().await;
        let expired = state
            .delegation_tokens
            .iter()
            .filter(|(_, token)| token.is_expired(now_ms))
            .take(limit)
            .map(|(token_id, _)| token_id.clone())
            .collect::<Vec<_>>();
        for token_id in &expired {
            state.delegation_tokens.remove(token_id);
        }
        Ok(expired.len() as u64)
    }

    async fn authorize(
        &self,
        principal: &str,
        host: &str,
        resource_type: AclResourceType,
        resource_name: &str,
        operation: AclOperation,
        allow_if_no_acl: bool,
    ) -> Result<bool, ControlError> {
        #[cfg(feature = "test-support")]
        let authorization_failure = self
            .authorization_failure
            .load(std::sync::atomic::Ordering::Relaxed);
        #[cfg(feature = "test-support")]
        if authorization_failure == -1 || authorization_failure == resource_type.code() {
            return Err(ControlError::Database(sqlx::Error::PoolClosed));
        }
        Ok(acls::authorize_rules(
            &self.state.read().await.acl_rules,
            principal,
            host,
            resource_type,
            resource_name,
            operation,
            allow_if_no_acl,
        ))
    }

    async fn authorize_by_resource_type(
        &self,
        principal: &str,
        host: &str,
        resource_type: AclResourceType,
        operation: AclOperation,
        allow_if_no_acl: bool,
    ) -> Result<bool, ControlError> {
        #[cfg(feature = "test-support")]
        let authorization_failure = self
            .authorization_failure
            .load(std::sync::atomic::Ordering::Relaxed);
        #[cfg(feature = "test-support")]
        if authorization_failure == -1 || authorization_failure == resource_type.code() {
            return Err(ControlError::Database(sqlx::Error::PoolClosed));
        }
        Ok(acls::authorize_by_resource_type_rules(
            &self.state.read().await.acl_rules,
            principal,
            host,
            resource_type,
            operation,
            allow_if_no_acl,
        ))
    }

    async fn apply_retention_page(
        &self,
        now_ms: i64,
        object_delete_grace_ms: i64,
        page: RetentionPage<'_>,
    ) -> Result<RetentionResult, ControlError> {
        let mut state = self.state.write().await;
        let ongoing_transactions = state
            .transactions
            .iter()
            .filter_map(|(transaction_id, transaction)| {
                (transaction.status == TransactionStatus::Ongoing).then_some(*transaction_id)
            })
            .collect::<HashSet<_>>();
        let configs = state.topic_configs.clone();
        let partition_limit = page.max_partitions.max(1);
        let span_limit = page.max_spans.max(1);
        let object_limit = page.max_objects.max(1);
        let mut partitions = state
            .partitions
            .keys()
            .filter(|partition| {
                page.start_after_partition.is_none_or(|cursor| {
                    (partition.topic.as_str(), partition.partition)
                        > (cursor.topic.as_str(), cursor.partition)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        partitions.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then_with(|| left.partition.cmp(&right.partition))
        });
        partitions.truncate(partition_limit.saturating_add(1));
        let has_more_partitions = partitions.len() > partition_limit;
        partitions.truncate(partition_limit);
        let selected_partition_count = partitions.len();
        let mut removed_spans = 0u64;
        let mut deferred_objects = Vec::new();
        let mut truncated_partitions = Vec::new();
        let mut processed_partitions = Vec::new();
        for partition in partitions {
            if removed_spans >= span_limit as u64 {
                break;
            }
            processed_partitions.push(partition.clone());
            let Some(config) = configs.get(&partition.topic) else {
                continue;
            };
            if !config.deletes_records() {
                continue;
            }
            let Some(log) = state.partitions.get_mut(&partition) else {
                continue;
            };
            let mut retained_bytes = log
                .spans
                .iter()
                .map(|span| span.byte_end.saturating_sub(span.byte_start))
                .sum::<u64>();
            let mut remove_count = 0usize;
            let remaining_spans =
                span_limit.saturating_sub(usize::try_from(removed_spans).unwrap_or(usize::MAX));
            for span in log.spans.iter().take(remaining_spans) {
                if span
                    .transaction_id
                    .is_some_and(|transaction_id| ongoing_transactions.contains(&transaction_id))
                {
                    break;
                }
                let expired_by_time = config.retention_ms >= 0
                    && span.timestamp_ms <= now_ms.saturating_sub(config.retention_ms);
                let over_size =
                    config.retention_bytes >= 0 && retained_bytes > config.retention_bytes as u64;
                if !expired_by_time && !over_size {
                    break;
                }
                retained_bytes =
                    retained_bytes.saturating_sub(span.byte_end.saturating_sub(span.byte_start));
                remove_count += 1;
            }
            if remove_count == 0 {
                continue;
            }
            removed_spans += remove_count as u64;
            let delete_after = now_ms.saturating_add(config.file_delete_delay_ms);
            deferred_objects.extend(
                log.spans
                    .iter()
                    .take(remove_count)
                    .map(|span| (span.object_key.clone(), delete_after)),
            );
            log.spans.drain(..remove_count);
            log.log_start_offset = log
                .spans
                .first()
                .map_or(log.next_offset, |span| span.base_offset);
            truncated_partitions.push(partition.clone());
        }
        for partition in &truncated_partitions {
            reconcile_memory_producer_sequences(&mut state, partition);
        }
        for (object_key, delete_after) in deferred_objects {
            state
                .object_delete_after
                .entry(object_key)
                .and_modify(|current| *current = (*current).max(delete_after))
                .or_insert(delete_after);
        }

        let referenced_objects = state
            .partitions
            .values()
            .flat_map(|partition| partition.spans.iter())
            .map(|span| span.object_key.clone())
            .collect::<HashSet<_>>();
        let object_keys = state.objects.keys().cloned().collect::<Vec<_>>();
        for object_key in object_keys {
            if referenced_objects.contains(&object_key) {
                state.unreferenced_objects.remove(&object_key);
            } else {
                state
                    .unreferenced_objects
                    .entry(object_key)
                    .or_insert(now_ms);
            }
        }
        let grace_ms = object_delete_grace_ms.max(0);
        let mut deletable_objects = state
            .unreferenced_objects
            .iter()
            .filter_map(|(object_key, unreferenced_at)| {
                let policy_elapsed = state
                    .object_delete_after
                    .get(object_key)
                    .is_none_or(|delete_after| now_ms >= *delete_after);
                (now_ms.saturating_sub(*unreferenced_at) >= grace_ms
                    && policy_elapsed
                    && page
                        .start_after_object
                        .is_none_or(|cursor| object_key.as_str() > cursor))
                .then_some(object_key.clone())
            })
            .collect::<Vec<_>>();
        deletable_objects.sort();
        deletable_objects.truncate(object_limit.saturating_add(1));
        let next_object = (deletable_objects.len() > object_limit)
            .then(|| deletable_objects[object_limit - 1].clone());
        deletable_objects.truncate(object_limit);
        let next_partition =
            if has_more_partitions || processed_partitions.len() < selected_partition_count {
                processed_partitions.last().cloned()
            } else {
                None
            };
        Ok(RetentionResult {
            removed_spans,
            deletable_objects,
            next_partition,
            next_object,
        })
    }

    async fn complete_object_deletion(&self, key: &str) -> Result<bool, ControlError> {
        let mut state = self.state.write().await;
        if !state.unreferenced_objects.contains_key(key)
            || state
                .partitions
                .values()
                .any(|partition| partition.spans.iter().any(|span| span.object_key == key))
        {
            return Ok(false);
        }
        state.unreferenced_objects.remove(key);
        state.object_delete_after.remove(key);
        Ok(state.objects.remove(key).is_some())
    }

    async fn claim_compaction(
        &self,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<CompactionPlan>, ControlError> {
        compaction::claim_memory(&self.state, now_ms, lease_ms).await
    }

    async fn commit_compaction(
        &self,
        plan: &CompactionPlan,
        objects: Vec<CompactedObject>,
        recheck_at_ms: Option<i64>,
        now_ms: i64,
    ) -> Result<bool, ControlError> {
        compaction::commit_memory(&self.state, plan, objects, recheck_at_ms, now_ms).await
    }

    async fn release_compaction(
        &self,
        partition: &PartitionKey,
        lease_id: Uuid,
    ) -> Result<(), ControlError> {
        compaction::release_memory(&self.state, partition, lease_id).await
    }

    async fn abort_expired_transactions(&self) -> Result<u64, ControlError> {
        self.abort_expired_transactions_batch(usize::MAX).await
    }

    async fn abort_expired_transactions_batch(&self, limit: usize) -> Result<u64, ControlError> {
        if limit == 0 {
            return Ok(0);
        }
        let mut state = self.state.write().await;
        let now = Utc::now();
        let mut expired = state
            .transactions
            .iter()
            .filter_map(|(transaction_id, transaction)| {
                (transaction.status == TransactionStatus::Ongoing
                    && transaction
                        .expires_at
                        .is_some_and(|expires_at| expires_at <= now))
                .then_some(*transaction_id)
            })
            .collect::<Vec<_>>();
        expired.sort_unstable();
        expired.truncate(limit);
        let expired = expired.into_iter().collect::<HashSet<_>>();
        for transaction_id in &expired {
            state
                .transactions
                .get_mut(transaction_id)
                .expect("expired transaction exists")
                .status = TransactionStatus::Aborted;
        }
        for producer in state.producers.values_mut() {
            if producer
                .current_transaction_id
                .is_some_and(|transaction_id| expired.contains(&transaction_id))
            {
                producer.current_transaction_id = None;
                producer.last_transaction_update_ms = Utc::now().timestamp_millis();
            }
        }
        Ok(expired.len() as u64)
    }

    async fn claim_stale_objects(
        &self,
        before_ms: i64,
        limit: i64,
    ) -> Result<Vec<String>, ControlError> {
        let mut state = self.state.write().await;
        let mut stale = state
            .staged_objects
            .iter()
            .filter_map(|(key, staged_at)| {
                (*staged_at <= before_ms || state.orphan_gc_claims.contains_key(key))
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        stale.sort_by(|left, right| {
            state
                .orphan_gc_claims
                .get(left)
                .copied()
                .unwrap_or(state.staged_objects[left])
                .cmp(
                    &state
                        .orphan_gc_claims
                        .get(right)
                        .copied()
                        .unwrap_or(state.staged_objects[right]),
                )
                .then_with(|| left.cmp(right))
        });
        stale.truncate(usize::try_from(limit.max(1)).unwrap_or(usize::MAX));
        let claimed_at = Utc::now().timestamp_millis();
        for key in &stale {
            state.orphan_gc_claims.insert(key.clone(), claimed_at);
        }
        Ok(stale)
    }

    async fn complete_stale_object_deletion(&self, key: &str) -> Result<bool, ControlError> {
        let mut state = self.state.write().await;
        if state.orphan_gc_claims.remove(key).is_none() {
            return Ok(false);
        }
        Ok(state.staged_objects.remove(key).is_some())
    }

    async fn object_committed(&self, key: &str) -> Result<bool, ControlError> {
        Ok(self.state.read().await.objects.contains_key(key))
    }

    async fn object_staged(&self, key: &str) -> Result<bool, ControlError> {
        Ok(self.state.read().await.staged_objects.contains_key(key))
    }

    async fn check(&self) -> Result<(), ControlError> {
        Ok(())
    }
}

fn describe_memory_transaction(
    state: &MemoryState,
    transactional_id: &str,
) -> Option<TransactionDescription> {
    let producer_id = state
        .transactional_producers
        .get(transactional_id)
        .copied()?;
    let producer = state.producers.get(&producer_id)?;
    let latest = state
        .transactions
        .values()
        .filter(|transaction| transaction.transactional_id == transactional_id)
        .max_by_key(|transaction| transaction.started_at);
    let (transaction_state, start_time_ms, mut partitions) = latest.map_or_else(
        || (TransactionState::Empty, -1, Vec::new()),
        |transaction| {
            let partitions = if transaction.status == TransactionStatus::Ongoing {
                transaction.partitions.iter().cloned().collect()
            } else {
                Vec::new()
            };
            (
                TransactionState::from(transaction.status),
                transaction.started_at.timestamp_millis(),
                partitions,
            )
        },
    );
    partitions
        .sort_by(|left, right| (&left.topic, left.partition).cmp(&(&right.topic, right.partition)));
    Some(TransactionDescription {
        transactional_id: transactional_id.to_owned(),
        producer: ProducerSession {
            producer_id,
            producer_epoch: producer.epoch,
        },
        state: transaction_state,
        timeout_ms: producer.transaction_timeout_ms,
        start_time_ms,
        partitions,
    })
}

pub(crate) fn filter_transaction_descriptions(
    descriptions: Vec<TransactionDescription>,
    filter: &TransactionFilter,
    now_ms: i64,
) -> Result<Vec<TransactionDescription>, ControlError> {
    let pattern = filter
        .transactional_id_pattern
        .as_deref()
        .filter(|pattern| !pattern.is_empty())
        .map(|pattern| regex::Regex::new(&format!("^(?:{pattern})$")))
        .transpose()
        .map_err(|error| {
            ControlError::InvalidRegularExpression(format!(
                "invalid transactional id pattern: {error}"
            ))
        })?;
    let mut filtered = descriptions
        .into_iter()
        .filter(|description| {
            (filter.state_filters.is_empty()
                || filter
                    .state_filters
                    .iter()
                    .any(|state| state == description.state.kafka_name()))
                && (filter.producer_id_filters.is_empty()
                    || filter
                        .producer_id_filters
                        .contains(&description.producer.producer_id))
                && filter.min_duration_ms.is_none_or(|minimum| {
                    description.state == TransactionState::Ongoing
                        && description.start_time_ms >= 0
                        && now_ms.saturating_sub(description.start_time_ms) >= minimum
                })
                && pattern
                    .as_ref()
                    .is_none_or(|pattern| pattern.is_match(&description.transactional_id))
        })
        .collect::<Vec<_>>();
    filtered.sort_by(|left, right| left.transactional_id.cmp(&right.transactional_id));
    Ok(filtered)
}

fn validate_transaction_timeout(timeout_ms: i32) -> Result<(), ControlError> {
    if timeout_ms <= 0 {
        return Err(ControlError::InvalidTransactionTimeout(timeout_ms));
    }
    Ok(())
}

fn effective_transaction_timeout(
    transactional_id: Option<&str>,
    timeout_ms: i32,
) -> Result<i32, ControlError> {
    if transactional_id.is_none() {
        // Kafka defines this field as irrelevant for idempotent-only producers, while the
        // persisted producer row still requires a positive placeholder.
        return Ok(60_000);
    }
    validate_transaction_timeout(timeout_ms)?;
    Ok(timeout_ms)
}

fn validate_two_phase_options(
    transactional_id: Option<&str>,
    enable_2_pc: bool,
    keep_prepared_txn: bool,
) -> Result<(), ControlError> {
    if transactional_id.is_none() && (enable_2_pc || keep_prepared_txn) {
        return Err(ControlError::InvalidRequest(
            "two-phase commit options require a transactional id".to_owned(),
        ));
    }
    Ok(())
}

fn validate_current_producer(
    producer_id: i64,
    expected_epoch: i16,
    current: Option<ProducerSession>,
) -> Result<(), ControlError> {
    if let Some(current) = current {
        if current.producer_id != producer_id {
            return Err(ControlError::UnknownProducer(current.producer_id));
        }
        if current.producer_epoch != expected_epoch {
            return Err(ControlError::ProducerFenced {
                producer_id,
                expected_epoch,
                actual_epoch: current.producer_epoch,
            });
        }
    }
    Ok(())
}

fn allocate_memory_producer(
    state: &mut MemoryState,
    transactional_id: Option<String>,
    transaction_timeout_ms: i32,
    two_phase_commit: bool,
) -> ProducerSession {
    state.next_producer_id += 1;
    let session = ProducerSession {
        producer_id: state.next_producer_id,
        producer_epoch: 0,
    };
    state.producers.insert(
        session.producer_id,
        MemoryProducer {
            epoch: session.producer_epoch,
            transactional_id,
            transaction_timeout_ms,
            two_phase_commit,
            current_transaction_id: None,
            last_transaction_update_ms: Utc::now().timestamp_millis(),
            sequences: HashMap::new(),
        },
    );
    session
}

fn preserved_memory_transaction(
    state: &MemoryState,
    transaction_id: Option<Uuid>,
) -> Result<Option<ProducerSession>, ControlError> {
    let Some(transaction_id) = transaction_id else {
        return Ok(None);
    };
    let transaction = state.transactions.get(&transaction_id).ok_or_else(|| {
        ControlError::InvalidTransactionState("producer points to a missing transaction".to_owned())
    })?;
    if transaction.status != TransactionStatus::Ongoing {
        return Err(ControlError::InvalidTransactionState(
            "transaction is no longer ongoing".to_owned(),
        ));
    }
    Ok(Some(transaction.producer))
}

fn bump_memory_transactional_producer(
    state: &mut MemoryState,
    transactional_id: &str,
    producer_id: i64,
    transaction_timeout_ms: i32,
    two_phase_commit: bool,
    current_transaction_id: Option<Uuid>,
) -> Result<ProducerSession, ControlError> {
    let producer = state
        .producers
        .get_mut(&producer_id)
        .expect("transactional producer index is consistent");
    if let Some(next_epoch) = producer.epoch.checked_add(1) {
        producer.epoch = next_epoch;
        producer.transaction_timeout_ms = transaction_timeout_ms;
        producer.two_phase_commit = two_phase_commit;
        producer.current_transaction_id = current_transaction_id;
        producer.last_transaction_update_ms = Utc::now().timestamp_millis();
        producer.sequences.clear();
        return Ok(ProducerSession {
            producer_id,
            producer_epoch: next_epoch,
        });
    }

    producer.transactional_id = None;
    producer.current_transaction_id = None;
    producer.last_transaction_update_ms = Utc::now().timestamp_millis();
    let session = allocate_memory_producer(
        state,
        Some(transactional_id.to_owned()),
        transaction_timeout_ms,
        two_phase_commit,
    );
    state
        .producers
        .get_mut(&session.producer_id)
        .expect("new producer exists")
        .current_transaction_id = current_transaction_id;
    state
        .transactional_producers
        .insert(transactional_id.to_owned(), session.producer_id);
    Ok(session)
}

fn validate_memory_transactional_producer(
    state: &MemoryState,
    transactional_id: &str,
    producer: ProducerSession,
) -> Result<(), ControlError> {
    let indexed_producer = state
        .transactional_producers
        .get(transactional_id)
        .copied()
        .ok_or_else(|| ControlError::TransactionNotFound(transactional_id.to_owned()))?;
    if indexed_producer != producer.producer_id {
        return Err(ControlError::UnknownProducer(producer.producer_id));
    }
    let stored = state
        .producers
        .get(&producer.producer_id)
        .ok_or(ControlError::UnknownProducer(producer.producer_id))?;
    if stored.epoch != producer.producer_epoch {
        return Err(ControlError::ProducerFenced {
            producer_id: producer.producer_id,
            expected_epoch: stored.epoch,
            actual_epoch: producer.producer_epoch,
        });
    }
    Ok(())
}

fn memory_transaction_id(
    state: &mut MemoryState,
    transactional_id: &str,
    producer: ProducerSession,
    create: bool,
    allow_prepared_recovery: bool,
) -> Result<Uuid, ControlError> {
    validate_memory_transactional_producer(state, transactional_id, producer)?;
    let current_transaction_id = state
        .producers
        .get(&producer.producer_id)
        .expect("validated producer exists")
        .current_transaction_id;
    if let Some(transaction_id) = current_transaction_id {
        let transaction = state.transactions.get(&transaction_id).ok_or_else(|| {
            ControlError::InvalidTransactionState(
                "producer points to a missing transaction".to_owned(),
            )
        })?;
        if transaction.status == TransactionStatus::Ongoing {
            if !allow_prepared_recovery && transaction.producer != producer {
                return Err(ControlError::InvalidTransactionState(
                    "prepared transaction can only be completed".to_owned(),
                ));
            }
            return Ok(transaction_id);
        }
        if !create {
            return Err(ControlError::InvalidTransactionState(
                "transaction is no longer ongoing".to_owned(),
            ));
        }
        state
            .producers
            .get_mut(&producer.producer_id)
            .expect("validated producer exists")
            .current_transaction_id = None;
    }
    if !create {
        return Err(ControlError::InvalidTransactionState(
            "transaction has not started".to_owned(),
        ));
    }
    let stored = state
        .producers
        .get(&producer.producer_id)
        .expect("validated producer exists");
    let timeout_ms = stored.transaction_timeout_ms;
    let two_phase_commit = stored.two_phase_commit;
    let transaction_id = Uuid::new_v4();
    let started_at = Utc::now();
    state.transactions.insert(
        transaction_id,
        MemoryTransaction {
            transactional_id: transactional_id.to_owned(),
            producer,
            status: TransactionStatus::Ongoing,
            partitions: HashSet::new(),
            groups: HashSet::new(),
            offsets: HashMap::new(),
            started_at,
            expires_at: (!two_phase_commit)
                .then(|| started_at + chrono::Duration::milliseconds(i64::from(timeout_ms))),
            marker_producer_epoch: None,
            marker_coordinator_epoch: None,
            marker_transaction_version: None,
        },
    );
    state
        .producers
        .get_mut(&producer.producer_id)
        .expect("validated producer exists")
        .current_transaction_id = Some(transaction_id);
    touch_memory_producer(state, producer.producer_id);
    Ok(transaction_id)
}

fn finish_memory_transaction(
    state: &mut MemoryState,
    transactional_id: &str,
    producer: ProducerSession,
    committed: bool,
    bump_epoch: bool,
) -> Result<ProducerSession, ControlError> {
    let target = if committed {
        TransactionStatus::Committed
    } else {
        TransactionStatus::Aborted
    };
    if bump_epoch {
        let current_producer_id = state
            .transactional_producers
            .get(transactional_id)
            .copied()
            .ok_or_else(|| ControlError::TransactionNotFound(transactional_id.to_owned()))?;
        let current = state
            .producers
            .get(&current_producer_id)
            .ok_or(ControlError::UnknownProducer(current_producer_id))?;
        if let Some(transaction_id) = current.current_transaction_id
            && let Some(transaction) = state.transactions.get(&transaction_id)
            && transaction.status != TransactionStatus::Ongoing
            && transaction.producer == producer
        {
            if transaction.status != target {
                return Err(ControlError::InvalidTransactionState(
                    "the completed transaction has the opposite result".to_owned(),
                ));
            }
            return Ok(ProducerSession {
                producer_id: current_producer_id,
                producer_epoch: current.epoch,
            });
        }
    }

    let transaction_id = memory_transaction_id(
        state,
        transactional_id,
        producer,
        bump_epoch && !committed,
        true,
    )?;
    let offsets = {
        let transaction = state
            .transactions
            .get_mut(&transaction_id)
            .expect("current transaction exists");
        transaction.status = target;
        if bump_epoch {
            transaction.producer = producer;
        }
        committed.then(|| transaction.offsets.clone())
    };
    if let Some(offsets) = offsets {
        state.offsets.extend(offsets);
    }
    if !bump_epoch {
        let stored = state
            .producers
            .get_mut(&producer.producer_id)
            .expect("validated producer exists");
        stored.current_transaction_id = None;
        stored.last_transaction_update_ms = Utc::now().timestamp_millis();
        return Ok(producer);
    }

    let producer_config = state
        .producers
        .get(&producer.producer_id)
        .expect("validated producer exists");
    let timeout_ms = producer_config.transaction_timeout_ms;
    let two_phase_commit = producer_config.two_phase_commit;
    bump_memory_transactional_producer(
        state,
        transactional_id,
        producer.producer_id,
        timeout_ms,
        two_phase_commit,
        Some(transaction_id),
    )
}

fn write_memory_transaction_marker(
    state: &mut MemoryState,
    producer: ProducerSession,
    partitions: &[PartitionKey],
    committed: bool,
    coordinator_epoch: i32,
    transaction_version: i8,
) -> Result<(), ControlError> {
    if partitions.is_empty() {
        return Err(ControlError::InvalidRequest(
            "transaction marker must contain at least one partition".to_owned(),
        ));
    }
    let transaction_id = state
        .transactions
        .iter()
        .filter(|(_, transaction)| transaction.producer.producer_id == producer.producer_id)
        .max_by_key(|(_, transaction)| {
            (
                transaction.status == TransactionStatus::Ongoing,
                transaction.started_at,
            )
        })
        .map(|(transaction_id, _)| *transaction_id)
        .ok_or(ControlError::UnknownProducer(producer.producer_id))?;
    let target = if committed {
        TransactionStatus::Committed
    } else {
        TransactionStatus::Aborted
    };
    let transaction = state
        .transactions
        .get(&transaction_id)
        .expect("selected transaction exists");
    let current_producer_epoch = transaction
        .marker_producer_epoch
        .unwrap_or(transaction.producer.producer_epoch);
    let was_ongoing = transaction.status == TransactionStatus::Ongoing;
    let completed_marker_retry = transaction.status != TransactionStatus::Ongoing
        && transaction.marker_producer_epoch == Some(producer.producer_epoch);
    let invalid_producer_epoch = if transaction_version >= 2 {
        producer.producer_epoch <= current_producer_epoch && !completed_marker_retry
    } else {
        producer.producer_epoch < current_producer_epoch
    };
    if invalid_producer_epoch {
        return Err(ControlError::ProducerFenced {
            producer_id: producer.producer_id,
            expected_epoch: current_producer_epoch,
            actual_epoch: producer.producer_epoch,
        });
    }
    if let Some(current_epoch) = state
        .transactions
        .values()
        .filter(|transaction| transaction.producer.producer_id == producer.producer_id)
        .filter_map(|transaction| transaction.marker_coordinator_epoch)
        .max()
        && coordinator_epoch < current_epoch
    {
        return Err(ControlError::TransactionCoordinatorFenced {
            producer_id: producer.producer_id,
            current_epoch,
            requested_epoch: coordinator_epoch,
        });
    }
    let offsets = {
        let transaction = state
            .transactions
            .get_mut(&transaction_id)
            .expect("selected transaction exists");
        if transaction
            .partitions
            .iter()
            .any(|required| !partitions.contains(required))
        {
            return Err(ControlError::InvalidTransactionState(
                "transaction marker does not cover every registered partition".to_owned(),
            ));
        }
        if transaction.status != TransactionStatus::Ongoing {
            if transaction.status != target {
                return Err(ControlError::InvalidTransactionState(
                    "the completed transaction has the opposite result".to_owned(),
                ));
            }
            transaction.marker_producer_epoch = Some(producer.producer_epoch);
            transaction.marker_coordinator_epoch = Some(coordinator_epoch);
            transaction.marker_transaction_version = Some(transaction_version);
            None
        } else {
            transaction.status = target;
            transaction.marker_producer_epoch = Some(producer.producer_epoch);
            transaction.marker_coordinator_epoch = Some(coordinator_epoch);
            transaction.marker_transaction_version = Some(transaction_version);
            committed.then(|| transaction.offsets.clone())
        }
    };
    if let Some(offsets) = offsets {
        state.offsets.extend(offsets);
    }
    if let Some(stored) = state.producers.get_mut(&producer.producer_id) {
        stored.epoch = stored.epoch.max(producer.producer_epoch);
        if was_ongoing && stored.current_transaction_id == Some(transaction_id) {
            stored.current_transaction_id = None;
        }
        stored.last_transaction_update_ms = Utc::now().timestamp_millis();
    }
    Ok(())
}

fn touch_memory_producer(state: &mut MemoryState, producer_id: i64) {
    state
        .producers
        .get_mut(&producer_id)
        .expect("validated producer exists")
        .last_transaction_update_ms = Utc::now().timestamp_millis();
}

fn commit_memory_object(
    state: &mut MemoryState,
    object: ObjectRef,
    batches: Vec<BatchDraft>,
) -> Result<Vec<StoredSpan>, ControlError> {
    if batches.is_empty() {
        return Err(ControlError::InvalidRequest(
            "an object must contain at least one batch".to_owned(),
        ));
    }
    if state.objects.contains_key(&object.key) {
        return Err(ControlError::InvalidRequest(format!(
            "object {} is already committed",
            object.key
        )));
    }
    let mut indexed_batches = batches.into_iter().enumerate().collect::<Vec<_>>();
    indexed_batches.sort_by(|(left_index, left), (right_index, right)| {
        (&left.partition.topic, left.partition.partition, left_index).cmp(&(
            &right.partition.topic,
            right.partition.partition,
            right_index,
        ))
    });
    let mut committed = vec![None; indexed_batches.len()];
    let mut new_spans = 0usize;
    for (batch_index, batch) in indexed_batches {
        validate_batch_bounds(&batch)?;
        let transaction_id = validate_memory_produce(state, &batch)?;
        let mut previous_history_start_offset = None;
        if let Some(producer) = batch.producer {
            let stored = state
                .producers
                .get(&producer.producer_id)
                .expect("producer was validated");
            let sequence = stored
                .sequences
                .get(&batch.partition)
                .filter(|sequence| sequence.epoch == producer.producer_epoch)
                .cloned();
            previous_history_start_offset = sequence
                .as_ref()
                .map(|sequence| sequence.history_start_offset);
            let supports_epoch_bump = stored.two_phase_commit && batch.transactional_id.is_some();
            let partition = state.partitions.get(&batch.partition).ok_or_else(|| {
                ControlError::PartitionNotFound {
                    topic: batch.partition.topic.clone(),
                    partition: batch.partition.partition,
                }
            })?;
            if let Some(sequence) = sequence.as_ref() {
                if let Some(existing) = partition
                    .spans
                    .iter()
                    .rev()
                    .filter(|span| {
                        span.base_offset >= sequence.history_start_offset
                            && span.producer.is_some_and(|existing| {
                                existing.producer_id == producer.producer_id
                                    && existing.producer_epoch == producer.producer_epoch
                            })
                    })
                    .take(PRODUCER_BATCH_HISTORY_LIMIT)
                    .find(|span| {
                        span.producer.is_some_and(|existing| {
                            existing.first_sequence == producer.first_sequence
                                && existing.last_sequence == producer.last_sequence
                        })
                    })
                {
                    committed[batch_index] = Some(existing.clone());
                    continue;
                }
            }
            if let Some(sequence) = sequence {
                let expected = increment_producer_sequence(sequence.last_sequence, 1);
                if producer.first_sequence != expected {
                    return Err(ControlError::OutOfOrderSequence {
                        producer_id: producer.producer_id,
                        partition: batch.partition,
                        expected,
                        actual: producer.first_sequence,
                    });
                }
            } else if supports_epoch_bump && producer.first_sequence != 0 {
                return Err(ControlError::OutOfOrderSequence {
                    producer_id: producer.producer_id,
                    partition: batch.partition,
                    expected: 0,
                    actual: producer.first_sequence,
                });
            }
        }

        let partition = state.partitions.get_mut(&batch.partition).ok_or_else(|| {
            ControlError::PartitionNotFound {
                topic: batch.partition.topic.clone(),
                partition: batch.partition.partition,
            }
        })?;
        let base_offset = partition.next_offset;
        let last_offset = base_offset + i64::from(batch.record_count) - 1;
        partition.next_offset = last_offset + 1;
        let span = StoredSpan {
            partition: batch.partition.clone(),
            object_key: object.key.clone(),
            byte_start: batch.byte_start,
            byte_end: batch.byte_end,
            base_offset,
            last_offset,
            record_count: batch.record_count,
            timestamp_ms: batch.timestamp_ms,
            integrity: SpanIntegrity::from_checksum(batch.checksum),
            producer: batch.producer,
            transaction_id,
            offsets_preserved: false,
        };
        partition.spans.push(span.clone());
        if let Some(producer) = batch.producer {
            let history_start_offset = previous_history_start_offset.map_or(base_offset, |floor| {
                partition
                    .spans
                    .iter()
                    .rev()
                    .filter(|span| {
                        span.base_offset >= floor
                            && span.producer.is_some_and(|existing| {
                                existing.producer_id == producer.producer_id
                                    && existing.producer_epoch == producer.producer_epoch
                            })
                    })
                    .take(PRODUCER_BATCH_HISTORY_LIMIT)
                    .map(|span| span.base_offset)
                    .min()
                    .unwrap_or(base_offset)
            });
            state
                .producers
                .get_mut(&producer.producer_id)
                .expect("producer was validated")
                .sequences
                .insert(
                    batch.partition,
                    MemoryProducerSequence {
                        epoch: producer.producer_epoch,
                        last_sequence: producer.last_sequence,
                        last_timestamp: batch.timestamp_ms,
                        history_start_offset,
                    },
                );
        }
        committed[batch_index] = Some(span);
        new_spans += 1;
    }
    if new_spans > 0 {
        state.unreferenced_objects.remove(&object.key);
        state.object_delete_after.remove(&object.key);
        state.objects.insert(object.key.clone(), object);
    }
    Ok(committed
        .into_iter()
        .map(|span| span.expect("every validated batch returns a span"))
        .collect())
}

fn reconcile_memory_producer_sequences(state: &mut MemoryState, partition: &PartitionKey) {
    let Some(log) = state.partitions.get(partition) else {
        return;
    };
    let changes = state
        .producers
        .iter()
        .filter_map(|(producer_id, producer)| {
            let sequence = producer.sequences.get(partition)?;
            let mut recent = log
                .spans
                .iter()
                .rev()
                .filter(|span| {
                    span.producer.is_some_and(|batch| {
                        batch.producer_id == *producer_id && batch.producer_epoch == sequence.epoch
                    })
                })
                .take(PRODUCER_BATCH_HISTORY_LIMIT);
            let replacement = recent.next().map(|latest| {
                let batch = latest
                    .producer
                    .expect("producer span matching requires producer metadata");
                let history_start_offset = recent.fold(latest.base_offset, |start, span| {
                    start.min(span.base_offset)
                });
                MemoryProducerSequence {
                    epoch: batch.producer_epoch,
                    last_sequence: batch.last_sequence,
                    last_timestamp: latest.timestamp_ms,
                    history_start_offset,
                }
            });
            Some((*producer_id, replacement))
        })
        .collect::<Vec<_>>();
    for (producer_id, replacement) in changes {
        let sequences = &mut state
            .producers
            .get_mut(&producer_id)
            .expect("producer state was collected above")
            .sequences;
        if let Some(replacement) = replacement {
            sequences.insert(partition.clone(), replacement);
        } else {
            sequences.remove(partition);
        }
    }
}

fn delete_memory_topic(state: &mut MemoryState, info: &TopicInfo) {
    let now_ms = Utc::now().timestamp_millis();
    let delete_delay_ms = state
        .topic_configs
        .get(&info.name)
        .map_or(TopicConfig::default().file_delete_delay_ms, |config| {
            config.file_delete_delay_ms
        });
    let object_keys = (0..info.partitions)
        .filter_map(|partition| {
            state
                .partitions
                .get(&PartitionKey::new(&info.name, partition))
        })
        .flat_map(|partition| partition.spans.iter())
        .map(|span| span.object_key.clone())
        .collect::<HashSet<_>>();
    state.topics.remove(&info.name);
    state.topic_configs.remove(&info.name);
    for partition in 0..info.partitions {
        state
            .partitions
            .remove(&PartitionKey::new(&info.name, partition));
    }
    defer_memory_object_delete(state, object_keys, now_ms, delete_delay_ms);
}

fn defer_memory_object_delete(
    state: &mut MemoryState,
    object_keys: impl IntoIterator<Item = String>,
    now_ms: i64,
    delay_ms: i64,
) {
    let delete_after = now_ms.saturating_add(delay_ms.max(0));
    for object_key in object_keys {
        state
            .object_delete_after
            .entry(object_key)
            .and_modify(|current| *current = (*current).max(delete_after))
            .or_insert(delete_after);
    }
}

fn validate_batch_bounds(batch: &BatchDraft) -> Result<(), ControlError> {
    if batch.record_count <= 0 || batch.byte_end < batch.byte_start {
        return Err(ControlError::InvalidRequest(
            "batch has invalid byte or record bounds".to_owned(),
        ));
    }
    Ok(())
}

fn validate_memory_produce(
    state: &MemoryState,
    batch: &BatchDraft,
) -> Result<Option<Uuid>, ControlError> {
    let Some(producer) = batch.producer else {
        if batch.transactional_id.is_some() {
            return Err(ControlError::InvalidTransactionState(
                "transactional records require a producer id".to_owned(),
            ));
        }
        return Ok(None);
    };
    let stored = state
        .producers
        .get(&producer.producer_id)
        .ok_or(ControlError::UnknownProducer(producer.producer_id))?;
    if stored.epoch != producer.producer_epoch {
        return Err(ControlError::ProducerFenced {
            producer_id: producer.producer_id,
            expected_epoch: stored.epoch,
            actual_epoch: producer.producer_epoch,
        });
    }
    match (&batch.transactional_id, &stored.transactional_id) {
        (None, None) => Ok(None),
        (Some(requested), Some(owned)) if requested == owned => {
            let transaction_id = stored.current_transaction_id.ok_or_else(|| {
                ControlError::InvalidTransactionState("transaction has not started".to_owned())
            })?;
            let transaction = state.transactions.get(&transaction_id).ok_or_else(|| {
                ControlError::InvalidTransactionState("transaction does not exist".to_owned())
            })?;
            if transaction.status != TransactionStatus::Ongoing
                || transaction.transactional_id != *requested
                || transaction.producer
                    != (ProducerSession {
                        producer_id: producer.producer_id,
                        producer_epoch: producer.producer_epoch,
                    })
                || (batch.verify_transaction_partition
                    && !transaction.partitions.contains(&batch.partition))
            {
                return Err(ControlError::InvalidTransactionState(format!(
                    "partition {}-{} was not added to the active transaction",
                    batch.partition.topic, batch.partition.partition
                )));
            }
            Ok(Some(transaction_id))
        }
        _ => Err(ControlError::InvalidTransactionState(
            "producer and transactional id do not match".to_owned(),
        )),
    }
}

#[derive(Clone)]
pub struct PostgresMetadataStore {
    pool: PgPool,
}

impl fmt::Debug for PostgresMetadataStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresMetadataStore")
            .finish_non_exhaustive()
    }
}

impl PostgresMetadataStore {
    pub async fn connect(database_url: &str) -> Result<Self, ControlError> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), ControlError> {
        sqlx::migrate!("../../migrations").run(&self.pool).await?;
        Ok(())
    }
}

#[async_trait]
impl MetadataStore for PostgresMetadataStore {
    async fn validate_topic_creation(&self, name: &str) -> Result<(), ControlError> {
        postgres_topics::validate_topic_creation(&self.pool, name).await
    }

    async fn create_topic(&self, name: &str, partitions: i32) -> Result<TopicInfo, ControlError> {
        self.create_topic_with_config(name, partitions, TopicConfig::default())
            .await
    }

    async fn create_topic_with_config(
        &self,
        name: &str,
        partitions: i32,
        config: TopicConfig,
    ) -> Result<TopicInfo, ControlError> {
        postgres_topics::create_topic(&self.pool, name, partitions, config).await
    }

    async fn create_partitions(
        &self,
        name: &str,
        new_count: i32,
    ) -> Result<TopicInfo, ControlError> {
        postgres_topics::create_partitions(&self.pool, name, new_count).await
    }

    async fn delete_topic(&self, name: &str) -> Result<(), ControlError> {
        postgres_topics::delete_topic(&self.pool, name).await
    }

    async fn delete_topic_by_id(&self, id: Uuid) -> Result<Option<TopicInfo>, ControlError> {
        postgres_topics::delete_topic_by_id(&self.pool, id).await
    }

    async fn topic(&self, name: &str) -> Result<Option<TopicInfo>, ControlError> {
        let row = sqlx::query("SELECT id, name, partition_count FROM topics WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|row| TopicInfo {
            id: row.get("id"),
            name: row.get("name"),
            partitions: row.get("partition_count"),
        }))
    }

    async fn topic_by_id(&self, id: Uuid) -> Result<Option<TopicInfo>, ControlError> {
        let row = sqlx::query("SELECT id, name, partition_count FROM topics WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|row| TopicInfo {
            id: row.get("id"),
            name: row.get("name"),
            partitions: row.get("partition_count"),
        }))
    }

    async fn topics(&self, names: Option<&[String]>) -> Result<Vec<TopicInfo>, ControlError> {
        let rows = sqlx::query("SELECT id, name, partition_count FROM topics ORDER BY name")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let name: String = row.get("name");
                if names.is_some_and(|names| !names.iter().any(|candidate| candidate == &name)) {
                    return None;
                }
                Some(TopicInfo {
                    id: row.get("id"),
                    name,
                    partitions: row.get("partition_count"),
                })
            })
            .collect())
    }

    async fn topic_config(&self, name: &str) -> Result<TopicConfig, ControlError> {
        postgres_retention::topic_config(&self.pool, name).await
    }

    async fn set_topic_config(&self, name: &str, config: TopicConfig) -> Result<(), ControlError> {
        postgres_retention::set_topic_config(&self.pool, name, config).await
    }

    async fn stage_object(&self, object: ObjectRef) -> Result<(), ControlError> {
        postgres_objects::stage(&self.pool, &object).await
    }

    async fn commit_object(
        &self,
        object: ObjectRef,
        batches: Vec<BatchDraft>,
    ) -> Result<Vec<StoredSpan>, ControlError> {
        if batches.is_empty() {
            return Err(ControlError::InvalidRequest(
                "an object must contain at least one batch".to_owned(),
            ));
        }
        for batch in &batches {
            validate_batch_bounds(batch)?;
        }
        let mut indexed_batches = batches.into_iter().enumerate().collect::<Vec<_>>();
        indexed_batches.sort_by(|(left_index, left), (right_index, right)| {
            (&left.partition.topic, left.partition.partition, left_index).cmp(&(
                &right.partition.topic,
                right.partition.partition,
                right_index,
            ))
        });
        let mut transaction = self.pool.begin().await?;
        postgres_objects::lock_staged(&mut transaction, &object).await?;
        let mut producer_ids = indexed_batches
            .iter()
            .filter_map(|(_, batch)| batch.producer.map(|producer| producer.producer_id))
            .collect::<Vec<_>>();
        producer_ids.sort_unstable();
        producer_ids.dedup();
        if !producer_ids.is_empty() {
            // Multi-producer metadata transactions must take producer rows in
            // one global order before partition or transaction state.
            sqlx::query(
                "SELECT producer_id
                 FROM producers
                 WHERE producer_id = ANY($1)
                 ORDER BY producer_id
                 FOR UPDATE",
            )
            .bind(producer_ids)
            .fetch_all(&mut *transaction)
            .await?;
        }
        let mut committed = vec![None; indexed_batches.len()];
        let mut object_used = false;
        for (batch_index, batch) in indexed_batches {
            let row = sqlx::query(
                "SELECT p.next_offset, t.id AS topic_id
                 FROM partitions p JOIN topics t ON t.id = p.topic_id
                 WHERE t.name = $1 AND p.partition_index = $2 FOR UPDATE",
            )
            .bind(&batch.partition.topic)
            .bind(batch.partition.partition)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| ControlError::PartitionNotFound {
                topic: batch.partition.topic.clone(),
                partition: batch.partition.partition,
            })?;
            let topic_id: Uuid = row.get("topic_id");
            let mut supports_epoch_bump = false;
            let mut previous_history_start_offset: Option<i64> = None;
            let transaction_id = if let Some(producer) = batch.producer {
                let producer_row = sqlx::query(
                    "SELECT producer_epoch, transactional_id, current_transaction_id,
                            two_phase_commit
                     FROM producers WHERE producer_id = $1 FOR UPDATE",
                )
                .bind(producer.producer_id)
                .fetch_optional(&mut *transaction)
                .await?
                .ok_or(ControlError::UnknownProducer(producer.producer_id))?;
                let expected_epoch: i16 = producer_row.get("producer_epoch");
                if expected_epoch != producer.producer_epoch {
                    return Err(ControlError::ProducerFenced {
                        producer_id: producer.producer_id,
                        expected_epoch,
                        actual_epoch: producer.producer_epoch,
                    });
                }
                supports_epoch_bump = producer_row.get::<bool, _>("two_phase_commit")
                    && batch.transactional_id.is_some();
                let owned_transactional_id: Option<String> = producer_row.get("transactional_id");
                match (&batch.transactional_id, owned_transactional_id.as_deref()) {
                    (None, None) => None,
                    (Some(requested), Some(owned)) if requested == owned => {
                        let transaction_id: Uuid = producer_row
                            .get::<Option<Uuid>, _>("current_transaction_id")
                            .ok_or_else(|| {
                                ControlError::InvalidTransactionState(
                                    "transaction has not started".to_owned(),
                                )
                            })?;
                        let transaction_row = sqlx::query(
                            "SELECT tx.status,
                                    EXISTS (
                                        SELECT 1 FROM transaction_partitions tp
                                        WHERE tp.transaction_id = tx.id
                                          AND tp.topic_id = $2
                                          AND tp.partition_index = $3
                                    ) AS partition_added
                             FROM transactions tx
                             WHERE tx.id = $1 AND tx.producer_id = $4
                               AND tx.producer_epoch = $5",
                        )
                        .bind(transaction_id)
                        .bind(topic_id)
                        .bind(batch.partition.partition)
                        .bind(producer.producer_id)
                        .bind(producer.producer_epoch)
                        .fetch_optional(&mut *transaction)
                        .await?
                        .ok_or_else(|| {
                            ControlError::InvalidTransactionState(
                                "active transaction does not match producer".to_owned(),
                            )
                        })?;
                        let status: String = transaction_row.get("status");
                        let partition_added: bool = transaction_row.get("partition_added");
                        if status != TransactionStatus::Ongoing.as_str()
                            || (batch.verify_transaction_partition && !partition_added)
                        {
                            return Err(ControlError::InvalidTransactionState(format!(
                                "partition {}-{} was not added to the active transaction",
                                batch.partition.topic, batch.partition.partition
                            )));
                        }
                        Some(transaction_id)
                    }
                    _ => {
                        return Err(ControlError::InvalidTransactionState(
                            "producer and transactional id do not match".to_owned(),
                        ));
                    }
                }
            } else if batch.transactional_id.is_some() {
                return Err(ControlError::InvalidTransactionState(
                    "transactional records require a producer id".to_owned(),
                ));
            } else {
                None
            };

            if let Some(producer) = batch.producer {
                let sequence = sqlx::query(
                    "SELECT producer_epoch, last_sequence, history_start_offset
                     FROM producer_sequences
                     WHERE producer_id = $1 AND topic_id = $2 AND partition_index = $3
                     FOR UPDATE",
                )
                .bind(producer.producer_id)
                .bind(topic_id)
                .bind(batch.partition.partition)
                .fetch_optional(&mut *transaction)
                .await?;
                let sequence = sequence
                    .filter(|row| row.get::<i16, _>("producer_epoch") == producer.producer_epoch);
                previous_history_start_offset =
                    sequence.as_ref().map(|row| row.get("history_start_offset"));
                let duplicate = if let Some(sequence) = sequence.as_ref() {
                    sqlx::query(
                        "SELECT object_key, byte_start, byte_end, base_offset, last_offset,
                                record_count, timestamp_ms, transaction_id,
                                offsets_preserved, format_version, checksum
                         FROM (
                             SELECT object_key, byte_start, byte_end, base_offset, last_offset,
                                    record_count, timestamp_ms, first_sequence, last_sequence,
                                    transaction_id, offsets_preserved, format_version, checksum
                             FROM object_spans
                             WHERE topic_id = $1 AND partition_index = $2
                               AND producer_id = $3 AND producer_epoch = $4
                               AND base_offset >= $5
                             ORDER BY base_offset DESC
                             LIMIT 5
                         ) recent
                         WHERE first_sequence = $6 AND last_sequence = $7
                         ORDER BY base_offset DESC
                         LIMIT 1",
                    )
                    .bind(topic_id)
                    .bind(batch.partition.partition)
                    .bind(producer.producer_id)
                    .bind(producer.producer_epoch)
                    .bind(sequence.get::<i64, _>("history_start_offset"))
                    .bind(producer.first_sequence)
                    .bind(producer.last_sequence)
                    .fetch_optional(&mut *transaction)
                    .await?
                } else {
                    None
                };
                if let Some(existing) = duplicate {
                    let record_count: i32 = existing.get("record_count");
                    committed[batch_index] = Some(StoredSpan {
                        partition: batch.partition,
                        object_key: existing.get("object_key"),
                        byte_start: existing.get::<i64, _>("byte_start") as u64,
                        byte_end: existing.get::<i64, _>("byte_end") as u64,
                        base_offset: existing.get("base_offset"),
                        last_offset: existing.get("last_offset"),
                        record_count,
                        timestamp_ms: existing.get("timestamp_ms"),
                        integrity: span_integrity::from_row(&existing)?,
                        producer: Some(producer),
                        transaction_id: existing.get("transaction_id"),
                        offsets_preserved: existing.get("offsets_preserved"),
                    });
                    continue;
                }
                if let Some(sequence) = sequence {
                    let expected =
                        increment_producer_sequence(sequence.get::<i32, _>("last_sequence"), 1);
                    if producer.first_sequence != expected {
                        return Err(ControlError::OutOfOrderSequence {
                            producer_id: producer.producer_id,
                            partition: batch.partition,
                            expected,
                            actual: producer.first_sequence,
                        });
                    }
                } else if supports_epoch_bump && producer.first_sequence != 0 {
                    return Err(ControlError::OutOfOrderSequence {
                        producer_id: producer.producer_id,
                        partition: batch.partition,
                        expected: 0,
                        actual: producer.first_sequence,
                    });
                }
            }

            object_used = true;
            let base_offset: i64 = row.get("next_offset");
            let last_offset = base_offset + i64::from(batch.record_count) - 1;
            let producer_id = batch.producer.map(|producer| producer.producer_id);
            let producer_epoch = batch.producer.map(|producer| producer.producer_epoch);
            let first_sequence = batch.producer.map(|producer| producer.first_sequence);
            let last_sequence = batch.producer.map(|producer| producer.last_sequence);
            let txn_state = if transaction_id.is_some() {
                "pending"
            } else {
                "visible"
            };
            sqlx::query(
                "INSERT INTO object_spans
                 (topic_id, partition_index, object_key, byte_start, byte_end,
                  base_offset, last_offset, record_count, timestamp_ms, txn_state,
                  producer_id, producer_epoch, first_sequence, last_sequence, transaction_id,
                  format_version, checksum)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                         $11, $12, $13, $14, $15, $16, $17)",
            )
            .bind(topic_id)
            .bind(batch.partition.partition)
            .bind(&object.key)
            .bind(batch.byte_start as i64)
            .bind(batch.byte_end as i64)
            .bind(base_offset)
            .bind(last_offset)
            .bind(batch.record_count)
            .bind(batch.timestamp_ms)
            .bind(txn_state)
            .bind(producer_id)
            .bind(producer_epoch)
            .bind(first_sequence)
            .bind(last_sequence)
            .bind(transaction_id)
            .bind(SpanIntegrity::from_checksum(batch.checksum).format_version)
            .bind(batch.checksum.map(|checksum| checksum.to_vec()))
            .execute(&mut *transaction)
            .await?;
            sqlx::query(
                "UPDATE partitions SET next_offset = $1
                 WHERE topic_id = $2 AND partition_index = $3",
            )
            .bind(last_offset + 1)
            .bind(topic_id)
            .bind(batch.partition.partition)
            .execute(&mut *transaction)
            .await?;
            if let Some(producer) = batch.producer {
                let history_start_offset =
                    if let Some(previous_history_start_offset) = previous_history_start_offset {
                        sqlx::query(
                            "SELECT MIN(base_offset) AS history_start_offset
                             FROM (
                                 SELECT base_offset
                                 FROM object_spans
                                 WHERE topic_id = $1 AND partition_index = $2
                                   AND producer_id = $3 AND producer_epoch = $4
                                   AND base_offset >= $5
                                 ORDER BY base_offset DESC
                                 LIMIT 5
                             ) recent",
                        )
                        .bind(topic_id)
                        .bind(batch.partition.partition)
                        .bind(producer.producer_id)
                        .bind(producer.producer_epoch)
                        .bind(previous_history_start_offset)
                        .fetch_one(&mut *transaction)
                        .await?
                        .get::<Option<i64>, _>("history_start_offset")
                        .unwrap_or(base_offset)
                    } else {
                        base_offset
                    };
                sqlx::query(
                    "INSERT INTO producer_sequences
                     (producer_id, topic_id, partition_index, producer_epoch,
                      last_sequence, last_offset, last_timestamp, history_start_offset)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                     ON CONFLICT (producer_id, topic_id, partition_index)
                     DO UPDATE SET producer_epoch = EXCLUDED.producer_epoch,
                                   last_sequence = EXCLUDED.last_sequence,
                                   last_offset = EXCLUDED.last_offset,
                                   last_timestamp = EXCLUDED.last_timestamp,
                                   history_start_offset = EXCLUDED.history_start_offset,
                                   updated_at = now()",
                )
                .bind(producer.producer_id)
                .bind(topic_id)
                .bind(batch.partition.partition)
                .bind(producer.producer_epoch)
                .bind(producer.last_sequence)
                .bind(last_offset)
                .bind(batch.timestamp_ms)
                .bind(history_start_offset)
                .execute(&mut *transaction)
                .await?;
            }
            committed[batch_index] = Some(StoredSpan {
                partition: batch.partition,
                object_key: object.key.clone(),
                byte_start: batch.byte_start,
                byte_end: batch.byte_end,
                base_offset,
                last_offset,
                record_count: batch.record_count,
                timestamp_ms: batch.timestamp_ms,
                integrity: SpanIntegrity::from_checksum(batch.checksum),
                producer: batch.producer,
                transaction_id,
                offsets_preserved: false,
            });
        }
        if object_used {
            postgres_objects::mark_committed(&mut transaction, &object.key).await?;
        }
        transaction.commit().await?;
        Ok(committed
            .into_iter()
            .map(|span| span.expect("every validated batch returns a span"))
            .collect())
    }

    async fn fetch(
        &self,
        partition: &PartitionKey,
        offset: i64,
        max_bytes: usize,
        isolation: FetchIsolation,
    ) -> Result<PartitionFetch, ControlError> {
        let watermarks = postgres_offsets::partition_watermarks(&self.pool, partition).await?;
        if offset < watermarks.log_start_offset || offset > watermarks.high_watermark {
            return Err(ControlError::OffsetOutOfRange {
                partition: partition.clone(),
                offset,
                start: watermarks.log_start_offset,
                end: watermarks.high_watermark,
            });
        }
        let rows = sqlx::query(
            "SELECT s.object_key, s.byte_start, s.byte_end, s.base_offset,
                    s.last_offset, s.record_count, s.timestamp_ms, s.txn_state,
                    s.producer_id, s.producer_epoch, s.first_sequence,
                    s.last_sequence, s.transaction_id, s.offsets_preserved,
                    s.format_version, s.checksum
             FROM object_spans s
             JOIN topics t ON t.id = s.topic_id
             WHERE t.name = $1 AND s.partition_index = $2 AND s.last_offset >= $3
             ORDER BY s.base_offset",
        )
        .bind(&partition.topic)
        .bind(partition.partition)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        let mut bytes = 0usize;
        let mut spans = Vec::new();
        for row in rows {
            let base_offset: i64 = row.get("base_offset");
            if isolation == FetchIsolation::ReadCommitted {
                if base_offset >= watermarks.last_stable_offset {
                    break;
                }
                let txn_state: String = row.get("txn_state");
                if txn_state != "visible" && txn_state != "committed" {
                    continue;
                }
            }
            let byte_start: i64 = row.get("byte_start");
            let byte_end: i64 = row.get("byte_end");
            let span_size = (byte_end - byte_start).max(0) as usize;
            if !spans.is_empty() && bytes + span_size > max_bytes {
                break;
            }
            bytes += span_size;
            spans.push(StoredSpan {
                partition: partition.clone(),
                object_key: row.get("object_key"),
                byte_start: byte_start as u64,
                byte_end: byte_end as u64,
                base_offset,
                last_offset: row.get("last_offset"),
                record_count: row.get("record_count"),
                timestamp_ms: row.get("timestamp_ms"),
                integrity: span_integrity::from_row(&row)?,
                producer: row.get::<Option<i64>, _>("producer_id").map(|producer_id| {
                    ProducerBatch {
                        producer_id,
                        producer_epoch: row.get("producer_epoch"),
                        first_sequence: row.get("first_sequence"),
                        last_sequence: row.get("last_sequence"),
                    }
                }),
                transaction_id: row.get("transaction_id"),
                offsets_preserved: row.get("offsets_preserved"),
            });
        }
        Ok(PartitionFetch {
            spans,
            high_watermark: watermarks.high_watermark,
            last_stable_offset: watermarks.last_stable_offset,
            log_start_offset: watermarks.log_start_offset,
        })
    }

    async fn partition_watermarks(
        &self,
        partition: &PartitionKey,
    ) -> Result<PartitionWatermarks, ControlError> {
        postgres_offsets::partition_watermarks(&self.pool, partition).await
    }

    async fn list_offset(
        &self,
        partition: &PartitionKey,
        timestamp_ms: i64,
    ) -> Result<i64, ControlError> {
        let row = sqlx::query(
            "SELECT p.next_offset, p.log_start_offset
             FROM partitions p JOIN topics t ON t.id = p.topic_id
             WHERE t.name = $1 AND p.partition_index = $2",
        )
        .bind(&partition.topic)
        .bind(partition.partition)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ControlError::PartitionNotFound {
            topic: partition.topic.clone(),
            partition: partition.partition,
        })?;
        if timestamp_ms == -2 {
            return Ok(row.get("log_start_offset"));
        }
        if timestamp_ms == -1 {
            return Ok(row.get("next_offset"));
        }
        let result = sqlx::query(
            "SELECT base_offset FROM object_spans s JOIN topics t ON t.id = s.topic_id
             WHERE t.name = $1 AND s.partition_index = $2 AND s.timestamp_ms >= $3
             ORDER BY s.base_offset LIMIT 1",
        )
        .bind(&partition.topic)
        .bind(partition.partition)
        .bind(timestamp_ms)
        .fetch_optional(&self.pool)
        .await?;
        Ok(result
            .map(|row| row.get("base_offset"))
            .unwrap_or_else(|| row.get("next_offset")))
    }

    async fn partition_size(&self, partition: &PartitionKey) -> Result<i64, ControlError> {
        postgres_log_dirs::partition_size(&self.pool, partition).await
    }

    async fn partition_retention_sizes(
        &self,
        limit: usize,
    ) -> Result<Vec<PartitionRetentionSize>, ControlError> {
        postgres_observability::partition_retention_sizes(&self.pool, limit).await
    }

    async fn describe_producers(
        &self,
        partition: &PartitionKey,
    ) -> Result<Vec<ActiveProducer>, ControlError> {
        postgres_producers::describe(&self.pool, partition).await
    }

    async fn expire_producer_sequences(
        &self,
        now_ms: i64,
        expiration_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        postgres_producers::expire(&self.pool, now_ms, expiration_ms, limit).await
    }

    async fn expire_consumer_offsets(
        &self,
        now_ms: i64,
        retention_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        postgres_offsets::expire(&self.pool, now_ms, retention_ms, limit).await
    }

    async fn delete_records(
        &self,
        partition: &PartitionKey,
        before_offset: i64,
    ) -> Result<i64, ControlError> {
        postgres_offsets::delete_records(&self.pool, partition, before_offset).await
    }

    async fn commit_offsets(
        &self,
        group_id: &str,
        offsets: Vec<OffsetCommit>,
    ) -> Result<(), ControlError> {
        postgres_offsets::commit(&self.pool, group_id, offsets).await
    }

    async fn commit_member_offsets(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        generation_or_epoch: i32,
        api_version: i16,
        offsets: Vec<OffsetCommit>,
    ) -> Result<Vec<bool>, ControlError> {
        let count = offsets.len();
        if let Some(validity) = postgres_consumer_groups::commit_offsets(
            &self.pool,
            group_id,
            member_id,
            generation_or_epoch,
            api_version,
            offsets.clone(),
        )
        .await?
        {
            return Ok(validity);
        }
        self.validate_group_member(group_id, member_id, group_instance_id, generation_or_epoch)
            .await?;
        postgres_offsets::commit(&self.pool, group_id, offsets).await?;
        Ok(vec![true; count])
    }

    async fn fetch_offsets(
        &self,
        group_id: &str,
        partitions: &[PartitionKey],
    ) -> Result<HashMap<PartitionKey, CommittedOffset>, ControlError> {
        let mut result = HashMap::new();
        for partition in partitions {
            let row = sqlx::query(
                "SELECT p.topic_id, co.committed_offset,
                        co.committed_leader_epoch, co.metadata,
                        co.commit_timestamp_ms, co.expire_timestamp_ms
                 FROM partitions p
                 JOIN topics t ON t.id = p.topic_id
                 LEFT JOIN consumer_offsets co
                   ON co.topic_id = p.topic_id
                  AND co.partition_index = p.partition_index
                  AND co.group_id = $1
                 WHERE t.name = $2 AND p.partition_index = $3",
            )
            .bind(group_id)
            .bind(&partition.topic)
            .bind(partition.partition)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| ControlError::PartitionNotFound {
                topic: partition.topic.clone(),
                partition: partition.partition,
            })?;
            let offset: Option<i64> = row.get("committed_offset");
            if let Some(offset) = offset {
                result.insert(
                    partition.clone(),
                    CommittedOffset {
                        offset,
                        leader_epoch: row.get("committed_leader_epoch"),
                        metadata: row.get("metadata"),
                        commit_timestamp_ms: row.get("commit_timestamp_ms"),
                        expire_timestamp_ms: row.get("expire_timestamp_ms"),
                    },
                );
            }
        }
        Ok(result)
    }

    async fn consumer_lags(&self, limit: usize) -> Result<Vec<ConsumerLag>, ControlError> {
        postgres_observability::consumer_lags(&self.pool, limit).await
    }

    async fn delete_offsets(
        &self,
        group_id: &str,
        partitions: &[PartitionKey],
    ) -> Result<HashSet<PartitionKey>, ControlError> {
        postgres_offsets::delete_offsets(&self.pool, group_id, partitions).await
    }

    async fn join_group(
        &self,
        group_id: &str,
        requested_member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: &[(String, Vec<u8>)],
        client: (&str, &str, &[String], i32),
        api_version: i16,
    ) -> Result<JoinGroupResult, ControlError> {
        postgres_classic_groups::join(
            &self.pool,
            group_id,
            requested_member_id,
            group_instance_id,
            protocol_type,
            protocols,
            client,
            api_version,
        )
        .await
    }

    async fn begin_join_group(
        &self,
        group_id: &str,
        requested_member_id: &str,
        group_instance_id: Option<&str>,
        protocol_type: &str,
        protocols: &[(String, Vec<u8>)],
        client: (&str, &str, &[String], i32),
        rebalance_timeout_ms: i32,
        initial_rebalance_delay_ms: i32,
        max_size: i32,
        api_version: i16,
    ) -> Result<JoinGroupResult, ControlError> {
        postgres_classic_join_barrier::begin(
            &self.pool,
            group_id,
            requested_member_id,
            group_instance_id,
            protocol_type,
            protocols,
            client,
            rebalance_timeout_ms,
            initial_rebalance_delay_ms,
            max_size,
            api_version,
        )
        .await
    }

    async fn poll_join_group(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        rebalance_id: Uuid,
        api_version: i16,
    ) -> Result<JoinGroupResult, ControlError> {
        postgres_classic_join_barrier::poll(
            &self.pool,
            group_id,
            member_id,
            group_instance_id,
            rebalance_id,
            api_version,
        )
        .await
    }

    async fn sync_group(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
        assignments: Vec<GroupAssignment>,
    ) -> Result<Vec<u8>, ControlError> {
        postgres_classic_groups::sync(
            &self.pool,
            group_id,
            generation_id,
            member_id,
            group_instance_id,
            assignments,
        )
        .await
    }

    async fn heartbeat_group(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        group_instance_id: Option<&str>,
    ) -> Result<(), ControlError> {
        postgres_classic_groups::heartbeat(
            &self.pool,
            group_id,
            generation_id,
            member_id,
            group_instance_id,
        )
        .await
    }

    async fn leave_group(
        &self,
        group_id: &str,
        members: &[GroupMemberIdentity],
    ) -> Result<Vec<LeaveGroupMemberResult>, ControlError> {
        postgres_classic_groups::leave(&self.pool, group_id, members).await
    }

    async fn consumer_group_heartbeat(
        &self,
        heartbeat: ConsumerGroupHeartbeat,
    ) -> Result<ConsumerGroupHeartbeatResult, ControlError> {
        postgres_consumer_groups::heartbeat(&self.pool, heartbeat).await
    }

    async fn consumer_group_heartbeat_deferred(
        &self,
        heartbeat: ConsumerGroupHeartbeat,
    ) -> Result<GroupHeartbeatOutcome<ConsumerGroupHeartbeatResult>, ControlError> {
        postgres_consumer_groups::heartbeat_deferred(&self.pool, heartbeat).await
    }

    async fn describe_consumer_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, ConsumerGroupDescription>, ControlError> {
        postgres_consumer_groups::describe(&self.pool, group_ids).await
    }

    async fn streams_group_heartbeat(
        &self,
        heartbeat: StreamsGroupHeartbeat,
    ) -> Result<StreamsGroupHeartbeatResult, ControlError> {
        postgres_streams_groups::heartbeat(&self.pool, heartbeat).await
    }

    async fn streams_group_heartbeat_deferred(
        &self,
        heartbeat: StreamsGroupHeartbeat,
    ) -> Result<GroupHeartbeatOutcome<StreamsGroupHeartbeatResult>, ControlError> {
        postgres_streams_groups::heartbeat_deferred(&self.pool, heartbeat).await
    }

    async fn describe_streams_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, StreamsGroupDescription>, ControlError> {
        postgres_streams_groups::describe(&self.pool, group_ids).await
    }

    async fn share_group_heartbeat(
        &self,
        heartbeat: ShareGroupHeartbeat,
    ) -> Result<ShareGroupHeartbeatResult, ControlError> {
        postgres_share_groups::heartbeat(&self.pool, heartbeat).await
    }

    async fn share_group_heartbeat_deferred(
        &self,
        heartbeat: ShareGroupHeartbeat,
    ) -> Result<GroupHeartbeatOutcome<ShareGroupHeartbeatResult>, ControlError> {
        postgres_share_groups::heartbeat_deferred(&self.pool, heartbeat).await
    }

    async fn complete_group_assignment(
        &self,
        task: GroupAssignmentTask,
    ) -> Result<GroupAssignmentCompletion, ControlError> {
        match task.protocol {
            AssignmentProtocol::Consumer => {
                postgres_consumer_groups::complete_assignment(&self.pool, &task).await
            }
            AssignmentProtocol::Share => {
                postgres_share_groups::complete_assignment(&self.pool, &task).await
            }
            AssignmentProtocol::Streams => {
                postgres_streams_groups::complete_assignment(&self.pool, &task).await
            }
        }
    }

    async fn describe_share_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, ShareGroupDescription>, ControlError> {
        postgres_share_groups::describe(&self.pool, group_ids).await
    }

    async fn update_share_fetch_session(
        &self,
        update: ShareFetchSessionUpdate,
    ) -> Result<ShareFetchSession, ControlError> {
        postgres_share_records::update_session(&self.pool, update).await
    }

    async fn existing_share_partition_state(
        &self,
        group_id: &str,
        member_id: &str,
        partition: &ShareSessionPartition,
    ) -> Result<Option<SharePartitionState>, ControlError> {
        postgres_share_records::existing_partition_state(&self.pool, group_id, member_id, partition)
            .await
    }

    async fn share_partition_state(
        &self,
        group_id: &str,
        member_id: &str,
        partition: &ShareSessionPartition,
        reset: ShareAutoOffsetReset,
    ) -> Result<SharePartitionState, ControlError> {
        postgres_share_records::partition_state(&self.pool, group_id, member_id, partition, reset)
            .await
    }

    async fn describe_share_group_offsets(
        &self,
        group_id: &str,
        partitions: Option<&[PartitionKey]>,
    ) -> Result<Vec<SharePartitionOffset>, ControlError> {
        postgres_share_offsets::describe(&self.pool, group_id, partitions).await
    }

    async fn alter_share_group_offsets(
        &self,
        group_id: &str,
        updates: &[ShareOffsetUpdate],
    ) -> Result<Vec<ShareOffsetUpdateResult>, ControlError> {
        postgres_share_offsets::alter(&self.pool, group_id, updates).await
    }

    async fn delete_share_group_offsets(
        &self,
        group_id: &str,
        topics: &[String],
    ) -> Result<Vec<ShareOffsetDeleteResult>, ControlError> {
        postgres_share_offsets::delete(&self.pool, group_id, topics).await
    }

    async fn acquire_share_records(
        &self,
        request: ShareAcquireRequest,
    ) -> Result<Vec<ShareAcquiredRecord>, ControlError> {
        postgres_share_records::acquire(&self.pool, request).await
    }

    async fn acknowledge_share_records(
        &self,
        request: ShareAcknowledgeRecords,
    ) -> Result<(), ControlError> {
        postgres_share_records::acknowledge(&self.pool, request).await
    }

    async fn initialize_share_group_state(
        &self,
        initialization: ShareStateInitialization,
    ) -> Result<(), ControlError> {
        postgres_share_state::initialize(&self.pool, initialization).await
    }

    async fn read_share_group_state(
        &self,
        read: ShareStateRead,
    ) -> Result<ShareStateSnapshot, ControlError> {
        postgres_share_state::read(&self.pool, read).await
    }

    async fn write_share_group_state(&self, write: ShareStateWrite) -> Result<(), ControlError> {
        postgres_share_state::write(&self.pool, write).await
    }

    async fn delete_share_group_state(&self, key: &ShareStateKey) -> Result<(), ControlError> {
        postgres_share_state::delete(&self.pool, key).await
    }

    async fn summarize_share_group_state(
        &self,
        key: &ShareStateKey,
    ) -> Result<Option<ShareStateSummary>, ControlError> {
        postgres_share_state::summarize(&self.pool, key).await
    }

    async fn list_groups(&self) -> Result<Vec<GroupSummary>, ControlError> {
        postgres_group_admin::list(&self.pool).await
    }

    async fn list_groups_limited(&self, limit: usize) -> Result<Vec<GroupSummary>, ControlError> {
        postgres_group_admin::list_limited(&self.pool, limit).await
    }

    async fn describe_classic_groups(
        &self,
        group_ids: &[String],
    ) -> Result<HashMap<String, ClassicGroupDescription>, ControlError> {
        postgres_group_admin::describe_classic(&self.pool, group_ids).await
    }

    async fn delete_group(&self, group_id: &str) -> Result<(), ControlError> {
        postgres_group_admin::delete(&self.pool, group_id).await
    }

    async fn validate_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        generation_or_epoch: i32,
    ) -> Result<(), ControlError> {
        if member_id.is_empty() && generation_or_epoch < 0 {
            return Ok(());
        }
        if postgres_consumer_groups::validate_member(
            &self.pool,
            group_id,
            member_id,
            generation_or_epoch,
        )
        .await?
        {
            return Ok(());
        }
        if postgres_streams_groups::validate_member(
            &self.pool,
            group_id,
            member_id,
            generation_or_epoch,
        )
        .await?
        {
            return Ok(());
        }
        postgres_classic_groups::validate_member(
            &self.pool,
            group_id,
            member_id,
            group_instance_id,
            generation_or_epoch,
        )
        .await
    }

    async fn init_producer_with_options(
        &self,
        transactional_id: Option<&str>,
        transaction_timeout_ms: i32,
        current: Option<ProducerSession>,
        enable_2_pc: bool,
        keep_prepared_txn: bool,
    ) -> Result<ProducerInitialization, ControlError> {
        postgres_transactions::init_producer(
            &self.pool,
            transactional_id,
            transaction_timeout_ms,
            current,
            enable_2_pc,
            keep_prepared_txn,
        )
        .await
    }

    async fn add_partitions_to_transaction(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        partitions: &[PartitionKey],
        verify_only: bool,
    ) -> Result<(), ControlError> {
        postgres_transactions::add_partitions(
            &self.pool,
            transactional_id,
            producer,
            partitions,
            verify_only,
        )
        .await
    }

    async fn add_offsets_to_transaction(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        group_id: &str,
    ) -> Result<(), ControlError> {
        postgres_transactions::add_offsets(&self.pool, transactional_id, producer, group_id).await
    }

    async fn commit_transaction_offsets(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        group_id: &str,
        offsets: Vec<OffsetCommit>,
    ) -> Result<(), ControlError> {
        postgres_transactions::commit_offsets(
            &self.pool,
            transactional_id,
            producer,
            group_id,
            offsets,
        )
        .await
    }

    async fn commit_transaction_member_offsets(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        group_id: &str,
        member_id: &str,
        group_instance_id: Option<&str>,
        generation_or_epoch: i32,
        add_group: bool,
        offsets: Vec<OffsetCommit>,
    ) -> Result<(), ControlError> {
        let anonymous_transactional_commit =
            generation_or_epoch == -1 && member_id.is_empty() && group_instance_id.is_none();
        if !anonymous_transactional_commit
            && postgres_transactions::commit_consumer_offsets(
                &self.pool,
                transactional_id,
                producer,
                group_id,
                member_id,
                group_instance_id,
                generation_or_epoch,
                add_group,
                offsets.clone(),
            )
            .await?
        {
            return Ok(());
        }
        if !anonymous_transactional_commit {
            self.validate_group_member(group_id, member_id, group_instance_id, generation_or_epoch)
                .await?;
        }
        postgres_transactions::commit_offsets_with_options(
            &self.pool,
            transactional_id,
            producer,
            group_id,
            add_group,
            offsets,
        )
        .await
    }

    async fn end_transaction(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        committed: bool,
    ) -> Result<(), ControlError> {
        postgres_transactions::end(&self.pool, transactional_id, producer, committed).await
    }

    async fn end_transaction_with_epoch_bump(
        &self,
        transactional_id: &str,
        producer: ProducerSession,
        committed: bool,
    ) -> Result<ProducerSession, ControlError> {
        postgres_transactions::end_with_epoch_bump(
            &self.pool,
            transactional_id,
            producer,
            committed,
        )
        .await
    }

    async fn write_transaction_marker(
        &self,
        producer: ProducerSession,
        partitions: &[PartitionKey],
        committed: bool,
        coordinator_epoch: i32,
        transaction_version: i8,
    ) -> Result<(), ControlError> {
        postgres_transactions::write_marker(
            &self.pool,
            producer,
            partitions,
            committed,
            coordinator_epoch,
            transaction_version,
        )
        .await
    }

    async fn describe_transactions(
        &self,
        transactional_ids: &[String],
    ) -> Result<HashMap<String, TransactionDescription>, ControlError> {
        postgres_transactions::describe(&self.pool, transactional_ids).await
    }

    async fn list_transactions(
        &self,
        filter: &TransactionFilter,
    ) -> Result<Vec<TransactionDescription>, ControlError> {
        postgres_transactions::list(&self.pool, filter).await
    }

    async fn transaction_state_counts(&self) -> Result<TransactionStateCounts, ControlError> {
        postgres_observability::transaction_state_counts(&self.pool).await
    }

    async fn expire_transactional_ids(
        &self,
        now_ms: i64,
        expiration_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        postgres_transactions::expire_transactional_ids(&self.pool, now_ms, expiration_ms, limit)
            .await
    }

    async fn create_acl(&self, rule: AclRule) -> Result<(), ControlError> {
        postgres_acls::create(&self.pool, &rule).await
    }

    async fn describe_acls(&self, filter: &AclFilter) -> Result<Vec<AclRule>, ControlError> {
        postgres_acls::describe(&self.pool, filter).await
    }

    async fn delete_acls(&self, filters: &[AclFilter]) -> Result<Vec<Vec<AclRule>>, ControlError> {
        postgres_acls::delete(&self.pool, filters).await
    }

    async fn scram_credentials(
        &self,
        users: Option<&[String]>,
    ) -> Result<Vec<ScramCredential>, ControlError> {
        postgres_scram::describe(&self.pool, users).await
    }

    async fn alter_scram_credentials(
        &self,
        alterations: Vec<ScramCredentialAlteration>,
    ) -> Result<HashSet<String>, ControlError> {
        postgres_scram::alter(&self.pool, alterations).await
    }

    async fn client_quotas(&self) -> Result<Vec<ClientQuota>, ControlError> {
        postgres_client_quotas::describe(&self.pool).await
    }

    async fn alter_client_quotas(
        &self,
        alterations: Vec<ClientQuotaAlteration>,
    ) -> Result<(), ControlError> {
        postgres_client_quotas::alter(&self.pool, alterations).await
    }

    async fn client_metric_subscriptions(
        &self,
    ) -> Result<Vec<ClientMetricSubscription>, ControlError> {
        postgres_client_metrics::list(&self.pool).await
    }

    async fn client_metric_subscription(
        &self,
        name: &str,
    ) -> Result<Option<ClientMetricSubscription>, ControlError> {
        postgres_client_metrics::get(&self.pool, name).await
    }

    async fn alter_client_metric_subscription(
        &self,
        alteration: ClientMetricConfigAlteration,
        validate_only: bool,
    ) -> Result<(), ControlError> {
        postgres_client_metrics::alter(&self.pool, alteration, validate_only).await
    }

    async fn group_config(&self, group_id: &str) -> Result<BTreeMap<String, String>, ControlError> {
        postgres_group_configs::get(&self.pool, group_id).await
    }

    async fn group_config_ids(&self) -> Result<Vec<String>, ControlError> {
        postgres_group_configs::ids(&self.pool).await
    }

    async fn alter_group_config(
        &self,
        group_id: &str,
        changes: BTreeMap<String, Option<String>>,
        validate_only: bool,
    ) -> Result<(), ControlError> {
        postgres_group_configs::alter(&self.pool, group_id, changes, validate_only).await
    }

    async fn broker_config(&self) -> Result<BTreeMap<String, String>, ControlError> {
        postgres_broker_configs::get(&self.pool).await
    }

    async fn alter_broker_config(
        &self,
        changes: BTreeMap<String, Option<String>>,
        validate_only: bool,
    ) -> Result<(), ControlError> {
        postgres_broker_configs::alter(&self.pool, changes, validate_only).await
    }

    async fn features(&self) -> Result<FeatureMetadata, ControlError> {
        postgres_features::describe(&self.pool).await
    }

    async fn update_features(
        &self,
        updates: Vec<FeatureLevelUpdate>,
        validate_only: bool,
    ) -> Result<FeatureMetadata, ControlError> {
        postgres_features::update(&self.pool, updates, validate_only).await
    }

    async fn create_delegation_token(&self, token: DelegationToken) -> Result<(), ControlError> {
        postgres_delegation_tokens::create(&self.pool, token).await
    }

    async fn delegation_token_by_id(
        &self,
        token_id: &str,
        now_ms: i64,
    ) -> Result<Option<DelegationToken>, ControlError> {
        postgres_delegation_tokens::by_id(&self.pool, token_id, now_ms).await
    }

    async fn delegation_tokens(&self, now_ms: i64) -> Result<Vec<DelegationToken>, ControlError> {
        postgres_delegation_tokens::list(&self.pool, now_ms).await
    }

    async fn renew_delegation_token(
        &self,
        hmac: &[u8],
        principal: &str,
        now_ms: i64,
        requested_period_ms: i64,
        default_period_ms: i64,
    ) -> Result<i64, ControlError> {
        postgres_delegation_tokens::renew(
            &self.pool,
            hmac,
            principal,
            now_ms,
            requested_period_ms,
            default_period_ms,
        )
        .await
    }

    async fn expire_delegation_token(
        &self,
        hmac: &[u8],
        principal: &str,
        now_ms: i64,
        expiry_period_ms: i64,
    ) -> Result<i64, ControlError> {
        postgres_delegation_tokens::expire(&self.pool, hmac, principal, now_ms, expiry_period_ms)
            .await
    }

    async fn delete_expired_delegation_tokens(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<u64, ControlError> {
        postgres_delegation_tokens::delete_expired(&self.pool, now_ms, limit).await
    }

    async fn authorize(
        &self,
        principal: &str,
        host: &str,
        resource_type: AclResourceType,
        resource_name: &str,
        operation: AclOperation,
        allow_if_no_acl: bool,
    ) -> Result<bool, ControlError> {
        postgres_acls::authorize(
            &self.pool,
            principal,
            host,
            resource_type,
            resource_name,
            operation,
            allow_if_no_acl,
        )
        .await
    }

    async fn authorize_by_resource_type(
        &self,
        principal: &str,
        host: &str,
        resource_type: AclResourceType,
        operation: AclOperation,
        allow_if_no_acl: bool,
    ) -> Result<bool, ControlError> {
        postgres_acls::authorize_by_resource_type(
            &self.pool,
            principal,
            host,
            resource_type,
            operation,
            allow_if_no_acl,
        )
        .await
    }

    async fn apply_retention_page(
        &self,
        now_ms: i64,
        object_delete_grace_ms: i64,
        page: RetentionPage<'_>,
    ) -> Result<RetentionResult, ControlError> {
        postgres_retention::apply_page(&self.pool, now_ms, object_delete_grace_ms, page).await
    }

    async fn complete_object_deletion(&self, key: &str) -> Result<bool, ControlError> {
        postgres_retention::complete_object_deletion(&self.pool, key).await
    }

    async fn claim_compaction(
        &self,
        now_ms: i64,
        lease_ms: i64,
    ) -> Result<Option<CompactionPlan>, ControlError> {
        postgres_compaction::claim(&self.pool, now_ms, lease_ms).await
    }

    async fn commit_compaction(
        &self,
        plan: &CompactionPlan,
        objects: Vec<CompactedObject>,
        recheck_at_ms: Option<i64>,
        now_ms: i64,
    ) -> Result<bool, ControlError> {
        postgres_compaction::commit(&self.pool, plan, objects, recheck_at_ms, now_ms).await
    }

    async fn release_compaction(
        &self,
        partition: &PartitionKey,
        lease_id: Uuid,
    ) -> Result<(), ControlError> {
        postgres_compaction::release(&self.pool, partition, lease_id).await
    }

    async fn abort_expired_transactions(&self) -> Result<u64, ControlError> {
        self.abort_expired_transactions_batch(usize::MAX).await
    }

    async fn abort_expired_transactions_batch(&self, limit: usize) -> Result<u64, ControlError> {
        postgres_transactions::abort_expired(&self.pool, limit).await
    }

    async fn claim_stale_objects(
        &self,
        before_ms: i64,
        limit: i64,
    ) -> Result<Vec<String>, ControlError> {
        postgres_objects::claim_stale(&self.pool, before_ms, limit).await
    }

    async fn complete_stale_object_deletion(&self, key: &str) -> Result<bool, ControlError> {
        postgres_objects::complete_stale_deletion(&self.pool, key).await
    }

    async fn object_committed(&self, key: &str) -> Result<bool, ControlError> {
        Ok(sqlx::query(
            "SELECT 1 FROM objects
             WHERE object_key = $1 AND committed = TRUE",
        )
        .bind(key)
        .fetch_optional(&self.pool)
        .await?
        .is_some())
    }

    async fn object_staged(&self, key: &str) -> Result<bool, ControlError> {
        postgres_objects::staged(&self.pool, key).await
    }

    async fn check(&self) -> Result<(), ControlError> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }
}

pub fn now() -> DateTime<Utc> {
    Utc::now()
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
#[path = "transaction_recovery_tests.rs"]
mod transaction_recovery_tests;
