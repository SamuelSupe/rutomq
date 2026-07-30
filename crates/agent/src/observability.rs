use crate::Metrics;
use anyhow::Result;
use rutomq_control::{GroupSummary, MetadataStore, TransactionStateCounts};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{MissedTickBehavior, interval};
use tracing::warn;

struct GroupObservation {
    protocol: &'static str,
    group_id: String,
    state: String,
    members: i64,
    rebalance_epoch: i64,
}

struct SnapshotTruncation {
    groups: bool,
    lags: bool,
    retention_sizes: bool,
}

pub(crate) fn spawn(
    metadata: Arc<dyn MetadataStore>,
    metrics: Arc<Metrics>,
    period: Duration,
    max_groups: usize,
    max_lag_series: usize,
    max_retention_series: usize,
) {
    tokio::spawn(async move {
        let mut ticker = interval(period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(error) = collect(
                &metadata,
                &metrics,
                max_groups,
                max_lag_series,
                max_retention_series,
            )
            .await
            {
                metrics.observability_collection_errors.inc();
                warn!(%error, "control-plane observability snapshot failed");
            }
        }
    });
}

async fn collect(
    metadata: &Arc<dyn MetadataStore>,
    metrics: &Metrics,
    max_groups: usize,
    max_lag_series: usize,
    max_retention_series: usize,
) -> Result<()> {
    let mut groups = metadata.list_groups().await?;
    groups.sort_by(|left, right| left.group_id.cmp(&right.group_id));
    let groups_truncated = groups.len() > max_groups;
    groups.truncate(max_groups);

    let classic_ids = group_ids(&groups, "Classic");
    let consumer_ids = group_ids(&groups, "Consumer");
    let streams_ids = group_ids(&groups, "Streams");
    let share_ids = group_ids(&groups, "Share");
    let lag_limit = max_lag_series.saturating_add(1);
    let retention_limit = max_retention_series.saturating_add(1);
    let (classic, consumer, streams, share, transactions, mut lags, mut retention_sizes) = tokio::try_join!(
        metadata.describe_classic_groups(&classic_ids),
        metadata.describe_consumer_groups(&consumer_ids),
        metadata.describe_streams_groups(&streams_ids),
        metadata.describe_share_groups(&share_ids),
        metadata.transaction_state_counts(),
        metadata.consumer_lags(lag_limit),
        metadata.partition_retention_sizes(retention_limit),
    )?;

    let observations = groups
        .into_iter()
        .map(|summary| match summary.group_type.as_str() {
            "Classic" => classic
                .get(&summary.group_id)
                .map(|group| GroupObservation {
                    protocol: "classic",
                    group_id: group.group_id.clone(),
                    state: group.state.clone(),
                    members: count(group.members.len()),
                    rebalance_epoch: i64::from(group.generation_id),
                })
                .unwrap_or_else(|| empty_group("classic", summary)),
            "Consumer" => consumer
                .get(&summary.group_id)
                .map(|group| GroupObservation {
                    protocol: "consumer",
                    group_id: group.group_id.clone(),
                    state: group.state.clone(),
                    members: count(group.members.len()),
                    rebalance_epoch: i64::from(group.assignment_epoch),
                })
                .unwrap_or_else(|| empty_group("consumer", summary)),
            "Streams" => streams
                .get(&summary.group_id)
                .map(|group| GroupObservation {
                    protocol: "streams",
                    group_id: group.group_id.clone(),
                    state: group.state.clone(),
                    members: count(group.members.len()),
                    rebalance_epoch: i64::from(group.assignment_epoch),
                })
                .unwrap_or_else(|| empty_group("streams", summary)),
            "Share" => share
                .get(&summary.group_id)
                .map(|group| GroupObservation {
                    protocol: "share",
                    group_id: group.group_id.clone(),
                    state: group.state.clone(),
                    members: count(group.members.len()),
                    rebalance_epoch: i64::from(group.assignment_epoch),
                })
                .unwrap_or_else(|| empty_group("share", summary)),
            _ => empty_group("unknown", summary),
        })
        .collect::<Vec<_>>();

    let lag_truncated = lags.len() > max_lag_series;
    lags.truncate(max_lag_series);
    let retention_truncated = retention_sizes.len() > max_retention_series;
    retention_sizes.truncate(max_retention_series);
    replace_metrics(
        metrics,
        &observations,
        transactions,
        &lags,
        &retention_sizes,
        SnapshotTruncation {
            groups: groups_truncated,
            lags: lag_truncated,
            retention_sizes: retention_truncated,
        },
    );
    Ok(())
}

fn group_ids(groups: &[GroupSummary], group_type: &str) -> Vec<String> {
    groups
        .iter()
        .filter(|group| group.group_type == group_type)
        .map(|group| group.group_id.clone())
        .collect()
}

fn empty_group(protocol: &'static str, summary: GroupSummary) -> GroupObservation {
    GroupObservation {
        protocol,
        group_id: summary.group_id,
        state: summary.state,
        members: 0,
        rebalance_epoch: 0,
    }
}

fn count(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn replace_metrics(
    metrics: &Metrics,
    groups: &[GroupObservation],
    transactions: TransactionStateCounts,
    lags: &[rutomq_control::ConsumerLag],
    retention_sizes: &[rutomq_control::PartitionRetentionSize],
    truncation: SnapshotTruncation,
) {
    metrics.group_members.reset();
    metrics.group_rebalance_epoch.reset();
    for group in groups {
        metrics
            .group_members
            .with_label_values(&[group.protocol, &group.group_id, &group.state])
            .set(group.members);
        metrics
            .group_rebalance_epoch
            .with_label_values(&[group.protocol, &group.group_id])
            .set(group.rebalance_epoch);
    }

    metrics.transaction_states.reset();
    for (state, value) in [
        ("Empty", transactions.empty),
        ("Ongoing", transactions.ongoing),
        ("CompleteCommit", transactions.complete_commit),
        ("CompleteAbort", transactions.complete_abort),
    ] {
        metrics
            .transaction_states
            .with_label_values(&[state])
            .set(value);
    }

    metrics.consumer_group_lag.reset();
    for lag in lags {
        metrics
            .consumer_group_lag
            .with_label_values(&[
                &lag.group_id,
                &lag.partition.topic,
                &lag.partition.partition.to_string(),
            ])
            .set(lag.lag);
    }
    metrics.partition_retention_size_percent.reset();
    for retention in retention_sizes {
        metrics
            .partition_retention_size_percent
            .with_label_values(&[
                &retention.partition.topic,
                &retention.partition.partition.to_string(),
            ])
            .set(retention.percent());
    }
    metrics
        .observability_groups_truncated
        .set(i64::from(truncation.groups));
    metrics
        .consumer_group_lag_truncated
        .set(i64::from(truncation.lags));
    metrics
        .partition_retention_size_truncated
        .set(i64::from(truncation.retention_sizes));
}

#[cfg(test)]
mod tests {
    use super::*;
    use rutomq_control::{
        BatchDraft, MemoryMetadataStore, ObjectRef, OffsetCommit, PartitionKey, TopicConfig,
    };

    #[tokio::test]
    async fn snapshot_replaces_lag_group_and_transaction_state() {
        let store = Arc::new(MemoryMetadataStore::new());
        let partition = PartitionKey::new("events", 0);
        store.create_topic("events", 1).await.unwrap();
        let object = ObjectRef {
            key: "objects/observability".to_owned(),
            size: 1,
        };
        store.stage_object(object.clone()).await.unwrap();
        store
            .commit_object(
                object,
                vec![BatchDraft {
                    partition: partition.clone(),
                    byte_start: 0,
                    byte_end: 1,
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
        store
            .commit_offsets(
                "workers",
                vec![OffsetCommit {
                    partition: partition.clone(),
                    offset: 0,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                }],
            )
            .await
            .unwrap();
        let producer = store
            .init_producer(Some("observability-tx"), 60_000, None)
            .await
            .unwrap();
        store
            .add_partitions_to_transaction(
                "observability-tx",
                producer,
                std::slice::from_ref(&partition),
                false,
            )
            .await
            .unwrap();

        let metadata: Arc<dyn MetadataStore> = store.clone();
        let metrics = Metrics::new().unwrap();
        collect(&metadata, &metrics, 10, 10, 10).await.unwrap();
        assert_eq!(
            metrics
                .consumer_group_lag
                .with_label_values(&["workers", "events", "0"])
                .get(),
            1
        );
        assert_eq!(
            metrics
                .group_members
                .with_label_values(&["classic", "workers", "Empty"])
                .get(),
            0
        );
        assert_eq!(
            metrics
                .transaction_states
                .with_label_values(&["Ongoing"])
                .get(),
            1
        );

        store
            .commit_offsets(
                "workers",
                vec![OffsetCommit {
                    partition,
                    offset: 1,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                }],
            )
            .await
            .unwrap();
        store
            .end_transaction("observability-tx", producer, true)
            .await
            .unwrap();
        collect(&metadata, &metrics, 10, 10, 10).await.unwrap();
        assert_eq!(
            metrics
                .consumer_group_lag
                .with_label_values(&["workers", "events", "0"])
                .get(),
            0
        );
        assert_eq!(
            metrics
                .transaction_states
                .with_label_values(&["Ongoing"])
                .get(),
            0
        );
        assert_eq!(
            metrics
                .transaction_states
                .with_label_values(&["CompleteCommit"])
                .get(),
            1
        );
    }

    #[tokio::test]
    async fn snapshot_bounds_partition_retention_pressure_and_tracks_removed_spans() {
        let store = Arc::new(MemoryMetadataStore::new());
        store.create_topic("a-unlimited", 1).await.unwrap();
        let config = TopicConfig {
            retention_bytes: 2,
            ..TopicConfig::default()
        };
        store
            .create_topic_with_config("b-retained", 2, config)
            .await
            .unwrap();
        let object = ObjectRef {
            key: "objects/retention-observability".to_owned(),
            size: 4,
        };
        store.stage_object(object.clone()).await.unwrap();
        store
            .commit_object(
                object,
                vec![
                    BatchDraft {
                        partition: PartitionKey::new("b-retained", 0),
                        byte_start: 0,
                        byte_end: 3,
                        record_count: 1,
                        timestamp_ms: 1,
                        checksum: None,
                        producer: None,
                        transactional_id: None,
                        verify_transaction_partition: true,
                    },
                    BatchDraft {
                        partition: PartitionKey::new("b-retained", 1),
                        byte_start: 3,
                        byte_end: 4,
                        record_count: 1,
                        timestamp_ms: 1,
                        checksum: None,
                        producer: None,
                        transactional_id: None,
                        verify_transaction_partition: true,
                    },
                ],
            )
            .await
            .unwrap();

        let metadata: Arc<dyn MetadataStore> = store.clone();
        let metrics = Metrics::new().unwrap();
        collect(&metadata, &metrics, 10, 10, 2).await.unwrap();
        assert_eq!(
            metrics
                .partition_retention_size_percent
                .with_label_values(&["a-unlimited", "0"])
                .get(),
            0
        );
        assert_eq!(
            metrics
                .partition_retention_size_percent
                .with_label_values(&["b-retained", "0"])
                .get(),
            150
        );
        assert_eq!(metrics.partition_retention_size_truncated.get(), 1);
        assert_eq!(partition_retention_metric_count(&metrics), 2);

        collect(&metadata, &metrics, 10, 10, 10).await.unwrap();
        assert_eq!(
            metrics
                .partition_retention_size_percent
                .with_label_values(&["b-retained", "1"])
                .get(),
            50
        );
        assert_eq!(metrics.partition_retention_size_truncated.get(), 0);
        assert_eq!(partition_retention_metric_count(&metrics), 3);

        store.apply_retention(1, 0).await.unwrap();
        collect(&metadata, &metrics, 10, 10, 10).await.unwrap();
        assert_eq!(
            metrics
                .partition_retention_size_percent
                .with_label_values(&["b-retained", "0"])
                .get(),
            0
        );
    }

    fn partition_retention_metric_count(metrics: &Metrics) -> usize {
        metrics
            .registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "rutomq_partition_retention_size_percent")
            .map_or(0, |family| family.get_metric().len())
    }
}
