use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use kafka_protocol::messages::ApiKey;
use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::info;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub produce_requests: IntCounter,
    pub fetch_requests: IntCounter,
    pub kafka_requests: IntCounterVec,
    pub committed_objects: IntCounter,
    pub active_connections: IntGauge,
    pub produce_flush_duration: Histogram,
    pub produce_metadata_commit_duration: Histogram,
    pub object_store_requests: IntCounterVec,
    pub object_store_errors: IntCounterVec,
    pub object_store_bytes: IntCounterVec,
    pub object_store_duration: HistogramVec,
    pub fetch_cache_hits: IntCounter,
    pub fetch_cache_misses: IntCounter,
    pub fetch_cache_evictions: IntCounter,
    pub fetch_cache_bytes: IntGauge,
    pub object_integrity_failures: IntCounter,
    pub orphan_gc_deleted: IntCounter,
    pub orphan_gc_errors: IntCounter,
    pub committed_transactions: IntCounter,
    pub aborted_transactions: IntCounter,
    pub expired_transactions: IntCounter,
    pub transaction_maintenance_errors: IntCounter,
    pub expired_consumer_offsets: IntCounter,
    pub consumer_offset_maintenance_errors: IntCounter,
    pub expired_transactional_ids: IntCounter,
    pub transactional_id_maintenance_errors: IntCounter,
    pub retention_removed_spans: IntCounter,
    pub retention_deleted_objects: IntCounter,
    pub retention_errors: IntCounter,
    pub compaction_runs: IntCounter,
    pub compaction_removed_records: IntCounter,
    pub compaction_bytes_written: IntCounter,
    pub compaction_conflicts: IntCounter,
    pub compaction_errors: IntCounter,
    pub sasl_authentications: IntCounter,
    pub sasl_authentication_failures: IntCounter,
    pub sasl_reauthentications: IntCounter,
    pub sasl_reauthentication_failures: IntCounter,
    pub quota_throttled_requests: IntCounter,
    pub quota_throttle_time_ms: IntCounter,
    pub client_telemetry_instances: IntGauge,
    pub client_telemetry_pushes: IntCounter,
    pub client_telemetry_bytes: IntCounter,
    pub client_telemetry_errors: IntCounter,
    pub group_members: IntGaugeVec,
    pub group_rebalance_epoch: IntGaugeVec,
    pub group_assignment_background_queue_duration: HistogramVec,
    pub group_assignment_background_processing_duration: HistogramVec,
    pub group_assignment_background_completions: IntCounterVec,
    pub group_assignment_background_queued: IntGauge,
    pub group_assignment_background_active: IntGauge,
    pub group_assignment_background_idle_ratio: Gauge,
    pub transaction_states: IntGaugeVec,
    pub consumer_group_lag: IntGaugeVec,
    pub partition_retention_size_percent: IntGaugeVec,
    pub observability_groups_truncated: IntGauge,
    pub consumer_group_lag_truncated: IntGauge,
    pub partition_retention_size_truncated: IntGauge,
    pub observability_collection_errors: IntCounter,
    ready: Arc<AtomicBool>,
}

impl Metrics {
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();
        let produce_requests =
            IntCounter::new("rutomq_produce_requests_total", "Produce requests")?;
        let fetch_requests = IntCounter::new("rutomq_fetch_requests_total", "Fetch requests")?;
        let kafka_requests = IntCounterVec::new(
            Opts::new(
                "rutomq_kafka_requests_total",
                "Accepted Kafka requests by API key and wire version",
            ),
            &["api", "version"],
        )?;
        let committed_objects =
            IntCounter::new("rutomq_committed_objects_total", "Committed objects")?;
        let active_connections =
            IntGauge::new("rutomq_active_connections", "Active Kafka connections")?;
        let produce_flush_duration = latency_histogram(
            "rutomq_produce_flush_duration_seconds",
            "End-to-end duration of one bounded Produce flush",
        )?;
        let produce_metadata_commit_duration = latency_histogram(
            "rutomq_produce_metadata_commit_duration_seconds",
            "Duration of the PostgreSQL metadata transaction after object upload",
        )?;
        let object_store_requests = IntCounterVec::new(
            Opts::new(
                "rutomq_object_store_requests_total",
                "OpenDAL object-store operations",
            ),
            &["operation"],
        )?;
        let object_store_errors = IntCounterVec::new(
            Opts::new(
                "rutomq_object_store_errors_total",
                "Failed OpenDAL object-store operations",
            ),
            &["operation"],
        )?;
        let object_store_bytes = IntCounterVec::new(
            Opts::new(
                "rutomq_object_store_bytes_total",
                "Bytes transferred through OpenDAL by operation",
            ),
            &["operation"],
        )?;
        let object_store_duration = HistogramVec::new(
            latency_options(
                "rutomq_object_store_duration_seconds",
                "OpenDAL object-store operation latency",
            )?,
            &["operation"],
        )?;
        let fetch_cache_hits = IntCounter::new(
            "rutomq_fetch_cache_hits_total",
            "Fetch object-range cache hits",
        )?;
        let fetch_cache_misses = IntCounter::new(
            "rutomq_fetch_cache_misses_total",
            "Fetch object-range cache misses",
        )?;
        let fetch_cache_evictions = IntCounter::new(
            "rutomq_fetch_cache_evictions_total",
            "Fetch object-range cache evictions",
        )?;
        let fetch_cache_bytes = IntGauge::new(
            "rutomq_fetch_cache_bytes",
            "Bytes currently held in the Agent-local Fetch cache",
        )?;
        let object_integrity_failures = IntCounter::new(
            "rutomq_object_integrity_failures_total",
            "Object span checksum or format failures observed by Fetch",
        )?;
        let orphan_gc_deleted = IntCounter::new(
            "rutomq_orphan_gc_deleted_total",
            "Deleted unreferenced object-store objects",
        )?;
        let orphan_gc_errors = IntCounter::new(
            "rutomq_orphan_gc_errors_total",
            "Orphan object garbage collection errors",
        )?;
        let committed_transactions = IntCounter::new(
            "rutomq_committed_transactions_total",
            "Committed Kafka transactions",
        )?;
        let aborted_transactions = IntCounter::new(
            "rutomq_aborted_transactions_total",
            "Explicitly aborted Kafka transactions",
        )?;
        let expired_transactions = IntCounter::new(
            "rutomq_expired_transactions_total",
            "Transactions aborted after their timeout",
        )?;
        let transaction_maintenance_errors = IntCounter::new(
            "rutomq_transaction_maintenance_errors_total",
            "Transaction timeout maintenance failures",
        )?;
        let expired_consumer_offsets = IntCounter::new(
            "rutomq_expired_consumer_offsets_total",
            "Committed consumer offsets removed after Kafka-compatible expiration",
        )?;
        let consumer_offset_maintenance_errors = IntCounter::new(
            "rutomq_consumer_offset_maintenance_errors_total",
            "Consumer offset expiration maintenance failures",
        )?;
        let expired_transactional_ids = IntCounter::new(
            "rutomq_expired_transactional_ids_total",
            "Transactional IDs removed after their idle expiration",
        )?;
        let transactional_id_maintenance_errors = IntCounter::new(
            "rutomq_transactional_id_maintenance_errors_total",
            "Transactional ID expiration maintenance failures",
        )?;
        let retention_removed_spans = IntCounter::new(
            "rutomq_retention_removed_spans_total",
            "Object span indexes removed by retention",
        )?;
        let retention_deleted_objects = IntCounter::new(
            "rutomq_retention_deleted_objects_total",
            "Unreferenced objects deleted after the retention grace period",
        )?;
        let retention_errors = IntCounter::new(
            "rutomq_retention_errors_total",
            "Retention metadata or object deletion failures",
        )?;
        let compaction_runs = IntCounter::new(
            "rutomq_compaction_runs_total",
            "Successfully committed partition compactions",
        )?;
        let compaction_removed_records = IntCounter::new(
            "rutomq_compaction_removed_records_total",
            "Records removed by key compaction",
        )?;
        let compaction_bytes_written = IntCounter::new(
            "rutomq_compaction_bytes_written_total",
            "Object-store bytes written by compaction",
        )?;
        let compaction_conflicts = IntCounter::new(
            "rutomq_compaction_conflicts_total",
            "Compaction leases or source spans changed before commit",
        )?;
        let compaction_errors = IntCounter::new(
            "rutomq_compaction_errors_total",
            "Compaction metadata, object read, or object write failures",
        )?;
        let sasl_authentications = IntCounter::new(
            "rutomq_sasl_authentications_total",
            "Successful SASL authentications",
        )?;
        let sasl_authentication_failures = IntCounter::new(
            "rutomq_sasl_authentication_failures_total",
            "Failed SASL authentication exchanges",
        )?;
        let sasl_reauthentications = IntCounter::new(
            "rutomq_sasl_reauthentications_total",
            "Successful SASL re-authentications",
        )?;
        let sasl_reauthentication_failures = IntCounter::new(
            "rutomq_sasl_reauthentication_failures_total",
            "Failed SASL re-authentication exchanges",
        )?;
        let quota_throttled_requests = IntCounter::new(
            "rutomq_quota_throttled_requests_total",
            "Kafka requests or connections throttled by client quotas",
        )?;
        let quota_throttle_time_ms = IntCounter::new(
            "rutomq_quota_throttle_time_ms_total",
            "Total milliseconds of client quota throttling",
        )?;
        let client_telemetry_instances = IntGauge::new(
            "rutomq_client_telemetry_instances",
            "Client telemetry instances currently cached by this Agent",
        )?;
        let client_telemetry_pushes = IntCounter::new(
            "rutomq_client_telemetry_pushes_total",
            "Accepted Kafka client telemetry pushes",
        )?;
        let client_telemetry_bytes = IntCounter::new(
            "rutomq_client_telemetry_bytes_total",
            "Compressed Kafka client telemetry bytes accepted",
        )?;
        let client_telemetry_errors = IntCounter::new(
            "rutomq_client_telemetry_errors_total",
            "Rejected Kafka client telemetry requests",
        )?;
        let group_members = IntGaugeVec::new(
            Opts::new(
                "rutomq_group_members",
                "Members in PostgreSQL-backed Kafka groups",
            ),
            &["protocol", "group", "state"],
        )?;
        let group_rebalance_epoch = IntGaugeVec::new(
            Opts::new(
                "rutomq_group_rebalance_epoch",
                "Persisted generation or assignment epoch by Kafka group",
            ),
            &["protocol", "group"],
        )?;
        let group_assignment_background_queue_duration = HistogramVec::new(
            latency_options(
                "rutomq_group_assignment_background_queue_duration_seconds",
                "Time group assignment work waits for a dedicated background worker",
            )?,
            &["protocol"],
        )?;
        let group_assignment_background_processing_duration = HistogramVec::new(
            latency_options(
                "rutomq_group_assignment_background_processing_duration_seconds",
                "Time spent computing and publishing group assignments",
            )?,
            &["protocol"],
        )?;
        let group_assignment_background_completions = IntCounterVec::new(
            Opts::new(
                "rutomq_group_assignment_background_completions_total",
                "Background group assignment outcomes",
            ),
            &["protocol", "result"],
        )?;
        let group_assignment_background_queued = IntGauge::new(
            "rutomq_group_assignment_background_queued",
            "Group assignments waiting for a background worker",
        )?;
        let group_assignment_background_active = IntGauge::new(
            "rutomq_group_assignment_background_active",
            "Group assignments currently running on background workers",
        )?;
        let group_assignment_background_idle_ratio = Gauge::new(
            "rutomq_group_assignment_background_idle_ratio",
            "Instantaneous fraction of idle group assignment background workers",
        )?;
        let coordinator_batch_buffer_cache_bytes = IntGaugeVec::new(
            Opts::new(
                "rutomq_coordinator_batch_buffer_cache_bytes",
                "Reusable coordinator append-buffer bytes retained by this Agent",
            ),
            &["coordinator"],
        )?;
        let coordinator_batch_buffer_cache_discards = IntCounterVec::new(
            Opts::new(
                "rutomq_coordinator_batch_buffer_cache_discards_total",
                "Oversized reusable coordinator append buffers discarded by this Agent",
            ),
            &["coordinator"],
        )?;
        for coordinator in ["group", "share"] {
            drop(coordinator_batch_buffer_cache_bytes.with_label_values(&[coordinator]));
            drop(coordinator_batch_buffer_cache_discards.with_label_values(&[coordinator]));
        }
        let transaction_states = IntGaugeVec::new(
            Opts::new(
                "rutomq_transactions_by_state",
                "Latest PostgreSQL-backed transactional IDs by Kafka state",
            ),
            &["state"],
        )?;
        let consumer_group_lag = IntGaugeVec::new(
            Opts::new(
                "rutomq_consumer_group_lag",
                "High watermark minus committed consumer offset",
            ),
            &["group", "topic", "partition"],
        )?;
        let partition_retention_size_percent = IntGaugeVec::new(
            Opts::new(
                "rutomq_partition_retention_size_percent",
                "Logical object-span bytes as an integer percentage of topic retention.bytes",
            ),
            &["topic", "partition"],
        )?;
        let observability_groups_truncated = IntGauge::new(
            "rutomq_observability_groups_truncated",
            "Whether group metric collection exceeded its configured bound",
        )?;
        let consumer_group_lag_truncated = IntGauge::new(
            "rutomq_consumer_group_lag_truncated",
            "Whether consumer-lag collection exceeded its configured series bound",
        )?;
        let partition_retention_size_truncated = IntGauge::new(
            "rutomq_partition_retention_size_truncated",
            "Whether partition retention-size collection exceeded its configured series bound",
        )?;
        let observability_collection_errors = IntCounter::new(
            "rutomq_observability_collection_errors_total",
            "Failed PostgreSQL-backed observability snapshots",
        )?;
        registry.register(Box::new(produce_requests.clone()))?;
        registry.register(Box::new(fetch_requests.clone()))?;
        registry.register(Box::new(kafka_requests.clone()))?;
        registry.register(Box::new(committed_objects.clone()))?;
        registry.register(Box::new(active_connections.clone()))?;
        registry.register(Box::new(produce_flush_duration.clone()))?;
        registry.register(Box::new(produce_metadata_commit_duration.clone()))?;
        registry.register(Box::new(object_store_requests.clone()))?;
        registry.register(Box::new(object_store_errors.clone()))?;
        registry.register(Box::new(object_store_bytes.clone()))?;
        registry.register(Box::new(object_store_duration.clone()))?;
        registry.register(Box::new(fetch_cache_hits.clone()))?;
        registry.register(Box::new(fetch_cache_misses.clone()))?;
        registry.register(Box::new(fetch_cache_evictions.clone()))?;
        registry.register(Box::new(fetch_cache_bytes.clone()))?;
        registry.register(Box::new(object_integrity_failures.clone()))?;
        registry.register(Box::new(orphan_gc_deleted.clone()))?;
        registry.register(Box::new(orphan_gc_errors.clone()))?;
        registry.register(Box::new(committed_transactions.clone()))?;
        registry.register(Box::new(aborted_transactions.clone()))?;
        registry.register(Box::new(expired_transactions.clone()))?;
        registry.register(Box::new(transaction_maintenance_errors.clone()))?;
        registry.register(Box::new(expired_consumer_offsets.clone()))?;
        registry.register(Box::new(consumer_offset_maintenance_errors.clone()))?;
        registry.register(Box::new(expired_transactional_ids.clone()))?;
        registry.register(Box::new(transactional_id_maintenance_errors.clone()))?;
        registry.register(Box::new(retention_removed_spans.clone()))?;
        registry.register(Box::new(retention_deleted_objects.clone()))?;
        registry.register(Box::new(retention_errors.clone()))?;
        registry.register(Box::new(compaction_runs.clone()))?;
        registry.register(Box::new(compaction_removed_records.clone()))?;
        registry.register(Box::new(compaction_bytes_written.clone()))?;
        registry.register(Box::new(compaction_conflicts.clone()))?;
        registry.register(Box::new(compaction_errors.clone()))?;
        registry.register(Box::new(sasl_authentications.clone()))?;
        registry.register(Box::new(sasl_authentication_failures.clone()))?;
        registry.register(Box::new(sasl_reauthentications.clone()))?;
        registry.register(Box::new(sasl_reauthentication_failures.clone()))?;
        registry.register(Box::new(quota_throttled_requests.clone()))?;
        registry.register(Box::new(quota_throttle_time_ms.clone()))?;
        registry.register(Box::new(client_telemetry_instances.clone()))?;
        registry.register(Box::new(client_telemetry_pushes.clone()))?;
        registry.register(Box::new(client_telemetry_bytes.clone()))?;
        registry.register(Box::new(client_telemetry_errors.clone()))?;
        registry.register(Box::new(group_members.clone()))?;
        registry.register(Box::new(group_rebalance_epoch.clone()))?;
        registry.register(Box::new(group_assignment_background_queue_duration.clone()))?;
        registry.register(Box::new(
            group_assignment_background_processing_duration.clone(),
        ))?;
        registry.register(Box::new(group_assignment_background_completions.clone()))?;
        registry.register(Box::new(group_assignment_background_queued.clone()))?;
        registry.register(Box::new(group_assignment_background_active.clone()))?;
        registry.register(Box::new(group_assignment_background_idle_ratio.clone()))?;
        registry.register(Box::new(coordinator_batch_buffer_cache_bytes))?;
        registry.register(Box::new(coordinator_batch_buffer_cache_discards))?;
        registry.register(Box::new(transaction_states.clone()))?;
        registry.register(Box::new(consumer_group_lag.clone()))?;
        registry.register(Box::new(partition_retention_size_percent.clone()))?;
        registry.register(Box::new(observability_groups_truncated.clone()))?;
        registry.register(Box::new(consumer_group_lag_truncated.clone()))?;
        registry.register(Box::new(partition_retention_size_truncated.clone()))?;
        registry.register(Box::new(observability_collection_errors.clone()))?;
        Ok(Self {
            registry,
            produce_requests,
            fetch_requests,
            kafka_requests,
            committed_objects,
            active_connections,
            produce_flush_duration,
            produce_metadata_commit_duration,
            object_store_requests,
            object_store_errors,
            object_store_bytes,
            object_store_duration,
            fetch_cache_hits,
            fetch_cache_misses,
            fetch_cache_evictions,
            fetch_cache_bytes,
            object_integrity_failures,
            orphan_gc_deleted,
            orphan_gc_errors,
            committed_transactions,
            aborted_transactions,
            expired_transactions,
            transaction_maintenance_errors,
            expired_consumer_offsets,
            consumer_offset_maintenance_errors,
            expired_transactional_ids,
            transactional_id_maintenance_errors,
            retention_removed_spans,
            retention_deleted_objects,
            retention_errors,
            compaction_runs,
            compaction_removed_records,
            compaction_bytes_written,
            compaction_conflicts,
            compaction_errors,
            sasl_authentications,
            sasl_authentication_failures,
            sasl_reauthentications,
            sasl_reauthentication_failures,
            quota_throttled_requests,
            quota_throttle_time_ms,
            client_telemetry_instances,
            client_telemetry_pushes,
            client_telemetry_bytes,
            client_telemetry_errors,
            group_members,
            group_rebalance_epoch,
            group_assignment_background_queue_duration,
            group_assignment_background_processing_duration,
            group_assignment_background_completions,
            group_assignment_background_queued,
            group_assignment_background_active,
            group_assignment_background_idle_ratio,
            transaction_states,
            consumer_group_lag,
            partition_retention_size_percent,
            observability_groups_truncated,
            consumer_group_lag_truncated,
            partition_retention_size_truncated,
            observability_collection_errors,
            ready: Arc::new(AtomicBool::new(false)),
        })
    }

    pub fn record_quota_throttle(&self, delay: std::time::Duration) {
        if delay.is_zero() {
            return;
        }
        self.quota_throttled_requests.inc();
        self.quota_throttle_time_ms
            .inc_by(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
    }

    pub fn record_kafka_request(&self, api_key: ApiKey, version: i16) {
        self.kafka_requests
            .with_label_values(&[&format!("{api_key:?}"), &version.to_string()])
            .inc();
    }

    pub fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub async fn serve(
        self: Arc<Self>,
        listener: TcpListener,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let metrics = self.clone();
        let readiness = self.clone();
        let app = Router::new()
            .route("/health/live", get(live))
            .route("/health/ready", get(move || ready(readiness.clone())))
            .route("/metrics", get(move || metrics_handler(metrics.clone())));
        info!(address = ?listener.local_addr()?, "admin listener started");
        axum::serve(listener, app)
            .with_graceful_shutdown(wait_for_shutdown(shutdown))
            .await?;
        Ok(())
    }
}

fn latency_histogram(name: &str, help: &str) -> anyhow::Result<Histogram> {
    Ok(Histogram::with_opts(latency_options(name, help)?)?)
}

fn latency_options(name: &str, help: &str) -> anyhow::Result<HistogramOpts> {
    Ok(HistogramOpts::new(name, help).buckets(prometheus::exponential_buckets(0.0005, 2.0, 18)?))
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

async fn live() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

async fn ready(metrics: Arc<Metrics>) -> impl IntoResponse {
    if metrics.is_ready() {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

async fn metrics_handler(metrics: Arc<Metrics>) -> impl IntoResponse {
    let families = metrics.registry.gather();
    let mut output = Vec::new();
    let encoder = TextEncoder::new();
    if encoder.encode(&families, &mut output).is_err() {
        return (StatusCode::INTERNAL_SERVER_ERROR, String::new());
    }
    (
        StatusCode::OK,
        String::from_utf8(output).unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stateless_coordinator_buffer_metrics_are_present_and_zero() {
        let metrics = Metrics::new().unwrap();
        let families = metrics
            .registry
            .gather()
            .into_iter()
            .map(|family| (family.name().to_owned(), family))
            .collect::<std::collections::BTreeMap<_, _>>();

        let bytes = families
            .get("rutomq_coordinator_batch_buffer_cache_bytes")
            .unwrap();
        assert_eq!(bytes.get_metric().len(), 2);
        assert!(
            bytes
                .get_metric()
                .iter()
                .all(|metric| metric.get_gauge().value() == 0.0)
        );
        let discards = families
            .get("rutomq_coordinator_batch_buffer_cache_discards_total")
            .unwrap();
        assert_eq!(discards.get_metric().len(), 2);
        assert!(
            discards
                .get_metric()
                .iter()
                .all(|metric| metric.get_counter().value() == 0.0)
        );
    }
}
