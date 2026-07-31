use anyhow::{Context, Result, bail};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub const DEFAULT_LOG_FILTER: &str = "rutomq=info,tower_http=info";

#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub kafka_addr: SocketAddr,
    pub admin_addr: SocketAddr,
    pub advertise_host: String,
    pub advertise_port: i32,
    pub cluster_id: String,
    pub log_filter: String,
    pub num_partitions: i32,
    pub default_replication_factor: i16,
    pub auto_create_topics_enable: bool,
    pub flush_interval: Duration,
    pub max_batch_bytes: usize,
    pub max_frame_size: usize,
    pub max_inflight_request_bytes: usize,
    pub max_inflight_response_bytes: usize,
    pub connection_io_timeout: Duration,
    pub max_connections: usize,
    pub max_fetch_bytes: usize,
    pub max_request_partition_size_limit: i32,
    pub fetch_cache_bytes: usize,
    pub telemetry_max_bytes: usize,
    pub observability_interval: Duration,
    pub observability_max_groups: usize,
    pub consumer_lag_max_series: usize,
    pub partition_retention_max_series: usize,
    pub orphan_gc_interval: Duration,
    pub orphan_gc_grace: Duration,
    pub shutdown_grace: Duration,
    pub retention_interval: Duration,
    pub object_delete_grace: Duration,
    pub compaction_interval: Duration,
    pub compaction_lease: Duration,
    pub compaction_max_object_bytes: usize,
    pub producer_id_expiration_ms: i64,
    pub producer_id_expiration_check_interval: Duration,
    pub offset_metadata_max_bytes: usize,
    pub offsets_retention_minutes: i32,
    pub offsets_retention_check_interval: Duration,
    pub transactional_id_expiration_ms: i64,
    pub transactional_id_expiration_check_interval: Duration,
    pub transaction_abort_timed_out_cleanup_interval: Duration,
    pub transaction_partition_verification_enable: bool,
    pub transaction_max_timeout_ms: i32,
    pub transaction_two_phase_commit_enable: bool,
    pub classic_group_initial_rebalance_delay_ms: i32,
    pub classic_group_min_session_timeout_ms: i32,
    pub classic_group_max_session_timeout_ms: i32,
    pub classic_group_max_size: i32,
    pub group_coordinator_background_threads: usize,
    pub group_coordinator_cached_buffer_max_bytes: i32,
    pub share_coordinator_cached_buffer_max_bytes: i32,
    pub consumer_assignor_offload_enable: bool,
    pub share_assignor_offload_enable: bool,
    pub streams_assignor_offload_enable: bool,
    pub group_heartbeat_interval_ms: i32,
    pub consumer_group_min_heartbeat_interval_ms: i32,
    pub consumer_group_max_heartbeat_interval_ms: i32,
    pub group_session_timeout_ms: i32,
    pub consumer_group_min_session_timeout_ms: i32,
    pub consumer_group_max_session_timeout_ms: i32,
    pub consumer_group_max_size: i32,
    pub consumer_group_assignors: Vec<String>,
    pub consumer_group_regex_refresh_interval_ms: i32,
    pub group_assignment_interval_ms: i32,
    pub group_min_assignment_interval_ms: i32,
    pub group_max_assignment_interval_ms: i32,
    pub streams_group_heartbeat_interval_ms: i32,
    pub streams_group_min_heartbeat_interval_ms: i32,
    pub streams_group_max_heartbeat_interval_ms: i32,
    pub streams_group_session_timeout_ms: i32,
    pub streams_group_min_session_timeout_ms: i32,
    pub streams_group_max_session_timeout_ms: i32,
    pub streams_group_max_size: i32,
    pub streams_group_assignment_interval_ms: i32,
    pub streams_group_min_assignment_interval_ms: i32,
    pub streams_group_max_assignment_interval_ms: i32,
    pub streams_group_num_standby_replicas: i32,
    pub streams_group_max_standby_replicas: i32,
    pub streams_group_initial_rebalance_delay_ms: i32,
    pub streams_acceptable_recovery_lag: i32,
    pub streams_task_offset_interval_ms: i32,
    pub share_group_heartbeat_interval_ms: i32,
    pub share_group_min_heartbeat_interval_ms: i32,
    pub share_group_max_heartbeat_interval_ms: i32,
    pub share_group_session_timeout_ms: i32,
    pub share_group_min_session_timeout_ms: i32,
    pub share_group_max_session_timeout_ms: i32,
    pub share_group_max_size: i32,
    pub share_group_assignors: Vec<String>,
    pub share_group_assignment_interval_ms: i32,
    pub share_group_min_assignment_interval_ms: i32,
    pub share_group_max_assignment_interval_ms: i32,
    pub share_record_lock_duration_ms: i32,
    pub share_min_record_lock_duration_ms: i32,
    pub share_max_record_lock_duration_ms: i32,
    pub share_record_delivery_count_limit: i16,
    pub share_min_delivery_count_limit: i16,
    pub share_max_delivery_count_limit: i16,
    pub share_partition_max_record_locks: i32,
    pub share_min_partition_max_record_locks: i32,
    pub share_max_partition_max_record_locks: i32,
    pub security: SecurityConfig,
}

#[derive(Clone)]
pub struct SecurityConfig {
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    pub scram_users: HashMap<String, String>,
    pub scram_iterations: u32,
    pub sasl_max_reauth_ms: i64,
    pub delegation_token_secret: Option<String>,
    pub delegation_token_max_lifetime_ms: i64,
    pub delegation_token_expiry_ms: i64,
    pub sasl_enabled: bool,
    pub acl_enabled: bool,
    pub allow_everyone_if_no_acl_found: bool,
    pub super_users: HashSet<String>,
}

impl fmt::Debug for SecurityConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut users = self.scram_users.keys().collect::<Vec<_>>();
        users.sort();
        formatter
            .debug_struct("SecurityConfig")
            .field("tls_cert_file", &self.tls_cert_file)
            .field("tls_key_file", &self.tls_key_file)
            .field("scram_users", &users)
            .field("scram_iterations", &self.scram_iterations)
            .field("sasl_max_reauth_ms", &self.sasl_max_reauth_ms)
            .field(
                "delegation_tokens_enabled",
                &self.delegation_token_secret.is_some(),
            )
            .field(
                "delegation_token_max_lifetime_ms",
                &self.delegation_token_max_lifetime_ms,
            )
            .field(
                "delegation_token_expiry_ms",
                &self.delegation_token_expiry_ms,
            )
            .field("sasl_enabled", &self.sasl_enabled)
            .field("acl_enabled", &self.acl_enabled)
            .field(
                "allow_everyone_if_no_acl_found",
                &self.allow_everyone_if_no_acl_found,
            )
            .field("super_users", &self.super_users)
            .finish()
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            tls_cert_file: None,
            tls_key_file: None,
            scram_users: HashMap::new(),
            scram_iterations: 4_096,
            sasl_max_reauth_ms: 0,
            delegation_token_secret: None,
            delegation_token_max_lifetime_ms: 7 * 24 * 60 * 60 * 1_000,
            delegation_token_expiry_ms: 24 * 60 * 60 * 1_000,
            sasl_enabled: false,
            acl_enabled: false,
            allow_everyone_if_no_acl_found: false,
            super_users: HashSet::new(),
        }
    }
}

impl SecurityConfig {
    pub fn tls_enabled(&self) -> bool {
        self.tls_cert_file.is_some()
    }

    pub fn sasl_enabled(&self) -> bool {
        self.sasl_enabled
    }
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            kafka_addr: "0.0.0.0:9092".parse().expect("valid default address"),
            admin_addr: "0.0.0.0:8080".parse().expect("valid default address"),
            advertise_host: "127.0.0.1".to_owned(),
            advertise_port: 9092,
            cluster_id: "rutomq-cluster".to_owned(),
            log_filter: DEFAULT_LOG_FILTER.to_owned(),
            num_partitions: 1,
            default_replication_factor: 1,
            auto_create_topics_enable: true,
            flush_interval: Duration::from_millis(250),
            max_batch_bytes: 8 * 1024 * 1024,
            max_frame_size: rutomq_protocol::MAX_FRAME_SIZE,
            max_inflight_request_bytes: 4 * rutomq_protocol::MAX_FRAME_SIZE,
            max_inflight_response_bytes: 4 * rutomq_protocol::MAX_FRAME_SIZE,
            connection_io_timeout: Duration::from_secs(30),
            max_connections: 1_024,
            max_fetch_bytes: 16 * 1024 * 1024,
            max_request_partition_size_limit: 2_000,
            fetch_cache_bytes: 256 * 1024 * 1024,
            telemetry_max_bytes: 1024 * 1024,
            observability_interval: Duration::from_secs(15),
            observability_max_groups: 1_000,
            consumer_lag_max_series: 10_000,
            partition_retention_max_series: 10_000,
            orphan_gc_interval: Duration::from_secs(60),
            orphan_gc_grace: Duration::from_secs(5 * 60),
            shutdown_grace: Duration::from_secs(25),
            retention_interval: Duration::from_secs(60),
            object_delete_grace: Duration::from_secs(5 * 60),
            compaction_interval: Duration::from_secs(60),
            compaction_lease: Duration::from_secs(5 * 60),
            compaction_max_object_bytes: 64 * 1024 * 1024,
            producer_id_expiration_ms: 24 * 60 * 60 * 1_000,
            producer_id_expiration_check_interval: Duration::from_secs(10 * 60),
            offset_metadata_max_bytes: 4_096,
            offsets_retention_minutes: 7 * 24 * 60,
            offsets_retention_check_interval: Duration::from_secs(10 * 60),
            transactional_id_expiration_ms: 7 * 24 * 60 * 60 * 1_000,
            transactional_id_expiration_check_interval: Duration::from_secs(60 * 60),
            transaction_abort_timed_out_cleanup_interval: Duration::from_secs(10),
            transaction_partition_verification_enable: true,
            transaction_max_timeout_ms: 15 * 60 * 1_000,
            transaction_two_phase_commit_enable: false,
            classic_group_initial_rebalance_delay_ms: 3_000,
            classic_group_min_session_timeout_ms: 6_000,
            classic_group_max_session_timeout_ms: 30 * 60 * 1_000,
            classic_group_max_size: i32::MAX,
            group_coordinator_background_threads: 2,
            group_coordinator_cached_buffer_max_bytes: 1_048_588,
            share_coordinator_cached_buffer_max_bytes: 1_048_588,
            consumer_assignor_offload_enable: true,
            share_assignor_offload_enable: true,
            streams_assignor_offload_enable: true,
            group_heartbeat_interval_ms: 5_000,
            consumer_group_min_heartbeat_interval_ms: 5_000,
            consumer_group_max_heartbeat_interval_ms: 15_000,
            group_session_timeout_ms: 45_000,
            consumer_group_min_session_timeout_ms: 45_000,
            consumer_group_max_session_timeout_ms: 60_000,
            consumer_group_max_size: i32::MAX,
            consumer_group_assignors: vec!["uniform".to_owned(), "range".to_owned()],
            consumer_group_regex_refresh_interval_ms: 10 * 60 * 1_000,
            group_assignment_interval_ms: 1_000,
            group_min_assignment_interval_ms: 0,
            group_max_assignment_interval_ms: 15_000,
            streams_group_heartbeat_interval_ms: 5_000,
            streams_group_min_heartbeat_interval_ms: 5_000,
            streams_group_max_heartbeat_interval_ms: 15_000,
            streams_group_session_timeout_ms: 45_000,
            streams_group_min_session_timeout_ms: 45_000,
            streams_group_max_session_timeout_ms: 60_000,
            streams_group_max_size: i32::MAX,
            streams_group_assignment_interval_ms: 1_000,
            streams_group_min_assignment_interval_ms: 0,
            streams_group_max_assignment_interval_ms: 15_000,
            streams_group_num_standby_replicas: 0,
            streams_group_max_standby_replicas: 2,
            streams_group_initial_rebalance_delay_ms: 3_000,
            streams_acceptable_recovery_lag: 10_000,
            streams_task_offset_interval_ms: 10_000,
            share_group_heartbeat_interval_ms: 5_000,
            share_group_min_heartbeat_interval_ms: 5_000,
            share_group_max_heartbeat_interval_ms: 15_000,
            share_group_session_timeout_ms: 45_000,
            share_group_min_session_timeout_ms: 45_000,
            share_group_max_session_timeout_ms: 60_000,
            share_group_max_size: 200,
            share_group_assignors: vec!["simple".to_owned()],
            share_group_assignment_interval_ms: 1_000,
            share_group_min_assignment_interval_ms: 0,
            share_group_max_assignment_interval_ms: 15_000,
            share_record_lock_duration_ms: 30_000,
            share_min_record_lock_duration_ms: 15_000,
            share_max_record_lock_duration_ms: 60_000,
            share_record_delivery_count_limit: 5,
            share_min_delivery_count_limit: 2,
            share_max_delivery_count_limit: 10,
            share_partition_max_record_locks: 2_000,
            share_min_partition_max_record_locks: 100,
            share_max_partition_max_record_locks: 4_000,
            security: SecurityConfig::default(),
        }
    }
}

impl AgentConfig {
    pub fn from_env() -> Result<Self> {
        let defaults = Self::default();
        let num_partitions = env_parse_result("RUTOMQ_NUM_PARTITIONS", defaults.num_partitions)?;
        let default_replication_factor = env_parse_result(
            "RUTOMQ_DEFAULT_REPLICATION_FACTOR",
            defaults.default_replication_factor,
        )?;
        validate_topic_creation_defaults(num_partitions, default_replication_factor)?;
        let classic_group_initial_rebalance_delay_ms = env_parse_result(
            "RUTOMQ_CLASSIC_GROUP_INITIAL_REBALANCE_DELAY_MS",
            defaults.classic_group_initial_rebalance_delay_ms,
        )?;
        if classic_group_initial_rebalance_delay_ms < 0 {
            bail!("RUTOMQ_CLASSIC_GROUP_INITIAL_REBALANCE_DELAY_MS must be non-negative");
        }
        let classic_group_min_session_timeout_ms = env_parse_result(
            "RUTOMQ_GROUP_MIN_SESSION_TIMEOUT_MS",
            defaults.classic_group_min_session_timeout_ms,
        )?;
        let classic_group_max_session_timeout_ms = env_parse_result(
            "RUTOMQ_GROUP_MAX_SESSION_TIMEOUT_MS",
            defaults.classic_group_max_session_timeout_ms,
        )?;
        validate_classic_group_session_timeout_bounds(
            classic_group_min_session_timeout_ms,
            classic_group_max_session_timeout_ms,
        )?;
        let classic_group_max_size =
            env_parse_result("RUTOMQ_GROUP_MAX_SIZE", defaults.classic_group_max_size)?;
        validate_group_max_size("classic", classic_group_max_size, i32::MAX)?;
        let group_coordinator_background_threads = env_parse_result(
            "RUTOMQ_GROUP_COORDINATOR_BACKGROUND_THREADS",
            defaults.group_coordinator_background_threads,
        )?;
        if group_coordinator_background_threads == 0 {
            bail!("RUTOMQ_GROUP_COORDINATOR_BACKGROUND_THREADS must be positive");
        }
        let group_coordinator_cached_buffer_max_bytes = env_parse_result(
            "RUTOMQ_GROUP_COORDINATOR_CACHED_BUFFER_MAX_BYTES",
            defaults.group_coordinator_cached_buffer_max_bytes,
        )?;
        let share_coordinator_cached_buffer_max_bytes = env_parse_result(
            "RUTOMQ_SHARE_COORDINATOR_CACHED_BUFFER_MAX_BYTES",
            defaults.share_coordinator_cached_buffer_max_bytes,
        )?;
        for (name, value) in [
            (
                "RUTOMQ_GROUP_COORDINATOR_CACHED_BUFFER_MAX_BYTES",
                group_coordinator_cached_buffer_max_bytes,
            ),
            (
                "RUTOMQ_SHARE_COORDINATOR_CACHED_BUFFER_MAX_BYTES",
                share_coordinator_cached_buffer_max_bytes,
            ),
        ] {
            if value < 524_288 {
                bail!("{name} must be at least 524288");
            }
        }
        let consumer_assignor_offload_enable = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_ASSIGNOR_OFFLOAD_ENABLE",
            defaults.consumer_assignor_offload_enable,
        )?;
        let share_assignor_offload_enable = env_parse_result(
            "RUTOMQ_GROUP_SHARE_ASSIGNOR_OFFLOAD_ENABLE",
            defaults.share_assignor_offload_enable,
        )?;
        let streams_assignor_offload_enable = env_parse_result(
            "RUTOMQ_GROUP_STREAMS_ASSIGNOR_OFFLOAD_ENABLE",
            defaults.streams_assignor_offload_enable,
        )?;
        let group_heartbeat_interval_ms = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_HEARTBEAT_INTERVAL_MS",
            defaults.group_heartbeat_interval_ms,
        )?;
        let consumer_group_min_heartbeat_interval_ms = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_MIN_HEARTBEAT_INTERVAL_MS",
            defaults.consumer_group_min_heartbeat_interval_ms,
        )?;
        let consumer_group_max_heartbeat_interval_ms = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_MAX_HEARTBEAT_INTERVAL_MS",
            defaults.consumer_group_max_heartbeat_interval_ms,
        )?;
        let group_session_timeout_ms = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_SESSION_TIMEOUT_MS",
            defaults.group_session_timeout_ms,
        )?;
        let consumer_group_min_session_timeout_ms = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_MIN_SESSION_TIMEOUT_MS",
            defaults.consumer_group_min_session_timeout_ms,
        )?;
        let consumer_group_max_session_timeout_ms = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_MAX_SESSION_TIMEOUT_MS",
            defaults.consumer_group_max_session_timeout_ms,
        )?;
        validate_group_timeout_bounds(
            "consumer",
            group_heartbeat_interval_ms,
            consumer_group_min_heartbeat_interval_ms,
            consumer_group_max_heartbeat_interval_ms,
            group_session_timeout_ms,
            consumer_group_min_session_timeout_ms,
            consumer_group_max_session_timeout_ms,
        )?;
        let consumer_group_max_size = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_MAX_SIZE",
            defaults.consumer_group_max_size,
        )?;
        validate_group_max_size("consumer", consumer_group_max_size, i32::MAX)?;
        let consumer_group_assignors = env_csv(
            "RUTOMQ_GROUP_CONSUMER_ASSIGNORS",
            &defaults.consumer_group_assignors,
        );
        validate_consumer_group_assignors(&consumer_group_assignors)?;
        let consumer_group_regex_refresh_interval_ms = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_REGEX_REFRESH_INTERVAL_MS",
            defaults.consumer_group_regex_refresh_interval_ms,
        )?;
        validate_consumer_regex_refresh_interval(consumer_group_regex_refresh_interval_ms)?;
        let group_assignment_interval_ms = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_ASSIGNMENT_INTERVAL_MS",
            defaults.group_assignment_interval_ms,
        )?;
        let group_min_assignment_interval_ms = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_MIN_ASSIGNMENT_INTERVAL_MS",
            defaults.group_min_assignment_interval_ms,
        )?;
        let group_max_assignment_interval_ms = env_parse_result(
            "RUTOMQ_GROUP_CONSUMER_MAX_ASSIGNMENT_INTERVAL_MS",
            defaults.group_max_assignment_interval_ms,
        )?;
        validate_assignment_interval(
            "consumer",
            group_assignment_interval_ms,
            group_min_assignment_interval_ms,
            group_max_assignment_interval_ms,
        )?;
        let streams_group_heartbeat_interval_ms = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_HEARTBEAT_INTERVAL_MS",
            defaults.streams_group_heartbeat_interval_ms,
        )?;
        let streams_group_min_heartbeat_interval_ms = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS",
            defaults.streams_group_min_heartbeat_interval_ms,
        )?;
        let streams_group_max_heartbeat_interval_ms = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS",
            defaults.streams_group_max_heartbeat_interval_ms,
        )?;
        let streams_group_session_timeout_ms = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_SESSION_TIMEOUT_MS",
            defaults.streams_group_session_timeout_ms,
        )?;
        let streams_group_min_session_timeout_ms = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS",
            defaults.streams_group_min_session_timeout_ms,
        )?;
        let streams_group_max_session_timeout_ms = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS",
            defaults.streams_group_max_session_timeout_ms,
        )?;
        validate_group_timeout_bounds(
            "streams",
            streams_group_heartbeat_interval_ms,
            streams_group_min_heartbeat_interval_ms,
            streams_group_max_heartbeat_interval_ms,
            streams_group_session_timeout_ms,
            streams_group_min_session_timeout_ms,
            streams_group_max_session_timeout_ms,
        )?;
        let streams_group_max_size = env_parse_result(
            "RUTOMQ_GROUP_STREAMS_MAX_SIZE",
            defaults.streams_group_max_size,
        )?;
        validate_group_max_size("streams", streams_group_max_size, i32::MAX)?;
        let streams_group_assignment_interval_ms = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_ASSIGNMENT_INTERVAL_MS",
            defaults.streams_group_assignment_interval_ms,
        )?;
        let streams_group_min_assignment_interval_ms = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_MIN_ASSIGNMENT_INTERVAL_MS",
            defaults.streams_group_min_assignment_interval_ms,
        )?;
        let streams_group_max_assignment_interval_ms = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_MAX_ASSIGNMENT_INTERVAL_MS",
            defaults.streams_group_max_assignment_interval_ms,
        )?;
        validate_assignment_interval(
            "streams",
            streams_group_assignment_interval_ms,
            streams_group_min_assignment_interval_ms,
            streams_group_max_assignment_interval_ms,
        )?;
        let streams_group_num_standby_replicas = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_NUM_STANDBY_REPLICAS",
            defaults.streams_group_num_standby_replicas,
        )?;
        let streams_group_max_standby_replicas = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_MAX_STANDBY_REPLICAS",
            defaults.streams_group_max_standby_replicas,
        )?;
        validate_streams_standby_replicas(
            streams_group_num_standby_replicas,
            streams_group_max_standby_replicas,
        )?;
        let streams_group_initial_rebalance_delay_ms = env_parse_result(
            "RUTOMQ_STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS",
            defaults.streams_group_initial_rebalance_delay_ms,
        )?;
        let streams_acceptable_recovery_lag = env_parse_result(
            "RUTOMQ_STREAMS_ACCEPTABLE_RECOVERY_LAG",
            defaults.streams_acceptable_recovery_lag,
        )?;
        let streams_task_offset_interval_ms = env_parse_result(
            "RUTOMQ_STREAMS_TASK_OFFSET_INTERVAL_MS",
            defaults.streams_task_offset_interval_ms,
        )?;
        if streams_group_initial_rebalance_delay_ms < 0
            || streams_acceptable_recovery_lag < 0
            || streams_task_offset_interval_ms <= 0
        {
            bail!(
                "streams group initial delay and recovery lag must be non-negative, and task offset interval must be positive"
            );
        }
        let share_group_heartbeat_interval_ms = env_parse_result(
            "RUTOMQ_SHARE_GROUP_HEARTBEAT_INTERVAL_MS",
            defaults.share_group_heartbeat_interval_ms,
        )?;
        let share_group_min_heartbeat_interval_ms = env_parse_result(
            "RUTOMQ_SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS",
            defaults.share_group_min_heartbeat_interval_ms,
        )?;
        let share_group_max_heartbeat_interval_ms = env_parse_result(
            "RUTOMQ_SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS",
            defaults.share_group_max_heartbeat_interval_ms,
        )?;
        let share_group_session_timeout_ms = env_parse_result(
            "RUTOMQ_SHARE_GROUP_SESSION_TIMEOUT_MS",
            defaults.share_group_session_timeout_ms,
        )?;
        let share_group_min_session_timeout_ms = env_parse_result(
            "RUTOMQ_SHARE_GROUP_MIN_SESSION_TIMEOUT_MS",
            defaults.share_group_min_session_timeout_ms,
        )?;
        let share_group_max_session_timeout_ms = env_parse_result(
            "RUTOMQ_SHARE_GROUP_MAX_SESSION_TIMEOUT_MS",
            defaults.share_group_max_session_timeout_ms,
        )?;
        validate_group_timeout_bounds(
            "share",
            share_group_heartbeat_interval_ms,
            share_group_min_heartbeat_interval_ms,
            share_group_max_heartbeat_interval_ms,
            share_group_session_timeout_ms,
            share_group_min_session_timeout_ms,
            share_group_max_session_timeout_ms,
        )?;
        let share_group_max_size =
            env_parse_result("RUTOMQ_SHARE_GROUP_MAX_SIZE", defaults.share_group_max_size)?;
        validate_group_max_size("share", share_group_max_size, 1_000)?;
        let share_group_assignors = env_csv(
            "RUTOMQ_GROUP_SHARE_ASSIGNORS",
            &defaults.share_group_assignors,
        );
        validate_share_group_assignors(&share_group_assignors)?;
        let share_group_assignment_interval_ms = env_parse_result(
            "RUTOMQ_SHARE_GROUP_ASSIGNMENT_INTERVAL_MS",
            defaults.share_group_assignment_interval_ms,
        )?;
        let share_group_min_assignment_interval_ms = env_parse_result(
            "RUTOMQ_SHARE_GROUP_MIN_ASSIGNMENT_INTERVAL_MS",
            defaults.share_group_min_assignment_interval_ms,
        )?;
        let share_group_max_assignment_interval_ms = env_parse_result(
            "RUTOMQ_SHARE_GROUP_MAX_ASSIGNMENT_INTERVAL_MS",
            defaults.share_group_max_assignment_interval_ms,
        )?;
        validate_assignment_interval(
            "share",
            share_group_assignment_interval_ms,
            share_group_min_assignment_interval_ms,
            share_group_max_assignment_interval_ms,
        )?;
        let share_record_lock_duration_ms = env_parse_result(
            "RUTOMQ_SHARE_RECORD_LOCK_DURATION_MS",
            defaults.share_record_lock_duration_ms,
        )?;
        let share_min_record_lock_duration_ms = env_parse_result(
            "RUTOMQ_SHARE_MIN_RECORD_LOCK_DURATION_MS",
            defaults.share_min_record_lock_duration_ms,
        )?;
        let share_max_record_lock_duration_ms = env_parse_result(
            "RUTOMQ_SHARE_MAX_RECORD_LOCK_DURATION_MS",
            defaults.share_max_record_lock_duration_ms,
        )?;
        validate_share_record_lock_duration(
            share_record_lock_duration_ms,
            share_min_record_lock_duration_ms,
            share_max_record_lock_duration_ms,
        )?;
        let share_record_delivery_count_limit = env_parse_result(
            "RUTOMQ_SHARE_RECORD_DELIVERY_COUNT_LIMIT",
            defaults.share_record_delivery_count_limit,
        )?;
        let share_min_delivery_count_limit = env_parse_result(
            "RUTOMQ_SHARE_MIN_DELIVERY_COUNT_LIMIT",
            defaults.share_min_delivery_count_limit,
        )?;
        let share_max_delivery_count_limit = env_parse_result(
            "RUTOMQ_SHARE_MAX_DELIVERY_COUNT_LIMIT",
            defaults.share_max_delivery_count_limit,
        )?;
        if !(2..=10).contains(&share_record_delivery_count_limit)
            || !(2..=5).contains(&share_min_delivery_count_limit)
            || !(5..=25).contains(&share_max_delivery_count_limit)
            || !(share_min_delivery_count_limit..=share_max_delivery_count_limit)
                .contains(&share_record_delivery_count_limit)
        {
            bail!(
                "share delivery count defaults and bounds must satisfy Kafka limits and min <= default <= max"
            );
        }
        let share_partition_max_record_locks = env_parse_result(
            "RUTOMQ_SHARE_PARTITION_MAX_RECORD_LOCKS",
            defaults.share_partition_max_record_locks,
        )?;
        let share_min_partition_max_record_locks = env_parse_result(
            "RUTOMQ_SHARE_MIN_PARTITION_MAX_RECORD_LOCKS",
            defaults.share_min_partition_max_record_locks,
        )?;
        let share_max_partition_max_record_locks = env_parse_result(
            "RUTOMQ_SHARE_MAX_PARTITION_MAX_RECORD_LOCKS",
            defaults.share_max_partition_max_record_locks,
        )?;
        if !(100..=10_000).contains(&share_partition_max_record_locks)
            || !(100..=2_000).contains(&share_min_partition_max_record_locks)
            || !(2_000..=10_000).contains(&share_max_partition_max_record_locks)
            || !(share_min_partition_max_record_locks..=share_max_partition_max_record_locks)
                .contains(&share_partition_max_record_locks)
        {
            bail!(
                "share partition record-lock defaults and bounds must satisfy Kafka limits and min <= default <= max"
            );
        }
        let telemetry_max_bytes =
            env_parse_result("RUTOMQ_TELEMETRY_MAX_BYTES", defaults.telemetry_max_bytes)?;
        if telemetry_max_bytes == 0 || telemetry_max_bytes > i32::MAX as usize {
            bail!("RUTOMQ_TELEMETRY_MAX_BYTES must be between 1 and 2147483647");
        }
        let compaction_max_object_bytes = env_parse_result(
            "RUTOMQ_COMPACTION_MAX_OBJECT_BYTES",
            defaults.compaction_max_object_bytes,
        )?;
        if compaction_max_object_bytes == 0 {
            bail!("RUTOMQ_COMPACTION_MAX_OBJECT_BYTES must be positive");
        }
        let producer_id_expiration_ms = env_parse_result(
            "RUTOMQ_PRODUCER_ID_EXPIRATION_MS",
            defaults.producer_id_expiration_ms,
        )?;
        if !(1..=i64::from(i32::MAX)).contains(&producer_id_expiration_ms) {
            bail!("RUTOMQ_PRODUCER_ID_EXPIRATION_MS must be between 1 and 2147483647");
        }
        let producer_id_expiration_check_interval_ms = env_parse_result(
            "RUTOMQ_PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS",
            defaults.producer_id_expiration_check_interval.as_millis() as u64,
        )?;
        if producer_id_expiration_check_interval_ms == 0 {
            bail!("RUTOMQ_PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS must be positive");
        }
        let offset_metadata_max_bytes = env_parse_result(
            "RUTOMQ_OFFSET_METADATA_MAX_BYTES",
            defaults.offset_metadata_max_bytes,
        )?;
        let offsets_retention_minutes = env_parse_result(
            "RUTOMQ_OFFSETS_RETENTION_MINUTES",
            defaults.offsets_retention_minutes,
        )?;
        let offsets_retention_check_interval_ms = env_parse_result(
            "RUTOMQ_OFFSETS_RETENTION_CHECK_INTERVAL_MS",
            defaults.offsets_retention_check_interval.as_millis() as u64,
        )?;
        validate_offset_retention(
            offsets_retention_minutes,
            offsets_retention_check_interval_ms,
        )?;
        let transactional_id_expiration_ms = env_parse_result(
            "RUTOMQ_TRANSACTIONAL_ID_EXPIRATION_MS",
            defaults.transactional_id_expiration_ms,
        )?;
        if !(1..=i64::from(i32::MAX)).contains(&transactional_id_expiration_ms) {
            bail!("RUTOMQ_TRANSACTIONAL_ID_EXPIRATION_MS must be between 1 and 2147483647");
        }
        let transactional_id_expiration_check_interval_ms = env_parse_result(
            "RUTOMQ_TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS",
            defaults
                .transactional_id_expiration_check_interval
                .as_millis() as u64,
        )?;
        if !(1..=i32::MAX as u64).contains(&transactional_id_expiration_check_interval_ms) {
            bail!(
                "RUTOMQ_TRANSACTION_REMOVE_EXPIRED_TRANSACTION_CLEANUP_INTERVAL_MS must be between 1 and 2147483647"
            );
        }
        let transaction_abort_timed_out_cleanup_interval_ms = env_parse_result(
            "RUTOMQ_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS",
            defaults
                .transaction_abort_timed_out_cleanup_interval
                .as_millis() as u64,
        )?;
        if !(1..=i32::MAX as u64).contains(&transaction_abort_timed_out_cleanup_interval_ms) {
            bail!(
                "RUTOMQ_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS must be between 1 and 2147483647"
            );
        }
        let transaction_max_timeout_ms = env_parse_result(
            "RUTOMQ_TRANSACTION_MAX_TIMEOUT_MS",
            defaults.transaction_max_timeout_ms,
        )?;
        if transaction_max_timeout_ms <= 0 {
            bail!("RUTOMQ_TRANSACTION_MAX_TIMEOUT_MS must be positive");
        }
        let shutdown_grace_ms = env_parse_result(
            "RUTOMQ_SHUTDOWN_GRACE_MS",
            defaults.shutdown_grace.as_millis() as u64,
        )?;
        if shutdown_grace_ms == 0 {
            bail!("RUTOMQ_SHUTDOWN_GRACE_MS must be positive");
        }
        let fetch_cache_bytes =
            env_parse_result("RUTOMQ_FETCH_CACHE_BYTES", defaults.fetch_cache_bytes)?;
        let max_frame_size = env_parse_result("RUTOMQ_MAX_FRAME_SIZE", defaults.max_frame_size)?;
        if !(1..=i32::MAX as usize).contains(&max_frame_size) {
            bail!("RUTOMQ_MAX_FRAME_SIZE must be between 1 and 2147483647");
        }
        let max_inflight_request_bytes = env_parse_result(
            "RUTOMQ_MAX_INFLIGHT_REQUEST_BYTES",
            defaults.max_inflight_request_bytes,
        )?;
        let max_inflight_response_bytes = env_parse_result(
            "RUTOMQ_MAX_INFLIGHT_RESPONSE_BYTES",
            defaults.max_inflight_response_bytes,
        )?;
        let connection_io_timeout_ms = env_parse_result(
            "RUTOMQ_CONNECTION_IO_TIMEOUT_MS",
            defaults.connection_io_timeout.as_millis() as u64,
        )?;
        validate_io_resource_limits(
            max_frame_size,
            max_inflight_request_bytes,
            max_inflight_response_bytes,
            connection_io_timeout_ms,
        )?;
        let max_connections = env_parse_result("RUTOMQ_MAX_CONNECTIONS", defaults.max_connections)?;
        if max_connections == 0 || max_connections > tokio::sync::Semaphore::MAX_PERMITS {
            bail!("RUTOMQ_MAX_CONNECTIONS is outside the supported range");
        }
        let max_request_partition_size_limit = env_parse_result(
            "RUTOMQ_MAX_REQUEST_PARTITION_SIZE_LIMIT",
            defaults.max_request_partition_size_limit,
        )?;
        if max_request_partition_size_limit <= 0 {
            bail!("RUTOMQ_MAX_REQUEST_PARTITION_SIZE_LIMIT must be positive");
        }
        let observability_interval_ms = env_parse_result(
            "RUTOMQ_OBSERVABILITY_INTERVAL_MS",
            defaults.observability_interval.as_millis() as u64,
        )?;
        let observability_max_groups = env_parse_result(
            "RUTOMQ_OBSERVABILITY_MAX_GROUPS",
            defaults.observability_max_groups,
        )?;
        let consumer_lag_max_series = env_parse_result(
            "RUTOMQ_CONSUMER_LAG_MAX_SERIES",
            defaults.consumer_lag_max_series,
        )?;
        let partition_retention_max_series = env_parse_result(
            "RUTOMQ_PARTITION_RETENTION_MAX_SERIES",
            defaults.partition_retention_max_series,
        )?;
        if observability_interval_ms == 0
            || observability_max_groups == 0
            || consumer_lag_max_series == 0
            || partition_retention_max_series == 0
        {
            bail!("observability interval and metric cardinality bounds must be positive");
        }
        Ok(Self {
            kafka_addr: env_addr("KAFKA_LISTEN_ADDR", defaults.kafka_addr)?,
            admin_addr: env_addr("ADMIN_LISTEN_ADDR", defaults.admin_addr)?,
            advertise_host: env_string("KAFKA_ADVERTISE_HOST", defaults.advertise_host),
            advertise_port: env_parse("KAFKA_ADVERTISE_PORT", defaults.advertise_port),
            cluster_id: env_string("RUTOMQ_CLUSTER_ID", defaults.cluster_id),
            log_filter: env_string("RUST_LOG", defaults.log_filter),
            num_partitions,
            default_replication_factor,
            auto_create_topics_enable: env_parse_result(
                "RUTOMQ_AUTO_CREATE_TOPICS_ENABLE",
                defaults.auto_create_topics_enable,
            )?,
            flush_interval: Duration::from_millis(env_parse(
                "RUTOMQ_FLUSH_INTERVAL_MS",
                defaults.flush_interval.as_millis() as u64,
            )),
            max_batch_bytes: env_parse("RUTOMQ_MAX_BATCH_BYTES", defaults.max_batch_bytes),
            max_frame_size,
            max_inflight_request_bytes,
            max_inflight_response_bytes,
            connection_io_timeout: Duration::from_millis(connection_io_timeout_ms),
            max_connections,
            max_fetch_bytes: env_parse("RUTOMQ_MAX_FETCH_BYTES", defaults.max_fetch_bytes),
            max_request_partition_size_limit,
            fetch_cache_bytes,
            telemetry_max_bytes,
            observability_interval: Duration::from_millis(observability_interval_ms),
            observability_max_groups,
            consumer_lag_max_series,
            partition_retention_max_series,
            orphan_gc_interval: Duration::from_millis(env_parse(
                "RUTOMQ_ORPHAN_GC_INTERVAL_MS",
                defaults.orphan_gc_interval.as_millis() as u64,
            )),
            orphan_gc_grace: Duration::from_millis(env_parse(
                "RUTOMQ_ORPHAN_GC_GRACE_MS",
                defaults.orphan_gc_grace.as_millis() as u64,
            )),
            shutdown_grace: Duration::from_millis(shutdown_grace_ms),
            retention_interval: Duration::from_millis(env_parse(
                "RUTOMQ_RETENTION_INTERVAL_MS",
                defaults.retention_interval.as_millis() as u64,
            )),
            object_delete_grace: Duration::from_millis(env_parse(
                "RUTOMQ_OBJECT_DELETE_GRACE_MS",
                defaults.object_delete_grace.as_millis() as u64,
            )),
            compaction_interval: Duration::from_millis(env_parse(
                "RUTOMQ_COMPACTION_INTERVAL_MS",
                defaults.compaction_interval.as_millis() as u64,
            )),
            compaction_lease: Duration::from_millis(env_parse(
                "RUTOMQ_COMPACTION_LEASE_MS",
                defaults.compaction_lease.as_millis() as u64,
            )),
            compaction_max_object_bytes,
            producer_id_expiration_ms,
            producer_id_expiration_check_interval: Duration::from_millis(
                producer_id_expiration_check_interval_ms,
            ),
            offset_metadata_max_bytes,
            offsets_retention_minutes,
            offsets_retention_check_interval: Duration::from_millis(
                offsets_retention_check_interval_ms,
            ),
            transactional_id_expiration_ms,
            transactional_id_expiration_check_interval: Duration::from_millis(
                transactional_id_expiration_check_interval_ms,
            ),
            transaction_abort_timed_out_cleanup_interval: Duration::from_millis(
                transaction_abort_timed_out_cleanup_interval_ms,
            ),
            transaction_partition_verification_enable: env_parse_result(
                "RUTOMQ_TRANSACTION_PARTITION_VERIFICATION_ENABLE",
                defaults.transaction_partition_verification_enable,
            )?,
            transaction_max_timeout_ms,
            transaction_two_phase_commit_enable: env_parse_result(
                "RUTOMQ_TRANSACTION_TWO_PHASE_COMMIT_ENABLE",
                defaults.transaction_two_phase_commit_enable,
            )?,
            classic_group_initial_rebalance_delay_ms,
            classic_group_min_session_timeout_ms,
            classic_group_max_session_timeout_ms,
            classic_group_max_size,
            group_coordinator_background_threads,
            group_coordinator_cached_buffer_max_bytes,
            share_coordinator_cached_buffer_max_bytes,
            consumer_assignor_offload_enable,
            share_assignor_offload_enable,
            streams_assignor_offload_enable,
            group_heartbeat_interval_ms,
            consumer_group_min_heartbeat_interval_ms,
            consumer_group_max_heartbeat_interval_ms,
            group_session_timeout_ms,
            consumer_group_min_session_timeout_ms,
            consumer_group_max_session_timeout_ms,
            consumer_group_max_size,
            consumer_group_assignors,
            consumer_group_regex_refresh_interval_ms,
            group_assignment_interval_ms,
            group_min_assignment_interval_ms,
            group_max_assignment_interval_ms,
            streams_group_heartbeat_interval_ms,
            streams_group_min_heartbeat_interval_ms,
            streams_group_max_heartbeat_interval_ms,
            streams_group_session_timeout_ms,
            streams_group_min_session_timeout_ms,
            streams_group_max_session_timeout_ms,
            streams_group_max_size,
            streams_group_assignment_interval_ms,
            streams_group_min_assignment_interval_ms,
            streams_group_max_assignment_interval_ms,
            streams_group_num_standby_replicas,
            streams_group_max_standby_replicas,
            streams_group_initial_rebalance_delay_ms,
            streams_acceptable_recovery_lag,
            streams_task_offset_interval_ms,
            share_group_heartbeat_interval_ms,
            share_group_min_heartbeat_interval_ms,
            share_group_max_heartbeat_interval_ms,
            share_group_session_timeout_ms,
            share_group_min_session_timeout_ms,
            share_group_max_session_timeout_ms,
            share_group_max_size,
            share_group_assignors,
            share_group_assignment_interval_ms,
            share_group_min_assignment_interval_ms,
            share_group_max_assignment_interval_ms,
            share_record_lock_duration_ms,
            share_min_record_lock_duration_ms,
            share_max_record_lock_duration_ms,
            share_record_delivery_count_limit,
            share_min_delivery_count_limit,
            share_max_delivery_count_limit,
            share_partition_max_record_locks,
            share_min_partition_max_record_locks,
            share_max_partition_max_record_locks,
            security: security_from_env(defaults.security)?,
        })
    }
}

fn validate_topic_creation_defaults(
    num_partitions: i32,
    default_replication_factor: i16,
) -> Result<()> {
    if num_partitions <= 0 {
        bail!("RUTOMQ_NUM_PARTITIONS must be positive");
    }
    if default_replication_factor != 1 {
        bail!("RUTOMQ_DEFAULT_REPLICATION_FACTOR must be 1 for the one-replica virtual topology");
    }
    Ok(())
}

fn validate_assignment_interval(
    protocol: &str,
    default_ms: i32,
    minimum_ms: i32,
    maximum_ms: i32,
) -> Result<()> {
    if minimum_ms < 0 || maximum_ms < minimum_ms || !(minimum_ms..=maximum_ms).contains(&default_ms)
    {
        bail!(
            "{protocol} assignment interval bounds must be non-negative and satisfy min <= default <= max"
        );
    }
    Ok(())
}

fn security_from_env(defaults: SecurityConfig) -> Result<SecurityConfig> {
    let tls_cert_file = env_path("KAFKA_TLS_CERT_FILE");
    let tls_key_file = env_path("KAFKA_TLS_KEY_FILE");
    if tls_cert_file.is_some() != tls_key_file.is_some() {
        bail!("KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE must be configured together");
    }

    let scram_users = match std::env::var("RUTOMQ_SCRAM_USERS_JSON") {
        Ok(value) if !value.trim().is_empty() => serde_json::from_str::<HashMap<String, String>>(
            &value,
        )
        .context("RUTOMQ_SCRAM_USERS_JSON must be a JSON object of username/password pairs")?,
        _ => HashMap::new(),
    };
    if scram_users
        .iter()
        .any(|(username, password)| username.is_empty() || password.is_empty())
    {
        bail!("SCRAM usernames and passwords must not be empty");
    }
    let scram_iterations = env_parse_result(
        "RUTOMQ_SCRAM_ITERATIONS",
        defaults.scram_iterations.max(4_096),
    )?;
    if !(4_096..=1_000_000).contains(&scram_iterations) {
        bail!("RUTOMQ_SCRAM_ITERATIONS must be between 4096 and 1000000");
    }
    let sasl_enabled = env_parse_result("RUTOMQ_SASL_ENABLED", !scram_users.is_empty())?;
    if !sasl_enabled && !scram_users.is_empty() {
        bail!("RUTOMQ_SASL_ENABLED=false cannot be combined with SCRAM users");
    }
    let sasl_max_reauth_ms =
        env_parse_result("RUTOMQ_SASL_MAX_REAUTH_MS", defaults.sasl_max_reauth_ms)?;
    validate_sasl_max_reauth_ms(sasl_max_reauth_ms)?;
    let acl_enabled = env_parse_result("RUTOMQ_ACL_ENABLED", defaults.acl_enabled)?;
    let allow_everyone_if_no_acl_found = env_parse_result(
        "RUTOMQ_ALLOW_EVERYONE_IF_NO_ACL_FOUND",
        defaults.allow_everyone_if_no_acl_found,
    )?;
    let super_users = std::env::var("RUTOMQ_SUPER_USERS")
        .ok()
        .into_iter()
        .flat_map(|value| {
            value
                .split(';')
                .map(str::trim)
                .filter(|principal| !principal.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect::<HashSet<_>>();
    if super_users.iter().any(|principal| !principal.contains(':')) {
        bail!("RUTOMQ_SUPER_USERS entries must use PrincipalType:name form");
    }
    if acl_enabled && super_users.is_empty() && !allow_everyone_if_no_acl_found {
        bail!(
            "RUTOMQ_ACL_ENABLED requires RUTOMQ_SUPER_USERS or RUTOMQ_ALLOW_EVERYONE_IF_NO_ACL_FOUND=true"
        );
    }
    let delegation_token_secret = std::env::var("RUTOMQ_DELEGATION_TOKEN_SECRET")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let delegation_token_max_lifetime_ms = env_parse_result(
        "RUTOMQ_DELEGATION_TOKEN_MAX_LIFETIME_MS",
        defaults.delegation_token_max_lifetime_ms,
    )?;
    let delegation_token_expiry_ms = env_parse_result(
        "RUTOMQ_DELEGATION_TOKEN_EXPIRY_MS",
        defaults.delegation_token_expiry_ms,
    )?;
    if delegation_token_max_lifetime_ms <= 0 || delegation_token_expiry_ms <= 0 {
        bail!("delegation token lifetime and expiry must be positive");
    }

    Ok(SecurityConfig {
        tls_cert_file,
        tls_key_file,
        scram_users,
        scram_iterations,
        sasl_max_reauth_ms,
        delegation_token_secret,
        delegation_token_max_lifetime_ms,
        delegation_token_expiry_ms,
        sasl_enabled,
        acl_enabled,
        allow_everyone_if_no_acl_found,
        super_users,
    })
}

fn validate_sasl_max_reauth_ms(value: i64) -> Result<()> {
    if value < 0 {
        bail!("RUTOMQ_SASL_MAX_REAUTH_MS must be non-negative");
    }
    Ok(())
}

fn validate_offset_retention(minutes: i32, check_interval_ms: u64) -> Result<()> {
    if minutes <= 0 {
        bail!("RUTOMQ_OFFSETS_RETENTION_MINUTES must be positive");
    }
    if check_interval_ms == 0 {
        bail!("RUTOMQ_OFFSETS_RETENTION_CHECK_INTERVAL_MS must be positive");
    }
    Ok(())
}

fn validate_classic_group_session_timeout_bounds(minimum_ms: i32, maximum_ms: i32) -> Result<()> {
    if minimum_ms <= 0 || maximum_ms <= 0 {
        bail!("classic group session timeout bounds must be positive");
    }
    if minimum_ms > maximum_ms {
        bail!("classic group minimum session timeout must not exceed the maximum");
    }
    Ok(())
}

fn validate_group_timeout_bounds(
    protocol: &str,
    heartbeat_interval_ms: i32,
    minimum_heartbeat_interval_ms: i32,
    maximum_heartbeat_interval_ms: i32,
    session_timeout_ms: i32,
    minimum_session_timeout_ms: i32,
    maximum_session_timeout_ms: i32,
) -> Result<()> {
    if [
        heartbeat_interval_ms,
        minimum_heartbeat_interval_ms,
        maximum_heartbeat_interval_ms,
        session_timeout_ms,
        minimum_session_timeout_ms,
        maximum_session_timeout_ms,
    ]
    .into_iter()
    .any(|value| value <= 0)
    {
        bail!("{protocol} group heartbeat and session timeout settings must be positive");
    }
    if !(minimum_heartbeat_interval_ms..=maximum_heartbeat_interval_ms)
        .contains(&heartbeat_interval_ms)
    {
        bail!("{protocol} group heartbeat interval must be within its configured bounds");
    }
    if !(minimum_session_timeout_ms..=maximum_session_timeout_ms).contains(&session_timeout_ms) {
        bail!("{protocol} group session timeout must be within its configured bounds");
    }
    if heartbeat_interval_ms >= session_timeout_ms {
        bail!("{protocol} group heartbeat interval must be lower than the session timeout");
    }
    Ok(())
}

fn validate_streams_standby_replicas(default: i32, maximum: i32) -> Result<()> {
    if maximum < 0 || !(0..=maximum).contains(&default) {
        bail!("streams standby replicas must satisfy 0 <= default <= max");
    }
    Ok(())
}

fn validate_group_max_size(protocol: &str, value: i32, hard_maximum: i32) -> Result<()> {
    if !(1..=hard_maximum).contains(&value) {
        bail!("{protocol} group maximum size must be between 1 and {hard_maximum}");
    }
    Ok(())
}

fn validate_io_resource_limits(
    max_frame_size: usize,
    max_inflight_request_bytes: usize,
    max_inflight_response_bytes: usize,
    connection_io_timeout_ms: u64,
) -> Result<()> {
    let minimum_request_budget = max_frame_size
        .checked_mul(2)
        .context("RUTOMQ_MAX_FRAME_SIZE is too large for the request memory budget")?;
    if !(minimum_request_budget..=tokio::sync::Semaphore::MAX_PERMITS)
        .contains(&max_inflight_request_bytes)
    {
        bail!(
            "RUTOMQ_MAX_INFLIGHT_REQUEST_BYTES must be at least twice RUTOMQ_MAX_FRAME_SIZE and within the supported semaphore range"
        );
    }
    let minimum_response_budget = rutomq_protocol::MAX_FRAME_SIZE
        .checked_add(4)
        .and_then(|size| size.checked_mul(2))
        .context("Kafka response frame size is too large for the response memory budget")?;
    if !(minimum_response_budget..=tokio::sync::Semaphore::MAX_PERMITS)
        .contains(&max_inflight_response_bytes)
    {
        bail!(
            "RUTOMQ_MAX_INFLIGHT_RESPONSE_BYTES must hold at least two maximum-size response frames and remain within the supported semaphore range"
        );
    }
    if connection_io_timeout_ms == 0 {
        bail!("RUTOMQ_CONNECTION_IO_TIMEOUT_MS must be positive");
    }
    Ok(())
}

fn validate_consumer_group_assignors(assignors: &[String]) -> Result<()> {
    let mut seen = HashSet::new();
    if assignors.is_empty()
        || assignors
            .iter()
            .any(|assignor| !matches!(assignor.as_str(), "uniform" | "range"))
        || assignors
            .iter()
            .any(|assignor| !seen.insert(assignor.as_str()))
    {
        bail!(
            "consumer group assignors must be a non-empty, duplicate-free list of uniform and range"
        );
    }
    Ok(())
}

fn validate_consumer_regex_refresh_interval(interval_ms: i32) -> Result<()> {
    if interval_ms < 10_000 {
        bail!("consumer group regex refresh interval must be at least 10000 ms");
    }
    Ok(())
}

fn validate_share_group_assignors(assignors: &[String]) -> Result<()> {
    if assignors != ["simple"] {
        bail!("share group assignors must contain exactly the built-in simple assignor");
    }
    Ok(())
}

fn validate_share_record_lock_duration(default: i32, minimum: i32, maximum: i32) -> Result<()> {
    if !(1_000..=3_600_000).contains(&default)
        || !(1_000..=30_000).contains(&minimum)
        || !(30_000..=3_600_000).contains(&maximum)
        || !(minimum..=maximum).contains(&default)
    {
        bail!(
            "share record lock duration settings must satisfy Kafka hard ranges and min <= default <= max"
        );
    }
    Ok(())
}

fn env_string(name: &str, default: String) -> String {
    std::env::var(name).unwrap_or(default)
}

fn env_csv(name: &str, default: &[String]) -> Vec<String> {
    std::env::var(name).map_or_else(
        |_| default.to_vec(),
        |value| {
            value
                .split(',')
                .map(|item| item.trim().to_owned())
                .collect()
        },
    )
}

fn env_parse<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr + Copy,
{
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_addr(name: &str, default: SocketAddr) -> Result<SocketAddr> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} has an invalid socket address")),
        Err(_) => Ok(default),
    }
}

fn env_path(name: &str) -> Option<PathBuf> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from)
}

fn env_parse_result<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} has an invalid value")),
        Err(_) => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        validate_classic_group_session_timeout_bounds, validate_consumer_group_assignors,
        validate_consumer_regex_refresh_interval, validate_group_max_size,
        validate_group_timeout_bounds, validate_io_resource_limits, validate_offset_retention,
        validate_sasl_max_reauth_ms, validate_share_group_assignors,
        validate_share_record_lock_duration, validate_streams_standby_replicas,
        validate_topic_creation_defaults,
    };

    #[test]
    fn virtual_controller_defaults_require_a_positive_single_replica_topology() {
        assert!(validate_topic_creation_defaults(3, 1).is_ok());
        assert!(validate_topic_creation_defaults(0, 1).is_err());
        assert!(validate_topic_creation_defaults(1, 2).is_err());
    }

    #[test]
    fn sasl_max_reauthentication_interval_may_be_disabled_but_not_negative() {
        assert!(validate_sasl_max_reauth_ms(0).is_ok());
        assert!(validate_sasl_max_reauth_ms(1).is_ok());
        assert!(validate_sasl_max_reauth_ms(-1).is_err());
    }

    #[test]
    fn consumer_offset_retention_requires_positive_duration_and_sweep_interval() {
        assert!(validate_offset_retention(10_080, 600_000).is_ok());
        assert!(validate_offset_retention(0, 600_000).is_err());
        assert!(validate_offset_retention(10_080, 0).is_err());
    }

    #[test]
    fn classic_group_session_timeout_bounds_are_positive_and_ordered() {
        assert!(validate_classic_group_session_timeout_bounds(6_000, 1_800_000).is_ok());
        assert!(validate_classic_group_session_timeout_bounds(0, 1_800_000).is_err());
        assert!(validate_classic_group_session_timeout_bounds(6_000, 0).is_err());
        assert!(validate_classic_group_session_timeout_bounds(10_000, 9_999).is_err());
    }

    #[test]
    fn group_timeout_defaults_must_be_positive_ordered_and_in_bounds() {
        assert!(
            validate_group_timeout_bounds("consumer", 5_000, 5_000, 15_000, 45_000, 45_000, 60_000)
                .is_ok()
        );
        assert!(
            validate_group_timeout_bounds("consumer", 4_999, 5_000, 15_000, 45_000, 45_000, 60_000)
                .is_err()
        );
        assert!(
            validate_group_timeout_bounds("consumer", 5_000, 15_000, 5_000, 45_000, 45_000, 60_000)
                .is_err()
        );
        assert!(
            validate_group_timeout_bounds("share", 5_000, 5_000, 15_000, 60_001, 45_000, 60_000)
                .is_err()
        );
        assert!(
            validate_group_timeout_bounds("share", 5_000, 1, 10_000, 5_000, 1, 10_000).is_err()
        );
        assert!(
            validate_group_timeout_bounds("streams", 15_000, 5_000, 15_000, 60_000, 45_000, 60_000)
                .is_ok()
        );
        assert!(
            validate_group_timeout_bounds("streams", 15_001, 5_000, 15_000, 60_000, 45_000, 60_000)
                .is_err()
        );
    }

    #[test]
    fn streams_standby_default_must_not_exceed_the_broker_maximum() {
        assert!(validate_streams_standby_replicas(0, 2).is_ok());
        assert!(validate_streams_standby_replicas(2, 2).is_ok());
        assert!(validate_streams_standby_replicas(-1, 2).is_err());
        assert!(validate_streams_standby_replicas(3, 2).is_err());
        assert!(validate_streams_standby_replicas(0, -1).is_err());
    }

    #[test]
    fn group_max_sizes_match_kafka_ranges() {
        assert!(validate_group_max_size("classic", 1, i32::MAX).is_ok());
        assert!(validate_group_max_size("classic", i32::MAX, i32::MAX).is_ok());
        assert!(validate_group_max_size("classic", 0, i32::MAX).is_err());
        assert!(validate_group_max_size("consumer", 1, i32::MAX).is_ok());
        assert!(validate_group_max_size("consumer", i32::MAX, i32::MAX).is_ok());
        assert!(validate_group_max_size("consumer", 0, i32::MAX).is_err());
        assert!(validate_group_max_size("streams", 1, i32::MAX).is_ok());
        assert!(validate_group_max_size("streams", i32::MAX, i32::MAX).is_ok());
        assert!(validate_group_max_size("streams", 0, i32::MAX).is_err());
        assert!(validate_group_max_size("share", 200, 1_000).is_ok());
        assert!(validate_group_max_size("share", 1_000, 1_000).is_ok());
        assert!(validate_group_max_size("share", 1_001, 1_000).is_err());
    }

    #[test]
    fn io_resource_limits_require_headroom_and_a_finite_timeout() {
        let response_budget = 2 * (rutomq_protocol::MAX_FRAME_SIZE + 4);
        assert!(validate_io_resource_limits(16, 32, response_budget, 1).is_ok());
        assert!(validate_io_resource_limits(16, 31, response_budget, 1).is_err());
        assert!(validate_io_resource_limits(16, 32, response_budget - 1, 1).is_err());
        assert!(validate_io_resource_limits(16, 32, response_budget, 0).is_err());
    }

    #[test]
    fn consumer_group_assignors_are_ordered_unique_builtins() {
        assert!(
            validate_consumer_group_assignors(&["range".to_owned(), "uniform".to_owned()]).is_ok()
        );
        assert!(validate_consumer_group_assignors(&["range".to_owned()]).is_ok());
        assert!(validate_consumer_group_assignors(&[]).is_err());
        assert!(
            validate_consumer_group_assignors(&["uniform".to_owned(), "uniform".to_owned()])
                .is_err()
        );
        assert!(validate_consumer_group_assignors(&["custom.Assignor".to_owned()]).is_err());
    }

    #[test]
    fn consumer_regex_refresh_interval_matches_kafka_minimum() {
        assert!(validate_consumer_regex_refresh_interval(10_000).is_ok());
        assert!(validate_consumer_regex_refresh_interval(600_000).is_ok());
        assert!(validate_consumer_regex_refresh_interval(9_999).is_err());
    }

    #[test]
    fn share_group_assignors_require_the_single_implemented_assignor() {
        assert!(validate_share_group_assignors(&["simple".to_owned()]).is_ok());
        assert!(validate_share_group_assignors(&[]).is_err());
        assert!(
            validate_share_group_assignors(&["simple".to_owned(), "simple".to_owned()]).is_err()
        );
        assert!(validate_share_group_assignors(&["custom.Assignor".to_owned()]).is_err());
    }

    #[test]
    fn share_record_lock_duration_defaults_and_bounds_match_kafka_ranges() {
        assert!(validate_share_record_lock_duration(30_000, 15_000, 60_000).is_ok());
        assert!(validate_share_record_lock_duration(1_000, 1_000, 30_000).is_ok());
        assert!(validate_share_record_lock_duration(999, 1_000, 30_000).is_err());
        assert!(validate_share_record_lock_duration(30_000, 30_001, 60_000).is_err());
        assert!(validate_share_record_lock_duration(30_000, 15_000, 29_999).is_err());
        assert!(validate_share_record_lock_duration(60_001, 15_000, 60_000).is_err());
    }
}
