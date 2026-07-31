use crate::{ControlError, PartitionKey};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;
pub const DEFAULT_MAX_MESSAGE_BYTES: i32 = 1_048_588;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicConfig {
    pub retention_ms: i64,
    pub retention_bytes: i64,
    pub cleanup_policy: String,
    pub file_delete_delay_ms: i64,
    pub flush_messages: i64,
    pub flush_ms: i64,
    pub delete_retention_ms: i64,
    pub min_compaction_lag_ms: i64,
    pub max_compaction_lag_ms: i64,
    pub min_cleanable_dirty_ratio: f64,
    pub min_insync_replicas: i32,
    pub max_message_bytes: i32,
    pub compression_type: String,
    pub compression_gzip_level: i32,
    pub compression_lz4_level: i32,
    pub compression_zstd_level: i32,
    pub message_timestamp_type: String,
    pub message_timestamp_before_max_ms: i64,
    pub message_timestamp_after_max_ms: i64,
    #[serde(default)]
    pub dynamic_config_names: BTreeSet<String>,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            retention_ms: DEFAULT_RETENTION_MS,
            retention_bytes: -1,
            cleanup_policy: "delete".to_owned(),
            file_delete_delay_ms: 60_000,
            flush_messages: i64::MAX,
            flush_ms: i64::MAX,
            delete_retention_ms: 24 * 60 * 60 * 1_000,
            min_compaction_lag_ms: 0,
            max_compaction_lag_ms: i64::MAX,
            min_cleanable_dirty_ratio: 0.5,
            min_insync_replicas: 1,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            compression_type: "producer".to_owned(),
            compression_gzip_level: -1,
            compression_lz4_level: 9,
            compression_zstd_level: 3,
            message_timestamp_type: "CreateTime".to_owned(),
            message_timestamp_before_max_ms: i64::MAX,
            message_timestamp_after_max_ms: 60 * 60 * 1_000,
            dynamic_config_names: BTreeSet::new(),
        }
    }
}

impl TopicConfig {
    pub fn is_dynamic(&self, name: &str) -> bool {
        self.dynamic_config_names.contains(name)
    }

    pub fn mark_dynamic(&mut self, name: &str) {
        self.dynamic_config_names.insert(name.to_owned());
    }

    pub fn reset_dynamic(&mut self, name: &str) {
        self.dynamic_config_names.remove(name);
    }

    pub fn validate(&self) -> Result<(), ControlError> {
        if self.retention_ms < -1
            || self.retention_bytes < -1
            || self.file_delete_delay_ms < 0
            || self.delete_retention_ms < 0
            || self.min_compaction_lag_ms < 0
        {
            return Err(ControlError::InvalidConfiguration(
                "retention values must be -1 (disabled) or non-negative".to_owned(),
            ));
        }
        if self.flush_messages < 1 || self.flush_ms < 0 {
            return Err(ControlError::InvalidConfiguration(
                "flush.messages must be positive and flush.ms must be non-negative".to_owned(),
            ));
        }
        if self.max_compaction_lag_ms < 1 || self.max_compaction_lag_ms < self.min_compaction_lag_ms
        {
            return Err(ControlError::InvalidConfiguration(
                "max.compaction.lag.ms must be positive and not less than min.compaction.lag.ms"
                    .to_owned(),
            ));
        }
        if !self.min_cleanable_dirty_ratio.is_finite()
            || !(0.0..=1.0).contains(&self.min_cleanable_dirty_ratio)
        {
            return Err(ControlError::InvalidConfiguration(
                "min.cleanable.dirty.ratio must be between 0 and 1".to_owned(),
            ));
        }
        if self.max_message_bytes < 0
            || self.message_timestamp_before_max_ms < 0
            || self.message_timestamp_after_max_ms < 0
        {
            return Err(ControlError::InvalidConfiguration(
                "record size and timestamp limits must be non-negative".to_owned(),
            ));
        }
        if self.min_insync_replicas < 1 {
            return Err(ControlError::InvalidConfiguration(
                "min.insync.replicas must be positive".to_owned(),
            ));
        }
        if (self.compression_gzip_level != -1 && !(1..=9).contains(&self.compression_gzip_level))
            || !(1..=17).contains(&self.compression_lz4_level)
            || !(-131_072..=22).contains(&self.compression_zstd_level)
        {
            return Err(ControlError::InvalidConfiguration(
                "compression levels are outside Kafka's supported ranges".to_owned(),
            ));
        }
        if !matches!(
            self.message_timestamp_type.as_str(),
            "CreateTime" | "LogAppendTime"
        ) {
            return Err(ControlError::InvalidConfiguration(format!(
                "unsupported message.timestamp.type {}",
                self.message_timestamp_type
            )));
        }
        if !matches!(
            self.compression_type.as_str(),
            "producer" | "uncompressed" | "gzip" | "snappy" | "lz4" | "zstd"
        ) {
            return Err(ControlError::InvalidConfiguration(format!(
                "unsupported compression.type {}",
                self.compression_type
            )));
        }
        if !matches!(
            self.cleanup_policy.as_str(),
            "delete" | "compact" | "compact,delete" | "delete,compact"
        ) {
            return Err(ControlError::InvalidConfiguration(format!(
                "unsupported cleanup.policy {}",
                self.cleanup_policy
            )));
        }
        Ok(())
    }

    pub(crate) fn deletes_records(&self) -> bool {
        self.cleanup_policy
            .split(',')
            .any(|policy| policy == "delete")
    }

    pub(crate) fn compacts_records(&self) -> bool {
        self.cleanup_policy
            .split(',')
            .any(|policy| policy == "compact")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compaction_schedule_values_are_bounded() {
        assert!(TopicConfig::default().validate().is_ok());
        let invalid_ratio = TopicConfig {
            min_cleanable_dirty_ratio: f64::NAN,
            ..TopicConfig::default()
        };
        assert!(invalid_ratio.validate().is_err());
        let invalid_lag = TopicConfig {
            min_compaction_lag_ms: 1_001,
            max_compaction_lag_ms: 1_000,
            ..TopicConfig::default()
        };
        assert!(invalid_lag.validate().is_err());
        let invalid_min_isr = TopicConfig {
            min_insync_replicas: 0,
            ..TopicConfig::default()
        };
        assert!(invalid_min_isr.validate().is_err());
        assert!(
            TopicConfig {
                flush_messages: 0,
                ..TopicConfig::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            TopicConfig {
                flush_ms: -1,
                ..TopicConfig::default()
            }
            .validate()
            .is_err()
        );
        for invalid in [
            TopicConfig {
                compression_gzip_level: 0,
                ..TopicConfig::default()
            },
            TopicConfig {
                compression_lz4_level: 18,
                ..TopicConfig::default()
            },
            TopicConfig {
                compression_zstd_level: -131_073,
                ..TopicConfig::default()
            },
        ] {
            assert!(invalid.validate().is_err());
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RetentionResult {
    pub removed_spans: u64,
    pub deletable_objects: Vec<String>,
    pub next_partition: Option<PartitionKey>,
    pub next_object: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct RetentionPage<'a> {
    pub start_after_partition: Option<&'a PartitionKey>,
    pub start_after_object: Option<&'a str>,
    pub max_partitions: usize,
    pub max_spans: usize,
    pub max_objects: usize,
}
