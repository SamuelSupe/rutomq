use crate::{ControlError, PartitionKey, RetentionPage, RetentionResult, TopicConfig};
use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub async fn topic_config(pool: &PgPool, name: &str) -> Result<TopicConfig, ControlError> {
    sqlx::query(
        "SELECT c.retention_ms, c.retention_bytes, c.cleanup_policy,
                c.file_delete_delay_ms, c.flush_messages, c.flush_ms,
                c.delete_retention_ms,
                c.min_compaction_lag_ms,
                c.max_compaction_lag_ms, c.min_cleanable_dirty_ratio,
                c.min_insync_replicas, c.max_message_bytes, c.compression_type,
                c.compression_gzip_level, c.compression_lz4_level,
                c.compression_zstd_level,
                c.message_timestamp_type, c.message_timestamp_before_max_ms,
                c.message_timestamp_after_max_ms, c.dynamic_config_names
         FROM topic_configs c
         JOIN topics t ON t.id = c.topic_id
         WHERE t.name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await?
    .map(|row| config_from_row(&row))
    .ok_or_else(|| ControlError::TopicNotFound(name.to_owned()))
}

pub async fn set_topic_config(
    pool: &PgPool,
    name: &str,
    config: TopicConfig,
) -> Result<(), ControlError> {
    config.validate()?;
    let dynamic_config_names = config
        .dynamic_config_names
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let result = sqlx::query(
        "INSERT INTO topic_configs (
             topic_id, retention_ms, retention_bytes, cleanup_policy,
             file_delete_delay_ms, flush_messages, flush_ms,
             delete_retention_ms, min_compaction_lag_ms,
             max_compaction_lag_ms,
             min_cleanable_dirty_ratio, min_insync_replicas, max_message_bytes,
             compression_type, compression_gzip_level, compression_lz4_level,
             compression_zstd_level, message_timestamp_type,
             message_timestamp_before_max_ms, message_timestamp_after_max_ms,
             dynamic_config_names
         )
         SELECT id, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                $15, $16, $17, $18, $19, $20, $21
         FROM topics WHERE name = $1
         ON CONFLICT (topic_id) DO UPDATE
         SET retention_ms = EXCLUDED.retention_ms,
             retention_bytes = EXCLUDED.retention_bytes,
             cleanup_policy = EXCLUDED.cleanup_policy,
             file_delete_delay_ms = EXCLUDED.file_delete_delay_ms,
             flush_messages = EXCLUDED.flush_messages,
             flush_ms = EXCLUDED.flush_ms,
             delete_retention_ms = EXCLUDED.delete_retention_ms,
             min_compaction_lag_ms = EXCLUDED.min_compaction_lag_ms,
             max_compaction_lag_ms = EXCLUDED.max_compaction_lag_ms,
             min_cleanable_dirty_ratio = EXCLUDED.min_cleanable_dirty_ratio,
             min_insync_replicas = EXCLUDED.min_insync_replicas,
             max_message_bytes = EXCLUDED.max_message_bytes,
             compression_type = EXCLUDED.compression_type,
             compression_gzip_level = EXCLUDED.compression_gzip_level,
             compression_lz4_level = EXCLUDED.compression_lz4_level,
             compression_zstd_level = EXCLUDED.compression_zstd_level,
             message_timestamp_type = EXCLUDED.message_timestamp_type,
             message_timestamp_before_max_ms = EXCLUDED.message_timestamp_before_max_ms,
             message_timestamp_after_max_ms = EXCLUDED.message_timestamp_after_max_ms,
             dynamic_config_names = EXCLUDED.dynamic_config_names,
             updated_at = now()",
    )
    .bind(name)
    .bind(config.retention_ms)
    .bind(config.retention_bytes)
    .bind(config.cleanup_policy)
    .bind(config.file_delete_delay_ms)
    .bind(config.flush_messages)
    .bind(config.flush_ms)
    .bind(config.delete_retention_ms)
    .bind(config.min_compaction_lag_ms)
    .bind(config.max_compaction_lag_ms)
    .bind(config.min_cleanable_dirty_ratio)
    .bind(config.min_insync_replicas)
    .bind(config.max_message_bytes)
    .bind(config.compression_type)
    .bind(config.compression_gzip_level)
    .bind(config.compression_lz4_level)
    .bind(config.compression_zstd_level)
    .bind(config.message_timestamp_type)
    .bind(config.message_timestamp_before_max_ms)
    .bind(config.message_timestamp_after_max_ms)
    .bind(dynamic_config_names)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(ControlError::TopicNotFound(name.to_owned()));
    }
    Ok(())
}

pub async fn apply_page(
    pool: &PgPool,
    now_ms: i64,
    object_delete_grace_ms: i64,
    page: RetentionPage<'_>,
) -> Result<RetentionResult, ControlError> {
    let now = DateTime::<Utc>::from_timestamp_millis(now_ms).ok_or_else(|| {
        ControlError::InvalidRequest(format!("invalid retention timestamp {now_ms}"))
    })?;
    let partition_limit = page.max_partitions.max(1).min(i64::MAX as usize);
    let span_limit = page.max_spans.max(1).min(i64::MAX as usize);
    let object_limit = page.max_objects.max(1).min(i64::MAX as usize);
    let mut transaction = pool.begin().await?;
    let mut partitions = sqlx::query(
        "SELECT p.topic_id, t.name AS topic_name, p.partition_index,
                c.retention_ms, c.retention_bytes, c.cleanup_policy,
                c.file_delete_delay_ms, c.flush_messages, c.flush_ms,
                c.delete_retention_ms,
                c.min_compaction_lag_ms,
                c.max_compaction_lag_ms, c.min_cleanable_dirty_ratio,
                c.min_insync_replicas, c.max_message_bytes, c.compression_type,
                c.compression_gzip_level, c.compression_lz4_level,
                c.compression_zstd_level,
                c.message_timestamp_type, c.message_timestamp_before_max_ms,
                c.message_timestamp_after_max_ms
         FROM partitions p
         JOIN topics t ON t.id = p.topic_id
         JOIN topic_configs c ON c.topic_id = p.topic_id
         WHERE ($1::TEXT IS NULL OR (t.name, p.partition_index) > ($1, $2))
         ORDER BY t.name, p.partition_index
         LIMIT $3
         FOR UPDATE OF p",
    )
    .bind(
        page.start_after_partition
            .map(|cursor| cursor.topic.as_str()),
    )
    .bind(
        page.start_after_partition
            .map_or(-1, |cursor| cursor.partition),
    )
    .bind(i64::try_from(partition_limit.saturating_add(1)).unwrap_or(i64::MAX))
    .fetch_all(&mut *transaction)
    .await?;
    let has_more_partitions = partitions.len() > partition_limit;
    partitions.truncate(partition_limit);
    let selected_partition_count = partitions.len();
    let mut removed_spans = 0u64;
    let mut processed_partitions = Vec::new();
    for partition in partitions {
        if removed_spans >= span_limit as u64 {
            break;
        }
        let partition_key = PartitionKey::new(
            partition.get::<String, _>("topic_name"),
            partition.get("partition_index"),
        );
        processed_partitions.push(partition_key);
        let config = config_from_row(&partition);
        if !config.deletes_records() {
            continue;
        }
        let topic_id: Uuid = partition.get("topic_id");
        let partition_index: i32 = partition.get("partition_index");
        let remaining_spans =
            span_limit.saturating_sub(usize::try_from(removed_spans).unwrap_or(usize::MAX));
        let mut retained_bytes = if config.retention_bytes >= 0 {
            sqlx::query_scalar::<_, i64>(
                "SELECT LEAST(
                     COALESCE(SUM((byte_end - byte_start)::NUMERIC), 0),
                     9223372036854775807
                 )::BIGINT
                 FROM object_spans
                 WHERE topic_id = $1 AND partition_index = $2",
            )
            .bind(topic_id)
            .bind(partition_index)
            .fetch_one(&mut *transaction)
            .await?
        } else {
            0
        };
        let spans = sqlx::query(
            "SELECT id, object_key, byte_start, byte_end, timestamp_ms, txn_state
             FROM object_spans
             WHERE topic_id = $1 AND partition_index = $2
             ORDER BY base_offset
             LIMIT $3
             FOR UPDATE",
        )
        .bind(topic_id)
        .bind(partition_index)
        .bind(i64::try_from(remaining_spans).unwrap_or(i64::MAX))
        .fetch_all(&mut *transaction)
        .await?;
        let mut delete_ids = Vec::new();
        let mut object_keys = Vec::new();
        for span in spans {
            if span.get::<String, _>("txn_state") == "pending" {
                break;
            }
            let expired_by_time = config.retention_ms >= 0
                && span.get::<i64, _>("timestamp_ms") <= now_ms.saturating_sub(config.retention_ms);
            let over_size = config.retention_bytes >= 0 && retained_bytes > config.retention_bytes;
            if !expired_by_time && !over_size {
                break;
            }
            retained_bytes = retained_bytes.saturating_sub(
                span.get::<i64, _>("byte_end")
                    .saturating_sub(span.get::<i64, _>("byte_start")),
            );
            delete_ids.push(span.get::<i64, _>("id"));
            object_keys.push(span.get::<String, _>("object_key"));
        }
        if delete_ids.is_empty() {
            continue;
        }
        removed_spans += sqlx::query("DELETE FROM object_spans WHERE id = ANY($1)")
            .bind(delete_ids)
            .execute(&mut *transaction)
            .await?
            .rows_affected();
        object_keys.sort();
        object_keys.dedup();
        crate::postgres_objects::defer_delete(
            &mut transaction,
            &object_keys,
            now_ms,
            config.file_delete_delay_ms,
        )
        .await?;
        sqlx::query(
            "UPDATE partitions p
             SET log_start_offset = COALESCE(
                 (
                     SELECT MIN(s.base_offset)
                     FROM object_spans s
                     WHERE s.topic_id = p.topic_id
                       AND s.partition_index = p.partition_index
                 ),
                 p.next_offset
             )
             WHERE p.topic_id = $1 AND p.partition_index = $2",
        )
        .bind(topic_id)
        .bind(partition_index)
        .execute(&mut *transaction)
        .await?;
        crate::postgres_producers::reconcile_after_log_truncation(
            &mut transaction,
            topic_id,
            partition_index,
        )
        .await?;
    }

    sqlx::query(
        "WITH candidates AS (
             SELECT o.object_key
             FROM objects o
             WHERE o.committed = TRUE
               AND o.unreferenced_at IS NULL
               AND NOT EXISTS (
                   SELECT 1 FROM object_spans s
                   WHERE s.object_key = o.object_key
               )
             ORDER BY o.object_key
             LIMIT $2
         )
         UPDATE objects o
         SET unreferenced_at = COALESCE(o.unreferenced_at, $1),
             delete_after = COALESCE(o.delete_after, $1)
         FROM candidates
         WHERE o.object_key = candidates.object_key",
    )
    .bind(now)
    .bind(i64::try_from(object_limit).unwrap_or(i64::MAX))
    .execute(&mut *transaction)
    .await?;

    let delete_before = now
        .checked_sub_signed(Duration::milliseconds(object_delete_grace_ms.max(0)))
        .unwrap_or(DateTime::<Utc>::MIN_UTC);
    let deletable_objects = sqlx::query(
        "SELECT o.object_key
         FROM objects o
         WHERE o.unreferenced_at <= $1
           AND COALESCE(o.delete_after, o.unreferenced_at) <= $2
           AND o.committed = TRUE
           AND ($3::TEXT IS NULL OR o.object_key > $3)
           AND NOT EXISTS (
               SELECT 1 FROM object_spans s
               WHERE s.object_key = o.object_key
           )
         ORDER BY o.object_key
         LIMIT $4",
    )
    .bind(delete_before)
    .bind(now)
    .bind(page.start_after_object)
    .bind(i64::try_from(object_limit.saturating_add(1)).unwrap_or(i64::MAX))
    .fetch_all(&mut *transaction)
    .await?
    .into_iter()
    .map(|row| row.get::<String, _>("object_key"))
    .collect::<Vec<_>>();
    let next_object = (deletable_objects.len() > object_limit)
        .then(|| deletable_objects[object_limit - 1].clone());
    let mut deletable_objects = deletable_objects;
    deletable_objects.truncate(object_limit);
    let next_partition =
        if has_more_partitions || processed_partitions.len() < selected_partition_count {
            processed_partitions.last().cloned()
        } else {
            None
        };
    transaction.commit().await?;
    Ok(RetentionResult {
        removed_spans,
        deletable_objects,
        next_partition,
        next_object,
    })
}

pub async fn complete_object_deletion(pool: &PgPool, key: &str) -> Result<bool, ControlError> {
    let result = sqlx::query(
        "DELETE FROM objects o
         WHERE o.object_key = $1
           AND o.committed = TRUE
           AND o.unreferenced_at IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM object_spans s
               WHERE s.object_key = o.object_key
           )",
    )
    .bind(key)
    .execute(pool)
    .await?;
    Ok(result.rows_affected() == 1)
}

fn config_from_row(row: &sqlx::postgres::PgRow) -> TopicConfig {
    TopicConfig {
        retention_ms: row.get("retention_ms"),
        retention_bytes: row.get("retention_bytes"),
        cleanup_policy: row.get("cleanup_policy"),
        file_delete_delay_ms: row.get("file_delete_delay_ms"),
        flush_messages: row.get("flush_messages"),
        flush_ms: row.get("flush_ms"),
        delete_retention_ms: row.get("delete_retention_ms"),
        min_compaction_lag_ms: row.get("min_compaction_lag_ms"),
        max_compaction_lag_ms: row.get("max_compaction_lag_ms"),
        min_cleanable_dirty_ratio: row.get("min_cleanable_dirty_ratio"),
        min_insync_replicas: row.get("min_insync_replicas"),
        max_message_bytes: row.get("max_message_bytes"),
        compression_type: row.get("compression_type"),
        compression_gzip_level: row.get("compression_gzip_level"),
        compression_lz4_level: row.get("compression_lz4_level"),
        compression_zstd_level: row.get("compression_zstd_level"),
        message_timestamp_type: row.get("message_timestamp_type"),
        message_timestamp_before_max_ms: row.get("message_timestamp_before_max_ms"),
        message_timestamp_after_max_ms: row.get("message_timestamp_after_max_ms"),
        dynamic_config_names: row
            .try_get::<Vec<String>, _>("dynamic_config_names")
            .unwrap_or_default()
            .into_iter()
            .collect(),
    }
}
