use crate::{
    ConsumerLag, ControlError, PartitionKey, PartitionRetentionSize, TransactionStateCounts,
};
use sqlx::{PgPool, Row};

pub(crate) async fn partition_retention_sizes(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<PartitionRetentionSize>, ControlError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "WITH selected AS (
             SELECT t.name AS topic_name, p.topic_id, p.partition_index,
                    c.retention_bytes
             FROM topics t
             JOIN partitions p ON p.topic_id = t.id
             JOIN topic_configs c ON c.topic_id = t.id
             ORDER BY t.name, p.partition_index
             LIMIT $1
         )
         SELECT selected.topic_name, selected.partition_index,
                COALESCE(SUM(s.byte_end - s.byte_start), 0)::BIGINT AS size_bytes,
                selected.retention_bytes
         FROM selected
         LEFT JOIN object_spans s
           ON s.topic_id = selected.topic_id
          AND s.partition_index = selected.partition_index
         GROUP BY selected.topic_name, selected.partition_index,
                  selected.retention_bytes
         ORDER BY selected.topic_name, selected.partition_index",
    )
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| PartitionRetentionSize {
            partition: PartitionKey::new(
                row.get::<String, _>("topic_name"),
                row.get("partition_index"),
            ),
            size_bytes: row.get("size_bytes"),
            retention_bytes: row.get("retention_bytes"),
        })
        .collect())
}

pub(crate) async fn consumer_lags(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<ConsumerLag>, ControlError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let rows = sqlx::query(
        "SELECT o.group_id, t.name AS topic_name, o.partition_index,
                o.committed_offset, p.next_offset AS high_watermark
         FROM consumer_offsets o
         JOIN topics t ON t.id = o.topic_id
         JOIN partitions p
           ON p.topic_id = o.topic_id
          AND p.partition_index = o.partition_index
         ORDER BY o.group_id, t.name, o.partition_index
         LIMIT $1",
    )
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let committed_offset = row.get("committed_offset");
            let high_watermark = row.get("high_watermark");
            ConsumerLag {
                group_id: row.get("group_id"),
                partition: PartitionKey::new(
                    row.get::<String, _>("topic_name"),
                    row.get("partition_index"),
                ),
                committed_offset,
                high_watermark,
                lag: (high_watermark - committed_offset).max(0),
            }
        })
        .collect())
}

pub(crate) async fn transaction_state_counts(
    pool: &PgPool,
) -> Result<TransactionStateCounts, ControlError> {
    let row = sqlx::query(
        "SELECT
             COUNT(*) FILTER (WHERE latest.status IS NULL)::BIGINT AS empty,
             COUNT(*) FILTER (WHERE latest.status = 'ongoing')::BIGINT AS ongoing,
             COUNT(*) FILTER (WHERE latest.status = 'committed')::BIGINT AS complete_commit,
             COUNT(*) FILTER (WHERE latest.status = 'aborted')::BIGINT AS complete_abort
         FROM producers p
         LEFT JOIN LATERAL (
             SELECT status
             FROM transactions
             WHERE transactional_id = p.transactional_id
             ORDER BY started_at DESC
             LIMIT 1
         ) latest ON TRUE
         WHERE p.transactional_id IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;
    Ok(TransactionStateCounts {
        empty: row.get("empty"),
        ongoing: row.get("ongoing"),
        complete_commit: row.get("complete_commit"),
        complete_abort: row.get("complete_abort"),
    })
}
