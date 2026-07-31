use crate::{
    ControlError, OffsetCommit, PartitionKey, ProducerInitialization, ProducerSession,
    TransactionDescription, TransactionFilter, TransactionState, TransactionStatus,
    effective_transaction_timeout, filter_transaction_descriptions, validate_current_producer,
    validate_two_phase_options,
};
use chrono::{DateTime, Utc};
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub async fn init_producer(
    pool: &PgPool,
    transactional_id: Option<&str>,
    timeout_ms: i32,
    current: Option<ProducerSession>,
    enable_2_pc: bool,
    keep_prepared_txn: bool,
) -> Result<ProducerInitialization, ControlError> {
    let timeout_ms = effective_transaction_timeout(transactional_id, timeout_ms)?;
    validate_two_phase_options(transactional_id, enable_2_pc, keep_prepared_txn)?;
    let mut transaction = pool.begin().await?;
    let initialization = if let Some(transactional_id) = transactional_id {
        let existing = sqlx::query(
            "SELECT producer_id, producer_epoch, current_transaction_id
             FROM producers WHERE transactional_id = $1 FOR UPDATE",
        )
        .bind(transactional_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(existing) = existing {
            let producer_id: i64 = existing.get("producer_id");
            let producer_epoch: i16 = existing.get("producer_epoch");
            validate_current_producer(producer_id, producer_epoch, current)?;
            let previous_transaction: Option<Uuid> = existing.get("current_transaction_id");
            let ongoing_transaction = if keep_prepared_txn {
                preserved_transaction(&mut transaction, previous_transaction).await?
            } else {
                if let Some(transaction_id) = previous_transaction {
                    abort_transaction(&mut transaction, transaction_id).await?;
                }
                None
            };
            if enable_2_pc && let Some(transaction_id) = previous_transaction {
                sqlx::query(
                    "UPDATE transactions
                     SET two_phase_commit = TRUE
                     WHERE id = $1 AND status = 'ongoing'",
                )
                .bind(transaction_id)
                .execute(&mut *transaction)
                .await?;
            }
            let producer = bump_transactional_producer(
                &mut transaction,
                transactional_id,
                ProducerSession {
                    producer_id,
                    producer_epoch,
                },
                timeout_ms,
                enable_2_pc,
                keep_prepared_txn.then_some(previous_transaction).flatten(),
            )
            .await?;
            ProducerInitialization {
                producer,
                ongoing_transaction,
            }
        } else {
            if current.is_some() {
                return Err(ControlError::TransactionNotFound(
                    transactional_id.to_owned(),
                ));
            }
            ProducerInitialization {
                producer: insert_producer(
                    &mut transaction,
                    Some(transactional_id),
                    timeout_ms,
                    enable_2_pc,
                    None,
                )
                .await?,
                ongoing_transaction: None,
            }
        }
    } else if let Some(current) = current {
        let row = sqlx::query(
            "SELECT producer_epoch, transactional_id
             FROM producers WHERE producer_id = $1 FOR UPDATE",
        )
        .bind(current.producer_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(ControlError::UnknownProducer(current.producer_id))?;
        if row.get::<Option<String>, _>("transactional_id").is_some() {
            return Err(ControlError::UnknownProducer(current.producer_id));
        }
        let expected_epoch: i16 = row.get("producer_epoch");
        validate_current_producer(current.producer_id, expected_epoch, Some(current))?;
        let producer = if let Some(producer_epoch) = expected_epoch.checked_add(1) {
            sqlx::query(
                "UPDATE producers SET producer_epoch = $2, updated_at = now()
                 WHERE producer_id = $1",
            )
            .bind(current.producer_id)
            .bind(producer_epoch)
            .execute(&mut *transaction)
            .await?;
            ProducerSession {
                producer_id: current.producer_id,
                producer_epoch,
            }
        } else {
            insert_producer(&mut transaction, None, timeout_ms, false, None).await?
        };
        ProducerInitialization {
            producer,
            ongoing_transaction: None,
        }
    } else {
        ProducerInitialization {
            producer: insert_producer(&mut transaction, None, timeout_ms, false, None).await?,
            ongoing_transaction: None,
        }
    };
    transaction.commit().await?;
    Ok(initialization)
}

pub async fn add_partitions(
    pool: &PgPool,
    transactional_id: &str,
    producer: ProducerSession,
    partitions: &[PartitionKey],
    verify_only: bool,
) -> Result<(), ControlError> {
    let mut transaction = pool.begin().await?;
    let mut resolved = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let row = sqlx::query(
            "SELECT p.topic_id
             FROM partitions p JOIN topics t ON t.id = p.topic_id
             WHERE t.name = $1 AND p.partition_index = $2",
        )
        .bind(&partition.topic)
        .bind(partition.partition)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or_else(|| ControlError::PartitionNotFound {
            topic: partition.topic.clone(),
            partition: partition.partition,
        })?;
        resolved.push((row.get::<Uuid, _>("topic_id"), partition.partition));
    }
    if verify_only {
        validate_producer(&mut transaction, transactional_id, producer).await?;
    } else {
        let transaction_id =
            current_transaction(&mut transaction, transactional_id, producer, true, false).await?;
        for (topic_id, partition) in resolved {
            sqlx::query(
                "INSERT INTO transaction_partitions
                 (transaction_id, topic_id, partition_index)
                 VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            )
            .bind(transaction_id)
            .bind(topic_id)
            .bind(partition)
            .execute(&mut *transaction)
            .await?;
        }
        touch_producer(&mut transaction, producer.producer_id).await?;
    }
    transaction.commit().await?;
    Ok(())
}

pub async fn add_offsets(
    pool: &PgPool,
    transactional_id: &str,
    producer: ProducerSession,
    group_id: &str,
) -> Result<(), ControlError> {
    if group_id.is_empty() {
        return Err(ControlError::InvalidRequest(
            "group id must not be empty".to_owned(),
        ));
    }
    let mut transaction = pool.begin().await?;
    let transaction_id =
        current_transaction(&mut transaction, transactional_id, producer, true, false).await?;
    sqlx::query(
        "INSERT INTO transaction_groups (transaction_id, group_id)
         VALUES ($1, $2) ON CONFLICT DO NOTHING",
    )
    .bind(transaction_id)
    .bind(group_id)
    .execute(&mut *transaction)
    .await?;
    touch_producer(&mut transaction, producer.producer_id).await?;
    transaction.commit().await?;
    Ok(())
}

pub async fn commit_offsets(
    pool: &PgPool,
    transactional_id: &str,
    producer: ProducerSession,
    group_id: &str,
    offsets: Vec<OffsetCommit>,
) -> Result<(), ControlError> {
    commit_offsets_with_options(pool, transactional_id, producer, group_id, false, offsets).await
}

pub async fn commit_offsets_with_options(
    pool: &PgPool,
    transactional_id: &str,
    producer: ProducerSession,
    group_id: &str,
    add_group: bool,
    offsets: Vec<OffsetCommit>,
) -> Result<(), ControlError> {
    let mut transaction = pool.begin().await?;
    commit_offsets_in_transaction(
        &mut transaction,
        transactional_id,
        producer,
        group_id,
        add_group,
        offsets,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn commit_consumer_offsets(
    pool: &PgPool,
    transactional_id: &str,
    producer: ProducerSession,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
    member_epoch: i32,
    add_group: bool,
    offsets: Vec<OffsetCommit>,
) -> Result<bool, ControlError> {
    let mut transaction = pool.begin().await?;
    let partitions = offsets
        .iter()
        .map(|offset| offset.partition.clone())
        .collect::<Vec<_>>();
    if crate::postgres_consumer_groups::validate_transaction_offset_commit_in_transaction(
        &mut transaction,
        group_id,
        member_id,
        group_instance_id,
        member_epoch,
        &partitions,
    )
    .await?
    .is_none()
    {
        return Ok(false);
    }
    commit_offsets_in_transaction(
        &mut transaction,
        transactional_id,
        producer,
        group_id,
        add_group,
        offsets,
    )
    .await?;
    transaction.commit().await?;
    Ok(true)
}

async fn commit_offsets_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    transactional_id: &str,
    producer: ProducerSession,
    group_id: &str,
    add_group: bool,
    offsets: Vec<OffsetCommit>,
) -> Result<(), ControlError> {
    let transaction_id =
        current_transaction(transaction, transactional_id, producer, add_group, false).await?;
    if add_group {
        sqlx::query(
            "INSERT INTO transaction_groups (transaction_id, group_id)
             VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(transaction_id)
        .bind(group_id)
        .execute(&mut **transaction)
        .await?;
    }
    let group_added = sqlx::query(
        "SELECT 1 FROM transaction_groups
         WHERE transaction_id = $1 AND group_id = $2",
    )
    .bind(transaction_id)
    .bind(group_id)
    .fetch_optional(&mut **transaction)
    .await?
    .is_some();
    if !group_added {
        return Err(ControlError::InvalidTransactionState(format!(
            "group {group_id} was not added to the transaction"
        )));
    }
    let commit_timestamp_ms = Utc::now().timestamp_millis();
    let mut ordered_offsets = Vec::with_capacity(offsets.len());
    for (request_index, offset) in offsets.into_iter().enumerate() {
        if offset.offset < 0 {
            return Err(ControlError::InvalidRequest(
                "committed offset must not be negative".to_owned(),
            ));
        }
        let topic_id = sqlx::query(
            "SELECT p.topic_id
             FROM partitions p JOIN topics t ON t.id = p.topic_id
             WHERE t.name = $1 AND p.partition_index = $2
             FOR KEY SHARE OF t",
        )
        .bind(&offset.partition.topic)
        .bind(offset.partition.partition)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| ControlError::PartitionNotFound {
            topic: offset.partition.topic.clone(),
            partition: offset.partition.partition,
        })?
        .get::<Uuid, _>("topic_id");
        ordered_offsets.push((
            topic_id,
            offset.partition.partition,
            request_index,
            offset.offset,
            offset.leader_epoch,
            offset.metadata,
            crate::offset_expire_timestamp(commit_timestamp_ms, offset.retention_time_ms)?,
        ));
    }
    ordered_offsets.sort_by_key(|(topic_id, partition, request_index, _, _, _, _)| {
        (*topic_id, *partition, *request_index)
    });
    for (topic_id, partition, _, offset, leader_epoch, metadata, expire_timestamp_ms) in
        ordered_offsets
    {
        sqlx::query(
            "INSERT INTO transaction_offset_commits
             (transaction_id, group_id, topic_id, partition_index,
              committed_offset, committed_leader_epoch, metadata,
              commit_timestamp_ms, expire_timestamp_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (transaction_id, group_id, topic_id, partition_index)
             DO UPDATE SET committed_offset = EXCLUDED.committed_offset,
                           committed_leader_epoch = EXCLUDED.committed_leader_epoch,
                           metadata = EXCLUDED.metadata,
                           commit_timestamp_ms = EXCLUDED.commit_timestamp_ms,
                           expire_timestamp_ms = EXCLUDED.expire_timestamp_ms",
        )
        .bind(transaction_id)
        .bind(group_id)
        .bind(topic_id)
        .bind(partition)
        .bind(offset)
        .bind(leader_epoch)
        .bind(metadata)
        .bind(commit_timestamp_ms)
        .bind(expire_timestamp_ms)
        .execute(&mut **transaction)
        .await?;
    }
    touch_producer(transaction, producer.producer_id).await?;
    Ok(())
}

pub async fn end(
    pool: &PgPool,
    transactional_id: &str,
    producer: ProducerSession,
    committed: bool,
) -> Result<(), ControlError> {
    end_inner(pool, transactional_id, producer, committed, false)
        .await
        .map(|_| ())
}

pub async fn end_with_epoch_bump(
    pool: &PgPool,
    transactional_id: &str,
    producer: ProducerSession,
    committed: bool,
) -> Result<ProducerSession, ControlError> {
    end_inner(pool, transactional_id, producer, committed, true).await
}

pub async fn write_marker(
    pool: &PgPool,
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
    let mut transaction = pool.begin().await?;
    if sqlx::query("SELECT producer_id FROM producers WHERE producer_id = $1 FOR UPDATE")
        .bind(producer.producer_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_none()
    {
        return Err(ControlError::UnknownProducer(producer.producer_id));
    }
    let row = sqlx::query(
        "SELECT id, transactional_id, status, producer_epoch,
                marker_producer_epoch, marker_coordinator_epoch
         FROM transactions
         WHERE producer_id = $1
         ORDER BY (status = 'ongoing') DESC, started_at DESC, id DESC
         LIMIT 1
         FOR UPDATE",
    )
    .bind(producer.producer_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or(ControlError::UnknownProducer(producer.producer_id))?;
    let transaction_id: Uuid = row.get("id");
    let transactional_id: String = row.get("transactional_id");
    let transaction_producer_epoch: i16 = row.get("producer_epoch");
    let marker_producer_epoch: Option<i16> = row.get("marker_producer_epoch");
    let status = row.get::<String, _>("status");
    let completed_marker_retry = status != TransactionStatus::Ongoing.as_str()
        && marker_producer_epoch == Some(producer.producer_epoch);
    let current_producer_epoch = marker_producer_epoch.unwrap_or(transaction_producer_epoch);
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
    let current_coordinator_epoch = sqlx::query(
        "SELECT MAX(marker_coordinator_epoch) AS marker_coordinator_epoch
         FROM transactions
         WHERE producer_id = $1",
    )
    .bind(producer.producer_id)
    .fetch_one(&mut *transaction)
    .await?
    .get::<Option<i32>, _>("marker_coordinator_epoch");
    if let Some(current_epoch) = current_coordinator_epoch
        && coordinator_epoch < current_epoch
    {
        return Err(ControlError::TransactionCoordinatorFenced {
            producer_id: producer.producer_id,
            current_epoch,
            requested_epoch: coordinator_epoch,
        });
    }
    let requested = partitions
        .iter()
        .map(|partition| (partition.topic.clone(), partition.partition))
        .collect::<HashSet<_>>();
    let registered = sqlx::query(
        "SELECT t.name, tp.partition_index
         FROM transaction_partitions tp
         JOIN topics t ON t.id = tp.topic_id
         WHERE tp.transaction_id = $1",
    )
    .bind(transaction_id)
    .fetch_all(&mut *transaction)
    .await?;
    if registered.iter().any(|partition| {
        !requested.contains(&(
            partition.get::<String, _>("name"),
            partition.get::<i32, _>("partition_index"),
        ))
    }) {
        return Err(ControlError::InvalidTransactionState(
            "transaction marker does not cover every registered partition".to_owned(),
        ));
    }

    let target = if committed {
        TransactionStatus::Committed
    } else {
        TransactionStatus::Aborted
    };
    if status != TransactionStatus::Ongoing.as_str() {
        if status != target.as_str() {
            return Err(ControlError::InvalidTransactionState(
                "the completed transaction has the opposite result".to_owned(),
            ));
        }
    } else {
        apply_transaction_outcome(
            &mut transaction,
            transaction_id,
            &transactional_id,
            ProducerSession {
                producer_id: producer.producer_id,
                producer_epoch: transaction_producer_epoch,
            },
            committed,
            false,
        )
        .await?;
    }
    sqlx::query(
        "UPDATE transactions
         SET marker_producer_epoch = $2,
             marker_coordinator_epoch = $3,
             marker_transaction_version = $4
         WHERE id = $1",
    )
    .bind(transaction_id)
    .bind(producer.producer_epoch)
    .bind(coordinator_epoch)
    .bind(i16::from(transaction_version))
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE producers
         SET producer_epoch = GREATEST(producer_epoch, $2), updated_at = now()
         WHERE producer_id = $1",
    )
    .bind(producer.producer_id)
    .bind(producer.producer_epoch)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn end_inner(
    pool: &PgPool,
    transactional_id: &str,
    producer: ProducerSession,
    committed: bool,
    bump_epoch: bool,
) -> Result<ProducerSession, ControlError> {
    let mut transaction = pool.begin().await?;
    if bump_epoch
        && let Some(result) =
            completed_end_retry(&mut transaction, transactional_id, producer, committed).await?
    {
        transaction.commit().await?;
        return Ok(result);
    }
    let transaction_id = current_transaction(
        &mut transaction,
        transactional_id,
        producer,
        bump_epoch && !committed,
        true,
    )
    .await?;
    let result = apply_transaction_outcome(
        &mut transaction,
        transaction_id,
        transactional_id,
        producer,
        committed,
        bump_epoch,
    )
    .await?;
    transaction.commit().await?;
    Ok(result)
}

async fn apply_transaction_outcome(
    transaction: &mut Transaction<'_, Postgres>,
    transaction_id: Uuid,
    transactional_id: &str,
    producer: ProducerSession,
    committed: bool,
    bump_epoch: bool,
) -> Result<ProducerSession, ControlError> {
    let status = if committed {
        TransactionStatus::Committed
    } else {
        TransactionStatus::Aborted
    };
    if committed {
        sqlx::query(
            "SELECT t.id
             FROM topics t
             JOIN (
                 SELECT DISTINCT topic_id
                 FROM transaction_offset_commits
                 WHERE transaction_id = $1
             ) pending ON pending.topic_id = t.id
             ORDER BY t.id
             FOR KEY SHARE OF t",
        )
        .bind(transaction_id)
        .fetch_all(&mut **transaction)
        .await?;
        sqlx::query(
            "INSERT INTO consumer_offsets
             (group_id, topic_id, partition_index, committed_offset,
              committed_leader_epoch, metadata, commit_timestamp_ms,
              expire_timestamp_ms)
             SELECT group_id, topic_id, partition_index, committed_offset,
                    committed_leader_epoch, metadata, commit_timestamp_ms,
                    expire_timestamp_ms
             FROM transaction_offset_commits
             WHERE transaction_id = $1
             ORDER BY group_id, topic_id, partition_index
             ON CONFLICT (group_id, topic_id, partition_index)
             DO UPDATE SET committed_offset = EXCLUDED.committed_offset,
                           committed_leader_epoch = EXCLUDED.committed_leader_epoch,
                           metadata = EXCLUDED.metadata,
                           commit_timestamp_ms = EXCLUDED.commit_timestamp_ms,
                           expire_timestamp_ms = EXCLUDED.expire_timestamp_ms,
                           expiration_checked_at_ms = 0,
                           updated_at = now()",
        )
        .bind(transaction_id)
        .execute(&mut **transaction)
        .await?;
    }
    sqlx::query(
        "UPDATE transactions
         SET status = $2, completed_at = now(),
             producer_id = CASE WHEN $3 THEN $4 ELSE producer_id END,
             producer_epoch = CASE WHEN $3 THEN $5 ELSE producer_epoch END
         WHERE id = $1",
    )
    .bind(transaction_id)
    .bind(status.as_str())
    .bind(bump_epoch)
    .bind(producer.producer_id)
    .bind(producer.producer_epoch)
    .execute(&mut **transaction)
    .await?;
    sqlx::query("UPDATE object_spans SET txn_state = $2 WHERE transaction_id = $1")
        .bind(transaction_id)
        .bind(status.as_str())
        .execute(&mut **transaction)
        .await?;
    let result = if bump_epoch {
        let row = sqlx::query(
            "SELECT transaction_timeout_ms, two_phase_commit
             FROM producers WHERE producer_id = $1",
        )
        .bind(producer.producer_id)
        .fetch_one(&mut **transaction)
        .await?;
        bump_transactional_producer(
            transaction,
            transactional_id,
            producer,
            row.get("transaction_timeout_ms"),
            row.get("two_phase_commit"),
            Some(transaction_id),
        )
        .await?
    } else {
        sqlx::query(
            "UPDATE producers SET current_transaction_id = NULL, updated_at = now()
             WHERE producer_id = $1 AND current_transaction_id = $2",
        )
        .bind(producer.producer_id)
        .bind(transaction_id)
        .execute(&mut **transaction)
        .await?;
        producer
    };
    Ok(result)
}

async fn completed_end_retry(
    transaction: &mut Transaction<'_, Postgres>,
    transactional_id: &str,
    producer: ProducerSession,
    committed: bool,
) -> Result<Option<ProducerSession>, ControlError> {
    let Some(current) = sqlx::query(
        "SELECT producer_id, producer_epoch, current_transaction_id
         FROM producers WHERE transactional_id = $1 FOR UPDATE",
    )
    .bind(transactional_id)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(None);
    };
    let Some(transaction_id) = current.get::<Option<Uuid>, _>("current_transaction_id") else {
        return Ok(None);
    };
    let Some(completed) = sqlx::query(
        "SELECT producer_id, producer_epoch, status
         FROM transactions WHERE id = $1",
    )
    .bind(transaction_id)
    .fetch_optional(&mut **transaction)
    .await?
    else {
        return Ok(None);
    };
    let status = completed.get::<String, _>("status");
    if status == TransactionStatus::Ongoing.as_str()
        || completed.get::<i64, _>("producer_id") != producer.producer_id
        || completed.get::<i16, _>("producer_epoch") != producer.producer_epoch
    {
        return Ok(None);
    }
    let expected = if committed {
        TransactionStatus::Committed.as_str()
    } else {
        TransactionStatus::Aborted.as_str()
    };
    if status != expected {
        return Err(ControlError::InvalidTransactionState(
            "the completed transaction has the opposite result".to_owned(),
        ));
    }
    Ok(Some(ProducerSession {
        producer_id: current.get("producer_id"),
        producer_epoch: current.get("producer_epoch"),
    }))
}

pub async fn describe(
    pool: &PgPool,
    transactional_ids: &[String],
) -> Result<HashMap<String, TransactionDescription>, ControlError> {
    if transactional_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let rows = sqlx::query(
        "SELECT p.transactional_id, p.producer_id, p.producer_epoch,
                p.transaction_timeout_ms, latest.id AS transaction_id,
                latest.status, latest.started_at
         FROM producers p
         LEFT JOIN LATERAL (
             SELECT id, status, started_at
             FROM transactions
             WHERE transactional_id = p.transactional_id
             ORDER BY started_at DESC
             LIMIT 1
         ) latest ON TRUE
         WHERE p.transactional_id = ANY($1)
         ORDER BY p.transactional_id",
    )
    .bind(transactional_ids.to_vec())
    .fetch_all(pool)
    .await?;
    let mut descriptions = HashMap::with_capacity(rows.len());
    for row in rows {
        let description = description_from_row(pool, row).await?;
        descriptions.insert(description.transactional_id.clone(), description);
    }
    Ok(descriptions)
}

pub async fn list(
    pool: &PgPool,
    filter: &TransactionFilter,
) -> Result<Vec<TransactionDescription>, ControlError> {
    let rows = sqlx::query(
        "SELECT p.transactional_id, p.producer_id, p.producer_epoch,
                p.transaction_timeout_ms, latest.id AS transaction_id,
                latest.status, latest.started_at
         FROM producers p
         LEFT JOIN LATERAL (
             SELECT id, status, started_at
             FROM transactions
             WHERE transactional_id = p.transactional_id
             ORDER BY started_at DESC
             LIMIT 1
         ) latest ON TRUE
         WHERE p.transactional_id IS NOT NULL
         ORDER BY p.transactional_id",
    )
    .fetch_all(pool)
    .await?;
    let mut descriptions = Vec::with_capacity(rows.len());
    for row in rows {
        descriptions.push(description_from_row(pool, row).await?);
    }
    filter_transaction_descriptions(descriptions, filter, Utc::now().timestamp_millis())
}

pub async fn expire_transactional_ids(
    pool: &PgPool,
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
    let expired = sqlx::query(
        "WITH candidates AS (
             SELECT p.producer_id
             FROM producers p
             WHERE p.transactional_id IS NOT NULL
               AND p.updated_at <= to_timestamp($1::double precision / 1000.0)
               AND NOT EXISTS (
                   SELECT 1
                   FROM transactions tx
                   WHERE tx.producer_id = p.producer_id
                     AND tx.status = 'ongoing'
               )
             ORDER BY p.updated_at, p.producer_id
             LIMIT $2
             FOR UPDATE OF p SKIP LOCKED
         )
         UPDATE producers p
         SET transactional_id = NULL,
             current_transaction_id = NULL,
             two_phase_commit = FALSE,
             updated_at = now()
         FROM candidates c
         WHERE p.producer_id = c.producer_id
         RETURNING p.producer_id",
    )
    .bind(cutoff_ms)
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(pool)
    .await?;
    Ok(expired.len() as u64)
}

pub async fn abort_expired(pool: &PgPool, limit: usize) -> Result<u64, ControlError> {
    if limit == 0 {
        return Ok(0);
    }
    let mut transaction = pool.begin().await?;
    // Producer-first ordering matches Produce and EndTxn. Lock every candidate
    // before touching transaction rows so a sweep cannot invert two producers.
    let candidates = sqlx::query(
        "SELECT p.producer_id, p.current_transaction_id
         FROM producers p
         JOIN transactions tx ON tx.id = p.current_transaction_id
         WHERE tx.status = 'ongoing'
           AND NOT tx.two_phase_commit
           AND tx.started_at + tx.timeout_ms * INTERVAL '1 millisecond' <= now()
         ORDER BY p.producer_id
         LIMIT $1
         FOR UPDATE OF p",
    )
    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
    .fetch_all(&mut *transaction)
    .await?;
    let transaction_ids = candidates
        .into_iter()
        .filter_map(|row| row.get::<Option<Uuid>, _>("current_transaction_id"))
        .collect::<Vec<_>>();
    if transaction_ids.is_empty() {
        transaction.commit().await?;
        return Ok(0);
    }
    let expired = sqlx::query(
        "UPDATE transactions
         SET status = 'aborted', completed_at = now()
         WHERE id = ANY($1)
           AND status = 'ongoing'
           AND NOT two_phase_commit
           AND started_at + timeout_ms * INTERVAL '1 millisecond' <= now()
         RETURNING id",
    )
    .bind(transaction_ids)
    .fetch_all(&mut *transaction)
    .await?;
    for row in &expired {
        let transaction_id: Uuid = row.get("id");
        sqlx::query(
            "UPDATE object_spans SET txn_state = 'aborted'
             WHERE transaction_id = $1 AND txn_state = 'pending'",
        )
        .bind(transaction_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            "UPDATE producers SET current_transaction_id = NULL, updated_at = now()
             WHERE current_transaction_id = $1",
        )
        .bind(transaction_id)
        .execute(&mut *transaction)
        .await?;
    }
    transaction.commit().await?;
    Ok(expired.len() as u64)
}

async fn description_from_row(
    pool: &PgPool,
    row: PgRow,
) -> Result<TransactionDescription, ControlError> {
    let transaction_id: Option<Uuid> = row.get("transaction_id");
    let state = match row.get::<Option<String>, _>("status").as_deref() {
        None => TransactionState::Empty,
        Some("ongoing") => TransactionState::Ongoing,
        Some("committed") => TransactionState::CompleteCommit,
        Some("aborted") => TransactionState::CompleteAbort,
        Some(status) => {
            return Err(ControlError::InvalidTransactionState(format!(
                "unknown persisted transaction state {status}"
            )));
        }
    };
    let mut partitions = if state == TransactionState::Ongoing {
        if let Some(transaction_id) = transaction_id {
            sqlx::query(
                "SELECT t.name, tp.partition_index
                 FROM transaction_partitions tp
                 JOIN topics t ON t.id = tp.topic_id
                 WHERE tp.transaction_id = $1
                 ORDER BY t.name, tp.partition_index",
            )
            .bind(transaction_id)
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|partition| {
                PartitionKey::new(
                    partition.get::<String, _>("name"),
                    partition.get("partition_index"),
                )
            })
            .collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    partitions
        .sort_by(|left, right| (&left.topic, left.partition).cmp(&(&right.topic, right.partition)));
    Ok(TransactionDescription {
        transactional_id: row.get("transactional_id"),
        producer: ProducerSession {
            producer_id: row.get("producer_id"),
            producer_epoch: row.get("producer_epoch"),
        },
        state,
        timeout_ms: row.get("transaction_timeout_ms"),
        start_time_ms: row
            .get::<Option<DateTime<Utc>>, _>("started_at")
            .map_or(-1, |started_at| started_at.timestamp_millis()),
        partitions,
    })
}

async fn insert_producer(
    transaction: &mut Transaction<'_, Postgres>,
    transactional_id: Option<&str>,
    timeout_ms: i32,
    two_phase_commit: bool,
    current_transaction_id: Option<Uuid>,
) -> Result<ProducerSession, ControlError> {
    let row = sqlx::query(
        "INSERT INTO producers
         (transactional_id, transaction_timeout_ms, two_phase_commit,
          current_transaction_id)
         VALUES ($1, $2, $3, $4) RETURNING producer_id, producer_epoch",
    )
    .bind(transactional_id)
    .bind(timeout_ms)
    .bind(two_phase_commit)
    .bind(current_transaction_id)
    .fetch_one(&mut **transaction)
    .await?;
    Ok(ProducerSession {
        producer_id: row.get("producer_id"),
        producer_epoch: row.get("producer_epoch"),
    })
}

async fn touch_producer(
    transaction: &mut Transaction<'_, Postgres>,
    producer_id: i64,
) -> Result<(), ControlError> {
    sqlx::query("UPDATE producers SET updated_at = now() WHERE producer_id = $1")
        .bind(producer_id)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn preserved_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    transaction_id: Option<Uuid>,
) -> Result<Option<ProducerSession>, ControlError> {
    let Some(transaction_id) = transaction_id else {
        return Ok(None);
    };
    let row = sqlx::query(
        "SELECT producer_id, producer_epoch, status
         FROM transactions WHERE id = $1 FOR UPDATE",
    )
    .bind(transaction_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| {
        ControlError::InvalidTransactionState("producer points to a missing transaction".to_owned())
    })?;
    if row.get::<String, _>("status") != TransactionStatus::Ongoing.as_str() {
        return Err(ControlError::InvalidTransactionState(
            "transaction is no longer ongoing".to_owned(),
        ));
    }
    Ok(Some(ProducerSession {
        producer_id: row.get("producer_id"),
        producer_epoch: row.get("producer_epoch"),
    }))
}

async fn bump_transactional_producer(
    transaction: &mut Transaction<'_, Postgres>,
    transactional_id: &str,
    current: ProducerSession,
    timeout_ms: i32,
    two_phase_commit: bool,
    current_transaction_id: Option<Uuid>,
) -> Result<ProducerSession, ControlError> {
    if let Some(producer_epoch) = current.producer_epoch.checked_add(1) {
        sqlx::query(
            "UPDATE producers
             SET producer_epoch = $2, transaction_timeout_ms = $3,
                 two_phase_commit = $4, current_transaction_id = $5,
                 updated_at = now()
             WHERE producer_id = $1",
        )
        .bind(current.producer_id)
        .bind(producer_epoch)
        .bind(timeout_ms)
        .bind(two_phase_commit)
        .bind(current_transaction_id)
        .execute(&mut **transaction)
        .await?;
        return Ok(ProducerSession {
            producer_id: current.producer_id,
            producer_epoch,
        });
    }

    sqlx::query(
        "UPDATE producers
         SET transactional_id = NULL, current_transaction_id = NULL,
             two_phase_commit = FALSE, updated_at = now()
         WHERE producer_id = $1",
    )
    .bind(current.producer_id)
    .execute(&mut **transaction)
    .await?;
    insert_producer(
        transaction,
        Some(transactional_id),
        timeout_ms,
        two_phase_commit,
        current_transaction_id,
    )
    .await
}

async fn validate_producer(
    transaction: &mut Transaction<'_, Postgres>,
    transactional_id: &str,
    producer: ProducerSession,
) -> Result<Option<Uuid>, ControlError> {
    let row = sqlx::query(
        "SELECT producer_epoch, transactional_id, current_transaction_id
         FROM producers WHERE producer_id = $1 FOR UPDATE",
    )
    .bind(producer.producer_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or(ControlError::UnknownProducer(producer.producer_id))?;
    let owned_transactional_id: Option<String> = row.get("transactional_id");
    if owned_transactional_id.as_deref() != Some(transactional_id) {
        return Err(ControlError::UnknownProducer(producer.producer_id));
    }
    let expected_epoch: i16 = row.get("producer_epoch");
    if expected_epoch != producer.producer_epoch {
        return Err(ControlError::ProducerFenced {
            producer_id: producer.producer_id,
            expected_epoch,
            actual_epoch: producer.producer_epoch,
        });
    }
    Ok(row.get("current_transaction_id"))
}

async fn current_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    transactional_id: &str,
    producer: ProducerSession,
    create: bool,
    allow_prepared_recovery: bool,
) -> Result<Uuid, ControlError> {
    if let Some(transaction_id) = validate_producer(transaction, transactional_id, producer).await?
    {
        let row = sqlx::query(
            "SELECT status, producer_id, producer_epoch
             FROM transactions WHERE id = $1",
        )
        .bind(transaction_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| {
            ControlError::InvalidTransactionState(
                "producer points to a missing transaction".to_owned(),
            )
        })?;
        let status: String = row.get("status");
        if status == TransactionStatus::Ongoing.as_str() {
            let prepared_producer = ProducerSession {
                producer_id: row.get("producer_id"),
                producer_epoch: row.get("producer_epoch"),
            };
            if !allow_prepared_recovery && prepared_producer != producer {
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
        sqlx::query(
            "UPDATE producers SET current_transaction_id = NULL, updated_at = now()
             WHERE producer_id = $1",
        )
        .bind(producer.producer_id)
        .execute(&mut **transaction)
        .await?;
    }
    if !create {
        return Err(ControlError::InvalidTransactionState(
            "transaction has not started".to_owned(),
        ));
    }
    let producer_config = sqlx::query(
        "SELECT transaction_timeout_ms, two_phase_commit
         FROM producers WHERE producer_id = $1",
    )
    .bind(producer.producer_id)
    .fetch_one(&mut **transaction)
    .await?;
    let timeout_ms: i32 = producer_config.get("transaction_timeout_ms");
    let two_phase_commit: bool = producer_config.get("two_phase_commit");
    let transaction_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO transactions
         (id, transactional_id, producer_id, producer_epoch, status, timeout_ms,
          two_phase_commit)
         VALUES ($1, $2, $3, $4, 'ongoing', $5, $6)",
    )
    .bind(transaction_id)
    .bind(transactional_id)
    .bind(producer.producer_id)
    .bind(producer.producer_epoch)
    .bind(timeout_ms)
    .bind(two_phase_commit)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE producers SET current_transaction_id = $2, updated_at = now()
         WHERE producer_id = $1",
    )
    .bind(producer.producer_id)
    .bind(transaction_id)
    .execute(&mut **transaction)
    .await?;
    Ok(transaction_id)
}

async fn abort_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    transaction_id: Uuid,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE transactions SET status = 'aborted', completed_at = now()
         WHERE id = $1 AND status = 'ongoing'",
    )
    .bind(transaction_id)
    .execute(&mut **transaction)
    .await?;
    sqlx::query(
        "UPDATE object_spans SET txn_state = 'aborted'
         WHERE transaction_id = $1 AND txn_state = 'pending'",
    )
    .bind(transaction_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
