use crate::streams_groups::{
    self, StreamsGroupDescription, StreamsGroupHeartbeat, StreamsGroupHeartbeatResult,
    StreamsGroupState, StreamsMemberState,
};
use crate::{
    ControlError, GroupAssignmentCompletion, GroupAssignmentTask, GroupHeartbeatOutcome, TopicInfo,
};
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;

pub(crate) async fn heartbeat(
    pool: &PgPool,
    heartbeat: StreamsGroupHeartbeat,
) -> Result<StreamsGroupHeartbeatResult, ControlError> {
    let mut transaction = pool.begin().await?;
    lock_group(&mut transaction, &heartbeat.group_id).await?;
    reject_other_protocols(&mut transaction, &heartbeat.group_id).await?;
    let current = load_group(&mut transaction, &heartbeat.group_id).await?;
    let topics = load_topics(&mut transaction).await?;
    let (group, result) =
        streams_groups::heartbeat(current, heartbeat, &topics, chrono::Utc::now())?;
    save_group(&mut transaction, &group).await?;
    transaction.commit().await?;
    Ok(result)
}

pub(crate) async fn heartbeat_deferred(
    pool: &PgPool,
    heartbeat: StreamsGroupHeartbeat,
) -> Result<GroupHeartbeatOutcome<StreamsGroupHeartbeatResult>, ControlError> {
    let mut transaction = pool.begin().await?;
    lock_group(&mut transaction, &heartbeat.group_id).await?;
    reject_other_protocols(&mut transaction, &heartbeat.group_id).await?;
    let current = load_group(&mut transaction, &heartbeat.group_id).await?;
    let topics = load_topics(&mut transaction).await?;
    let (group, result, assignment_task) =
        streams_groups::heartbeat_deferred(current, heartbeat, &topics, chrono::Utc::now())?;
    save_group(&mut transaction, &group).await?;
    transaction.commit().await?;
    Ok(GroupHeartbeatOutcome {
        result,
        assignment_task,
    })
}

pub(crate) async fn complete_assignment(
    pool: &PgPool,
    task: &GroupAssignmentTask,
) -> Result<GroupAssignmentCompletion, ControlError> {
    let mut transaction = pool.begin().await?;
    lock_group(&mut transaction, &task.group_id).await?;
    let Some(mut group) = load_group(&mut transaction, &task.group_id).await? else {
        transaction.commit().await?;
        return Ok(GroupAssignmentCompletion::GroupNotFound);
    };
    let topics = load_topics(&mut transaction).await?;
    let completion =
        streams_groups::complete_assignment(&mut group, &topics, task, chrono::Utc::now())?;
    if !matches!(completion, GroupAssignmentCompletion::Stale) {
        save_group(&mut transaction, &group).await?;
    }
    transaction.commit().await?;
    Ok(completion)
}

pub(crate) async fn describe(
    pool: &PgPool,
    group_ids: &[String],
) -> Result<HashMap<String, StreamsGroupDescription>, ControlError> {
    let mut descriptions = HashMap::new();
    for group_id in group_ids {
        let mut transaction = pool.begin().await?;
        lock_group(&mut transaction, group_id).await?;
        if let Some(mut group) = load_group(&mut transaction, group_id).await? {
            let topics = load_topics(&mut transaction).await?;
            let (changed, description) =
                streams_groups::expire_and_describe(&mut group, &topics, chrono::Utc::now())?;
            if changed {
                save_group(&mut transaction, &group).await?;
            }
            descriptions.insert(group_id.clone(), description);
        }
        transaction.commit().await?;
    }
    Ok(descriptions)
}

pub(crate) async fn validate_member(
    pool: &PgPool,
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
) -> Result<bool, ControlError> {
    let mut transaction = pool.begin().await?;
    lock_group(&mut transaction, group_id).await?;
    let Some(mut group) = load_group(&mut transaction, group_id).await? else {
        transaction.commit().await?;
        return Ok(false);
    };
    let topics = load_topics(&mut transaction).await?;
    let (changed, _) =
        streams_groups::expire_and_describe(&mut group, &topics, chrono::Utc::now())?;
    if changed {
        save_group(&mut transaction, &group).await?;
    }
    let result = streams_groups::validate_member(&group, member_id, member_epoch);
    transaction.commit().await?;
    result?;
    Ok(true)
}

pub(crate) async fn lock_group(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<(), ControlError> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 1))")
        .bind(group_id)
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

async fn reject_other_protocols(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<(), ControlError> {
    let conflict = sqlx::query(
        "SELECT
             EXISTS (SELECT 1 FROM consumer_groups WHERE group_id = $1)
             OR EXISTS (
                 SELECT 1 FROM consumer_protocol_groups WHERE group_id = $1
             )
             OR EXISTS (SELECT 1 FROM share_groups WHERE group_id = $1)
             AS conflict",
    )
    .bind(group_id)
    .fetch_one(&mut **transaction)
    .await?
    .get::<bool, _>("conflict");
    if conflict {
        return Err(ControlError::GroupProtocolMismatch(group_id.to_owned()));
    }
    Ok(())
}

async fn load_group(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<Option<StreamsGroupState>, ControlError> {
    let row = sqlx::query(
        "SELECT group_epoch, assignment_epoch, assignment_timestamp,
                assignment_interval_ms, endpoint_information_epoch,
                topology, statuses, shutdown_requested,
                num_standby_replicas, initial_rebalance_deadline
         FROM streams_protocol_groups WHERE group_id = $1 FOR UPDATE",
    )
    .bind(group_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let member_rows = sqlx::query(
        "SELECT member_id, member_epoch, previous_member_epoch,
                instance_id, rack_id,
                rebalance_timeout_ms, session_timeout_ms, topology_epoch,
                process_id, user_endpoint, client_tags, task_offsets,
                task_end_offsets, current_assignment, target_assignment,
                owned_assignment, client_id, client_host, last_heartbeat
         FROM streams_protocol_members
         WHERE group_id = $1 ORDER BY member_id",
    )
    .bind(group_id)
    .fetch_all(&mut **transaction)
    .await?;
    let members = member_rows
        .into_iter()
        .map(|member| {
            let member_id = member.get::<String, _>("member_id");
            Ok((
                member_id.clone(),
                StreamsMemberState {
                    member_id,
                    member_epoch: member.get("member_epoch"),
                    previous_member_epoch: member.get("previous_member_epoch"),
                    instance_id: member.get("instance_id"),
                    rack_id: member.get("rack_id"),
                    rebalance_timeout_ms: member.get("rebalance_timeout_ms"),
                    session_timeout_ms: member.get("session_timeout_ms"),
                    topology_epoch: member.get("topology_epoch"),
                    process_id: member.get("process_id"),
                    user_endpoint: optional_json(&member, "user_endpoint")?,
                    client_tags: json(&member, "client_tags")?,
                    task_offsets: json(&member, "task_offsets")?,
                    task_end_offsets: json(&member, "task_end_offsets")?,
                    current_assignment: json(&member, "current_assignment")?,
                    target_assignment: json(&member, "target_assignment")?,
                    owned_assignment: json(&member, "owned_assignment")?,
                    client_id: member.get("client_id"),
                    client_host: member.get("client_host"),
                    last_heartbeat: member.get("last_heartbeat"),
                },
            ))
        })
        .collect::<Result<HashMap<_, _>, ControlError>>()?;
    Ok(Some(StreamsGroupState {
        group_id: group_id.to_owned(),
        group_epoch: row.get("group_epoch"),
        assignment_epoch: row.get("assignment_epoch"),
        assignment_timestamp: row.get("assignment_timestamp"),
        assignment_interval_ms: row.get("assignment_interval_ms"),
        endpoint_information_epoch: row.get("endpoint_information_epoch"),
        topology: json(&row, "topology")?,
        statuses: json(&row, "statuses")?,
        shutdown_requested: row.get("shutdown_requested"),
        num_standby_replicas: row.get("num_standby_replicas"),
        initial_rebalance_deadline: row.get("initial_rebalance_deadline"),
        members,
    }))
}

async fn load_topics(
    transaction: &mut Transaction<'_, Postgres>,
) -> Result<Vec<TopicInfo>, ControlError> {
    Ok(
        sqlx::query("SELECT id, name, partition_count FROM topics ORDER BY name")
            .fetch_all(&mut **transaction)
            .await?
            .into_iter()
            .map(|row| TopicInfo {
                id: row.get("id"),
                name: row.get("name"),
                partitions: row.get("partition_count"),
            })
            .collect(),
    )
}

async fn save_group(
    transaction: &mut Transaction<'_, Postgres>,
    group: &StreamsGroupState,
) -> Result<(), ControlError> {
    sqlx::query(
        "INSERT INTO streams_protocol_groups
             (group_id, group_epoch, assignment_epoch, assignment_timestamp,
              assignment_interval_ms,
              endpoint_information_epoch, topology, statuses,
              shutdown_requested, num_standby_replicas,
              initial_rebalance_deadline)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
         ON CONFLICT (group_id) DO UPDATE
         SET group_epoch = EXCLUDED.group_epoch,
             assignment_epoch = EXCLUDED.assignment_epoch,
             assignment_timestamp = EXCLUDED.assignment_timestamp,
             assignment_interval_ms = EXCLUDED.assignment_interval_ms,
             endpoint_information_epoch = EXCLUDED.endpoint_information_epoch,
             topology = EXCLUDED.topology,
             statuses = EXCLUDED.statuses,
             shutdown_requested = EXCLUDED.shutdown_requested,
             num_standby_replicas = EXCLUDED.num_standby_replicas,
             initial_rebalance_deadline = EXCLUDED.initial_rebalance_deadline,
             updated_at = now()",
    )
    .bind(&group.group_id)
    .bind(group.group_epoch)
    .bind(group.assignment_epoch)
    .bind(group.assignment_timestamp)
    .bind(group.assignment_interval_ms)
    .bind(group.endpoint_information_epoch)
    .bind(Json(&group.topology))
    .bind(Json(&group.statuses))
    .bind(group.shutdown_requested)
    .bind(group.num_standby_replicas)
    .bind(group.initial_rebalance_deadline)
    .execute(&mut **transaction)
    .await?;

    let member_ids = group.members.keys().cloned().collect::<Vec<_>>();
    sqlx::query(
        "DELETE FROM streams_protocol_members
         WHERE group_id = $1 AND NOT (member_id = ANY($2::text[]))",
    )
    .bind(&group.group_id)
    .bind(&member_ids)
    .execute(&mut **transaction)
    .await?;
    let mut members = group.members.values().collect::<Vec<_>>();
    members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
    for member in members {
        sqlx::query(
            "INSERT INTO streams_protocol_members
                 (group_id, member_id, member_epoch, instance_id, rack_id,
                  rebalance_timeout_ms, session_timeout_ms, topology_epoch,
                  process_id, user_endpoint, client_tags, task_offsets,
                  task_end_offsets, current_assignment, target_assignment,
                  owned_assignment, client_id, client_host, last_heartbeat,
                  previous_member_epoch)
             VALUES
                 ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                  $13, $14, $15, $16, $17, $18, $19, $20)
             ON CONFLICT (group_id, member_id) DO UPDATE
             SET member_epoch = EXCLUDED.member_epoch,
                 instance_id = EXCLUDED.instance_id,
                 rack_id = EXCLUDED.rack_id,
                 rebalance_timeout_ms = EXCLUDED.rebalance_timeout_ms,
                 session_timeout_ms = EXCLUDED.session_timeout_ms,
                 topology_epoch = EXCLUDED.topology_epoch,
                 process_id = EXCLUDED.process_id,
                 user_endpoint = EXCLUDED.user_endpoint,
                 client_tags = EXCLUDED.client_tags,
                 task_offsets = EXCLUDED.task_offsets,
                 task_end_offsets = EXCLUDED.task_end_offsets,
                 current_assignment = EXCLUDED.current_assignment,
                 target_assignment = EXCLUDED.target_assignment,
                 owned_assignment = EXCLUDED.owned_assignment,
                 client_id = EXCLUDED.client_id,
                 client_host = EXCLUDED.client_host,
                 last_heartbeat = EXCLUDED.last_heartbeat,
                 previous_member_epoch = EXCLUDED.previous_member_epoch",
        )
        .bind(&group.group_id)
        .bind(&member.member_id)
        .bind(member.member_epoch)
        .bind(&member.instance_id)
        .bind(&member.rack_id)
        .bind(member.rebalance_timeout_ms)
        .bind(member.session_timeout_ms)
        .bind(member.topology_epoch)
        .bind(&member.process_id)
        .bind(member.user_endpoint.as_ref().map(Json))
        .bind(Json(&member.client_tags))
        .bind(Json(&member.task_offsets))
        .bind(Json(&member.task_end_offsets))
        .bind(Json(&member.current_assignment))
        .bind(Json(&member.target_assignment))
        .bind(Json(&member.owned_assignment))
        .bind(&member.client_id)
        .bind(&member.client_host)
        .bind(member.last_heartbeat)
        .bind(member.previous_member_epoch)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

fn json<T>(row: &sqlx::postgres::PgRow, column: &str) -> Result<T, ControlError>
where
    T: serde::de::DeserializeOwned,
{
    let value = row.get::<serde_json::Value, _>(column);
    serde_json::from_value(value).map_err(|error| {
        ControlError::InvalidRequest(format!("invalid stored streams {column}: {error}"))
    })
}

fn optional_json<T>(row: &sqlx::postgres::PgRow, column: &str) -> Result<Option<T>, ControlError>
where
    T: serde::de::DeserializeOwned,
{
    row.get::<Option<serde_json::Value>, _>(column)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| {
            ControlError::InvalidRequest(format!("invalid stored streams {column}: {error}"))
        })
}
