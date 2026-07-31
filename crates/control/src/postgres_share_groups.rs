use crate::share_groups::{ShareGroupState, ShareMemberState, apply_heartbeat};
use crate::{
    ControlError, GroupAssignmentCompletion, GroupAssignmentTask, GroupHeartbeatOutcome,
    ShareGroupDescription, ShareGroupHeartbeat, ShareGroupHeartbeatResult, ShareTopicAssignment,
    TopicInfo,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use std::collections::HashMap;

pub(crate) async fn heartbeat(
    pool: &PgPool,
    heartbeat: ShareGroupHeartbeat,
) -> Result<ShareGroupHeartbeatResult, ControlError> {
    let mut transaction = pool.begin().await?;
    lock_group(&mut transaction, &heartbeat.group_id).await?;
    let conflict = sqlx::query(
        "SELECT
             EXISTS (SELECT 1 FROM consumer_groups WHERE group_id = $1)
             OR EXISTS (
                 SELECT 1 FROM consumer_protocol_groups WHERE group_id = $1
             )
             OR EXISTS (
                 SELECT 1 FROM streams_protocol_groups WHERE group_id = $1
             ) AS conflict",
    )
    .bind(&heartbeat.group_id)
    .fetch_one(&mut *transaction)
    .await?
    .get::<bool, _>("conflict");
    if conflict {
        return Err(ControlError::GroupProtocolMismatch(
            heartbeat.group_id.clone(),
        ));
    }
    let mut group = load_group(&mut transaction, &heartbeat.group_id)
        .await?
        .unwrap_or_else(|| ShareGroupState::new(&heartbeat.group_id));
    let topics = load_topics(&mut transaction).await?;
    let result = apply_heartbeat(&mut group, heartbeat, &topics, chrono::Utc::now())?;
    save_group(&mut transaction, &group).await?;
    transaction.commit().await?;
    Ok(result)
}

pub(crate) async fn heartbeat_deferred(
    pool: &PgPool,
    heartbeat: ShareGroupHeartbeat,
) -> Result<GroupHeartbeatOutcome<ShareGroupHeartbeatResult>, ControlError> {
    let mut transaction = pool.begin().await?;
    lock_group(&mut transaction, &heartbeat.group_id).await?;
    let conflict = sqlx::query(
        "SELECT
             EXISTS (SELECT 1 FROM consumer_groups WHERE group_id = $1)
             OR EXISTS (
                 SELECT 1 FROM consumer_protocol_groups WHERE group_id = $1
             )
             OR EXISTS (
                 SELECT 1 FROM streams_protocol_groups WHERE group_id = $1
             ) AS conflict",
    )
    .bind(&heartbeat.group_id)
    .fetch_one(&mut *transaction)
    .await?
    .get::<bool, _>("conflict");
    if conflict {
        return Err(ControlError::GroupProtocolMismatch(
            heartbeat.group_id.clone(),
        ));
    }
    let mut group = load_group(&mut transaction, &heartbeat.group_id)
        .await?
        .unwrap_or_else(|| ShareGroupState::new(&heartbeat.group_id));
    let topics = load_topics(&mut transaction).await?;
    let (result, assignment_task) = crate::share_groups::apply_heartbeat_deferred(
        &mut group,
        heartbeat,
        &topics,
        chrono::Utc::now(),
    )?;
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
        crate::share_groups::complete_assignment(&mut group, &topics, task, chrono::Utc::now());
    if !matches!(completion, GroupAssignmentCompletion::Stale) {
        save_group(&mut transaction, &group).await?;
    }
    transaction.commit().await?;
    Ok(completion)
}

pub(crate) async fn describe(
    pool: &PgPool,
    group_ids: &[String],
) -> Result<HashMap<String, ShareGroupDescription>, ControlError> {
    let mut descriptions = HashMap::new();
    for group_id in group_ids {
        let mut transaction = pool.begin().await?;
        lock_group(&mut transaction, group_id).await?;
        if let Some(mut group) = load_group(&mut transaction, group_id).await? {
            let topics = load_topics(&mut transaction).await?;
            if crate::share_groups::expire_members(&mut group, &topics, chrono::Utc::now()) {
                save_group(&mut transaction, &group).await?;
            }
            descriptions.insert(group_id.clone(), group.description());
        }
        transaction.commit().await?;
    }
    Ok(descriptions)
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

pub(crate) async fn load_group(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<Option<ShareGroupState>, ControlError> {
    let row = sqlx::query(
        "SELECT group_epoch, assignment_epoch, assignment_timestamp,
                assignment_interval_ms
         FROM share_groups WHERE group_id = $1",
    )
    .bind(group_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut members = sqlx::query(
        "SELECT member_id, rack_id, member_epoch, previous_member_epoch,
                session_timeout_ms,
                subscribed_topic_names, client_id, client_host, last_heartbeat
         FROM share_group_members
         WHERE group_id = $1 ORDER BY member_id",
    )
    .bind(group_id)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|row| {
        let member_id: String = row.get("member_id");
        (
            member_id.clone(),
            ShareMemberState {
                member_id,
                rack_id: row.get("rack_id"),
                member_epoch: row.get("member_epoch"),
                previous_member_epoch: row.get("previous_member_epoch"),
                session_timeout_ms: row.get("session_timeout_ms"),
                subscribed_topic_names: row.get("subscribed_topic_names"),
                client_id: row.get("client_id"),
                client_host: row.get("client_host"),
                assignment: Vec::new(),
                last_heartbeat: row.get("last_heartbeat"),
            },
        )
    })
    .collect::<HashMap<_, _>>();
    for row in sqlx::query(
        "SELECT a.member_id, a.topic_id, t.name AS topic_name, a.partitions
         FROM share_group_assignments a
         JOIN topics t ON t.id = a.topic_id
         WHERE a.group_id = $1
         ORDER BY a.member_id, t.name",
    )
    .bind(group_id)
    .fetch_all(&mut **transaction)
    .await?
    {
        if let Some(member) = members.get_mut(&row.get::<String, _>("member_id")) {
            member.assignment.push(ShareTopicAssignment {
                topic_id: row.get("topic_id"),
                topic_name: row.get("topic_name"),
                partitions: row.get("partitions"),
            });
        }
    }
    Ok(Some(ShareGroupState {
        group_id: group_id.to_owned(),
        group_epoch: row.get("group_epoch"),
        assignment_epoch: row.get("assignment_epoch"),
        assignment_timestamp: row.get("assignment_timestamp"),
        assignment_interval_ms: row.get("assignment_interval_ms"),
        members,
    }))
}

pub(crate) async fn load_topics(
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

pub(crate) async fn save_group(
    transaction: &mut Transaction<'_, Postgres>,
    group: &ShareGroupState,
) -> Result<(), ControlError> {
    sqlx::query(
        "INSERT INTO share_groups (
             group_id, group_epoch, assignment_epoch, assignment_timestamp,
             assignment_interval_ms
         )
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (group_id) DO UPDATE
         SET group_epoch = EXCLUDED.group_epoch,
             assignment_epoch = EXCLUDED.assignment_epoch,
             assignment_timestamp = EXCLUDED.assignment_timestamp,
             assignment_interval_ms = EXCLUDED.assignment_interval_ms,
             updated_at = now()",
    )
    .bind(&group.group_id)
    .bind(group.group_epoch)
    .bind(group.assignment_epoch)
    .bind(group.assignment_timestamp)
    .bind(group.assignment_interval_ms)
    .execute(&mut **transaction)
    .await?;
    let member_ids = group.members.keys().cloned().collect::<Vec<_>>();
    sqlx::query(
        "DELETE FROM share_group_members
         WHERE group_id = $1 AND NOT (member_id = ANY($2::text[]))",
    )
    .bind(&group.group_id)
    .bind(&member_ids)
    .execute(&mut **transaction)
    .await?;
    for member in group.members.values() {
        sqlx::query(
            "INSERT INTO share_group_members (
                 group_id, member_id, rack_id, member_epoch, session_timeout_ms,
                 subscribed_topic_names, client_id, client_host, last_heartbeat,
                 previous_member_epoch
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
             ON CONFLICT (group_id, member_id) DO UPDATE
             SET rack_id = EXCLUDED.rack_id,
                 member_epoch = EXCLUDED.member_epoch,
                 session_timeout_ms = EXCLUDED.session_timeout_ms,
                 subscribed_topic_names = EXCLUDED.subscribed_topic_names,
                 client_id = EXCLUDED.client_id,
                 client_host = EXCLUDED.client_host,
                 last_heartbeat = EXCLUDED.last_heartbeat,
                 previous_member_epoch = EXCLUDED.previous_member_epoch",
        )
        .bind(&group.group_id)
        .bind(&member.member_id)
        .bind(&member.rack_id)
        .bind(member.member_epoch)
        .bind(member.session_timeout_ms)
        .bind(&member.subscribed_topic_names)
        .bind(&member.client_id)
        .bind(&member.client_host)
        .bind(member.last_heartbeat)
        .bind(member.previous_member_epoch)
        .execute(&mut **transaction)
        .await?;
    }
    sqlx::query("DELETE FROM share_group_assignments WHERE group_id = $1")
        .bind(&group.group_id)
        .execute(&mut **transaction)
        .await?;
    for member in group.members.values() {
        for assignment in &member.assignment {
            sqlx::query(
                "INSERT INTO share_group_assignments (
                     group_id, member_id, topic_id, partitions
                 ) VALUES ($1, $2, $3, $4)",
            )
            .bind(&group.group_id)
            .bind(&member.member_id)
            .bind(assignment.topic_id)
            .bind(&assignment.partitions)
            .execute(&mut **transaction)
            .await?;
        }
    }
    Ok(())
}
