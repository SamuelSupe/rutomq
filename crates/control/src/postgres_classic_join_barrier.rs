use crate::groups;
use crate::postgres_classic_group_store::{
    apply_selected_protocol, clear_assignments, delete_expired_pending_members, expire_members,
    load_members, reject_consumer_protocol_group, validate_identity, validate_join,
};
use crate::{ControlError, GroupMember, JoinGroupResult};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

const RETRY_AFTER_MS: i64 = 25;

#[derive(Debug)]
struct GroupRow {
    generation_id: i32,
    protocol_type: String,
    protocol_name: Option<String>,
    leader_id: Option<String>,
    rebalance_id: Option<Uuid>,
    rebalance_pending: bool,
    rebalance_timed_out: bool,
    initial_rebalance_elapsed: bool,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn begin(
    pool: &PgPool,
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
    let (client_id, client_host, subscribed_topics, session_timeout_ms) = client;
    validate_join(group_id, protocol_type, protocols, session_timeout_ms)?;
    if rebalance_timeout_ms <= 0 || initial_rebalance_delay_ms < 0 || max_size <= 0 {
        return Err(ControlError::InvalidRequest(
            "rebalance timeout and maximum size must be positive and initial delay non-negative"
                .to_owned(),
        ));
    }

    let mut transaction = pool.begin().await?;
    reject_consumer_protocol_group(&mut transaction, group_id).await?;
    delete_expired_pending_members(&mut transaction, group_id).await?;
    let mut group = load_group(&mut transaction, group_id).await?;
    let group_exists = group.is_some();
    let expired = if group_exists {
        expire_members(&mut transaction, group_id).await? > 0
    } else {
        false
    };
    let members = load_members(&mut transaction, group_id).await?;
    validate_protocols(group.as_ref(), &members, group_id, protocol_type, protocols)?;

    if group_instance_id.is_none() && requested_member_id.is_empty() && api_version >= 4 {
        if members.len() >= max_size as usize {
            return Err(ControlError::GroupMaxSizeReached(group_id.to_owned()));
        }
        let member_id = format!("{client_id}-{}", Uuid::new_v4());
        sqlx::query(
            "INSERT INTO classic_group_pending_members (group_id, member_id, expires_at)
             VALUES ($1, $2, now() + $3 * interval '1 millisecond')",
        )
        .bind(group_id)
        .bind(&member_id)
        .bind(session_timeout_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        return Err(ControlError::MemberIdRequired { member_id });
    }

    let mapped_static_member = group_instance_id.and_then(|instance_id| {
        members
            .iter()
            .find(|member| member.group_instance_id.as_deref() == Some(instance_id))
            .map(|member| member.member_id.clone())
    });
    let existing_requested_member = !requested_member_id.is_empty()
        && members
            .iter()
            .any(|member| member.member_id == requested_member_id);
    let (member_id, replaced_member_id) = resolve_member_id(
        &mut transaction,
        group_id,
        requested_member_id,
        group_instance_id,
        mapped_static_member,
        existing_requested_member,
        client_id,
    )
    .await?;
    let new_member = !existing_requested_member && replaced_member_id.is_none();
    if new_member && members.len() >= max_size as usize {
        return Err(ControlError::GroupMaxSizeReached(group_id.to_owned()));
    }

    if !group_exists {
        sqlx::query(
            "INSERT INTO consumer_groups
             (group_id, generation_id, protocol_type, protocol_name, leader_id)
             VALUES ($1, 0, $2, NULL, NULL)",
        )
        .bind(group_id)
        .bind(protocol_type)
        .execute(&mut *transaction)
        .await?;
        group = load_group(&mut transaction, group_id).await?;
    }
    let mut group = group.expect("classic group exists after insert");
    let assigned_count = assignment_count(&mut transaction, group_id).await?;
    let was_empty = members.is_empty();
    let was_stable =
        !group.rebalance_pending && !was_empty && assigned_count == members.len() as i64;
    let was_completing = !group.rebalance_pending && !was_empty && !was_stable;
    let previous_member = replaced_member_id
        .as_deref()
        .or(existing_requested_member.then_some(member_id.as_str()))
        .and_then(|id| members.iter().find(|member| member.member_id == id))
        .cloned();
    let incoming_protocols = groups::protocols(protocols);
    let protocol_name = select_protocol(
        &members,
        group_id,
        &member_id,
        replaced_member_id.as_deref(),
        &incoming_protocols,
    )?;
    let protocol_changed = group
        .protocol_name
        .as_deref()
        .is_some_and(|current| current != protocol_name);
    let member_changed = previous_member
        .as_ref()
        .is_some_and(|member| member.protocols != incoming_protocols);
    let identity_replaced = replaced_member_id.is_some();
    let old_leader = replaced_member_id
        .as_deref()
        .filter(|old_id| group.leader_id.as_deref() == Some(*old_id))
        .map(str::to_owned);

    if let Some(replaced_member_id) = &replaced_member_id {
        sqlx::query(
            "UPDATE consumer_group_members SET member_id = $3
             WHERE group_id = $1 AND member_id = $2",
        )
        .bind(group_id)
        .bind(replaced_member_id)
        .bind(&member_id)
        .execute(&mut *transaction)
        .await?;
        if group.leader_id.as_deref() == Some(replaced_member_id) {
            group.leader_id = Some(member_id.clone());
        }
    }

    let no_rebalance = !expired
        && ((identity_replaced && was_stable && !protocol_changed)
            || (!identity_replaced
                && previous_member.is_some()
                && was_completing
                && !member_changed
                && !protocol_changed)
            || (!identity_replaced
                && previous_member.is_some()
                && was_stable
                && group.leader_id.as_deref() != Some(member_id.as_str())
                && !member_changed
                && !protocol_changed));
    let active_rebalance_id = if group.rebalance_pending {
        group.rebalance_id
    } else if no_rebalance {
        None
    } else {
        Some(Uuid::new_v4())
    };
    upsert_member(
        &mut transaction,
        group_id,
        &member_id,
        group_instance_id,
        &protocol_name,
        &incoming_protocols,
        subscribed_topics,
        client_id,
        client_host,
        rebalance_timeout_ms,
        session_timeout_ms,
        active_rebalance_id.or_else(|| {
            previous_member
                .as_ref()
                .and_then(|member| member.joined_rebalance_id)
        }),
    )
    .await?;
    apply_selected_protocol(&mut transaction, group_id, &protocol_name).await?;

    if no_rebalance {
        let leader = group.leader_id.unwrap_or_else(|| member_id.clone());
        sqlx::query(
            "UPDATE consumer_groups
             SET protocol_type = $2, protocol_name = $3, leader_id = $4,
                 empty_since_ms = NULL, updated_at = now()
             WHERE group_id = $1",
        )
        .bind(group_id)
        .bind(protocol_type)
        .bind(&protocol_name)
        .bind(&leader)
        .execute(&mut *transaction)
        .await?;
        group = load_group(&mut transaction, group_id)
            .await?
            .expect("updated group exists");
        let response_leader = old_leader
            .filter(|_| api_version < 9)
            .unwrap_or_else(|| leader.clone());
        let skip_assignment = api_version >= 9 && identity_replaced && leader == member_id;
        let result = build_result(
            &mut transaction,
            group_id,
            &group,
            member_id,
            response_leader,
            skip_assignment,
        )
        .await?;
        transaction.commit().await?;
        return Ok(result);
    }

    if !group.rebalance_pending {
        start_rebalance(
            &mut transaction,
            group_id,
            active_rebalance_id.expect("new rebalance has an id"),
            was_empty,
            initial_rebalance_delay_ms,
            protocol_type,
            &protocol_name,
            group.leader_id.as_deref().unwrap_or(&member_id),
        )
        .await?;
    } else {
        extend_rebalance(&mut transaction, group_id, initial_rebalance_delay_ms).await?;
    }
    group = finish_if_ready(&mut transaction, group_id).await?;
    let result = build_result(
        &mut transaction,
        group_id,
        &group,
        member_id,
        group.leader_id.clone().unwrap_or_default(),
        false,
    )
    .await?;
    transaction.commit().await?;
    Ok(result)
}

pub(crate) async fn poll(
    pool: &PgPool,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
    rebalance_id: Uuid,
    _api_version: i16,
) -> Result<JoinGroupResult, ControlError> {
    let mut transaction = pool.begin().await?;
    super::postgres_streams_groups::lock_group(&mut transaction, group_id).await?;
    let mut group = load_group(&mut transaction, group_id)
        .await?
        .ok_or_else(|| ControlError::GroupNotFound(group_id.to_owned()))?;
    validate_identity(&mut transaction, group_id, member_id, group_instance_id).await?;
    if group.rebalance_id != Some(rebalance_id) {
        return Err(ControlError::RebalanceInProgress(group_id.to_owned()));
    }
    expire_members(&mut transaction, group_id).await?;
    group = finish_if_ready(&mut transaction, group_id).await?;
    let result = build_result(
        &mut transaction,
        group_id,
        &group,
        member_id.to_owned(),
        group.leader_id.clone().unwrap_or_default(),
        false,
    )
    .await?;
    transaction.commit().await?;
    Ok(result)
}

pub(crate) async fn finish_after_membership_change(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<(), ControlError> {
    finish_if_ready(transaction, group_id).await?;
    Ok(())
}

async fn load_group(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<Option<GroupRow>, ControlError> {
    Ok(sqlx::query(
        "SELECT generation_id, protocol_type, protocol_name, leader_id,
                classic_rebalance_id, classic_rebalance_pending,
                classic_rebalance_deadline IS NOT NULL
                    AND classic_rebalance_deadline <= clock_timestamp()
                    AS rebalance_timed_out,
                classic_initial_rebalance_deadline IS NULL
                    OR classic_initial_rebalance_deadline <= clock_timestamp()
                    AS initial_rebalance_elapsed
         FROM consumer_groups WHERE group_id = $1 FOR UPDATE",
    )
    .bind(group_id)
    .fetch_optional(&mut **transaction)
    .await?
    .map(|row| GroupRow {
        generation_id: row.get("generation_id"),
        protocol_type: row.get("protocol_type"),
        protocol_name: row.get("protocol_name"),
        leader_id: row.get("leader_id"),
        rebalance_id: row.get("classic_rebalance_id"),
        rebalance_pending: row.get("classic_rebalance_pending"),
        rebalance_timed_out: row.get("rebalance_timed_out"),
        initial_rebalance_elapsed: row.get("initial_rebalance_elapsed"),
    }))
}

fn validate_protocols(
    group: Option<&GroupRow>,
    members: &[GroupMember],
    group_id: &str,
    protocol_type: &str,
    protocols: &[(String, Vec<u8>)],
) -> Result<(), ControlError> {
    if !members.is_empty()
        && group.is_some_and(|group| group.protocol_type.as_str() != protocol_type)
    {
        return Err(ControlError::InconsistentGroupProtocol(group_id.to_owned()));
    }
    let incoming = groups::protocols(protocols);
    let mut sets = members
        .iter()
        .map(|member| member.protocols.as_slice())
        .collect::<Vec<_>>();
    sets.push(&incoming);
    if groups::select_protocol(&sets).is_none() {
        return Err(ControlError::InconsistentGroupProtocol(group_id.to_owned()));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn resolve_member_id(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    requested_member_id: &str,
    group_instance_id: Option<&str>,
    mapped_static_member: Option<String>,
    existing_requested_member: bool,
    client_id: &str,
) -> Result<(String, Option<String>), ControlError> {
    match (
        group_instance_id,
        requested_member_id.is_empty(),
        mapped_static_member,
    ) {
        (Some(_), true, existing) => Ok((format!("{client_id}-{}", Uuid::new_v4()), existing)),
        (Some(instance_id), false, Some(existing)) if existing != requested_member_id => {
            Err(ControlError::FencedInstanceId {
                group: group_id.to_owned(),
                instance_id: instance_id.to_owned(),
            })
        }
        (Some(_), false, Some(existing)) => Ok((existing, None)),
        (Some(_), false, None) => Err(ControlError::GroupMemberNotFound {
            group: group_id.to_owned(),
            member: requested_member_id.to_owned(),
        }),
        (None, true, _) => Ok((format!("{client_id}-{}", Uuid::new_v4()), None)),
        (None, false, _) if existing_requested_member => Ok((requested_member_id.to_owned(), None)),
        (None, false, _) => {
            let pending = sqlx::query(
                "DELETE FROM classic_group_pending_members
                 WHERE group_id = $1 AND member_id = $2 AND expires_at > now()
                 RETURNING member_id",
            )
            .bind(group_id)
            .bind(requested_member_id)
            .fetch_optional(&mut **transaction)
            .await?;
            if pending.is_none() {
                return Err(ControlError::GroupMemberNotFound {
                    group: group_id.to_owned(),
                    member: requested_member_id.to_owned(),
                });
            }
            Ok((requested_member_id.to_owned(), None))
        }
    }
}

fn select_protocol(
    members: &[GroupMember],
    group_id: &str,
    member_id: &str,
    replaced_member_id: Option<&str>,
    incoming: &[crate::GroupProtocol],
) -> Result<String, ControlError> {
    let mut sets = members
        .iter()
        .filter(|member| {
            replaced_member_id != Some(member.member_id.as_str()) && member.member_id != member_id
        })
        .map(|member| member.protocols.as_slice())
        .collect::<Vec<_>>();
    sets.push(incoming);
    groups::select_protocol(&sets)
        .ok_or_else(|| ControlError::InconsistentGroupProtocol(group_id.to_owned()))
}

#[allow(clippy::too_many_arguments)]
async fn upsert_member(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    member_id: &str,
    group_instance_id: Option<&str>,
    protocol_name: &str,
    protocols: &[crate::GroupProtocol],
    subscribed_topics: &[String],
    client_id: &str,
    client_host: &str,
    rebalance_timeout_ms: i32,
    session_timeout_ms: i32,
    joined_rebalance_id: Option<Uuid>,
) -> Result<(), ControlError> {
    let names = protocols
        .iter()
        .map(|protocol| protocol.name.clone())
        .collect::<Vec<_>>();
    let metadata_set = protocols
        .iter()
        .map(|protocol| protocol.metadata.clone())
        .collect::<Vec<_>>();
    let metadata = protocols
        .iter()
        .find(|protocol| protocol.name == protocol_name)
        .map(|protocol| protocol.metadata.clone())
        .expect("selected protocol is offered by joining member");
    sqlx::query(
        "INSERT INTO consumer_group_members
         (group_id, member_id, group_instance_id, protocol_name, protocol_metadata,
          protocol_names, protocol_metadata_set, subscribed_topics, client_id, client_host,
          rebalance_timeout_ms, session_timeout_ms, classic_joined_rebalance_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
         ON CONFLICT (group_id, member_id)
         DO UPDATE SET group_instance_id = EXCLUDED.group_instance_id,
                       protocol_name = EXCLUDED.protocol_name,
                       protocol_metadata = EXCLUDED.protocol_metadata,
                       protocol_names = EXCLUDED.protocol_names,
                       protocol_metadata_set = EXCLUDED.protocol_metadata_set,
                       subscribed_topics = EXCLUDED.subscribed_topics,
                       client_id = EXCLUDED.client_id,
                       client_host = EXCLUDED.client_host,
                       rebalance_timeout_ms = EXCLUDED.rebalance_timeout_ms,
                       session_timeout_ms = EXCLUDED.session_timeout_ms,
                       classic_joined_rebalance_id = EXCLUDED.classic_joined_rebalance_id,
                       last_heartbeat = now()",
    )
    .bind(group_id)
    .bind(member_id)
    .bind(group_instance_id)
    .bind(protocol_name)
    .bind(metadata)
    .bind(names)
    .bind(metadata_set)
    .bind(subscribed_topics)
    .bind(client_id)
    .bind(client_host)
    .bind(rebalance_timeout_ms)
    .bind(session_timeout_ms)
    .bind(joined_rebalance_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn assignment_count(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<i64, ControlError> {
    Ok(sqlx::query(
        "SELECT count(*) AS count FROM consumer_group_members
         WHERE group_id = $1 AND assignment IS NOT NULL",
    )
    .bind(group_id)
    .fetch_one(&mut **transaction)
    .await?
    .get("count"))
}

#[allow(clippy::too_many_arguments)]
async fn start_rebalance(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    rebalance_id: Uuid,
    initial: bool,
    initial_delay_ms: i32,
    protocol_type: &str,
    protocol_name: &str,
    leader_id: &str,
) -> Result<(), ControlError> {
    clear_assignments(transaction, group_id).await?;
    let timeout_ms = sqlx::query(
        "SELECT max(rebalance_timeout_ms)::integer AS timeout_ms
         FROM consumer_group_members WHERE group_id = $1",
    )
    .bind(group_id)
    .fetch_one(&mut **transaction)
    .await?
    .get::<Option<i32>, _>("timeout_ms")
    .unwrap_or(1);
    sqlx::query(
        "UPDATE consumer_groups
         SET protocol_type = $2, protocol_name = $3, leader_id = $4,
             classic_rebalance_id = $5, classic_rebalance_pending = TRUE,
             classic_rebalance_started_at = clock_timestamp(),
             classic_rebalance_deadline =
                 clock_timestamp() + $6 * interval '1 millisecond',
             classic_initial_rebalance_deadline =
                 CASE WHEN $7
                      THEN clock_timestamp() + LEAST($8, $6) * interval '1 millisecond'
                      ELSE NULL END,
             empty_since_ms = NULL,
             updated_at = now()
         WHERE group_id = $1",
    )
    .bind(group_id)
    .bind(protocol_type)
    .bind(protocol_name)
    .bind(leader_id)
    .bind(rebalance_id)
    .bind(timeout_ms)
    .bind(initial)
    .bind(initial_delay_ms)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn extend_rebalance(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    initial_delay_ms: i32,
) -> Result<(), ControlError> {
    sqlx::query(
        "UPDATE consumer_groups
         SET classic_rebalance_deadline = GREATEST(
                 classic_rebalance_deadline,
                 classic_rebalance_started_at + (
                     SELECT max(rebalance_timeout_ms)
                     FROM consumer_group_members WHERE group_id = $1
                 ) * interval '1 millisecond'
             ),
             classic_initial_rebalance_deadline = CASE
                 WHEN classic_initial_rebalance_deadline IS NULL THEN NULL
                 ELSE LEAST(
                     classic_rebalance_deadline,
                     clock_timestamp() + $2 * interval '1 millisecond'
                 )
             END,
             updated_at = now()
         WHERE group_id = $1",
    )
    .bind(group_id)
    .bind(initial_delay_ms)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn finish_if_ready(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
) -> Result<GroupRow, ControlError> {
    let mut group = load_group(transaction, group_id)
        .await?
        .ok_or_else(|| ControlError::GroupNotFound(group_id.to_owned()))?;
    if !group.rebalance_pending {
        return Ok(group);
    }
    let rebalance_id = group
        .rebalance_id
        .expect("pending PostgreSQL rebalance has an id");
    let all_joined = sqlx::query(
        "SELECT count(*) = count(*) FILTER (
             WHERE classic_joined_rebalance_id = $2
         ) AS all_joined
         FROM consumer_group_members WHERE group_id = $1",
    )
    .bind(group_id)
    .bind(rebalance_id)
    .fetch_one(&mut **transaction)
    .await?
    .get::<bool, _>("all_joined");
    // PostgreSQL creates the deadlines, so it must also evaluate them. Comparing
    // against an Agent clock can complete a generation early when hosts drift.
    let initial_elapsed = group.initial_rebalance_elapsed;
    let timed_out = group.rebalance_timed_out;
    if !(timed_out || (all_joined && initial_elapsed)) {
        return Ok(group);
    }

    if timed_out {
        sqlx::query(
            "DELETE FROM consumer_group_members
             WHERE group_id = $1
               AND group_instance_id IS NULL
               AND classic_joined_rebalance_id IS DISTINCT FROM $2",
        )
        .bind(group_id)
        .bind(rebalance_id)
        .execute(&mut **transaction)
        .await?;
    }
    let members = load_members(transaction, group_id).await?;
    let leader_is_joined = group.leader_id.as_ref().is_some_and(|leader| {
        members.iter().any(|member| {
            &member.member_id == leader && member.joined_rebalance_id == Some(rebalance_id)
        })
    });
    let leader = if leader_is_joined {
        group.leader_id.clone().unwrap_or_default()
    } else {
        members
            .iter()
            .filter(|member| member.joined_rebalance_id == Some(rebalance_id))
            .map(|member| member.member_id.clone())
            .min()
            .unwrap_or_default()
    };
    let sets = members
        .iter()
        .map(|member| member.protocols.as_slice())
        .collect::<Vec<_>>();
    let protocol_name = groups::select_protocol(&sets)
        .ok_or_else(|| ControlError::InconsistentGroupProtocol(group_id.to_owned()))?;
    apply_selected_protocol(transaction, group_id, &protocol_name).await?;
    sqlx::query(
        "UPDATE consumer_groups
         SET generation_id = generation_id + 1,
             protocol_name = $2, leader_id = $3,
             classic_rebalance_pending = FALSE,
             classic_rebalance_started_at = NULL,
             classic_rebalance_deadline = NULL,
             classic_initial_rebalance_deadline = NULL,
             empty_since_ms = NULL,
             updated_at = now()
         WHERE group_id = $1",
    )
    .bind(group_id)
    .bind(protocol_name)
    .bind(leader)
    .execute(&mut **transaction)
    .await?;
    group = load_group(transaction, group_id)
        .await?
        .expect("completed group exists");
    Ok(group)
}

async fn build_result(
    transaction: &mut Transaction<'_, Postgres>,
    group_id: &str,
    group: &GroupRow,
    member_id: String,
    leader: String,
    skip_assignment: bool,
) -> Result<JoinGroupResult, ControlError> {
    Ok(JoinGroupResult {
        generation_id: group.generation_id,
        protocol_type: group.protocol_type.clone(),
        protocol_name: group.protocol_name.clone().unwrap_or_default(),
        leader,
        member_id,
        members: load_members(transaction, group_id).await?,
        skip_assignment,
        pending_rebalance: group.rebalance_pending.then_some(
            group
                .rebalance_id
                .expect("pending PostgreSQL result has a rebalance id"),
        ),
        retry_after_ms: if group.rebalance_pending {
            RETRY_AFTER_MS
        } else {
            0
        },
    })
}
