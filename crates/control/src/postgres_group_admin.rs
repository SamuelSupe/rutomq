use crate::groups::classic_group_state;
use crate::{ClassicGroupDescription, ClassicGroupMemberDescription, ControlError, GroupSummary};
use sqlx::{PgPool, Row};
use std::collections::{BTreeMap, HashMap};

pub(crate) async fn list(pool: &PgPool) -> Result<Vec<GroupSummary>, ControlError> {
    list_limited(pool, usize::MAX).await
}

pub(crate) async fn list_limited(
    pool: &PgPool,
    limit: usize,
) -> Result<Vec<GroupSummary>, ControlError> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let mut groups = BTreeMap::new();
    for row in sqlx::query(
        "WITH selected AS (
             SELECT group_id
             FROM consumer_groups
             ORDER BY group_id
             LIMIT $1
         )
         SELECT g.group_id, g.protocol_type, g.classic_rebalance_pending,
                COUNT(m.member_id)::BIGINT AS member_count,
                COUNT(m.assignment)::BIGINT AS assignment_count
         FROM selected s
         JOIN consumer_groups g ON g.group_id = s.group_id
         LEFT JOIN consumer_group_members m ON m.group_id = g.group_id
         GROUP BY g.group_id, g.protocol_type, g.classic_rebalance_pending
         ORDER BY g.group_id",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?
    {
        let member_count = row.get::<i64, _>("member_count") as usize;
        let assignment_count = row.get::<i64, _>("assignment_count") as usize;
        let group_id: String = row.get("group_id");
        groups.insert(
            group_id.clone(),
            GroupSummary {
                group_id,
                protocol_type: row.get("protocol_type"),
                state: classic_group_state(
                    member_count,
                    assignment_count,
                    row.get("classic_rebalance_pending"),
                )
                .to_owned(),
                group_type: "Classic".to_owned(),
            },
        );
    }

    let consumer_ids = limited_ids(
        pool,
        "SELECT group_id
         FROM consumer_protocol_groups
         ORDER BY group_id
         LIMIT $1",
        limit,
    )
    .await?;
    for description in super::postgres_consumer_groups::describe(pool, &consumer_ids)
        .await?
        .into_values()
    {
        groups.insert(
            description.group_id.clone(),
            GroupSummary {
                group_id: description.group_id,
                protocol_type: "consumer".to_owned(),
                state: description.state,
                group_type: "Consumer".to_owned(),
            },
        );
    }

    let streams_ids = limited_ids(
        pool,
        "SELECT group_id
         FROM streams_protocol_groups
         ORDER BY group_id
         LIMIT $1",
        limit,
    )
    .await?;
    for description in super::postgres_streams_groups::describe(pool, &streams_ids)
        .await?
        .into_values()
    {
        groups.insert(
            description.group_id.clone(),
            GroupSummary {
                group_id: description.group_id,
                protocol_type: "streams".to_owned(),
                state: description.state,
                group_type: "Streams".to_owned(),
            },
        );
    }

    let share_ids = limited_ids(
        pool,
        "SELECT group_id
         FROM share_groups
         ORDER BY group_id
         LIMIT $1",
        limit,
    )
    .await?;
    for description in super::postgres_share_groups::describe(pool, &share_ids)
        .await?
        .into_values()
    {
        groups.insert(
            description.group_id.clone(),
            GroupSummary {
                group_id: description.group_id,
                protocol_type: "share".to_owned(),
                state: description.state,
                group_type: "Share".to_owned(),
            },
        );
    }

    for row in sqlx::query(
        "SELECT DISTINCT group_id
         FROM consumer_offsets
         ORDER BY group_id
         LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?
    {
        let group_id: String = row.get("group_id");
        groups.entry(group_id.clone()).or_insert(GroupSummary {
            group_id,
            protocol_type: String::new(),
            state: "Empty".to_owned(),
            group_type: "Classic".to_owned(),
        });
    }
    Ok(groups
        .into_values()
        .take(usize::try_from(limit).unwrap_or(usize::MAX))
        .collect())
}

async fn limited_ids(pool: &PgPool, query: &str, limit: i64) -> Result<Vec<String>, ControlError> {
    Ok(sqlx::query(query)
        .bind(limit)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("group_id"))
        .collect())
}

pub(crate) async fn describe_classic(
    pool: &PgPool,
    group_ids: &[String],
) -> Result<HashMap<String, ClassicGroupDescription>, ControlError> {
    let mut descriptions = HashMap::new();
    for group_id in group_ids {
        let group = sqlx::query(
            "SELECT generation_id, protocol_type, protocol_name, classic_rebalance_pending
             FROM consumer_groups WHERE group_id = $1",
        )
        .bind(group_id)
        .fetch_optional(pool)
        .await?;
        let Some(group) = group else {
            if offset_group_exists(pool, group_id).await? {
                descriptions.insert(group_id.clone(), empty_description(group_id));
            }
            continue;
        };
        let member_rows = sqlx::query(
            "SELECT member_id, group_instance_id, client_id, client_host,
                    protocol_metadata, assignment
             FROM consumer_group_members
             WHERE group_id = $1 ORDER BY member_id",
        )
        .bind(group_id)
        .fetch_all(pool)
        .await?;
        let mut assignment_count = 0;
        let mut members = Vec::with_capacity(member_rows.len());
        for row in member_rows {
            let assignment = row.get::<Option<Vec<u8>>, _>("assignment");
            assignment_count += usize::from(assignment.is_some());
            members.push(ClassicGroupMemberDescription {
                member_id: row.get("member_id"),
                group_instance_id: row.get("group_instance_id"),
                client_id: row.get("client_id"),
                client_host: row.get("client_host"),
                member_metadata: row.get("protocol_metadata"),
                member_assignment: assignment.unwrap_or_default(),
            });
        }
        descriptions.insert(
            group_id.clone(),
            ClassicGroupDescription {
                group_id: group_id.clone(),
                state: classic_group_state(
                    members.len(),
                    assignment_count,
                    group.get("classic_rebalance_pending"),
                )
                .to_owned(),
                generation_id: group.get("generation_id"),
                protocol_type: group.get("protocol_type"),
                protocol_data: group
                    .get::<Option<String>, _>("protocol_name")
                    .unwrap_or_default(),
                members,
            },
        );
    }
    Ok(descriptions)
}

pub(crate) async fn delete(pool: &PgPool, group_id: &str) -> Result<(), ControlError> {
    let mut transaction = pool.begin().await?;
    let classic_exists =
        sqlx::query("SELECT 1 FROM consumer_groups WHERE group_id = $1 FOR UPDATE")
            .bind(group_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
    let consumer_exists =
        sqlx::query("SELECT 1 FROM consumer_protocol_groups WHERE group_id = $1 FOR UPDATE")
            .bind(group_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
    let share_exists = sqlx::query("SELECT 1 FROM share_groups WHERE group_id = $1 FOR UPDATE")
        .bind(group_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
    let streams_exists =
        sqlx::query("SELECT 1 FROM streams_protocol_groups WHERE group_id = $1 FOR UPDATE")
            .bind(group_id)
            .fetch_optional(&mut *transaction)
            .await?
            .is_some();
    let member_count = sqlx::query(
        "SELECT
             (SELECT COUNT(*) FROM consumer_group_members WHERE group_id = $1) +
             (SELECT COUNT(*) FROM consumer_protocol_members WHERE group_id = $1) +
             (SELECT COUNT(*) FROM streams_protocol_members WHERE group_id = $1) +
             (SELECT COUNT(*) FROM share_group_members WHERE group_id = $1)
             AS member_count",
    )
    .bind(group_id)
    .fetch_one(&mut *transaction)
    .await?
    .get::<i64, _>("member_count");
    if member_count > 0 {
        return Err(ControlError::NonEmptyGroup(group_id.to_owned()));
    }
    let offsets_exist = sqlx::query("SELECT 1 FROM consumer_offsets WHERE group_id = $1 LIMIT 1")
        .bind(group_id)
        .fetch_optional(&mut *transaction)
        .await?
        .is_some();
    if !classic_exists && !consumer_exists && !streams_exists && !share_exists && !offsets_exist {
        return Err(ControlError::GroupNotFound(group_id.to_owned()));
    }

    sqlx::query("DELETE FROM consumer_groups WHERE group_id = $1")
        .bind(group_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM consumer_protocol_groups WHERE group_id = $1")
        .bind(group_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM share_partition_states WHERE group_id = $1")
        .bind(group_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM share_groups WHERE group_id = $1")
        .bind(group_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM streams_protocol_groups WHERE group_id = $1")
        .bind(group_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query(
        "SELECT topic_id, partition_index
         FROM consumer_offsets
         WHERE group_id = $1
         ORDER BY topic_id, partition_index
         FOR UPDATE",
    )
    .bind(group_id)
    .fetch_all(&mut *transaction)
    .await?;
    sqlx::query("DELETE FROM consumer_offsets WHERE group_id = $1")
        .bind(group_id)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}

fn empty_description(group_id: &str) -> ClassicGroupDescription {
    ClassicGroupDescription {
        group_id: group_id.to_owned(),
        state: "Empty".to_owned(),
        generation_id: 0,
        protocol_type: String::new(),
        protocol_data: String::new(),
        members: Vec::new(),
    }
}

async fn offset_group_exists(pool: &PgPool, group_id: &str) -> Result<bool, ControlError> {
    Ok(
        sqlx::query("SELECT 1 FROM consumer_offsets WHERE group_id = $1 LIMIT 1")
            .bind(group_id)
            .fetch_optional(pool)
            .await?
            .is_some(),
    )
}
