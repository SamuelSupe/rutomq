use rutomq_control::{
    ConsumerGroupHeartbeat, ControlError, GroupAssignment, GroupMemberIdentity, MetadataStore,
    OffsetCommit, PartitionKey, PostgresMetadataStore,
};
use uuid::Uuid;

#[tokio::test]
async fn postgres_group_admin_persists_describes_and_deletes_groups() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();

    let suffix = Uuid::new_v4().simple().to_string();
    let topic_name = format!("group-admin-topic-{suffix}");
    let classic_group = format!("classic-group-{suffix}");
    let consumer_group = format!("consumer-group-{suffix}");
    store.create_topic(&topic_name, 1).await.unwrap();
    store
        .commit_offsets(
            &classic_group,
            vec![OffsetCommit {
                partition: PartitionKey::new(&topic_name, 0),
                offset: 0,
                leader_epoch: -1,
                metadata: None,
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    let joined = store
        .join_group(
            &classic_group,
            "",
            Some("instance-a"),
            "consumer",
            &[("range".to_owned(), vec![1, 2, 3])],
            (
                "postgres-admin-test",
                "127.0.0.1",
                std::slice::from_ref(&topic_name),
                45_000,
            ),
            9,
        )
        .await
        .unwrap();
    store
        .sync_group(
            &classic_group,
            joined.generation_id,
            &joined.member_id,
            Some("instance-a"),
            vec![GroupAssignment {
                member_id: joined.member_id.clone(),
                assignment: vec![4, 5, 6],
            }],
        )
        .await
        .unwrap();

    let reconnected = PostgresMetadataStore::connect(&database_url).await.unwrap();
    let summary = reconnected
        .list_groups()
        .await
        .unwrap()
        .into_iter()
        .find(|group| group.group_id == classic_group)
        .unwrap();
    assert_eq!(summary.state, "Stable");
    assert_eq!(summary.group_type, "Classic");
    let limited = reconnected.list_groups_limited(1).await.unwrap();
    assert_eq!(limited.len(), 1);
    assert!(reconnected.list_groups_limited(0).await.unwrap().is_empty());
    let description = reconnected
        .describe_classic_groups(std::slice::from_ref(&classic_group))
        .await
        .unwrap()
        .remove(&classic_group)
        .unwrap();
    assert_eq!(description.members[0].client_id, "postgres-admin-test");
    assert_eq!(description.members[0].member_assignment, [4, 5, 6]);
    assert!(matches!(
        reconnected.delete_group(&classic_group).await,
        Err(ControlError::NonEmptyGroup(_))
    ));
    reconnected
        .leave_group(
            &classic_group,
            &[GroupMemberIdentity {
                member_id: joined.member_id,
                group_instance_id: Some("instance-a".to_owned()),
            }],
        )
        .await
        .unwrap();
    reconnected.delete_group(&classic_group).await.unwrap();

    let joined = reconnected
        .consumer_group_heartbeat(heartbeat(&consumer_group, "member-a", 0, Some(&topic_name)))
        .await
        .unwrap();
    let summary = reconnected
        .list_groups()
        .await
        .unwrap()
        .into_iter()
        .find(|group| group.group_id == consumer_group)
        .unwrap();
    assert_eq!(summary.group_type, "Consumer");
    assert!(matches!(
        reconnected.delete_group(&consumer_group).await,
        Err(ControlError::NonEmptyGroup(_))
    ));
    reconnected
        .consumer_group_heartbeat(heartbeat(&consumer_group, &joined.member_id, -1, None))
        .await
        .unwrap();
    reconnected.delete_group(&consumer_group).await.unwrap();
}

#[tokio::test]
async fn postgres_classic_group_expires_members_and_reelects_the_leader() {
    let Ok(database_url) = std::env::var("RUTOMQ_TEST_PG_URL") else {
        return;
    };
    let store = PostgresMetadataStore::connect(&database_url).await.unwrap();
    store.migrate().await.unwrap();
    let pool = sqlx::PgPool::connect(&database_url).await.unwrap();
    let group = format!("classic-expiry-{}", Uuid::new_v4().simple());
    let leader = store
        .join_group(
            &group,
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![1])],
            ("leader-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    let follower = store
        .join_group(
            &group,
            "",
            None,
            "consumer",
            &[("range".to_owned(), vec![2])],
            ("follower-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    assert_eq!(follower.leader, leader.member_id);
    sqlx::query(
        "UPDATE consumer_group_members
         SET last_heartbeat = now() - interval '1 minute'
         WHERE group_id = $1 AND member_id = $2",
    )
    .bind(&group)
    .bind(&leader.member_id)
    .execute(&pool)
    .await
    .unwrap();

    let rejoined = store
        .join_group(
            &group,
            &follower.member_id,
            None,
            "consumer",
            &[("range".to_owned(), vec![2])],
            ("follower-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    assert_eq!(rejoined.generation_id, follower.generation_id + 1);
    assert_eq!(rejoined.leader, follower.member_id);
    assert_eq!(rejoined.members.len(), 1);
    store
        .leave_group(
            &group,
            &[GroupMemberIdentity {
                member_id: follower.member_id,
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();
}

fn heartbeat(
    group_id: &str,
    member_id: &str,
    member_epoch: i32,
    topic: Option<&str>,
) -> ConsumerGroupHeartbeat {
    ConsumerGroupHeartbeat {
        group_id: group_id.to_owned(),
        member_id: member_id.to_owned(),
        member_epoch,
        instance_id: None,
        rack_id: None,
        rebalance_timeout_ms: if member_epoch == 0 { 30_000 } else { -1 },
        subscribed_topic_names: topic.map(|topic| vec![topic.to_owned()]),
        subscribed_topic_regex: None,
        server_assignor: None,
        configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
        owned_partitions: Some(Vec::new()),
        client_id: "postgres-admin-test".to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        regex_refresh_interval_ms: 600_000,
        assignment_interval_ms: 0,
        max_size: i32::MAX,
    }
}
