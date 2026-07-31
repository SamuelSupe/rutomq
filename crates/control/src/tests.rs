use super::*;

#[tokio::test]
async fn memory_store_allocates_contiguous_offsets() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    let first = store
        .commit_object(
            ObjectRef {
                key: "objects/a".into(),
                size: 10,
            },
            vec![BatchDraft {
                partition: PartitionKey::new("events", 0),
                byte_start: 0,
                byte_end: 10,
                record_count: 2,
                timestamp_ms: 1,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    let second = store
        .commit_object(
            ObjectRef {
                key: "objects/b".into(),
                size: 5,
            },
            vec![BatchDraft {
                partition: PartitionKey::new("events", 0),
                byte_start: 0,
                byte_end: 5,
                record_count: 1,
                timestamp_ms: 2,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    assert_eq!((first[0].base_offset, first[0].last_offset), (0, 1));
    assert_eq!((second[0].base_offset, second[0].last_offset), (2, 2));
}

#[tokio::test]
async fn memory_store_increases_partition_count_without_changing_existing_logs() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "objects/before-expansion".into(),
                size: 10,
            },
            vec![BatchDraft {
                partition: PartitionKey::new("events", 0),
                byte_start: 0,
                byte_end: 10,
                record_count: 1,
                timestamp_ms: 1,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    let topic = store.create_partitions("events", 3).await.unwrap();
    assert_eq!(topic.partitions, 3);
    assert_eq!(
        store
            .list_offset(&PartitionKey::new("events", 0), -1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .list_offset(&PartitionKey::new("events", 2), -1)
            .await
            .unwrap(),
        0
    );
    assert!(matches!(
        store.create_partitions("events", 3).await,
        Err(ControlError::InvalidPartitionCount { .. })
    ));
}

#[tokio::test]
async fn memory_store_administers_share_group_offsets() {
    let store = MemoryMetadataStore::new();
    store.create_topic("share-events", 2).await.unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "objects/share-admin".into(),
                size: 3,
            },
            vec![BatchDraft {
                partition: PartitionKey::new("share-events", 0),
                byte_start: 0,
                byte_end: 3,
                record_count: 3,
                timestamp_ms: 1,
                checksum: None,
                producer: None,
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    let results = store
        .alter_share_group_offsets(
            "share-admin",
            &[
                ShareOffsetUpdate {
                    partition: PartitionKey::new("share-events", 0),
                    start_offset: 1,
                },
                ShareOffsetUpdate {
                    partition: PartitionKey::new("share-events", 9),
                    start_offset: 0,
                },
                ShareOffsetUpdate {
                    partition: PartitionKey::new("missing", 0),
                    start_offset: 0,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        results
            .iter()
            .map(|result| result.updated)
            .collect::<Vec<_>>(),
        [true, false, false]
    );
    let offset = store
        .describe_share_group_offsets("share-admin", Some(&[PartitionKey::new("share-events", 0)]))
        .await
        .unwrap()
        .remove(0);
    assert_eq!((offset.start_offset, offset.high_watermark), (1, 3));
    assert_eq!(offset.lag(), 2);
    assert_eq!(
        store
            .describe_share_group_offsets("share-admin", None)
            .await
            .unwrap()
            .len(),
        1
    );

    let joined = store
        .share_group_heartbeat(ShareGroupHeartbeat {
            group_id: "share-admin".into(),
            member_id: "member-a".into(),
            member_epoch: 0,
            rack_id: None,
            subscribed_topic_names: Some(vec!["share-events".into()]),
            client_id: "test".into(),
            client_host: "127.0.0.1".into(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 100,
            assignment_interval_ms: 0,
            max_size: 200,
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .alter_share_group_offsets(
                "share-admin",
                &[ShareOffsetUpdate {
                    partition: PartitionKey::new("share-events", 0),
                    start_offset: 2,
                }],
            )
            .await,
        Err(ControlError::NonEmptyGroup(_))
    ));
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    let expired_reset = store
        .alter_share_group_offsets(
            "share-admin",
            &[ShareOffsetUpdate {
                partition: PartitionKey::new("share-events", 0),
                start_offset: 2,
            }],
        )
        .await
        .unwrap();
    assert!(expired_reset[0].updated);
    store
        .share_group_heartbeat(ShareGroupHeartbeat {
            group_id: "share-admin".into(),
            member_id: "member-a".into(),
            member_epoch: -1,
            rack_id: None,
            subscribed_topic_names: None,
            client_id: "test".into(),
            client_host: "127.0.0.1".into(),
            heartbeat_interval_ms: joined.heartbeat_interval_ms,
            session_timeout_ms: 100,
            assignment_interval_ms: 0,
            max_size: 200,
        })
        .await
        .unwrap();
    let deleted = store
        .delete_share_group_offsets("share-admin", &["share-events".into()])
        .await
        .unwrap();
    assert!(deleted[0].deleted);
    assert!(
        store
            .describe_share_group_offsets("share-admin", None)
            .await
            .unwrap()
            .is_empty()
    );
    store.delete_group("share-admin").await.unwrap();
}

#[tokio::test]
async fn memory_store_persists_share_coordinator_state_with_epoch_fencing() {
    let store = MemoryMetadataStore::new();
    let topic = store.create_topic("share-state-events", 1).await.unwrap();
    let key = ShareStateKey {
        group_id: "internal-share-state".into(),
        topic_id: topic.id,
        partition: 0,
    };
    assert_eq!(store.summarize_share_group_state(&key).await.unwrap(), None);

    store
        .initialize_share_group_state(ShareStateInitialization {
            key: key.clone(),
            state_epoch: 5,
            start_offset: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        store.summarize_share_group_state(&key).await.unwrap(),
        Some(ShareStateSummary {
            state_epoch: 5,
            leader_epoch: -1,
            start_offset: 10,
            delivery_complete_count: 0,
        })
    );
    assert!(matches!(
        store
            .initialize_share_group_state(ShareStateInitialization {
                key: key.clone(),
                state_epoch: 4,
                start_offset: 9,
            })
            .await,
        Err(ControlError::FencedShareStateEpoch {
            current: 5,
            requested: 4
        })
    ));

    let read = store
        .read_share_group_state(ShareStateRead {
            key: key.clone(),
            leader_epoch: 3,
        })
        .await
        .unwrap();
    assert_eq!(read.leader_epoch, 3);
    assert!(matches!(
        store
            .read_share_group_state(ShareStateRead {
                key: key.clone(),
                leader_epoch: 2,
            })
            .await,
        Err(ControlError::FencedShareLeaderEpoch {
            current: 3,
            requested: 2
        })
    ));

    store
        .write_share_group_state(ShareStateWrite {
            key: key.clone(),
            state_epoch: 5,
            leader_epoch: 3,
            start_offset: 10,
            delivery_complete_count: 3,
            state_batches: vec![
                ShareStateBatch {
                    first_offset: 10,
                    last_offset: 20,
                    delivery_state: AVAILABLE_DELIVERY_STATE,
                    delivery_count: 1,
                },
                ShareStateBatch {
                    first_offset: 15,
                    last_offset: 17,
                    delivery_state: ACKNOWLEDGED_DELIVERY_STATE,
                    delivery_count: 2,
                },
                ShareStateBatch {
                    first_offset: 18,
                    last_offset: 19,
                    delivery_state: ARCHIVED_DELIVERY_STATE,
                    delivery_count: 2,
                },
            ],
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .write_share_group_state(ShareStateWrite {
                key: key.clone(),
                state_epoch: 4,
                leader_epoch: 3,
                start_offset: -1,
                delivery_complete_count: 0,
                state_batches: Vec::new(),
            })
            .await,
        Err(ControlError::FencedShareStateEpoch { .. })
    ));
    store
        .write_share_group_state(ShareStateWrite {
            key: key.clone(),
            state_epoch: 5,
            leader_epoch: 3,
            start_offset: 16,
            delivery_complete_count: 4,
            state_batches: vec![ShareStateBatch {
                first_offset: 16,
                last_offset: 18,
                delivery_state: AVAILABLE_DELIVERY_STATE,
                delivery_count: 3,
            }],
        })
        .await
        .unwrap();
    let read = store
        .read_share_group_state(ShareStateRead {
            key: key.clone(),
            leader_epoch: -1,
        })
        .await
        .unwrap();
    assert_eq!(
        read,
        ShareStateSnapshot {
            state_epoch: 5,
            leader_epoch: 3,
            start_offset: 16,
            delivery_complete_count: 4,
            state_batches: vec![
                ShareStateBatch {
                    first_offset: 16,
                    last_offset: 18,
                    delivery_state: AVAILABLE_DELIVERY_STATE,
                    delivery_count: 3,
                },
                ShareStateBatch {
                    first_offset: 19,
                    last_offset: 19,
                    delivery_state: ARCHIVED_DELIVERY_STATE,
                    delivery_count: 2,
                },
                ShareStateBatch {
                    first_offset: 20,
                    last_offset: 20,
                    delivery_state: AVAILABLE_DELIVERY_STATE,
                    delivery_count: 1,
                },
            ],
        }
    );

    store.delete_share_group_state(&key).await.unwrap();
    store.delete_share_group_state(&key).await.unwrap();
    assert_eq!(store.summarize_share_group_state(&key).await.unwrap(), None);
}

#[tokio::test]
async fn memory_store_locks_partitions_in_order_but_preserves_response_order() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 2).await.unwrap();
    let spans = store
        .commit_object(
            ObjectRef {
                key: "objects/multi-partition".into(),
                size: 20,
            },
            vec![
                BatchDraft {
                    partition: PartitionKey::new("events", 1),
                    byte_start: 0,
                    byte_end: 10,
                    record_count: 1,
                    timestamp_ms: 1,
                    checksum: None,
                    producer: None,
                    transactional_id: None,
                    verify_transaction_partition: true,
                },
                BatchDraft {
                    partition: PartitionKey::new("events", 0),
                    byte_start: 10,
                    byte_end: 20,
                    record_count: 1,
                    timestamp_ms: 1,
                    checksum: None,
                    producer: None,
                    transactional_id: None,
                    verify_transaction_partition: true,
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(spans[0].partition.partition, 1);
    assert_eq!(spans[1].partition.partition, 0);
}

#[tokio::test]
async fn memory_store_persists_group_offsets() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    let partition = PartitionKey::new("events", 0);
    store
        .commit_offsets(
            "workers",
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 12,
                leader_epoch: 0,
                metadata: Some("checkpoint".into()),
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    let offsets = store
        .fetch_offsets("workers", std::slice::from_ref(&partition))
        .await
        .unwrap();
    assert_eq!(offsets[&partition].offset, 12);
    assert_eq!(offsets[&partition].leader_epoch, 0);
    assert_eq!(offsets[&partition].metadata.as_deref(), Some("checkpoint"));
}

#[tokio::test]
async fn memory_store_expires_custom_and_simple_offsets_from_commit_time() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    let partition = PartitionKey::new("events", 0);
    store
        .commit_offsets(
            "custom-retention",
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 4,
                leader_epoch: -1,
                metadata: None,
                retention_time_ms: Some(1),
            }],
        )
        .await
        .unwrap();
    store
        .commit_offsets(
            "broker-retention",
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 5,
                leader_epoch: -1,
                metadata: None,
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();

    let now_ms = Utc::now().timestamp_millis() + 100;
    assert_eq!(
        store
            .expire_consumer_offsets(now_ms, 10_000, 100)
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .fetch_offsets("custom-retention", std::slice::from_ref(&partition))
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store.expire_consumer_offsets(now_ms, 1, 100).await.unwrap(),
        1
    );
    assert!(
        store
            .fetch_offsets("broker-retention", std::slice::from_ref(&partition))
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn memory_store_does_not_expire_offsets_with_pending_transactional_updates() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    let partition = PartitionKey::new("events", 0);
    store
        .commit_offsets(
            "transactional-workers",
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 1,
                leader_epoch: -1,
                metadata: None,
                retention_time_ms: Some(0),
            }],
        )
        .await
        .unwrap();
    let producer = store
        .init_producer(Some("offset-expiration-tx"), 60_000, None)
        .await
        .unwrap();
    store
        .add_offsets_to_transaction("offset-expiration-tx", producer, "transactional-workers")
        .await
        .unwrap();
    store
        .commit_transaction_offsets(
            "offset-expiration-tx",
            producer,
            "transactional-workers",
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 2,
                leader_epoch: -1,
                metadata: None,
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    let now_ms = Utc::now().timestamp_millis() + 100;
    assert_eq!(
        store.expire_consumer_offsets(now_ms, 1, 100).await.unwrap(),
        0
    );
    assert_eq!(
        store
            .fetch_offsets("transactional-workers", std::slice::from_ref(&partition))
            .await
            .unwrap()[&partition]
            .offset,
        1
    );
    store
        .end_transaction("offset-expiration-tx", producer, false)
        .await
        .unwrap();
    assert_eq!(
        store.expire_consumer_offsets(now_ms, 1, 100).await.unwrap(),
        1
    );
}

#[tokio::test]
async fn memory_store_protects_subscribed_offsets_until_classic_group_is_empty() {
    let store = MemoryMetadataStore::new();
    store.create_topic("subscribed", 1).await.unwrap();
    store.create_topic("retired", 1).await.unwrap();
    let subscribed = PartitionKey::new("subscribed", 0);
    let retired = PartitionKey::new("retired", 0);
    let joined = store
        .join_group(
            "managed-workers",
            "",
            None,
            "consumer",
            &[("range".into(), vec![1])],
            (
                "control-test",
                "127.0.0.1",
                &["subscribed".to_owned()],
                45_000,
            ),
            3,
        )
        .await
        .unwrap();
    store
        .sync_group(
            "managed-workers",
            joined.generation_id,
            &joined.member_id,
            None,
            vec![GroupAssignment {
                member_id: joined.member_id.clone(),
                assignment: vec![1],
            }],
        )
        .await
        .unwrap();
    store
        .commit_offsets(
            "managed-workers",
            vec![
                OffsetCommit {
                    partition: subscribed.clone(),
                    offset: 8,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                },
                OffsetCommit {
                    partition: retired.clone(),
                    offset: 9,
                    leader_epoch: -1,
                    metadata: None,
                    retention_time_ms: None,
                },
            ],
        )
        .await
        .unwrap();

    assert_eq!(
        store
            .expire_consumer_offsets(Utc::now().timestamp_millis() + 100, 1, 100)
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .fetch_offsets("managed-workers", &[subscribed.clone(), retired.clone()])
            .await
            .unwrap()
            .contains_key(&subscribed)
    );
    store
        .leave_group(
            "managed-workers",
            &[GroupMemberIdentity {
                member_id: joined.member_id,
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();
    let empty_since_ms = Utc::now().timestamp_millis();
    assert_eq!(
        store
            .expire_consumer_offsets(empty_since_ms + 100, 10_000, 100)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .expire_consumer_offsets(empty_since_ms + 10_100, 10_000, 100)
            .await
            .unwrap(),
        1
    );
    assert!(
        store
            .describe_classic_groups(&["managed-workers".to_owned()])
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn memory_store_keeps_single_member_group_state() {
    let store = MemoryMetadataStore::new();
    let joined = store
        .join_group(
            "workers",
            "",
            None,
            "consumer",
            &[("range".into(), vec![1, 2, 3])],
            ("control-test", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    assert_eq!(joined.generation_id, 1);
    assert_eq!(joined.leader, joined.member_id);
    assert_eq!(joined.members.len(), 1);
    let assignment = vec![9, 8, 7];
    let returned = store
        .sync_group(
            "workers",
            joined.generation_id,
            &joined.member_id,
            None,
            vec![GroupAssignment {
                member_id: joined.member_id.clone(),
                assignment: assignment.clone(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(returned, assignment);
    store
        .heartbeat_group("workers", joined.generation_id, &joined.member_id, None)
        .await
        .unwrap();
    store
        .leave_group(
            "workers",
            &[GroupMemberIdentity {
                member_id: joined.member_id.clone(),
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .heartbeat_group("workers", 1, &joined.member_id, None)
            .await,
        Err(ControlError::IllegalGeneration { .. })
    ));
}

#[tokio::test]
async fn memory_store_expires_classic_members_and_reelects_the_leader() {
    let store = MemoryMetadataStore::new();
    let leader = store
        .join_group(
            "expiring-workers",
            "",
            None,
            "consumer",
            &[("range".into(), vec![1])],
            ("leader-client", "127.0.0.1", &[], 100),
            3,
        )
        .await
        .unwrap();
    let follower = store
        .join_group(
            "expiring-workers",
            "",
            None,
            "consumer",
            &[("range".into(), vec![2])],
            ("follower-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    assert_eq!(follower.leader, leader.member_id);

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let rejoined = store
        .join_group(
            "expiring-workers",
            &follower.member_id,
            None,
            "consumer",
            &[("range".into(), vec![2])],
            ("follower-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    assert_eq!(rejoined.generation_id, follower.generation_id + 1);
    assert_eq!(rejoined.leader, follower.member_id);
    assert_eq!(rejoined.members.len(), 1);
}

#[tokio::test]
async fn memory_store_rebalances_when_the_classic_leader_leaves() {
    let store = MemoryMetadataStore::new();
    let leader = store
        .join_group(
            "leaving-workers",
            "",
            None,
            "consumer",
            &[("range".into(), vec![1])],
            ("leader-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    let follower = store
        .join_group(
            "leaving-workers",
            "",
            None,
            "consumer",
            &[("range".into(), vec![2])],
            ("follower-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    store
        .leave_group(
            "leaving-workers",
            &[GroupMemberIdentity {
                member_id: leader.member_id,
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();
    let rejoined = store
        .join_group(
            "leaving-workers",
            &follower.member_id,
            None,
            "consumer",
            &[("range".into(), vec![2])],
            ("follower-client", "127.0.0.1", &[], 45_000),
            3,
        )
        .await
        .unwrap();
    assert_eq!(rejoined.generation_id, follower.generation_id + 1);
    assert_eq!(rejoined.leader, follower.member_id);
}

#[tokio::test]
async fn memory_store_administers_classic_and_consumer_groups() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    store
        .commit_offsets(
            "classic-workers",
            vec![OffsetCommit {
                partition: PartitionKey::new("events", 0),
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
            "classic-workers",
            "",
            Some("classic-instance"),
            "consumer",
            &[("range".into(), vec![1, 2, 3])],
            (
                "classic-client",
                "127.0.0.1",
                &["events".to_owned()],
                45_000,
            ),
            9,
        )
        .await
        .unwrap();
    let classic = store
        .describe_classic_groups(&["classic-workers".to_owned()])
        .await
        .unwrap()
        .remove("classic-workers")
        .unwrap();
    assert_eq!(classic.state, "CompletingRebalance");
    assert_eq!(classic.members[0].client_id, "classic-client");
    assert!(matches!(
        store.delete_group("classic-workers").await,
        Err(ControlError::NonEmptyGroup(_))
    ));

    store
        .sync_group(
            "classic-workers",
            joined.generation_id,
            &joined.member_id,
            Some("classic-instance"),
            vec![GroupAssignment {
                member_id: joined.member_id.clone(),
                assignment: vec![9],
            }],
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .list_groups()
            .await
            .unwrap()
            .into_iter()
            .find(|group| group.group_id == "classic-workers")
            .unwrap()
            .state,
        "Stable"
    );
    store
        .leave_group(
            "classic-workers",
            &[GroupMemberIdentity {
                member_id: joined.member_id,
                group_instance_id: Some("classic-instance".to_owned()),
            }],
        )
        .await
        .unwrap();
    let classic = store
        .describe_classic_groups(&["classic-workers".to_owned()])
        .await
        .unwrap()
        .remove("classic-workers")
        .unwrap();
    assert_eq!(classic.state, "Empty");
    assert!(classic.members.is_empty());
    store.delete_group("classic-workers").await.unwrap();

    let joined = store
        .consumer_group_heartbeat(ConsumerGroupHeartbeat {
            group_id: "consumer-workers".to_owned(),
            member_id: "consumer-member".to_owned(),
            member_epoch: 0,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: 30_000,
            subscribed_topic_names: Some(vec!["events".to_owned()]),
            subscribed_topic_regex: None,
            server_assignor: Some("uniform".to_owned()),
            configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
            owned_partitions: Some(Vec::new()),
            client_id: "consumer-client".to_owned(),
            client_host: "127.0.0.1".to_owned(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 45_000,
            regex_refresh_interval_ms: 600_000,
            assignment_interval_ms: 0,
            max_size: i32::MAX,
        })
        .await
        .unwrap();
    let consumer = store
        .list_groups()
        .await
        .unwrap()
        .into_iter()
        .find(|group| group.group_id == "consumer-workers")
        .unwrap();
    assert_eq!(consumer.group_type, "Consumer");
    assert!(matches!(
        store.delete_group("consumer-workers").await,
        Err(ControlError::NonEmptyGroup(_))
    ));
    store
        .consumer_group_heartbeat(ConsumerGroupHeartbeat {
            group_id: "consumer-workers".to_owned(),
            member_id: joined.member_id,
            member_epoch: -1,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: -1,
            subscribed_topic_names: None,
            subscribed_topic_regex: None,
            server_assignor: None,
            configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
            owned_partitions: None,
            client_id: "consumer-client".to_owned(),
            client_host: "127.0.0.1".to_owned(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 45_000,
            regex_refresh_interval_ms: 600_000,
            assignment_interval_ms: 0,
            max_size: i32::MAX,
        })
        .await
        .unwrap();
    store.delete_group("consumer-workers").await.unwrap();
    assert!(store.list_groups().await.unwrap().is_empty());
}

#[tokio::test]
async fn memory_store_converts_empty_classic_and_consumer_groups_without_losing_offsets() {
    let store = MemoryMetadataStore::new();
    store.create_topic("migration-events", 1).await.unwrap();
    let group_id = "migration-workers";
    let partition = PartitionKey::new("migration-events", 0);
    store
        .commit_offsets(
            group_id,
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 7,
                leader_epoch: -1,
                metadata: Some("preserved".to_owned()),
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    let classic = store
        .join_group(
            group_id,
            "",
            None,
            "consumer",
            &[("range".into(), vec![1])],
            (
                "classic-client",
                "127.0.0.1",
                &["migration-events".to_owned()],
                45_000,
            ),
            3,
        )
        .await
        .unwrap();
    let consumer_request =
        |member_id: &str, member_epoch: i32, instance_id: Option<&str>| ConsumerGroupHeartbeat {
            group_id: group_id.to_owned(),
            member_id: member_id.to_owned(),
            member_epoch,
            instance_id: instance_id.map(str::to_owned),
            rack_id: None,
            rebalance_timeout_ms: if member_epoch == 0 { 300_000 } else { -1 },
            subscribed_topic_names: (member_epoch == 0)
                .then(|| vec!["migration-events".to_owned()]),
            subscribed_topic_regex: None,
            server_assignor: None,
            configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
            owned_partitions: (member_epoch >= 0).then(Vec::new),
            client_id: "consumer-client".to_owned(),
            client_host: "127.0.0.1".to_owned(),
            heartbeat_interval_ms: 5_000,
            session_timeout_ms: 45_000,
            regex_refresh_interval_ms: 600_000,
            assignment_interval_ms: 0,
            max_size: i32::MAX,
        };

    assert!(matches!(
        store
            .consumer_group_heartbeat(consumer_request("consumer-a", 0, None))
            .await,
        Err(ControlError::GroupProtocolMismatch(_))
    ));
    store
        .leave_group(
            group_id,
            &[GroupMemberIdentity {
                member_id: classic.member_id,
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();
    let consumer = store
        .consumer_group_heartbeat(consumer_request("consumer-a", 0, None))
        .await
        .unwrap();
    assert_eq!(
        store
            .fetch_offsets(group_id, std::slice::from_ref(&partition))
            .await
            .unwrap()[&partition]
            .offset,
        7
    );
    assert!(matches!(
        store
            .join_group(
                group_id,
                "",
                None,
                "consumer",
                &[("range".into(), vec![1])],
                (
                    "classic-client",
                    "127.0.0.1",
                    &["migration-events".to_owned()],
                    45_000,
                ),
                3,
            )
            .await,
        Err(ControlError::GroupProtocolMismatch(_))
    ));

    store
        .consumer_group_heartbeat(consumer_request(&consumer.member_id, -1, None))
        .await
        .unwrap();
    let classic = store
        .join_group(
            group_id,
            "",
            None,
            "consumer",
            &[("range".into(), vec![1])],
            (
                "classic-client",
                "127.0.0.1",
                &["migration-events".to_owned()],
                45_000,
            ),
            3,
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .fetch_offsets(group_id, std::slice::from_ref(&partition))
            .await
            .unwrap()[&partition]
            .offset,
        7
    );
    assert!(matches!(
        store
            .consumer_group_heartbeat(consumer_request("consumer-b", 0, None))
            .await,
        Err(ControlError::GroupProtocolMismatch(_))
    ));
    store
        .leave_group(
            group_id,
            &[GroupMemberIdentity {
                member_id: classic.member_id,
                group_instance_id: None,
            }],
        )
        .await
        .unwrap();

    let static_group = "static-migration-workers";
    let static_request = |member_epoch: i32| ConsumerGroupHeartbeat {
        group_id: static_group.to_owned(),
        member_id: "static-member".to_owned(),
        member_epoch,
        instance_id: Some("static-instance".to_owned()),
        rack_id: None,
        rebalance_timeout_ms: if member_epoch == 0 { 300_000 } else { -1 },
        subscribed_topic_names: (member_epoch == 0).then(|| vec!["migration-events".to_owned()]),
        subscribed_topic_regex: None,
        server_assignor: None,
        configured_assignors: vec!["uniform".to_owned(), "range".to_owned()],
        owned_partitions: (member_epoch >= 0).then(Vec::new),
        client_id: "consumer-client".to_owned(),
        client_host: "127.0.0.1".to_owned(),
        heartbeat_interval_ms: 5_000,
        session_timeout_ms: 45_000,
        regex_refresh_interval_ms: 600_000,
        assignment_interval_ms: 0,
        max_size: i32::MAX,
    };
    store
        .consumer_group_heartbeat(static_request(0))
        .await
        .unwrap();
    store
        .consumer_group_heartbeat(static_request(-2))
        .await
        .unwrap();
    assert!(matches!(
        store
            .join_group(
                static_group,
                "",
                None,
                "consumer",
                &[("range".into(), vec![1])],
                (
                    "classic-client",
                    "127.0.0.1",
                    &["migration-events".to_owned()],
                    45_000,
                ),
                3,
            )
            .await,
        Err(ControlError::GroupProtocolMismatch(_))
    ));
}

#[tokio::test]
async fn memory_store_deduplicates_idempotent_produce() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    let producer = store.init_producer(None, 60_000, None).await.unwrap();
    let draft = BatchDraft {
        partition: PartitionKey::new("events", 0),
        byte_start: 0,
        byte_end: 10,
        record_count: 1,
        timestamp_ms: 1,
        checksum: None,
        producer: Some(ProducerBatch {
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            first_sequence: 0,
            last_sequence: 0,
        }),
        transactional_id: None,
        verify_transaction_partition: true,
    };
    store
        .stage_object(ObjectRef {
            key: "objects/idempotent-a".into(),
            size: 10,
        })
        .await
        .unwrap();
    let first = store
        .commit_object(
            ObjectRef {
                key: "objects/idempotent-a".into(),
                size: 10,
            },
            vec![draft.clone()],
        )
        .await
        .unwrap();
    store
        .stage_object(ObjectRef {
            key: "objects/idempotent-b".into(),
            size: 10,
        })
        .await
        .unwrap();
    let retry = store
        .commit_object(
            ObjectRef {
                key: "objects/idempotent-b".into(),
                size: 10,
            },
            vec![draft.clone()],
        )
        .await
        .unwrap();
    assert_eq!(first[0].base_offset, retry[0].base_offset);
    assert!(
        !store
            .object_committed("objects/idempotent-b")
            .await
            .unwrap()
    );
    assert!(store.object_staged("objects/idempotent-b").await.unwrap());
    assert_eq!(
        store
            .list_offset(&PartitionKey::new("events", 0), -1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .describe_producers(&PartitionKey::new("events", 0))
            .await
            .unwrap(),
        [ActiveProducer {
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            last_sequence: 0,
            last_timestamp: 1,
            current_transaction_start_offset: -1,
        }]
    );

    let error = store
        .commit_object(
            ObjectRef {
                key: "objects/idempotent-c".into(),
                size: 10,
            },
            vec![BatchDraft {
                partition: PartitionKey::new("events", 0),
                byte_start: 0,
                byte_end: 10,
                record_count: 1,
                timestamp_ms: 2,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: producer.producer_id,
                    producer_epoch: producer.producer_epoch,
                    first_sequence: 2,
                    last_sequence: 2,
                }),
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        ControlError::OutOfOrderSequence {
            expected: 1,
            actual: 2,
            ..
        }
    ));

    let next_batches = (1..=5)
        .enumerate()
        .map(|(index, sequence)| {
            let mut next = draft.clone();
            next.byte_start = (index * 10) as u64;
            next.byte_end = ((index + 1) * 10) as u64;
            next.timestamp_ms = i64::from(sequence + 1);
            next.producer = Some(ProducerBatch {
                producer_id: producer.producer_id,
                producer_epoch: producer.producer_epoch,
                first_sequence: sequence,
                last_sequence: sequence,
            });
            next
        })
        .collect::<Vec<_>>();
    let committed = store
        .commit_object(
            ObjectRef {
                key: "objects/idempotent-sequences".into(),
                size: 50,
            },
            next_batches,
        )
        .await
        .unwrap();
    for (index, span) in committed.iter().enumerate() {
        assert_eq!(span.base_offset, (index + 1) as i64);
    }

    let mut recent_retry = draft.clone();
    recent_retry.timestamp_ms = 2;
    recent_retry.producer = Some(ProducerBatch {
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        first_sequence: 1,
        last_sequence: 1,
    });
    let recent_retry = store
        .commit_object(
            ObjectRef {
                key: "objects/idempotent-recent-retry".into(),
                size: 10,
            },
            vec![recent_retry],
        )
        .await
        .unwrap();
    assert_eq!(recent_retry[0].base_offset, 1);

    let evicted_retry = store
        .commit_object(
            ObjectRef {
                key: "objects/idempotent-evicted-retry".into(),
                size: 10,
            },
            vec![draft.clone()],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        evicted_retry,
        ControlError::OutOfOrderSequence {
            expected: 6,
            actual: 0,
            ..
        }
    ));

    assert_eq!(
        store.expire_producer_sequences(106, 100, 10).await.unwrap(),
        1
    );
    assert!(
        store
            .describe_producers(&PartitionKey::new("events", 0))
            .await
            .unwrap()
            .is_empty()
    );
    let after_expiration = store
        .commit_object(
            ObjectRef {
                key: "objects/idempotent-after-expiration".into(),
                size: 10,
            },
            vec![draft.clone()],
        )
        .await
        .unwrap();
    assert_eq!(after_expiration[0].base_offset, 6);

    let mut sequence_one_after_expiration = draft;
    sequence_one_after_expiration.timestamp_ms = 107;
    sequence_one_after_expiration.producer = Some(ProducerBatch {
        producer_id: producer.producer_id,
        producer_epoch: producer.producer_epoch,
        first_sequence: 1,
        last_sequence: 1,
    });
    let sequence_one_after_expiration = store
        .commit_object(
            ObjectRef {
                key: "objects/idempotent-sequence-one-after-expiration".into(),
                size: 10,
            },
            vec![sequence_one_after_expiration],
        )
        .await
        .unwrap();
    assert_eq!(sequence_one_after_expiration[0].base_offset, 7);
    assert_eq!(
        store
            .list_offset(&PartitionKey::new("events", 0), -1)
            .await
            .unwrap(),
        8
    );
}

#[tokio::test]
async fn memory_producer_state_follows_delete_records_and_retention() {
    let store = MemoryMetadataStore::new();
    store.create_topic("truncated", 1).await.unwrap();
    let partition = PartitionKey::new("truncated", 0);
    let producer = store.init_producer(None, 60_000, None).await.unwrap();
    let draft = |sequence: i32, timestamp_ms: i64| BatchDraft {
        partition: partition.clone(),
        byte_start: 0,
        byte_end: 10,
        record_count: 1,
        timestamp_ms,
        checksum: None,
        producer: Some(ProducerBatch {
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            first_sequence: sequence,
            last_sequence: sequence,
        }),
        transactional_id: None,
        verify_transaction_partition: true,
    };
    for sequence in 0..=5 {
        store
            .commit_object(
                ObjectRef {
                    key: format!("objects/truncated-{sequence}"),
                    size: 10,
                },
                vec![draft(sequence, i64::from(sequence))],
            )
            .await
            .unwrap();
    }

    assert_eq!(store.delete_records(&partition, 2).await.unwrap(), 2);
    assert_eq!(
        store.describe_producers(&partition).await.unwrap()[0].last_sequence,
        5
    );
    let removed_retry = store
        .commit_object(
            ObjectRef {
                key: "objects/truncated-removed-retry".into(),
                size: 10,
            },
            vec![draft(1, 1)],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        removed_retry,
        ControlError::OutOfOrderSequence {
            expected: 6,
            actual: 1,
            ..
        }
    ));
    let retained_retry = store
        .commit_object(
            ObjectRef {
                key: "objects/truncated-retained-retry".into(),
                size: 10,
            },
            vec![draft(2, 2)],
        )
        .await
        .unwrap();
    assert_eq!(retained_retry[0].base_offset, 2);

    assert_eq!(store.delete_records(&partition, -1).await.unwrap(), 6);
    assert!(
        store
            .describe_producers(&partition)
            .await
            .unwrap()
            .is_empty()
    );
    let after_truncation = store
        .commit_object(
            ObjectRef {
                key: "objects/truncated-new-state".into(),
                size: 10,
            },
            vec![draft(5, 10)],
        )
        .await
        .unwrap();
    assert_eq!(after_truncation[0].base_offset, 6);

    store
        .create_topic_with_config(
            "retained-producer",
            1,
            TopicConfig {
                retention_ms: 0,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let retained_partition = PartitionKey::new("retained-producer", 0);
    let retained_producer = store.init_producer(None, 60_000, None).await.unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "objects/retained-producer".into(),
                size: 10,
            },
            vec![BatchDraft {
                partition: retained_partition.clone(),
                byte_start: 0,
                byte_end: 10,
                record_count: 1,
                timestamp_ms: -1_000,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: retained_producer.producer_id,
                    producer_epoch: retained_producer.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 0,
                }),
                transactional_id: None,
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    store.apply_retention(-1_000, 0).await.unwrap();
    assert!(
        store
            .describe_producers(&retained_partition)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn memory_producer_state_expiration_preserves_pending_transactions() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    let partition = PartitionKey::new("events", 0);
    let producer = store
        .init_producer(Some("orders-tx"), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            "orders-tx",
            producer,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "objects/pending-producer-state".into(),
                size: 10,
            },
            vec![transactional_draft(&partition, producer, 0)],
        )
        .await
        .unwrap();

    assert_eq!(store.delete_records(&partition, -1).await.unwrap(), 0);
    assert_eq!(store.describe_producers(&partition).await.unwrap().len(), 1);
    assert_eq!(store.expire_producer_sequences(10, 1, 10).await.unwrap(), 0);
    assert_eq!(store.describe_producers(&partition).await.unwrap().len(), 1);

    store
        .end_transaction("orders-tx", producer, false)
        .await
        .unwrap();
    assert_eq!(store.expire_producer_sequences(10, 1, 10).await.unwrap(), 1);
    assert!(
        store
            .describe_producers(&partition)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn memory_transactional_id_expiration_preserves_ongoing_transactions() {
    let store = MemoryMetadataStore::new();
    store
        .create_topic("transaction-expiration", 1)
        .await
        .unwrap();
    let partition = PartitionKey::new("transaction-expiration", 0);

    let idle_id = "idle-transactional-id";
    let completed_id = "completed-transactional-id";
    let ongoing_id = "ongoing-transactional-id";
    let idle = store
        .init_producer(Some(idle_id), 60_000, None)
        .await
        .unwrap();
    let completed = store
        .init_producer(Some(completed_id), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            completed_id,
            completed,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "objects/completed-transactional-id".into(),
                size: 10,
            },
            vec![BatchDraft {
                partition: partition.clone(),
                byte_start: 0,
                byte_end: 10,
                record_count: 1,
                timestamp_ms: 1,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: completed.producer_id,
                    producer_epoch: completed.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 0,
                }),
                transactional_id: Some(completed_id.to_owned()),
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    store
        .end_transaction(completed_id, completed, true)
        .await
        .unwrap();
    let ongoing = store
        .init_producer(Some(ongoing_id), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(ongoing_id, ongoing, std::slice::from_ref(&partition), false)
        .await
        .unwrap();

    let sweep_now_ms = Utc::now().timestamp_millis() + 60_000;
    assert_eq!(
        store
            .expire_transactional_ids(sweep_now_ms, 1, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .expire_transactional_ids(sweep_now_ms, 1, 10)
            .await
            .unwrap(),
        1
    );
    let descriptions = store
        .describe_transactions(&[
            idle_id.to_owned(),
            completed_id.to_owned(),
            ongoing_id.to_owned(),
        ])
        .await
        .unwrap();
    assert!(!descriptions.contains_key(idle_id));
    assert!(!descriptions.contains_key(completed_id));
    assert_eq!(descriptions[ongoing_id].state, TransactionState::Ongoing);
    assert_eq!(descriptions[ongoing_id].producer, ongoing);
    assert_eq!(
        store
            .fetch(&partition, 0, 1024, FetchIsolation::ReadCommitted)
            .await
            .unwrap()
            .spans
            .len(),
        1
    );

    let replacement_idle = store
        .init_producer(Some(idle_id), 60_000, None)
        .await
        .unwrap();
    let replacement_completed = store
        .init_producer(Some(completed_id), 60_000, None)
        .await
        .unwrap();
    assert_ne!(replacement_idle.producer_id, idle.producer_id);
    assert_ne!(replacement_completed.producer_id, completed.producer_id);
}

#[tokio::test]
async fn memory_store_commits_and_aborts_transaction_visibility() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    let partition = PartitionKey::new("events", 0);
    let producer = store
        .init_producer(Some("orders-tx"), 60_000, None)
        .await
        .unwrap();
    store
        .init_producer(Some("xorders-tx"), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            "orders-tx",
            producer,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "objects/tx-commit".into(),
                size: 10,
            },
            vec![transactional_draft(&partition, producer, 0)],
        )
        .await
        .unwrap();
    let pending = store
        .fetch(&partition, 0, 1024, FetchIsolation::ReadCommitted)
        .await
        .unwrap();
    assert!(pending.spans.is_empty());
    assert_eq!(pending.last_stable_offset, 0);
    assert_eq!(
        store.describe_producers(&partition).await.unwrap()[0].current_transaction_start_offset,
        0
    );

    store
        .add_offsets_to_transaction("orders-tx", producer, "workers")
        .await
        .unwrap();
    store
        .commit_transaction_offsets(
            "orders-tx",
            producer,
            "workers",
            vec![OffsetCommit {
                partition: partition.clone(),
                offset: 1,
                leader_epoch: 0,
                metadata: Some("tx".into()),
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();
    store
        .end_transaction("orders-tx", producer, true)
        .await
        .unwrap();
    assert_eq!(
        store.describe_producers(&partition).await.unwrap()[0].current_transaction_start_offset,
        -1
    );
    let committed = store
        .fetch(&partition, 0, 1024, FetchIsolation::ReadCommitted)
        .await
        .unwrap();
    assert_eq!(committed.spans.len(), 1);
    assert_eq!(committed.last_stable_offset, 1);
    let committed_offset = store
        .fetch_offsets("workers", std::slice::from_ref(&partition))
        .await
        .unwrap();
    assert_eq!(committed_offset[&partition].offset, 1);
    assert_eq!(committed_offset[&partition].leader_epoch, 0);

    store
        .add_partitions_to_transaction(
            "orders-tx",
            producer,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "objects/tx-abort".into(),
                size: 10,
            },
            vec![transactional_draft(&partition, producer, 1)],
        )
        .await
        .unwrap();
    store
        .end_transaction("orders-tx", producer, false)
        .await
        .unwrap();
    let read_committed = store
        .fetch(&partition, 0, 1024, FetchIsolation::ReadCommitted)
        .await
        .unwrap();
    let read_uncommitted = store
        .fetch(&partition, 0, 1024, FetchIsolation::ReadUncommitted)
        .await
        .unwrap();
    assert_eq!(read_committed.spans.len(), 1);
    assert_eq!(read_uncommitted.spans.len(), 2);
    assert_eq!(read_committed.high_watermark, 2);
}

#[tokio::test]
async fn memory_transaction_marker_requires_full_coverage_and_is_idempotent() {
    let store = MemoryMetadataStore::new();
    store.create_topic("marker-events", 2).await.unwrap();
    let first = PartitionKey::new("marker-events", 0);
    let second = PartitionKey::new("marker-events", 1);
    let producer = store
        .init_producer(Some("marker-tx"), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            "marker-tx",
            producer,
            &[first.clone(), second.clone()],
            false,
        )
        .await
        .unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "objects/marker-commit".into(),
                size: 10,
            },
            vec![BatchDraft {
                partition: first.clone(),
                byte_start: 0,
                byte_end: 10,
                record_count: 1,
                timestamp_ms: 1,
                checksum: None,
                producer: Some(ProducerBatch {
                    producer_id: producer.producer_id,
                    producer_epoch: producer.producer_epoch,
                    first_sequence: 0,
                    last_sequence: 0,
                }),
                transactional_id: Some("marker-tx".into()),
                verify_transaction_partition: true,
            }],
        )
        .await
        .unwrap();
    store
        .add_offsets_to_transaction("marker-tx", producer, "marker-group")
        .await
        .unwrap();
    store
        .commit_transaction_offsets(
            "marker-tx",
            producer,
            "marker-group",
            vec![OffsetCommit {
                partition: first.clone(),
                offset: 1,
                leader_epoch: 0,
                metadata: Some("marker".into()),
                retention_time_ms: None,
            }],
        )
        .await
        .unwrap();

    assert!(matches!(
        store
            .write_transaction_marker(producer, std::slice::from_ref(&first), true, 0, 0)
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));
    assert!(
        store
            .fetch(&first, 0, 1024, FetchIsolation::ReadCommitted)
            .await
            .unwrap()
            .spans
            .is_empty()
    );

    let marker_partitions = [first.clone(), second];
    store
        .write_transaction_marker(producer, &marker_partitions, true, 0, 0)
        .await
        .unwrap();
    store
        .write_transaction_marker(producer, &marker_partitions, true, 0, 0)
        .await
        .unwrap();
    assert_eq!(
        store
            .fetch(&first, 0, 1024, FetchIsolation::ReadCommitted)
            .await
            .unwrap()
            .spans
            .len(),
        1
    );
    assert_eq!(
        store
            .fetch_offsets("marker-group", std::slice::from_ref(&first))
            .await
            .unwrap()[&first]
            .offset,
        1
    );
    assert!(matches!(
        store
            .write_transaction_marker(producer, &marker_partitions, false, 0, 0)
            .await,
        Err(ControlError::InvalidTransactionState(_))
    ));
}

#[tokio::test]
async fn memory_transaction_marker_fences_tv2_and_coordinator_epochs() {
    let store = MemoryMetadataStore::new();
    store.create_topic("marker-fencing", 1).await.unwrap();
    let partition = PartitionKey::new("marker-fencing", 0);
    let producer = store
        .init_producer(Some("marker-fencing-tx"), 60_000, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            "marker-fencing-tx",
            producer,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();

    assert!(matches!(
        store
            .write_transaction_marker(producer, std::slice::from_ref(&partition), true, 5, 2,)
            .await,
        Err(ControlError::ProducerFenced {
            expected_epoch: 0,
            actual_epoch: 0,
            ..
        })
    ));

    let marker_epoch_one = ProducerSession {
        producer_id: producer.producer_id,
        producer_epoch: 1,
    };
    store
        .write_transaction_marker(
            marker_epoch_one,
            std::slice::from_ref(&partition),
            true,
            5,
            2,
        )
        .await
        .unwrap();
    store
        .write_transaction_marker(
            marker_epoch_one,
            std::slice::from_ref(&partition),
            true,
            5,
            2,
        )
        .await
        .unwrap();

    store
        .add_partitions_to_transaction(
            "marker-fencing-tx",
            marker_epoch_one,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    assert!(matches!(
        store
            .write_transaction_marker(
                marker_epoch_one,
                std::slice::from_ref(&partition),
                false,
                6,
                2,
            )
            .await,
        Err(ControlError::ProducerFenced {
            expected_epoch: 1,
            actual_epoch: 1,
            ..
        })
    ));

    let marker_epoch_two = ProducerSession {
        producer_id: producer.producer_id,
        producer_epoch: 2,
    };
    assert!(matches!(
        store
            .write_transaction_marker(
                marker_epoch_two,
                std::slice::from_ref(&partition),
                false,
                4,
                2,
            )
            .await,
        Err(ControlError::TransactionCoordinatorFenced {
            current_epoch: 5,
            requested_epoch: 4,
            ..
        })
    ));
    assert_eq!(
        store
            .describe_transactions(&["marker-fencing-tx".to_owned()])
            .await
            .unwrap()["marker-fencing-tx"]
            .state,
        TransactionState::Ongoing
    );

    store
        .write_transaction_marker(
            marker_epoch_two,
            std::slice::from_ref(&partition),
            false,
            6,
            2,
        )
        .await
        .unwrap();
    store
        .write_transaction_marker(
            marker_epoch_two,
            std::slice::from_ref(&partition),
            false,
            6,
            2,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn memory_store_aborts_expired_transactions() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    let partition = PartitionKey::new("events", 0);
    let producer = store
        .init_producer(Some("expiring-tx"), 1, None)
        .await
        .unwrap();
    store
        .add_partitions_to_transaction(
            "expiring-tx",
            producer,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    assert_eq!(store.abort_expired_transactions().await.unwrap(), 1);
    assert!(matches!(
        store.end_transaction("expiring-tx", producer, true).await,
        Err(ControlError::InvalidTransactionState(_))
    ));
}

#[tokio::test]
async fn memory_store_bounds_expired_transaction_batches() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    let partition = PartitionKey::new("events", 0);
    for transactional_id in ["expiring-tx-a", "expiring-tx-b"] {
        let producer = store
            .init_producer(Some(transactional_id), 1, None)
            .await
            .unwrap();
        store
            .add_partitions_to_transaction(
                transactional_id,
                producer,
                std::slice::from_ref(&partition),
                false,
            )
            .await
            .unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    assert_eq!(store.abort_expired_transactions_batch(1).await.unwrap(), 1);
    assert_eq!(store.abort_expired_transactions_batch(1).await.unwrap(), 1);
    assert_eq!(store.abort_expired_transactions_batch(1).await.unwrap(), 0);
}

#[tokio::test]
async fn memory_store_describes_and_filters_transactions() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events", 1).await.unwrap();
    let partition = PartitionKey::new("events", 0);
    let producer = store
        .init_producer(Some("orders-tx"), 60_000, None)
        .await
        .unwrap();
    let initialized = store
        .describe_transactions(&["orders-tx".to_owned(), "missing".to_owned()])
        .await
        .unwrap();
    assert_eq!(initialized["orders-tx"].state, TransactionState::Empty);
    assert!(!initialized.contains_key("missing"));

    store
        .add_partitions_to_transaction(
            "orders-tx",
            producer,
            std::slice::from_ref(&partition),
            false,
        )
        .await
        .unwrap();
    let active = store
        .describe_transactions(&["orders-tx".to_owned()])
        .await
        .unwrap();
    assert_eq!(active["orders-tx"].state, TransactionState::Ongoing);
    assert_eq!(active["orders-tx"].partitions, [partition]);
    let listed = store
        .list_transactions(&TransactionFilter {
            state_filters: vec!["Ongoing".to_owned()],
            transactional_id_pattern: Some("orders-.*".to_owned()),
            ..TransactionFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].producer, producer);
    let exact = store
        .list_transactions(&TransactionFilter {
            transactional_id_pattern: Some("orders-tx".to_owned()),
            ..TransactionFilter::default()
        })
        .await
        .unwrap();
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].transactional_id, "orders-tx");

    store
        .end_transaction("orders-tx", producer, true)
        .await
        .unwrap();
    let completed = store
        .describe_transactions(&["orders-tx".to_owned()])
        .await
        .unwrap();
    assert_eq!(
        completed["orders-tx"].state,
        TransactionState::CompleteCommit
    );
    assert!(completed["orders-tx"].partitions.is_empty());
    assert!(matches!(
        store
            .list_transactions(&TransactionFilter {
                transactional_id_pattern: Some("[".to_owned()),
                ..TransactionFilter::default()
            })
            .await,
        Err(ControlError::InvalidRegularExpression(_))
    ));
}

#[tokio::test]
async fn memory_retention_preserves_shared_objects_until_every_span_expires() {
    let store = MemoryMetadataStore::new();
    store.create_topic("events-a", 1).await.unwrap();
    store.create_topic("events-b", 1).await.unwrap();
    store
        .set_topic_config(
            "events-a",
            TopicConfig {
                retention_ms: 0,
                file_delete_delay_ms: 200,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    store
        .set_topic_config(
            "events-b",
            TopicConfig {
                retention_ms: 1_000,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    store
        .commit_object(
            ObjectRef {
                key: "objects/shared".into(),
                size: 20,
            },
            vec![
                BatchDraft {
                    partition: PartitionKey::new("events-a", 0),
                    byte_start: 0,
                    byte_end: 10,
                    record_count: 1,
                    timestamp_ms: 1,
                    checksum: None,
                    producer: None,
                    transactional_id: None,
                    verify_transaction_partition: true,
                },
                BatchDraft {
                    partition: PartitionKey::new("events-b", 0),
                    byte_start: 10,
                    byte_end: 20,
                    record_count: 1,
                    timestamp_ms: 1,
                    checksum: None,
                    producer: None,
                    transactional_id: None,
                    verify_transaction_partition: true,
                },
            ],
        )
        .await
        .unwrap();

    let first = store.apply_retention(10, 100).await.unwrap();
    assert_eq!(first.removed_spans, 1);
    assert!(first.deletable_objects.is_empty());
    assert!(store.object_committed("objects/shared").await.unwrap());
    assert_eq!(
        store
            .list_offset(&PartitionKey::new("events-a", 0), -2)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .fetch(
                &PartitionKey::new("events-b", 0),
                0,
                1024,
                FetchIsolation::ReadUncommitted,
            )
            .await
            .unwrap()
            .spans
            .len(),
        1
    );

    store
        .set_topic_config(
            "events-b",
            TopicConfig {
                retention_ms: 0,
                file_delete_delay_ms: 500,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let second = store.apply_retention(10, 100).await.unwrap();
    assert_eq!(second.removed_spans, 1);
    assert!(second.deletable_objects.is_empty());
    assert!(store.object_committed("objects/shared").await.unwrap());

    let safety_elapsed = store.apply_retention(110, 100).await.unwrap();
    assert!(safety_elapsed.deletable_objects.is_empty());
    let policy_pending = store.apply_retention(509, 100).await.unwrap();
    assert!(policy_pending.deletable_objects.is_empty());
    let matured = store.apply_retention(510, 100).await.unwrap();
    assert_eq!(matured.deletable_objects, ["objects/shared"]);
    assert!(store.object_committed("objects/shared").await.unwrap());
    let retry = store.apply_retention(510, 100).await.unwrap();
    assert_eq!(retry.deletable_objects, ["objects/shared"]);
    assert!(
        store
            .complete_object_deletion("objects/shared")
            .await
            .unwrap()
    );
    assert!(!store.object_committed("objects/shared").await.unwrap());
    assert!(
        !store
            .complete_object_deletion("objects/shared")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn memory_retention_pages_partitions_and_bounds_removed_spans() {
    let store = MemoryMetadataStore::new();
    for topic in ["retention-a", "retention-b"] {
        store.create_topic(topic, 1).await.unwrap();
        store
            .set_topic_config(
                topic,
                TopicConfig {
                    retention_ms: 0,
                    file_delete_delay_ms: 0,
                    ..TopicConfig::default()
                },
            )
            .await
            .unwrap();
        store
            .commit_object(
                ObjectRef {
                    key: format!("objects/{topic}"),
                    size: 10,
                },
                vec![BatchDraft {
                    partition: PartitionKey::new(topic, 0),
                    byte_start: 0,
                    byte_end: 10,
                    record_count: 1,
                    timestamp_ms: 1,
                    checksum: None,
                    producer: None,
                    transactional_id: None,
                    verify_transaction_partition: true,
                }],
            )
            .await
            .unwrap();
    }

    let first = store
        .apply_retention_page(
            10,
            100,
            RetentionPage {
                start_after_partition: None,
                start_after_object: None,
                max_partitions: 1,
                max_spans: 1,
                max_objects: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(first.removed_spans, 1);
    assert_eq!(
        first.next_partition,
        Some(PartitionKey::new("retention-a", 0))
    );
    assert_eq!(
        store
            .list_offset(&PartitionKey::new("retention-b", 0), -2)
            .await
            .unwrap(),
        0
    );

    let second = store
        .apply_retention_page(
            10,
            100,
            RetentionPage {
                start_after_partition: first.next_partition.as_ref(),
                start_after_object: first.next_object.as_deref(),
                max_partitions: 1,
                max_spans: 1,
                max_objects: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(second.removed_spans, 1);
    assert_eq!(second.next_partition, None);
    assert_eq!(
        store
            .list_offset(&PartitionKey::new("retention-b", 0), -2)
            .await
            .unwrap(),
        1
    );

    let first_objects = store
        .apply_retention_page(
            110,
            100,
            RetentionPage {
                start_after_partition: None,
                start_after_object: None,
                max_partitions: 1,
                max_spans: 1,
                max_objects: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(first_objects.deletable_objects, ["objects/retention-a"]);
    assert_eq!(
        first_objects.next_object.as_deref(),
        Some("objects/retention-a")
    );

    let second_objects = store
        .apply_retention_page(
            110,
            100,
            RetentionPage {
                start_after_partition: first_objects.next_partition.as_ref(),
                start_after_object: first_objects.next_object.as_deref(),
                max_partitions: 1,
                max_spans: 1,
                max_objects: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(second_objects.deletable_objects, ["objects/retention-b"]);
    assert_eq!(second_objects.next_object, None);
}

fn transactional_draft(
    partition: &PartitionKey,
    producer: ProducerSession,
    sequence: i32,
) -> BatchDraft {
    BatchDraft {
        partition: partition.clone(),
        byte_start: 0,
        byte_end: 10,
        record_count: 1,
        timestamp_ms: 1,
        checksum: None,
        producer: Some(ProducerBatch {
            producer_id: producer.producer_id,
            producer_epoch: producer.producer_epoch,
            first_sequence: sequence,
            last_sequence: sequence,
        }),
        transactional_id: Some("orders-tx".into()),
        verify_transaction_partition: true,
    }
}
