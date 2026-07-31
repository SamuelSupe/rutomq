use super::authorization::AuthorizationContext;
use super::*;
use crate::kafka_error::{CLUSTER_AUTHORIZATION_FAILED, UNKNOWN_SERVER_ERROR};
use bytes::Buf;
use kafka_protocol::messages::create_acls_request::AclCreation;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::delete_acls_request::DeleteAclsFilter;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::streams_group_heartbeat_request::{
    KeyValue as StreamsKeyValue, Subtopology, TaskIds, TopicInfo as StreamsTopicInfo, Topology,
};
use kafka_protocol::messages::{
    CreateAclsRequest, CreateAclsResponse, CreateTopicsRequest, CreateTopicsResponse,
    DeleteAclsRequest, DeleteAclsResponse, DescribeAclsRequest, DescribeAclsResponse, GroupId,
    HeartbeatRequest, HeartbeatResponse, InitProducerIdRequest, InitProducerIdResponse,
    ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader, StreamsGroupDescribeRequest,
    StreamsGroupDescribeResponse, StreamsGroupHeartbeatRequest, StreamsGroupHeartbeatResponse,
    TransactionalId,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use rutomq_control::{
    AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, MemoryMetadataStore,
    STREAMS_STATUS_MISSING_INTERNAL_TOPICS,
};
use rutomq_storage::OpenDalObjectStore;

pub(super) fn acl_broker() -> (Broker, Arc<MemoryMetadataStore>) {
    let metadata = Arc::new(MemoryMetadataStore::new());
    let mut config = AgentConfig::default();
    config.security.acl_enabled = true;
    config.security.super_users.insert("User:admin".to_owned());
    (
        Broker::new(
            metadata.clone(),
            Arc::new(OpenDalObjectStore::memory().unwrap()),
            config,
            Arc::new(Metrics::new().unwrap()),
        ),
        metadata,
    )
}

fn request_frame<T: Encodable>(
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    body: &T,
) -> Bytes {
    let mut payload = BytesMut::new();
    RequestHeader::default()
        .with_request_api_key(api_key as i16)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .encode(&mut payload, api_key.request_header_version(version))
        .unwrap();
    body.encode(&mut payload, version).unwrap();
    payload.freeze()
}

pub(super) fn decode_response<T: Decodable>(api_key: ApiKey, version: i16, frame: Bytes) -> T {
    let mut input = frame;
    let frame_size = input.get_i32() as usize;
    assert_eq!(frame_size, input.remaining());
    ResponseHeader::decode(&mut input, api_key.response_header_version(version)).unwrap();
    T::decode(&mut input, version).unwrap()
}

pub(super) async fn handle_as(
    broker: &Broker,
    username: &str,
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    body: &impl Encodable,
) -> Bytes {
    let request = supported_request(request_frame(api_key, version, correlation_id, body)).unwrap();
    broker
        .dispatch_request(
            request,
            &AuthorizationContext::authenticated(username, std::net::Ipv4Addr::LOCALHOST.into()),
        )
        .await
        .unwrap()
        .into_bytes()
}

pub(super) fn topic_rule(
    principal: &str,
    topic: &str,
    operation: AclOperation,
    permission: AclPermission,
) -> AclRule {
    AclRule {
        resource_type: AclResourceType::Topic,
        resource_name: topic.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation,
        permission,
    }
}

fn transactional_rule(principal: &str, transactional_id: &str, operation: AclOperation) -> AclRule {
    AclRule {
        resource_type: AclResourceType::TransactionalId,
        resource_name: transactional_id.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation,
        permission: AclPermission::Allow,
    }
}

#[tokio::test]
async fn create_topics_succeeds_when_config_description_is_denied() {
    let (broker, metadata) = acl_broker();
    for (operation, permission) in [
        (AclOperation::Create, AclPermission::Allow),
        (AclOperation::DescribeConfigs, AclPermission::Deny),
    ] {
        metadata
            .create_acl(topic_rule(
                "User:alice",
                "config-hidden",
                operation,
                permission,
            ))
            .await
            .unwrap();
    }
    let request = CreateTopicsRequest::default().with_topics(vec![
        CreatableTopic::default()
            .with_name(topic_name("config-hidden"))
            .with_num_partitions(2)
            .with_replication_factor(1),
    ]);

    let response = handle_as(&broker, "alice", ApiKey::CreateTopics, 7, 5, &request).await;
    let response: CreateTopicsResponse = decode_response(ApiKey::CreateTopics, 7, response);
    let result = &response.topics[0];

    assert_eq!(result.error_code, NO_ERROR);
    assert_eq!(result.topic_config_error_code, TOPIC_AUTHORIZATION_FAILED);
    assert!(!result.topic_id.is_nil());
    assert_eq!(result.num_partitions, -1);
    assert_eq!(result.replication_factor, -1);
    assert!(result.configs.as_ref().unwrap().is_empty());
    assert_eq!(
        metadata
            .topic("config-hidden")
            .await
            .unwrap()
            .unwrap()
            .partitions,
        2
    );
}

#[tokio::test]
async fn kafka_acl_management_apis_create_describe_and_delete() {
    let (broker, _) = acl_broker();
    let create = CreateAclsRequest::default().with_creations(vec![
        AclCreation::default()
            .with_resource_type(AclResourceType::Topic.code())
            .with_resource_name(StrBytes::from_string("orders".to_owned()))
            .with_resource_pattern_type(AclPatternType::Literal.code())
            .with_principal(StrBytes::from_string("User:alice".to_owned()))
            .with_host(StrBytes::from_string("*".to_owned()))
            .with_operation(AclOperation::Read.code())
            .with_permission_type(AclPermission::Allow.code()),
    ]);
    let response = handle_as(&broker, "admin", ApiKey::CreateAcls, 3, 1, &create).await;
    let response: CreateAclsResponse = decode_response(ApiKey::CreateAcls, 3, response);
    assert_eq!(response.results[0].error_code, NO_ERROR);

    let describe = DescribeAclsRequest::default()
        .with_resource_type_filter(AclResourceType::Topic.code())
        .with_resource_name_filter(Some(StrBytes::from_string("orders".to_owned())))
        .with_pattern_type_filter(2)
        .with_principal_filter(Some(StrBytes::from_string("User:alice".to_owned())))
        .with_host_filter(None)
        .with_operation(1)
        .with_permission_type(1);
    let response = handle_as(&broker, "admin", ApiKey::DescribeAcls, 3, 2, &describe).await;
    let response: DescribeAclsResponse = decode_response(ApiKey::DescribeAcls, 3, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.resources.len(), 1);
    assert_eq!(
        response.resources[0].acls[0].principal.as_str(),
        "User:alice"
    );

    let delete = DeleteAclsRequest::default().with_filters(vec![
        DeleteAclsFilter::default()
            .with_resource_type_filter(AclResourceType::Topic.code())
            .with_resource_name_filter(Some(StrBytes::from_string("orders".to_owned())))
            .with_pattern_type_filter(AclPatternType::Literal.code())
            .with_principal_filter(Some(StrBytes::from_string("User:alice".to_owned())))
            .with_host_filter(None)
            .with_operation(AclOperation::Read.code())
            .with_permission_type(AclPermission::Allow.code()),
    ]);
    let response = handle_as(&broker, "admin", ApiKey::DeleteAcls, 3, 3, &delete).await;
    let response: DeleteAclsResponse = decode_response(ApiKey::DeleteAcls, 3, response);
    assert_eq!(response.filter_results[0].error_code, NO_ERROR);
    assert_eq!(response.filter_results[0].matching_acls.len(), 1);

    let response = handle_as(&broker, "admin", ApiKey::DescribeAcls, 3, 4, &describe).await;
    let response: DescribeAclsResponse = decode_response(ApiKey::DescribeAcls, 3, response);
    assert!(response.resources.is_empty());
}

#[tokio::test]
async fn two_phase_commit_acl_round_trips_through_kafka_management_apis() {
    let (broker, _) = acl_broker();
    let create = CreateAclsRequest::default().with_creations(vec![
        AclCreation::default()
            .with_resource_type(AclResourceType::TransactionalId.code())
            .with_resource_name(StrBytes::from_string("managed-2pc".to_owned()))
            .with_resource_pattern_type(AclPatternType::Literal.code())
            .with_principal(StrBytes::from_string("User:alice".to_owned()))
            .with_host(StrBytes::from_string("*".to_owned()))
            .with_operation(AclOperation::TwoPhaseCommit.code())
            .with_permission_type(AclPermission::Allow.code()),
    ]);
    let response = handle_as(&broker, "admin", ApiKey::CreateAcls, 3, 40, &create).await;
    let response: CreateAclsResponse = decode_response(ApiKey::CreateAcls, 3, response);
    assert_eq!(response.results[0].error_code, NO_ERROR);

    let describe = DescribeAclsRequest::default()
        .with_resource_type_filter(AclResourceType::TransactionalId.code())
        .with_resource_name_filter(Some(StrBytes::from_string("managed-2pc".to_owned())))
        .with_pattern_type_filter(AclPatternType::Literal.code())
        .with_principal_filter(Some(StrBytes::from_string("User:alice".to_owned())))
        .with_host_filter(None)
        .with_operation(AclOperation::TwoPhaseCommit.code())
        .with_permission_type(AclPermission::Allow.code());
    let response = handle_as(&broker, "admin", ApiKey::DescribeAcls, 3, 41, &describe).await;
    let response: DescribeAclsResponse = decode_response(ApiKey::DescribeAcls, 3, response);
    assert_eq!(response.resources.len(), 1);
    assert_eq!(
        response.resources[0].acls[0].operation,
        AclOperation::TwoPhaseCommit.code()
    );

    let delete = DeleteAclsRequest::default().with_filters(vec![
        DeleteAclsFilter::default()
            .with_resource_type_filter(AclResourceType::TransactionalId.code())
            .with_resource_name_filter(Some(StrBytes::from_string("managed-2pc".to_owned())))
            .with_pattern_type_filter(AclPatternType::Literal.code())
            .with_principal_filter(Some(StrBytes::from_string("User:alice".to_owned())))
            .with_host_filter(None)
            .with_operation(AclOperation::TwoPhaseCommit.code())
            .with_permission_type(AclPermission::Allow.code()),
    ]);
    let response = handle_as(&broker, "admin", ApiKey::DeleteAcls, 3, 42, &delete).await;
    let response: DeleteAclsResponse = decode_response(ApiKey::DeleteAcls, 3, response);
    assert_eq!(response.filter_results[0].matching_acls.len(), 1);
    assert_eq!(
        response.filter_results[0].matching_acls[0].operation,
        AclOperation::TwoPhaseCommit.code()
    );
}

#[tokio::test]
async fn authorization_denials_use_kafka_errors_and_do_not_append() {
    let (broker, metadata) = acl_broker();
    metadata.create_topic("protected", 1).await.unwrap();
    metadata
        .create_acl(topic_rule(
            "User:alice",
            "protected",
            AclOperation::Write,
            AclPermission::Allow,
        ))
        .await
        .unwrap();

    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("protected"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(super::tests::sample_records())),
                ]),
        ]);
    let response = handle_as(&broker, "alice", ApiKey::Produce, 3, 10, &produce).await;
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );

    let response = handle_as(&broker, "bob", ApiKey::Produce, 3, 11, &produce).await;
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );
    assert_eq!(
        metadata
            .list_offset(&PartitionKey::new("protected", 0), -1)
            .await
            .unwrap(),
        1
    );

    let fetch = FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name("protected"))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1024),
                ]),
        ]);
    let response = handle_as(&broker, "alice", ApiKey::Fetch, 4, 12, &fetch).await;
    let response: FetchResponse = decode_response(ApiKey::Fetch, 4, response);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        TOPIC_AUTHORIZATION_FAILED
    );
}

#[tokio::test]
async fn group_and_idempotent_producer_checks_use_resource_specific_errors() {
    let (broker, metadata) = acl_broker();
    let heartbeat = HeartbeatRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("workers".to_owned()),
        ))
        .with_generation_id(1)
        .with_member_id(StrBytes::from_string("member".to_owned()));
    let response = handle_as(&broker, "alice", ApiKey::Heartbeat, 4, 20, &heartbeat).await;
    let response: HeartbeatResponse = decode_response(ApiKey::Heartbeat, 4, response);
    assert_eq!(response.error_code, GROUP_AUTHORIZATION_FAILED);

    let init = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(60_000);
    let response = handle_as(&broker, "alice", ApiKey::InitProducerId, 4, 21, &init).await;
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    assert_eq!(response.error_code, CLUSTER_AUTHORIZATION_FAILED);

    metadata
        .create_acl(topic_rule(
            "User:alice",
            "alice-can-write-one-topic",
            AclOperation::Write,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let response = handle_as(&broker, "alice", ApiKey::InitProducerId, 4, 22, &init).await;
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    assert_eq!(response.error_code, NO_ERROR);

    metadata.set_authorization_failure_for(Some(AclResourceType::Topic));
    let response = handle_as(
        &broker,
        "backend-failure",
        ApiKey::InitProducerId,
        4,
        23,
        &init,
    )
    .await;
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    assert_eq!(response.error_code, UNKNOWN_SERVER_ERROR);
    metadata.set_authorization_failure_for(None);

    metadata
        .create_acl(AclRule {
            resource_type: AclResourceType::Cluster,
            resource_name: authorization::CLUSTER_RESOURCE_NAME.to_owned(),
            pattern_type: AclPatternType::Literal,
            principal: "User:bob".to_owned(),
            host: "*".to_owned(),
            operation: AclOperation::IdempotentWrite,
            permission: AclPermission::Allow,
        })
        .await
        .unwrap();
    let response = handle_as(&broker, "bob", ApiKey::InitProducerId, 4, 24, &init).await;
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    assert_eq!(response.error_code, NO_ERROR);
}

#[tokio::test]
async fn init_producer_id_v6_requires_write_and_two_phase_commit_acls() {
    let (mut broker, metadata) = acl_broker();
    broker.config.transaction_two_phase_commit_enable = true;
    let request = |transactional_id: &str, enable_2_pc: bool| {
        InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId::from(StrBytes::from_string(
                transactional_id.to_owned(),
            ))))
            .with_transaction_timeout_ms(60_000)
            .with_enable_2_pc(enable_2_pc)
    };

    metadata
        .create_acl(transactional_rule(
            "User:alice",
            "write-only",
            AclOperation::Write,
        ))
        .await
        .unwrap();
    let response = handle_as(
        &broker,
        "alice",
        ApiKey::InitProducerId,
        6,
        50,
        &request("write-only", false),
    )
    .await;
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(response.error_code, NO_ERROR);
    let response = handle_as(
        &broker,
        "alice",
        ApiKey::InitProducerId,
        6,
        51,
        &request("write-only", true),
    )
    .await;
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(response.error_code, TRANSACTIONAL_ID_AUTHORIZATION_FAILED);

    metadata
        .create_acl(transactional_rule(
            "User:alice",
            "write-only",
            AclOperation::TwoPhaseCommit,
        ))
        .await
        .unwrap();
    let response = handle_as(
        &broker,
        "alice",
        ApiKey::InitProducerId,
        6,
        52,
        &request("write-only", true),
    )
    .await;
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(response.error_code, NO_ERROR);

    metadata
        .create_acl(transactional_rule(
            "User:alice",
            "two-phase-only",
            AclOperation::TwoPhaseCommit,
        ))
        .await
        .unwrap();
    let response = handle_as(
        &broker,
        "alice",
        ApiKey::InitProducerId,
        6,
        53,
        &request("two-phase-only", true),
    )
    .await;
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(response.error_code, TRANSACTIONAL_ID_AUTHORIZATION_FAILED);

    metadata
        .create_acl(transactional_rule(
            "User:alice",
            "all-operations",
            AclOperation::All,
        ))
        .await
        .unwrap();
    let response = handle_as(
        &broker,
        "alice",
        ApiKey::InitProducerId,
        6,
        54,
        &request("all-operations", true),
    )
    .await;
    let response: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 6, response);
    assert_eq!(response.error_code, NO_ERROR);
}

#[tokio::test]
async fn streams_regex_topology_authorizes_every_resolved_topic_before_join() {
    let (broker, metadata) = acl_broker();
    metadata.create_topic("streams-allowed", 1).await.unwrap();
    metadata.create_topic("streams-denied", 1).await.unwrap();
    metadata
        .create_acl(AclRule {
            resource_type: AclResourceType::Group,
            resource_name: "streams-workers".to_owned(),
            pattern_type: AclPatternType::Literal,
            principal: "User:alice".to_owned(),
            host: "*".to_owned(),
            operation: AclOperation::Read,
            permission: AclPermission::Allow,
        })
        .await
        .unwrap();
    metadata
        .create_acl(topic_rule(
            "User:alice",
            "streams-allowed",
            AclOperation::Describe,
            AclPermission::Allow,
        ))
        .await
        .unwrap();

    let heartbeat = StreamsGroupHeartbeatRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_string(
            "streams-workers".to_owned(),
        )))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(0)
        .with_endpoint_information_epoch(-1)
        .with_rebalance_timeout_ms(300_000)
        .with_topology(Some(Topology::default().with_epoch(0).with_subtopologies(
            vec![
                Subtopology::default()
                    .with_subtopology_id(StrBytes::from_string("0".to_owned()))
                    .with_source_topic_regex(vec![StrBytes::from_string(
                        "streams-.*".to_owned(),
                    )]),
            ],
        )))
        .with_active_tasks(Some(Vec::<TaskIds>::new()))
        .with_standby_tasks(Some(Vec::new()))
        .with_warmup_tasks(Some(Vec::new()))
        .with_process_id(Some(StrBytes::from_string("process-a".to_owned())))
        .with_client_tags(Some(Vec::new()));
    let response = handle_as(
        &broker,
        "alice",
        ApiKey::StreamsGroupHeartbeat,
        0,
        30,
        &heartbeat,
    )
    .await;
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, TOPIC_AUTHORIZATION_FAILED);
    assert!(
        metadata
            .describe_streams_groups(&["streams-workers".to_owned()])
            .await
            .unwrap()
            .is_empty()
    );

    metadata
        .create_acl(topic_rule(
            "User:alice",
            "streams-denied",
            AclOperation::Describe,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let response = handle_as(
        &broker,
        "alice",
        ApiKey::StreamsGroupHeartbeat,
        0,
        31,
        &heartbeat,
    )
    .await;
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);

    let describe = StreamsGroupDescribeRequest::default()
        .with_group_ids(vec![GroupId::from(StrBytes::from_string(
            "streams-workers".to_owned(),
        ))])
        .with_include_authorized_operations(true);
    let response = handle_as(
        &broker,
        "carol",
        ApiKey::StreamsGroupDescribe,
        0,
        32,
        &describe,
    )
    .await;
    let response: StreamsGroupDescribeResponse =
        decode_response(ApiKey::StreamsGroupDescribe, 0, response);
    assert_eq!(response.groups[0].error_code, GROUP_AUTHORIZATION_FAILED);

    metadata
        .create_acl(AclRule {
            resource_type: AclResourceType::Group,
            resource_name: "streams-workers".to_owned(),
            pattern_type: AclPatternType::Literal,
            principal: "User:bob".to_owned(),
            host: "*".to_owned(),
            operation: AclOperation::Describe,
            permission: AclPermission::Allow,
        })
        .await
        .unwrap();
    metadata
        .create_acl(topic_rule(
            "User:bob",
            "streams-allowed",
            AclOperation::Describe,
            AclPermission::Allow,
        ))
        .await
        .unwrap();
    let response = handle_as(
        &broker,
        "bob",
        ApiKey::StreamsGroupDescribe,
        0,
        33,
        &describe,
    )
    .await;
    let response: StreamsGroupDescribeResponse =
        decode_response(ApiKey::StreamsGroupDescribe, 0, response);
    assert_eq!(response.groups[0].error_code, TOPIC_AUTHORIZATION_FAILED);

    let response = handle_as(
        &broker,
        "alice",
        ApiKey::StreamsGroupDescribe,
        0,
        34,
        &describe,
    )
    .await;
    let response: StreamsGroupDescribeResponse =
        decode_response(ApiKey::StreamsGroupDescribe, 0, response);
    assert_eq!(response.groups[0].error_code, NO_ERROR);
    assert_eq!(
        response.groups[0]
            .topology
            .as_ref()
            .unwrap()
            .subtopologies
            .as_ref()
            .unwrap()[0]
            .source_topics
            .len(),
        2
    );
}

#[tokio::test]
async fn streams_internal_topic_creation_requires_create_acl() {
    let (broker, metadata) = acl_broker();
    let source = "streams-stateful-input";
    let changelog = "streams-stateful-app-store-changelog";
    metadata.create_topic(source, 2).await.unwrap();
    metadata
        .create_acl(AclRule {
            resource_type: AclResourceType::Group,
            resource_name: "streams-stateful-app".to_owned(),
            pattern_type: AclPatternType::Literal,
            principal: "User:alice".to_owned(),
            host: "*".to_owned(),
            operation: AclOperation::Read,
            permission: AclPermission::Allow,
        })
        .await
        .unwrap();
    for topic in [source, changelog] {
        metadata
            .create_acl(topic_rule(
                "User:alice",
                topic,
                AclOperation::Describe,
                AclPermission::Allow,
            ))
            .await
            .unwrap();
    }

    let heartbeat = StreamsGroupHeartbeatRequest::default()
        .with_group_id(GroupId::from(StrBytes::from_string(
            "streams-stateful-app".to_owned(),
        )))
        .with_member_id(StrBytes::from_string("member-a".to_owned()))
        .with_member_epoch(0)
        .with_endpoint_information_epoch(-1)
        .with_rebalance_timeout_ms(300_000)
        .with_topology(Some(Topology::default().with_epoch(0).with_subtopologies(
            vec![
                Subtopology::default()
                    .with_subtopology_id(StrBytes::from_string("0".to_owned()))
                    .with_source_topics(vec![StrBytes::from_string(source.to_owned()).into()])
                    .with_state_changelog_topics(vec![
                        StreamsTopicInfo::default()
                            .with_name(StrBytes::from_string(changelog.to_owned()).into())
                            .with_partitions(0)
                            .with_replication_factor(1)
                            .with_topic_configs(vec![
                                StreamsKeyValue::default()
                                    .with_key(StrBytes::from_string(
                                        "cleanup.policy".to_owned(),
                                    ))
                                    .with_value(StrBytes::from_string("compact".to_owned())),
                            ]),
                    ]),
            ],
        )))
        .with_active_tasks(Some(Vec::<TaskIds>::new()))
        .with_standby_tasks(Some(Vec::new()))
        .with_warmup_tasks(Some(Vec::new()))
        .with_process_id(Some(StrBytes::from_string("process-a".to_owned())))
        .with_client_tags(Some(Vec::new()));
    let response = handle_as(
        &broker,
        "alice",
        ApiKey::StreamsGroupHeartbeat,
        0,
        35,
        &heartbeat,
    )
    .await;
    let response: StreamsGroupHeartbeatResponse =
        decode_response(ApiKey::StreamsGroupHeartbeat, 0, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(
        response.status.as_ref().unwrap()[0].status_code,
        STREAMS_STATUS_MISSING_INTERNAL_TOPICS
    );
    assert!(
        response.status.as_ref().unwrap()[0]
            .status_detail
            .as_str()
            .contains("Create ACL")
    );
    assert!(response.active_tasks.as_ref().is_some_and(Vec::is_empty));
    assert!(metadata.topic(changelog).await.unwrap().is_none());
}
