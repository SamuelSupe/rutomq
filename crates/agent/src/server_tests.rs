use super::*;
use crate::config::SecurityConfig;
use crate::kafka_error::{
    ILLEGAL_SASL_STATE, SASL_AUTHENTICATION_FAILED, UNSUPPORTED_SASL_MECHANISM, UNSUPPORTED_VERSION,
};
use crate::scram::ScramMechanism;
use bytes::Buf;
use kafka_protocol::messages::ProduceResponse;
use kafka_protocol::messages::add_partitions_to_txn_request::{
    AddPartitionsToTxnTopic, AddPartitionsToTxnTransaction,
};
use kafka_protocol::messages::add_partitions_to_txn_response::AddPartitionsToTxnResponse;
use kafka_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsTopic,
};
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_producers_request::TopicRequest;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic, ReplicaState};
use kafka_protocol::messages::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig,
};
use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
use kafka_protocol::messages::leave_group_request::MemberIdentity;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use kafka_protocol::messages::offset_fetch_request::{
    OffsetFetchRequestTopic, OffsetFetchRequestTopics,
};
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
use kafka_protocol::messages::txn_offset_commit_request::{
    TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
};
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    ApiVersionsResponse, DeleteRecordsRequest, DeleteRecordsResponse, DescribeConfigsResponse,
    DescribeProducersRequest, DescribeProducersResponse, EndTxnRequest, EndTxnResponse,
    HeartbeatRequest, HeartbeatResponse, IncrementalAlterConfigsResponse, InitProducerIdRequest,
    InitProducerIdResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest,
    LeaveGroupResponse, MetadataResponse, RequestHeader, ResponseHeader, SaslAuthenticateResponse,
    SaslHandshakeResponse, SyncGroupRequest, SyncGroupResponse, TransactionalId,
    TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use kafka_protocol::records::{Compression, Record, RecordBatchDecoder, RecordBatchEncoder};
use rutomq_control::MemoryMetadataStore;
use rutomq_protocol::{advertised_api_versions, body_version};
use rutomq_storage::OpenDalObjectStore;
use std::time::Duration;

pub(super) fn sample_records() -> Bytes {
    sample_records_count(1)
}

pub(super) fn sample_records_count(count: usize) -> Bytes {
    let records = (0..count)
        .map(|offset| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: kafka_protocol::records::TimestampType::Creation,
            offset: offset as i64,
            sequence: i32::try_from(offset)
                .expect("test record offset fits in i32")
                .wrapping_sub(1),
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(b"hello")),
            headers: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut bytes = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut bytes,
        records.iter(),
        &kafka_protocol::records::RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    bytes.freeze()
}

pub(super) fn producer_records(
    producer_id: i64,
    producer_epoch: i16,
    sequence: i32,
    transactional: bool,
    value: &'static [u8],
) -> Bytes {
    producer_records_with_sequences(
        producer_id,
        producer_epoch,
        &[sequence],
        transactional,
        value,
    )
}

fn producer_records_with_sequences(
    producer_id: i64,
    producer_epoch: i16,
    sequences: &[i32],
    transactional: bool,
    value: &'static [u8],
) -> Bytes {
    let records = sequences
        .iter()
        .enumerate()
        .map(|(offset, sequence)| Record {
            transactional,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id,
            producer_epoch,
            timestamp_type: kafka_protocol::records::TimestampType::Creation,
            offset: offset as i64,
            sequence: *sequence,
            timestamp: 1,
            key: None,
            value: Some(Bytes::from_static(value)),
            headers: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut bytes = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut bytes,
        records.iter(),
        &kafka_protocol::records::RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    bytes.freeze()
}

fn producer_record_batches(
    producer_id: i64,
    producer_epoch: i16,
    sequences: &[i32],
    transactional: bool,
    value: &'static [u8],
) -> Bytes {
    let mut batches = BytesMut::new();
    for sequence in sequences {
        batches.extend_from_slice(&producer_records(
            producer_id,
            producer_epoch,
            *sequence,
            transactional,
            value,
        ));
    }
    batches.freeze()
}

fn with_record_batch_header_i32(records: Bytes, offset: usize, value: i32) -> Bytes {
    let mut bytes = records.to_vec();
    bytes[offset..offset + size_of::<i32>()].copy_from_slice(&value.to_be_bytes());
    let crc = crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(bytes)
}

fn with_record_batch_base_offset(records: Bytes, value: i64) -> Bytes {
    let mut bytes = records.to_vec();
    bytes[..size_of::<i64>()].copy_from_slice(&value.to_be_bytes());
    Bytes::from(bytes)
}

fn with_corrupted_record_batch_crc(records: Bytes) -> Bytes {
    let mut bytes = records.to_vec();
    bytes[17] ^= 1;
    Bytes::from(bytes)
}

fn records_with_noncanonical_null_lengths() -> Bytes {
    let records = [Record {
        transactional: false,
        control: false,
        delete_horizon: false,
        partition_leader_epoch: -1,
        producer_id: -1,
        producer_epoch: -1,
        timestamp_type: kafka_protocol::records::TimestampType::Creation,
        offset: 0,
        sequence: -1,
        timestamp: 0,
        key: None,
        value: None,
        headers: [("h".into(), None)].into(),
    }];
    let mut bytes = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut bytes,
        records.iter(),
        &kafka_protocol::records::RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    const RECORDS_OFFSET: usize = 61;
    assert_eq!(
        &bytes[RECORDS_OFFSET..],
        &[18, 0, 0, 0, 1, 1, 2, 2, b'h', 1]
    );
    bytes[RECORDS_OFFSET + 4] = 3;
    bytes[RECORDS_OFFSET + 5] = 3;
    bytes[RECORDS_OFFSET + 9] = 3;
    let crc = crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
    bytes.freeze()
}

fn with_record_batch_padding(records: Bytes, pad_record_body: bool) -> Bytes {
    let mut bytes = records.to_vec();
    if pad_record_body {
        const RECORDS_OFFSET: usize = 61;
        assert_eq!(bytes[RECORDS_OFFSET] & 0x80, 0);
        bytes[RECORDS_OFFSET] += 2;
    }
    bytes.push(0);
    let batch_length = i32::from_be_bytes(bytes[8..12].try_into().unwrap()) + 1;
    bytes[8..12].copy_from_slice(&batch_length.to_be_bytes());
    let crc = crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(bytes)
}

fn with_impossible_record_header_count(records: Bytes) -> Bytes {
    const RECORDS_OFFSET: usize = 61;
    let mut bytes = records.to_vec();
    assert_eq!(bytes[RECORDS_OFFSET] & 0x80, 0);
    assert_eq!(bytes.last(), Some(&0));
    bytes[RECORDS_OFFSET] += 8;
    bytes.pop();
    bytes.extend_from_slice(&[254, 255, 255, 255, 15]);
    let batch_length = i32::from_be_bytes(bytes[8..12].try_into().unwrap()) + 4;
    bytes[8..12].copy_from_slice(&batch_length.to_be_bytes());
    let crc = crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(bytes)
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0x82f6_3b78 & 0u32.wrapping_sub(crc & 1));
        }
    }
    !crc
}

pub(super) fn broker() -> Broker {
    let config = AgentConfig {
        classic_group_initial_rebalance_delay_ms: 0,
        streams_group_initial_rebalance_delay_ms: 0,
        group_assignment_interval_ms: 0,
        share_group_assignment_interval_ms: 0,
        streams_group_assignment_interval_ms: 0,
        consumer_assignor_offload_enable: false,
        share_assignor_offload_enable: false,
        streams_assignor_offload_enable: false,
        ..AgentConfig::default()
    };
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

#[tokio::test]
async fn broker_bounds_connections_and_in_flight_request_bytes() {
    let config = AgentConfig {
        max_connections: 1,
        max_frame_size: 16,
        max_inflight_request_bytes: 32,
        max_inflight_response_bytes: 32,
        ..AgentConfig::default()
    };
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );

    let connection = broker.connection_slots.clone().try_acquire_owned().unwrap();
    assert!(broker.connection_slots.clone().try_acquire_owned().is_err());
    drop(connection);

    let request = broker
        .request_bytes
        .clone()
        .try_acquire_many_owned(16)
        .unwrap();
    let concurrent_request = broker
        .request_bytes
        .clone()
        .try_acquire_many_owned(16)
        .unwrap();
    assert!(broker.request_bytes.clone().try_acquire_owned().is_err());
    drop(concurrent_request);
    drop(request);

    let response = broker
        .response_bytes
        .clone()
        .try_acquire_many_owned(16)
        .unwrap();
    let concurrent_response = broker
        .response_bytes
        .clone()
        .try_acquire_many_owned(16)
        .unwrap();
    assert!(broker.response_bytes.clone().try_acquire_owned().is_err());
    drop(concurrent_response);
    drop(response);
}

#[tokio::test]
async fn incomplete_frame_releases_request_memory_after_read_timeout() {
    let config = AgentConfig {
        max_frame_size: 16,
        max_inflight_request_bytes: 32,
        connection_io_timeout: Duration::from_millis(25),
        ..AgentConfig::default()
    };
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    let (mut client, server) = tokio::io::duplex(64);
    let (_shutdown, receiver) = watch::channel(false);
    let connection = {
        let broker = broker.clone();
        tokio::spawn(async move {
            broker
                .serve_connection(server, "127.0.0.1:19092".parse().unwrap(), receiver)
                .await
        })
    };

    client.write_i32(16).await.unwrap();
    for _ in 0..100 {
        if broker.request_bytes.available_permits() == 16 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(broker.request_bytes.available_permits(), 16);
    let concurrent_request = broker
        .request_bytes
        .clone()
        .try_acquire_many_owned(16)
        .unwrap();
    drop(concurrent_request);

    let error = tokio::time::timeout(Duration::from_secs(1), connection)
        .await
        .expect("incomplete frame must time out")
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("frame body read timed out"));
    assert_eq!(broker.request_bytes.available_permits(), 32);
}

#[tokio::test]
async fn unread_response_releases_the_connection_after_io_timeout() {
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig {
            connection_io_timeout: Duration::from_millis(25),
            ..AgentConfig::default()
        },
        Arc::new(Metrics::new().unwrap()),
    );
    let (mut client, server) = tokio::io::duplex(64);
    let (_shutdown, receiver) = watch::channel(false);
    let connection = tokio::spawn(async move {
        broker
            .serve_connection(server, "127.0.0.1:19092".parse().unwrap(), receiver)
            .await
    });
    let request = ApiVersionsRequest::default();
    write_sized_packet(
        &mut client,
        &request_frame(ApiKey::ApiVersions, 0, 1, &request),
    )
    .await
    .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(1), connection)
        .await
        .expect("unread response must time out")
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("response write timed out"));
}

#[tokio::test]
async fn response_memory_budget_exhaustion_closes_the_connection() {
    let broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig {
            max_inflight_response_bytes: 1,
            ..AgentConfig::default()
        },
        Arc::new(Metrics::new().unwrap()),
    );
    let (mut client, server) = tokio::io::duplex(4096);
    let (_shutdown, receiver) = watch::channel(false);
    let connection = tokio::spawn(async move {
        broker
            .serve_connection(server, "127.0.0.1:19092".parse().unwrap(), receiver)
            .await
    });
    let request = ApiVersionsRequest::default();
    write_sized_packet(
        &mut client,
        &request_frame(ApiKey::ApiVersions, 0, 1, &request),
    )
    .await
    .unwrap();

    let error = tokio::time::timeout(Duration::from_secs(1), connection)
        .await
        .expect("response budget exhaustion must close the connection")
        .unwrap()
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("response memory budget exhausted")
    );
}

struct EncodeMustNotRun;

impl Encodable for EncodeMustNotRun {
    fn encode<B: kafka_protocol::protocol::buf::ByteBufMut>(
        &self,
        _buffer: &mut B,
        _version: i16,
    ) -> anyhow::Result<()> {
        panic!("response encoding must happen after memory reservation")
    }

    fn compute_size(&self, _version: i16) -> anyhow::Result<usize> {
        Ok(1)
    }
}

#[tokio::test]
async fn response_memory_is_reserved_before_encoding() {
    let exhausted_broker = Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        AgentConfig {
            max_inflight_response_bytes: 1,
            ..AgentConfig::default()
        },
        Arc::new(Metrics::new().unwrap()),
    );

    let error = match exhausted_broker.encode_response(ApiKey::ApiVersions, 0, 1, &EncodeMustNotRun)
    {
        Ok(_) => panic!("response budget should be exhausted"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("response memory budget exhausted")
    );

    let holding_broker = broker();
    let available = holding_broker.response_bytes.available_permits();
    let response = holding_broker
        .encode_response(ApiKey::ApiVersions, 0, 1, &ApiVersionsResponse::default())
        .unwrap()
        .into_bytes();
    assert!(holding_broker.response_bytes.available_permits() < available);
    drop(response);
    assert_eq!(holding_broker.response_bytes.available_permits(), available);
}

fn sasl_broker() -> Broker {
    sasl_broker_with_max_reauth(0)
}

fn sasl_broker_with_max_reauth(max_reauth_ms: i64) -> Broker {
    let config = AgentConfig {
        security: SecurityConfig {
            scram_users: HashMap::from([
                ("alice".to_owned(), "secret".to_owned()),
                ("bob".to_owned(), "bob-secret".to_owned()),
            ]),
            sasl_max_reauth_ms: max_reauth_ms,
            sasl_enabled: true,
            ..SecurityConfig::default()
        },
        ..AgentConfig::default()
    };
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

async fn write_sized_packet(
    stream: &mut tokio::io::DuplexStream,
    payload: &[u8],
) -> std::io::Result<()> {
    stream
        .write_i32(i32::try_from(payload.len()).unwrap())
        .await?;
    stream.write_all(payload).await
}

async fn read_sized_packet(stream: &mut tokio::io::DuplexStream) -> std::io::Result<Bytes> {
    let size = stream.read_i32().await?;
    if size < 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "negative packet size",
        ));
    }
    let mut payload = vec![0; size as usize];
    stream.read_exact(&mut payload).await?;
    Ok(Bytes::from(payload))
}

fn kafka_response_frame(payload: Bytes) -> Bytes {
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as i32).to_be_bytes());
    frame.extend_from_slice(&payload);
    Bytes::from(frame)
}

fn spawn_sasl_connection(
    broker: Broker,
) -> (
    tokio::io::DuplexStream,
    watch::Sender<bool>,
    tokio::task::JoinHandle<Result<()>>,
) {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let (shutdown, receiver) = watch::channel(false);
    let task = tokio::spawn(async move {
        broker
            .serve_connection(server, "127.0.0.1:19092".parse().unwrap(), receiver)
            .await
    });
    (client, shutdown, task)
}

async fn select_legacy_scram(client: &mut tokio::io::DuplexStream) {
    let handshake =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("SCRAM-SHA-256"));
    write_sized_packet(
        client,
        &request_frame(ApiKey::SaslHandshake, 0, 1, &handshake),
    )
    .await
    .unwrap();
    let response = read_sized_packet(client).await.unwrap();
    let response: SaslHandshakeResponse =
        decode_response(ApiKey::SaslHandshake, 0, kafka_response_frame(response));
    assert_eq!(response.error_code, NO_ERROR);
}

async fn select_framed_scram(client: &mut tokio::io::DuplexStream) {
    select_framed_scram_with(client, ScramMechanism::Sha256, 1).await;
}

async fn select_framed_scram_with(
    client: &mut tokio::io::DuplexStream,
    mechanism: ScramMechanism,
    correlation_id: i32,
) {
    let mechanism = match mechanism {
        ScramMechanism::Sha256 => "SCRAM-SHA-256",
        ScramMechanism::Sha512 => "SCRAM-SHA-512",
    };
    let handshake =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str(mechanism));
    write_sized_packet(
        client,
        &request_frame(ApiKey::SaslHandshake, 1, correlation_id, &handshake),
    )
    .await
    .unwrap();
    let response = read_sized_packet(client).await.unwrap();
    let response: SaslHandshakeResponse =
        decode_response(ApiKey::SaslHandshake, 1, kafka_response_frame(response));
    assert_eq!(response.error_code, NO_ERROR);
}

async fn exchange_framed_scram(
    client: &mut tokio::io::DuplexStream,
    username: &str,
    password: &str,
    nonce: &str,
    correlation_id: i32,
) -> SaslAuthenticateResponse {
    let client_first_bare = format!("n={username},r={nonce}");
    let client_first = Bytes::from(format!("n,,{client_first_bare}"));
    let request = SaslAuthenticateRequest::default().with_auth_bytes(client_first);
    write_sized_packet(
        client,
        &request_frame(ApiKey::SaslAuthenticate, 2, correlation_id, &request),
    )
    .await
    .unwrap();
    let response = read_sized_packet(client).await.unwrap();
    let response: SaslAuthenticateResponse =
        decode_response(ApiKey::SaslAuthenticate, 2, kafka_response_frame(response));
    assert_eq!(response.error_code, NO_ERROR);

    let client_final = crate::scram::tests::client_final(
        ScramMechanism::Sha256,
        password,
        &client_first_bare,
        std::str::from_utf8(&response.auth_bytes).unwrap(),
    );
    let request = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(client_final));
    write_sized_packet(
        client,
        &request_frame(ApiKey::SaslAuthenticate, 2, correlation_id + 1, &request),
    )
    .await
    .unwrap();
    let response = read_sized_packet(client).await.unwrap();
    decode_response(ApiKey::SaslAuthenticate, 2, kafka_response_frame(response))
}

pub(super) fn request_frame<T: Encodable>(
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
    body.encode(&mut payload, body_version(api_key, version))
        .unwrap();
    payload.freeze()
}

pub(super) fn decode_response<T: Decodable>(api_key: ApiKey, version: i16, frame: Bytes) -> T {
    let mut input = frame;
    let frame_size = input.get_i32() as usize;
    assert_eq!(frame_size, input.remaining());
    ResponseHeader::decode(&mut input, api_key.response_header_version(version)).unwrap();
    T::decode(&mut input, body_version(api_key, version)).unwrap()
}

#[tokio::test]
async fn sasl_handshake_v0_exchanges_opaque_scram_tokens_then_kafka_frames() {
    let (mut client, shutdown, task) = spawn_sasl_connection(sasl_broker());
    select_legacy_scram(&mut client).await;

    let client_first = b"n,,n=alice,r=client-nonce";
    write_sized_packet(&mut client, client_first).await.unwrap();
    let server_first = read_sized_packet(&mut client).await.unwrap();
    let client_final = crate::scram::tests::client_final(
        ScramMechanism::Sha256,
        "secret",
        "n=alice,r=client-nonce",
        std::str::from_utf8(&server_first).unwrap(),
    );
    write_sized_packet(&mut client, client_final.as_bytes())
        .await
        .unwrap();
    let server_final = read_sized_packet(&mut client).await.unwrap();
    assert!(server_final.starts_with(b"v="));

    write_sized_packet(
        &mut client,
        &request_frame(ApiKey::Metadata, 13, 2, &MetadataRequest::default()),
    )
    .await
    .unwrap();
    let response = read_sized_packet(&mut client).await.unwrap();
    let response: MetadataResponse =
        decode_response(ApiKey::Metadata, 13, kafka_response_frame(response));
    assert_eq!(response.brokers.len(), 1);

    drop(client);
    drop(shutdown);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn sasl_handshake_v0_invalid_proof_closes_the_connection() {
    let (mut client, shutdown, task) = spawn_sasl_connection(sasl_broker());
    select_legacy_scram(&mut client).await;

    write_sized_packet(&mut client, b"n,,n=alice,r=client-nonce")
        .await
        .unwrap();
    let server_first = read_sized_packet(&mut client).await.unwrap();
    let client_final = crate::scram::tests::client_final(
        ScramMechanism::Sha256,
        "wrong",
        "n=alice,r=client-nonce",
        std::str::from_utf8(&server_first).unwrap(),
    );
    write_sized_packet(&mut client, client_final.as_bytes())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_sized_packet(&mut client))
            .await
            .unwrap()
            .is_err()
    );

    drop(client);
    drop(shutdown);
    assert!(
        task.await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("legacy SASL authentication failed")
    );
}

#[tokio::test]
async fn sasl_handshake_v0_oversized_token_closes_the_connection() {
    let (mut client, shutdown, task) = spawn_sasl_connection(sasl_broker());
    select_legacy_scram(&mut client).await;

    write_sized_packet(&mut client, &vec![b'x'; 16 * 1024 + 1])
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_sized_packet(&mut client))
            .await
            .unwrap()
            .is_err()
    );

    drop(client);
    drop(shutdown);
    assert!(
        task.await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("legacy SASL authentication failed")
    );
}

#[tokio::test]
async fn unsupported_sasl_handshake_responds_then_closes_the_connection() {
    let (mut client, shutdown, task) = spawn_sasl_connection(sasl_broker());
    let handshake =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN"));
    write_sized_packet(
        &mut client,
        &request_frame(ApiKey::SaslHandshake, 1, 1, &handshake),
    )
    .await
    .unwrap();
    let response = read_sized_packet(&mut client).await.unwrap();
    let response: SaslHandshakeResponse =
        decode_response(ApiKey::SaslHandshake, 1, kafka_response_frame(response));
    assert_eq!(response.error_code, UNSUPPORTED_SASL_MECHANISM);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_sized_packet(&mut client))
            .await
            .unwrap()
            .is_err()
    );

    drop(client);
    drop(shutdown);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn repeated_sasl_handshake_responds_then_closes_the_connection() {
    let (mut client, shutdown, task) = spawn_sasl_connection(sasl_broker());
    select_framed_scram(&mut client).await;

    let handshake =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("SCRAM-SHA-256"));
    write_sized_packet(
        &mut client,
        &request_frame(ApiKey::SaslHandshake, 1, 2, &handshake),
    )
    .await
    .unwrap();
    let response = read_sized_packet(&mut client).await.unwrap();
    let response: SaslHandshakeResponse =
        decode_response(ApiKey::SaslHandshake, 1, kafka_response_frame(response));
    assert_eq!(response.error_code, ILLEGAL_SASL_STATE);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_sized_packet(&mut client))
            .await
            .unwrap()
            .is_err()
    );

    drop(client);
    drop(shutdown);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn framed_sasl_reauthentication_preserves_principal_and_connection() {
    let broker = sasl_broker_with_max_reauth(60_000);
    let metrics = broker.metrics.clone();
    let (mut client, shutdown, task) = spawn_sasl_connection(broker);
    select_framed_scram(&mut client).await;
    let initial = exchange_framed_scram(&mut client, "alice", "secret", "initial-nonce", 2).await;
    assert_eq!(initial.error_code, NO_ERROR);
    assert_eq!(initial.session_lifetime_ms, 60_000);

    select_framed_scram_with(&mut client, ScramMechanism::Sha256, 4).await;
    let reauthenticated =
        exchange_framed_scram(&mut client, "alice", "secret", "reauth-nonce", 5).await;
    assert_eq!(reauthenticated.error_code, NO_ERROR);
    assert_eq!(reauthenticated.session_lifetime_ms, 60_000);

    write_sized_packet(
        &mut client,
        &request_frame(ApiKey::Metadata, 13, 7, &MetadataRequest::default()),
    )
    .await
    .unwrap();
    let response = read_sized_packet(&mut client).await.unwrap();
    let response: MetadataResponse =
        decode_response(ApiKey::Metadata, 13, kafka_response_frame(response));
    assert_eq!(response.brokers.len(), 1);
    assert_eq!(metrics.sasl_authentications.get(), 1);
    assert_eq!(metrics.sasl_reauthentications.get(), 1);
    assert_eq!(metrics.sasl_authentication_failures.get(), 0);
    assert_eq!(metrics.sasl_reauthentication_failures.get(), 0);

    drop(client);
    drop(shutdown);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn framed_sasl_reauthentication_rejects_mechanism_change() {
    let broker = sasl_broker();
    let metrics = broker.metrics.clone();
    let (mut client, shutdown, task) = spawn_sasl_connection(broker);
    select_framed_scram(&mut client).await;
    let initial = exchange_framed_scram(&mut client, "alice", "secret", "initial-nonce", 2).await;
    assert_eq!(initial.error_code, NO_ERROR);

    select_framed_scram_with(&mut client, ScramMechanism::Sha512, 4).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_sized_packet(&mut client))
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(metrics.sasl_authentications.get(), 1);
    assert_eq!(metrics.sasl_reauthentication_failures.get(), 1);

    drop(client);
    drop(shutdown);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn framed_sasl_reauthentication_rejects_principal_change() {
    let broker = sasl_broker();
    let metrics = broker.metrics.clone();
    let (mut client, shutdown, task) = spawn_sasl_connection(broker);
    select_framed_scram(&mut client).await;
    let initial = exchange_framed_scram(&mut client, "alice", "secret", "initial-nonce", 2).await;
    assert_eq!(initial.error_code, NO_ERROR);

    select_framed_scram_with(&mut client, ScramMechanism::Sha256, 4).await;
    let rejected = exchange_framed_scram(&mut client, "bob", "bob-secret", "reauth-nonce", 5).await;
    assert_eq!(rejected.error_code, SASL_AUTHENTICATION_FAILED);
    assert!(
        rejected
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("Cannot change principals"))
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_sized_packet(&mut client))
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(metrics.sasl_authentications.get(), 1);
    assert_eq!(metrics.sasl_reauthentication_failures.get(), 1);

    drop(client);
    drop(shutdown);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn expired_sasl_session_closes_on_an_ordinary_request() {
    let broker = sasl_broker_with_max_reauth(1);
    let (mut client, shutdown, task) = spawn_sasl_connection(broker);
    select_framed_scram(&mut client).await;
    let initial = exchange_framed_scram(&mut client, "alice", "secret", "initial-nonce", 2).await;
    assert_eq!(initial.error_code, NO_ERROR);
    assert_eq!(initial.session_lifetime_ms, 1);
    tokio::time::sleep(Duration::from_millis(10)).await;

    write_sized_packet(
        &mut client,
        &request_frame(ApiKey::Metadata, 13, 4, &MetadataRequest::default()),
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_sized_packet(&mut client))
            .await
            .unwrap()
            .is_err()
    );

    drop(client);
    drop(shutdown);
    assert!(
        task.await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("SASL authentication is required")
    );
}

#[tokio::test]
async fn expired_sasl_session_still_accepts_reauthentication_requests() {
    let broker = sasl_broker_with_max_reauth(20);
    let metrics = broker.metrics.clone();
    let (mut client, shutdown, task) = spawn_sasl_connection(broker);
    select_framed_scram(&mut client).await;
    let initial = exchange_framed_scram(&mut client, "alice", "secret", "initial-nonce", 2).await;
    assert_eq!(initial.session_lifetime_ms, 20);
    tokio::time::sleep(Duration::from_millis(30)).await;

    select_framed_scram_with(&mut client, ScramMechanism::Sha256, 4).await;
    let reauthenticated =
        exchange_framed_scram(&mut client, "alice", "secret", "reauth-nonce", 5).await;
    assert_eq!(reauthenticated.error_code, NO_ERROR);
    assert_eq!(reauthenticated.session_lifetime_ms, 20);
    assert_eq!(metrics.sasl_authentications.get(), 1);
    assert_eq!(metrics.sasl_reauthentications.get(), 1);

    drop(client);
    drop(shutdown);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn framed_sasl_failure_responds_then_closes_the_connection() {
    let (mut client, shutdown, task) = spawn_sasl_connection(sasl_broker());
    select_framed_scram(&mut client).await;

    let client_first = Bytes::from_static(b"n,,n=alice,r=client-nonce");
    let request = SaslAuthenticateRequest::default().with_auth_bytes(client_first);
    write_sized_packet(
        &mut client,
        &request_frame(ApiKey::SaslAuthenticate, 2, 2, &request),
    )
    .await
    .unwrap();
    let response = read_sized_packet(&mut client).await.unwrap();
    let response: SaslAuthenticateResponse =
        decode_response(ApiKey::SaslAuthenticate, 2, kafka_response_frame(response));
    assert_eq!(response.error_code, NO_ERROR);
    let client_final = crate::scram::tests::client_final(
        ScramMechanism::Sha256,
        "wrong",
        "n=alice,r=client-nonce",
        std::str::from_utf8(&response.auth_bytes).unwrap(),
    );
    let request = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(client_final));
    write_sized_packet(
        &mut client,
        &request_frame(ApiKey::SaslAuthenticate, 2, 3, &request),
    )
    .await
    .unwrap();
    let response = read_sized_packet(&mut client).await.unwrap();
    let response: SaslAuthenticateResponse =
        decode_response(ApiKey::SaslAuthenticate, 2, kafka_response_frame(response));
    assert_eq!(response.error_code, SASL_AUTHENTICATION_FAILED);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_sized_packet(&mut client))
            .await
            .unwrap()
            .is_err()
    );

    drop(client);
    drop(shutdown);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unsupported_api_versions_uses_v0_fallback_and_keeps_connection_open() {
    let (mut client, shutdown, task) = spawn_sasl_connection(broker());
    let mut future_request =
        request_frame(ApiKey::ApiVersions, 4, 41, &ApiVersionsRequest::default()).to_vec();
    future_request[2..4].copy_from_slice(&5_i16.to_be_bytes());
    write_sized_packet(&mut client, &future_request)
        .await
        .unwrap();

    let response = read_sized_packet(&mut client).await.unwrap();
    assert_eq!(i32::from_be_bytes(response[..4].try_into().unwrap()), 41);
    let response: ApiVersionsResponse =
        decode_response(ApiKey::ApiVersions, 0, kafka_response_frame(response));
    assert_eq!(response.error_code, UNSUPPORTED_VERSION);
    assert_eq!(
        response
            .api_keys
            .iter()
            .map(|api| (api.api_key, api.min_version, api.max_version))
            .collect::<Vec<_>>(),
        advertised_api_versions()
    );

    write_sized_packet(
        &mut client,
        &request_frame(ApiKey::ApiVersions, 4, 42, &ApiVersionsRequest::default()),
    )
    .await
    .unwrap();
    let response = read_sized_packet(&mut client).await.unwrap();
    let response: ApiVersionsResponse =
        decode_response(ApiKey::ApiVersions, 4, kafka_response_frame(response));
    assert_eq!(response.error_code, NO_ERROR);

    drop(client);
    drop(shutdown);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unsupported_non_api_versions_still_closes_the_connection() {
    let (mut client, shutdown, task) = spawn_sasl_connection(broker());
    let mut future_request =
        request_frame(ApiKey::Metadata, 13, 43, &MetadataRequest::default()).to_vec();
    future_request[2..4].copy_from_slice(&14_i16.to_be_bytes());
    write_sized_packet(&mut client, &future_request)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read_sized_packet(&mut client))
            .await
            .unwrap()
            .is_err()
    );

    drop(client);
    drop(shutdown);
    assert!(
        task.await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("Metadata version 14 is not advertised")
    );
}

#[tokio::test]
async fn produce_fetch_and_offset_protocols_round_trip() {
    let broker = broker();
    broker.metadata.create_topic("events", 1).await.unwrap();
    let records = sample_records();
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("events"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(records)),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 1, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(response.responses[0].partition_responses[0].base_offset, 0);

    let fetch = FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name("events"))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1024 * 1024),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Fetch, 4, 2, &fetch))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 4, response);
    let fetched = response.responses[0].partitions[0].records.clone().unwrap();
    let mut input = fetched;
    let batches = RecordBatchDecoder::decode_all(&mut input).unwrap();
    assert_eq!(batches[0].records[0].offset, 0);
    assert_eq!(batches[0].records[0].partition_leader_epoch, 0);
    assert_eq!(
        batches[0].records[0].value,
        Some(Bytes::from_static(b"hello"))
    );

    let topic_id = broker.metadata.topic("events").await.unwrap().unwrap().id;
    let produce_by_id = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_topic_id(topic_id)
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(sample_records())),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 13, 3, &produce_by_id))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 13, response);
    assert_eq!(response.responses[0].topic_id, topic_id);
    assert_eq!(response.responses[0].partition_responses[0].base_offset, 1);

    let fetch_by_id = FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024 * 1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1024 * 1024),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Fetch, 18, 4, &fetch_by_id))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 18, response);
    assert_eq!(response.responses[0].topic_id, topic_id);
    assert!(response.node_endpoints.is_empty());
    assert!(
        response.responses[0].partitions[0]
            .records
            .as_ref()
            .is_some_and(|records| !records.is_empty())
    );

    let fenced_fetch = FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic_id(topic_id)
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_current_leader_epoch(-2)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1024),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Fetch, 18, 5, &fenced_fetch))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 18, response);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        crate::kafka_error::FENCED_LEADER_EPOCH
    );
    assert_eq!(
        response.responses[0].partitions[0].current_leader.leader_id,
        BrokerId::from(0)
    );
    assert_eq!(response.node_endpoints.len(), 1);
    assert_eq!(response.node_endpoints[0].node_id, BrokerId::from(0));

    let follower_fetch = FetchRequest::default()
        .with_replica_state(
            ReplicaState::default()
                .with_replica_id(BrokerId::from(1))
                .with_replica_epoch(0),
        )
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024);
    let response = broker
        .handle_request(request_frame(ApiKey::Fetch, 18, 6, &follower_fetch))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 18, response);
    assert_eq!(response.error_code, crate::kafka_error::INVALID_REQUEST);

    let commit = OffsetCommitRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("workers".into()),
        ))
        .with_member_id(StrBytes::from_string(String::new()))
        .with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(topic_name("events"))
                .with_partitions(vec![
                    OffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(1),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetCommit, 2, 4, &commit))
        .await
        .unwrap();
    let response: OffsetCommitResponse = decode_response(ApiKey::OffsetCommit, 2, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);

    let fetch_offsets = OffsetFetchRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("workers".into()),
        ))
        .with_topics(Some(vec![
            OffsetFetchRequestTopic::default()
                .with_name(topic_name("events"))
                .with_partition_indexes(vec![0]),
        ]));
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetFetch, 1, 4, &fetch_offsets))
        .await
        .unwrap();
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 1, response);
    assert_eq!(response.topics[0].partitions[0].committed_offset, 1);

    let flexible_fetch = OffsetFetchRequest::default().with_groups(vec![
        kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestGroup::default()
            .with_group_id(kafka_protocol::messages::GroupId::from(
                StrBytes::from_string("workers".into()),
            ))
            .with_topics(Some(vec![
                OffsetFetchRequestTopics::default()
                    .with_name(topic_name("events"))
                    .with_partition_indexes(vec![0]),
            ])),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetFetch, 8, 5, &flexible_fetch))
        .await
        .unwrap();
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 8, response);
    assert_eq!(
        response.groups[0].topics[0].partitions[0].committed_offset,
        1
    );

    broker
        .metadata
        .create_topic("without-commits", 1)
        .await
        .unwrap();
    let fetch_all = OffsetFetchRequest::default().with_groups(vec![
        kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestGroup::default()
            .with_group_id(kafka_protocol::messages::GroupId::from(
                StrBytes::from_string("workers".into()),
            ))
            .with_topics(None),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetFetch, 8, 6, &fetch_all))
        .await
        .unwrap();
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 8, response);
    assert_eq!(response.groups[0].topics.len(), 1);
    assert_eq!(response.groups[0].topics[0].name.as_str(), "events");
    assert_eq!(response.groups[0].topics[0].partitions.len(), 1);

    let join = JoinGroupRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("workers".into()),
        ))
        .with_session_timeout_ms(10_000)
        .with_rebalance_timeout_ms(10_000)
        .with_protocol_type(StrBytes::from_string("consumer".into()))
        .with_protocols(vec![
            JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_string("range".into()))
                .with_metadata(Bytes::from_static(b"subscription")),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::JoinGroup, 4, 6, &join))
        .await
        .unwrap();
    let response: JoinGroupResponse = decode_response(ApiKey::JoinGroup, 4, response);
    assert_eq!(response.error_code, MEMBER_ID_REQUIRED);
    assert!(!response.member_id.is_empty());
    assert!(response.members.is_empty());

    let join = join.with_member_id(response.member_id);
    let response = broker
        .handle_request(request_frame(ApiKey::JoinGroup, 4, 7, &join))
        .await
        .unwrap();
    let response: JoinGroupResponse = decode_response(ApiKey::JoinGroup, 4, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.members.len(), 1);
    let member_id = response.member_id.clone();
    let generation_id = response.generation_id;

    let sync = SyncGroupRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("workers".into()),
        ))
        .with_generation_id(generation_id)
        .with_member_id(member_id.clone())
        .with_assignments(vec![
            SyncGroupRequestAssignment::default()
                .with_member_id(member_id.clone())
                .with_assignment(Bytes::from_static(b"assignment")),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::SyncGroup, 4, 7, &sync))
        .await
        .unwrap();
    let response: SyncGroupResponse = decode_response(ApiKey::SyncGroup, 4, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.assignment, Bytes::from_static(b"assignment"));

    let heartbeat = HeartbeatRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("workers".into()),
        ))
        .with_generation_id(generation_id)
        .with_member_id(member_id.clone());
    let response = broker
        .handle_request(request_frame(ApiKey::Heartbeat, 4, 8, &heartbeat))
        .await
        .unwrap();
    let response: HeartbeatResponse = decode_response(ApiKey::Heartbeat, 4, response);
    assert_eq!(response.error_code, NO_ERROR);

    let leave = LeaveGroupRequest::default()
        .with_group_id(kafka_protocol::messages::GroupId::from(
            StrBytes::from_string("workers".into()),
        ))
        .with_members(vec![MemberIdentity::default().with_member_id(member_id)]);
    let response = broker
        .handle_request(request_frame(ApiKey::LeaveGroup, 4, 9, &leave))
        .await
        .unwrap();
    let response: LeaveGroupResponse = decode_response(ApiKey::LeaveGroup, 4, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.members.len(), 1);
    assert_eq!(response.members[0].error_code, NO_ERROR);
}

#[tokio::test]
async fn metadata_supports_legacy_names_and_kafka_four_topic_ids() {
    let broker = broker();
    let topic = broker.metadata.create_topic("metadata", 1).await.unwrap();
    let legacy = MetadataRequest::default().with_topics(None);
    let response = broker
        .handle_request(request_frame(ApiKey::Metadata, 0, 10, &legacy))
        .await
        .unwrap();
    let response: MetadataResponse = decode_response(ApiKey::Metadata, 0, response);
    assert_eq!(
        response.topics[0].name.as_ref().unwrap().as_str(),
        "metadata"
    );
    assert_eq!(response.topics[0].topic_id, Uuid::nil());

    let by_id = MetadataRequest::default().with_topics(Some(vec![
        MetadataRequestTopic::default()
            .with_topic_id(topic.id)
            .with_name(None),
    ]));
    let response = broker
        .handle_request(request_frame(ApiKey::Metadata, 13, 11, &by_id))
        .await
        .unwrap();
    let response: MetadataResponse = decode_response(ApiKey::Metadata, 13, response);
    assert_eq!(response.topics[0].topic_id, topic.id);
    assert_eq!(
        response.topics[0].name.as_ref().unwrap().as_str(),
        "metadata"
    );
}

#[tokio::test]
async fn concurrent_produce_requests_share_one_object_flush() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    metadata.create_topic("batched", 1).await.unwrap();
    let object_store = OpenDalObjectStore::memory().unwrap();
    let config = AgentConfig {
        flush_interval: Duration::from_millis(50),
        max_batch_bytes: 1024 * 1024,
        ..AgentConfig::default()
    };
    let broker = Broker::new(
        metadata,
        Arc::new(object_store.clone()),
        config,
        Arc::new(Metrics::new().unwrap()),
    );
    let request = || {
        ProduceRequest::default()
            .with_acks(-1)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name("batched"))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(sample_records())),
                    ]),
            ])
    };
    let first = broker.handle_request(request_frame(ApiKey::Produce, 3, 10, &request()));
    let second = broker.handle_request(request_frame(ApiKey::Produce, 3, 11, &request()));
    let (first, second) = tokio::join!(first, second);
    assert!(first.is_ok());
    assert!(second.is_ok());
    let objects = object_store.list("data/rutomq-cluster/").await.unwrap();
    assert_eq!(objects.len(), 1);
}

#[tokio::test]
async fn topic_flush_policies_commit_before_the_agent_window() {
    let metadata = Arc::new(MemoryMetadataStore::new());
    for (topic, config) in [
        (
            "flush-by-message",
            TopicConfig {
                flush_messages: 1,
                ..TopicConfig::default()
            },
        ),
        (
            "flush-by-time",
            TopicConfig {
                flush_ms: 0,
                ..TopicConfig::default()
            },
        ),
    ] {
        metadata.create_topic(topic, 1).await.unwrap();
        metadata.set_topic_config(topic, config).await.unwrap();
    }
    let object_store = OpenDalObjectStore::memory().unwrap();
    let broker = Broker::new(
        metadata,
        Arc::new(object_store.clone()),
        AgentConfig {
            flush_interval: Duration::from_secs(60 * 60),
            ..AgentConfig::default()
        },
        Arc::new(Metrics::new().unwrap()),
    );

    for (correlation, topic) in [(20, "flush-by-message"), (21, "flush-by-time")] {
        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name(topic))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(sample_records())),
                    ]),
            ]);
        tokio::time::timeout(
            Duration::from_secs(1),
            broker.handle_request(request_frame(ApiKey::Produce, 3, correlation, &request)),
        )
        .await
        .expect("topic policy must bypass the one-hour Agent flush window")
        .unwrap();
    }
    assert_eq!(
        object_store
            .list("data/rutomq-cluster/")
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn produce_with_acks_zero_persists_without_a_response_frame() {
    let broker = broker();
    broker
        .metadata
        .create_topic("fire-and-forget", 1)
        .await
        .unwrap();
    let request = ProduceRequest::default().with_acks(0).with_topic_data(vec![
        TopicProduceData::default()
            .with_name(topic_name("fire-and-forget"))
            .with_partition_data(vec![
                PartitionProduceData::default()
                    .with_index(0)
                    .with_records(Some(sample_records())),
            ]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 12, &request))
        .await
        .unwrap();
    assert!(response.is_empty());
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("fire-and-forget", 0), -1)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn produce_validates_required_acks_against_the_virtual_isr() {
    use crate::kafka_error::{INVALID_REQUIRED_ACKS, NOT_ENOUGH_REPLICAS};

    let broker = broker();
    broker
        .metadata
        .create_topic("minimum-isr", 1)
        .await
        .unwrap();
    broker
        .metadata
        .set_topic_config(
            "minimum-isr",
            TopicConfig {
                min_insync_replicas: 2,
                ..TopicConfig::default()
            },
        )
        .await
        .unwrap();
    let request = |acks| {
        ProduceRequest::default()
            .with_acks(acks)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name("minimum-isr"))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(sample_records())),
                    ]),
            ])
    };

    let invalid = request(2);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 13, &invalid))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        INVALID_REQUIRED_ACKS
    );

    let all = request(-1);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 14, &all))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NOT_ENOUGH_REPLICAS
    );
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("minimum-isr", 0), -1)
            .await
            .unwrap(),
        0
    );

    let one = request(1);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 15, &one))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );
    assert_eq!(response.responses[0].partition_responses[0].base_offset, 0);
}

#[tokio::test]
async fn idempotent_produce_retry_reuses_the_assigned_offset() {
    let broker = broker();
    broker.metadata.create_topic("idempotent", 1).await.unwrap();
    let init = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(60_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 20, &init))
        .await
        .unwrap();
    let producer: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    assert_eq!(producer.error_code, NO_ERROR);

    let produce = |sequence| {
        ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(1_000)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name("idempotent"))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(producer_records(
                                producer.producer_id.0,
                                producer.producer_epoch,
                                sequence,
                                false,
                                b"idempotent",
                            ))),
                    ]),
            ])
    };
    let first = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 21, &produce(0)))
        .await
        .unwrap();
    let first: ProduceResponse = decode_response(ApiKey::Produce, 3, first);
    let retry = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 22, &produce(0)))
        .await
        .unwrap();
    let retry: ProduceResponse = decode_response(ApiKey::Produce, 3, retry);
    assert_eq!(
        first.responses[0].partition_responses[0].base_offset,
        retry.responses[0].partition_responses[0].base_offset
    );

    let out_of_order = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 23, &produce(2)))
        .await
        .unwrap();
    let out_of_order: ProduceResponse = decode_response(ApiKey::Produce, 3, out_of_order);
    assert_eq!(
        out_of_order.responses[0].partition_responses[0].error_code,
        crate::kafka_error::OUT_OF_ORDER_SEQUENCE_NUMBER
    );

    for sequence in 1..=5 {
        let response = broker
            .handle_request(request_frame(
                ApiKey::Produce,
                3,
                30 + sequence,
                &produce(sequence),
            ))
            .await
            .unwrap();
        let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
        let partition = &response.responses[0].partition_responses[0];
        assert_eq!(partition.error_code, NO_ERROR);
        assert_eq!(partition.base_offset, i64::from(sequence));
    }

    let recent_retry = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 40, &produce(1)))
        .await
        .unwrap();
    let recent_retry: ProduceResponse = decode_response(ApiKey::Produce, 3, recent_retry);
    assert_eq!(
        recent_retry.responses[0].partition_responses[0].base_offset,
        1
    );

    let evicted_retry = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 41, &produce(0)))
        .await
        .unwrap();
    let evicted_retry: ProduceResponse = decode_response(ApiKey::Produce, 3, evicted_retry);
    assert_eq!(
        evicted_retry.responses[0].partition_responses[0].error_code,
        crate::kafka_error::OUT_OF_ORDER_SEQUENCE_NUMBER
    );
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("idempotent", 0), -1)
            .await
            .unwrap(),
        6
    );

    let delete = DeleteRecordsRequest::default().with_topics(vec![
        DeleteRecordsTopic::default()
            .with_name(topic_name("idempotent"))
            .with_partitions(vec![
                DeleteRecordsPartition::default()
                    .with_partition_index(0)
                    .with_offset(-1),
            ]),
    ]);
    let deleted = broker
        .handle_request(request_frame(ApiKey::DeleteRecords, 2, 42, &delete))
        .await
        .unwrap();
    let deleted: DeleteRecordsResponse = decode_response(ApiKey::DeleteRecords, 2, deleted);
    assert_eq!(deleted.topics[0].partitions[0].error_code, NO_ERROR);
    assert_eq!(deleted.topics[0].partitions[0].low_watermark, 6);

    let describe = DescribeProducersRequest::default().with_topics(vec![
        TopicRequest::default()
            .with_name(topic_name("idempotent"))
            .with_partition_indexes(vec![0]),
    ]);
    let described = broker
        .handle_request(request_frame(ApiKey::DescribeProducers, 0, 43, &describe))
        .await
        .unwrap();
    let described: DescribeProducersResponse =
        decode_response(ApiKey::DescribeProducers, 0, described);
    assert!(
        described.topics[0].partitions[0]
            .active_producers
            .is_empty()
    );

    let after_truncation = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 44, &produce(6)))
        .await
        .unwrap();
    let after_truncation: ProduceResponse = decode_response(ApiKey::Produce, 3, after_truncation);
    let partition = &after_truncation.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, NO_ERROR);
    assert_eq!(partition.base_offset, 6);
}

#[tokio::test]
async fn producer_sequences_roll_over_from_i32_max_to_zero() {
    let broker = broker();
    broker
        .metadata
        .create_topic("sequence-rollover", 2)
        .await
        .unwrap();
    let init = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(60_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 45, &init))
        .await
        .unwrap();
    let producer: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    let request = |partitions: Vec<(i32, Bytes)>| {
        ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(1_000)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(topic_name("sequence-rollover"))
                    .with_partition_data(
                        partitions
                            .into_iter()
                            .map(|(index, records)| {
                                PartitionProduceData::default()
                                    .with_index(index)
                                    .with_records(Some(records))
                            })
                            .collect(),
                    ),
            ])
    };
    let initial = request(vec![
        (
            0,
            producer_records_with_sequences(
                producer.producer_id.0,
                producer.producer_epoch,
                &[i32::MAX, 0],
                false,
                b"in-batch-rollover",
            ),
        ),
        (
            1,
            producer_records(
                producer.producer_id.0,
                producer.producer_epoch,
                i32::MAX,
                false,
                b"cross-batch-before-rollover",
            ),
        ),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 46, &initial))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    assert!(
        response.responses[0]
            .partition_responses
            .iter()
            .all(|partition| partition.error_code == NO_ERROR && partition.base_offset == 0)
    );

    let next = request(vec![
        (
            0,
            producer_records(
                producer.producer_id.0,
                producer.producer_epoch,
                1,
                false,
                b"after-in-batch-rollover",
            ),
        ),
        (
            1,
            producer_records(
                producer.producer_id.0,
                producer.producer_epoch,
                0,
                false,
                b"cross-batch-after-rollover",
            ),
        ),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 47, &next))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    let partitions = &response.responses[0].partition_responses;
    assert_eq!(partitions[0].error_code, NO_ERROR);
    assert_eq!(partitions[0].base_offset, 2);
    assert_eq!(partitions[1].error_code, NO_ERROR);
    assert_eq!(partitions[1].base_offset, 1);

    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 48, &initial))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    assert!(
        response.responses[0]
            .partition_responses
            .iter()
            .all(|partition| partition.error_code == NO_ERROR && partition.base_offset == 0)
    );

    let describe = DescribeProducersRequest::default().with_topics(vec![
        TopicRequest::default()
            .with_name(topic_name("sequence-rollover"))
            .with_partition_indexes(vec![0, 1]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeProducers, 0, 49, &describe))
        .await
        .unwrap();
    let response: DescribeProducersResponse =
        decode_response(ApiKey::DescribeProducers, 0, response);
    assert_eq!(
        response.topics[0].partitions[0].active_producers[0].last_sequence,
        1
    );
    assert_eq!(
        response.topics[0].partitions[1].active_producers[0].last_sequence,
        0
    );
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("sequence-rollover", 0), -1)
            .await
            .unwrap(),
        3
    );
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("sequence-rollover", 1), -1)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn multi_record_batch_produce_is_rejected_without_allocating_offsets() {
    let broker = broker();
    broker
        .metadata
        .create_topic("multi-batch-idempotent", 1)
        .await
        .unwrap();
    let init = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(60_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 50, &init))
        .await
        .unwrap();
    let producer: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("multi-batch-idempotent"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(producer_record_batches(
                            producer.producer_id.0,
                            producer.producer_epoch,
                            &[0, 1],
                            false,
                            b"multi-batch",
                        ))),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 51, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].partition_responses.len(), 1);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        crate::kafka_error::INVALID_RECORD
    );
    assert_eq!(response.responses[0].partition_responses[0].base_offset, -1);
    assert!(
        response.responses[0].partition_responses[0]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("more than one batch"))
    );
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("multi-batch-idempotent", 0), -1)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn invalid_record_batch_headers_are_rejected_without_mutation() {
    let broker = broker();
    broker
        .metadata
        .create_topic("invalid-batch-headers", 5)
        .await
        .unwrap();
    let mut config = broker
        .metadata
        .topic_config("invalid-batch-headers")
        .await
        .unwrap();
    config.compression_type = "zstd".to_owned();
    config.message_timestamp_type = "LogAppendTime".to_owned();
    broker
        .metadata
        .set_topic_config("invalid-batch-headers", config)
        .await
        .unwrap();
    let init = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(60_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 52, &init))
        .await
        .unwrap();
    let producer: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);

    let zero_count = with_record_batch_header_i32(
        producer_records(
            producer.producer_id.0,
            producer.producer_epoch,
            0,
            false,
            b"zero-count",
        ),
        57,
        0,
    );
    let inconsistent_range = with_record_batch_header_i32(
        producer_records(
            producer.producer_id.0,
            producer.producer_epoch,
            0,
            false,
            b"inconsistent-range",
        ),
        23,
        1,
    );
    let negative_sequence = with_record_batch_header_i32(
        producer_records(
            producer.producer_id.0,
            producer.producer_epoch,
            0,
            false,
            b"negative-sequence",
        ),
        53,
        -2,
    );
    let impossible_count = with_record_batch_header_i32(
        with_record_batch_header_i32(
            producer_records(
                producer.producer_id.0,
                producer.producer_epoch,
                0,
                false,
                b"impossible-count",
            ),
            23,
            i32::MAX - 1,
        ),
        57,
        i32::MAX,
    );
    let nonzero_base_offset = with_record_batch_base_offset(
        producer_records(
            producer.producer_id.0,
            producer.producer_epoch,
            0,
            false,
            b"nonzero-base-offset",
        ),
        1,
    );
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("invalid-batch-headers"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(zero_count)),
                    PartitionProduceData::default()
                        .with_index(1)
                        .with_records(Some(inconsistent_range)),
                    PartitionProduceData::default()
                        .with_index(2)
                        .with_records(Some(negative_sequence)),
                    PartitionProduceData::default()
                        .with_index(3)
                        .with_records(Some(impossible_count)),
                    PartitionProduceData::default()
                        .with_index(4)
                        .with_records(Some(nonzero_base_offset)),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 53, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    let partitions = &response.responses[0].partition_responses;
    assert_eq!(partitions.len(), 5);
    for partition in partitions {
        assert_eq!(partition.error_code, crate::kafka_error::INVALID_RECORD);
        assert_eq!(partition.base_offset, -1);
        assert_eq!(
            broker
                .metadata
                .list_offset(
                    &PartitionKey::new("invalid-batch-headers", partition.index),
                    -1,
                )
                .await
                .unwrap(),
            0
        );
    }
    assert!(
        partitions[0]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("invalid record count"))
    );
    assert!(
        partitions[1]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("offset range"))
    );
    assert!(
        partitions[2]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("negative base sequence"))
    );
    assert!(
        partitions[3]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("record count 2147483647"))
    );
    assert!(
        partitions[4]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("base offset must be 0"))
    );

    let describe = DescribeProducersRequest::default().with_topics(vec![
        TopicRequest::default()
            .with_name(topic_name("invalid-batch-headers"))
            .with_partition_indexes(vec![0, 1, 2, 3, 4]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeProducers, 0, 54, &describe))
        .await
        .unwrap();
    let response: DescribeProducersResponse =
        decode_response(ApiKey::DescribeProducers, 0, response);
    assert!(
        response.topics[0]
            .partitions
            .iter()
            .all(|partition| partition.active_producers.is_empty())
    );
    assert!(broker.objects.list("").await.unwrap().is_empty());
}

#[tokio::test]
async fn valid_crc_malformed_record_bodies_are_rejected_without_mutation() {
    let broker = broker();
    broker
        .metadata
        .create_topic("invalid-batch-padding", 3)
        .await
        .unwrap();
    let mut config = broker
        .metadata
        .topic_config("invalid-batch-padding")
        .await
        .unwrap();
    config.compression_type = "zstd".to_owned();
    config.message_timestamp_type = "LogAppendTime".to_owned();
    broker
        .metadata
        .set_topic_config("invalid-batch-padding", config)
        .await
        .unwrap();

    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("invalid-batch-padding"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(with_record_batch_padding(sample_records(), true))),
                    PartitionProduceData::default()
                        .with_index(1)
                        .with_records(Some(with_record_batch_padding(sample_records(), false))),
                    PartitionProduceData::default()
                        .with_index(2)
                        .with_records(Some(with_impossible_record_header_count(sample_records()))),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 55, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    let partitions = &response.responses[0].partition_responses;
    assert_eq!(partitions.len(), 3);
    for partition in partitions {
        assert_eq!(partition.error_code, crate::kafka_error::INVALID_RECORD);
        assert_eq!(partition.base_offset, -1);
        assert_eq!(
            broker
                .metadata
                .list_offset(
                    &PartitionKey::new("invalid-batch-padding", partition.index),
                    -1,
                )
                .await
                .unwrap(),
            0
        );
    }
    assert!(
        partitions[0]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("record body has 1 trailing byte"))
    );
    assert!(partitions[1].error_message.as_ref().is_some_and(|message| {
        message
            .as_str()
            .contains("record batch has 1 trailing byte")
    }));
    assert!(
        partitions[2]
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("record header count 2147483647"))
    );
    assert!(broker.objects.list("").await.unwrap().is_empty());
}

#[tokio::test]
async fn crc_mismatch_returns_corrupt_message_without_mutation() {
    let broker = broker();
    broker
        .metadata
        .create_topic("corrupt-record-batch", 1)
        .await
        .unwrap();
    let init = InitProducerIdRequest::default()
        .with_transactional_id(None)
        .with_transaction_timeout_ms(60_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 56, &init))
        .await
        .unwrap();
    let producer: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    let records = with_corrupted_record_batch_crc(producer_records(
        producer.producer_id.0,
        producer.producer_epoch,
        0,
        false,
        b"corrupt-crc",
    ));
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("corrupt-record-batch"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(records)),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 57, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, crate::kafka_error::CORRUPT_MESSAGE);
    assert_eq!(partition.base_offset, -1);
    assert!(
        partition
            .error_message
            .as_ref()
            .is_some_and(|message| message.as_str().contains("Cyclic redundancy check failed"))
    );
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("corrupt-record-batch", 0), -1)
            .await
            .unwrap(),
        0
    );

    let describe = DescribeProducersRequest::default().with_topics(vec![
        TopicRequest::default()
            .with_name(topic_name("corrupt-record-batch"))
            .with_partition_indexes(vec![0]),
    ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeProducers, 0, 58, &describe))
        .await
        .unwrap();
    let response: DescribeProducersResponse =
        decode_response(ApiKey::DescribeProducers, 0, response);
    assert!(response.topics[0].partitions[0].active_producers.is_empty());
    assert!(broker.objects.list("").await.unwrap().is_empty());
}

#[tokio::test]
async fn negative_nullable_record_lengths_are_accepted_and_normalized() {
    let broker = broker();
    broker
        .metadata
        .create_topic("negative-null-lengths", 1)
        .await
        .unwrap();
    let produce = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("negative-null-lengths"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(records_with_noncanonical_null_lengths())),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 8, 59, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 8, response);
    let partition = &response.responses[0].partition_responses[0];
    assert_eq!(partition.error_code, NO_ERROR);
    assert_eq!(partition.base_offset, 0);

    let fetch = FetchRequest::default()
        .with_replica_id(BrokerId::from(-1))
        .with_max_wait_ms(0)
        .with_min_bytes(1)
        .with_max_bytes(1024)
        .with_topics(vec![
            FetchTopic::default()
                .with_topic(topic_name("negative-null-lengths"))
                .with_partitions(vec![
                    FetchPartition::default()
                        .with_partition(0)
                        .with_fetch_offset(0)
                        .with_partition_max_bytes(1024),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Fetch, 4, 60, &fetch))
        .await
        .unwrap();
    let response: FetchResponse = decode_response(ApiKey::Fetch, 4, response);
    let mut records = response.responses[0].partitions[0].records.clone().unwrap();
    let decoded = RecordBatchDecoder::decode_all(&mut records).unwrap();
    let record = &decoded[0].records[0];
    assert_eq!(record.key, None);
    assert_eq!(record.value, None);
    assert_eq!(record.headers, [("h".into(), None)]);
    assert_eq!(
        broker
            .metadata
            .list_offset(&PartitionKey::new("negative-null-lengths", 0), -1)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn transaction_protocol_controls_read_committed_visibility_and_offsets() {
    let broker = broker();
    broker
        .metadata
        .create_topic("transactions", 1)
        .await
        .unwrap();
    let transactional_id = TransactionalId::from(StrBytes::from_string("orders-tx".into()));
    let init = InitProducerIdRequest::default()
        .with_transactional_id(Some(transactional_id.clone()))
        .with_transaction_timeout_ms(60_000);
    let response = broker
        .handle_request(request_frame(ApiKey::InitProducerId, 4, 30, &init))
        .await
        .unwrap();
    let producer: InitProducerIdResponse = decode_response(ApiKey::InitProducerId, 4, response);
    assert_eq!(producer.error_code, NO_ERROR);

    let verify_partitions = AddPartitionsToTxnRequest::default().with_transactions(vec![
        AddPartitionsToTxnTransaction::default()
            .with_transactional_id(transactional_id.clone())
            .with_producer_id(producer.producer_id)
            .with_producer_epoch(producer.producer_epoch)
            .with_verify_only(true)
            .with_topics(vec![
                AddPartitionsToTxnTopic::default()
                    .with_name(topic_name("transactions"))
                    .with_partitions(vec![0]),
            ]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::AddPartitionsToTxn,
            4,
            31,
            &verify_partitions,
        ))
        .await
        .unwrap();
    let response: AddPartitionsToTxnResponse =
        decode_response(ApiKey::AddPartitionsToTxn, 4, response);
    assert_eq!(
        response.results_by_transaction[0].topic_results[0].results_by_partition[0]
            .partition_error_code,
        NO_ERROR
    );

    let add_partitions = AddPartitionsToTxnRequest::default()
        .with_v3_and_below_transactional_id(transactional_id.clone())
        .with_v3_and_below_producer_id(producer.producer_id)
        .with_v3_and_below_producer_epoch(producer.producer_epoch)
        .with_v3_and_below_topics(vec![
            AddPartitionsToTxnTopic::default()
                .with_name(topic_name("transactions"))
                .with_partitions(vec![0]),
        ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::AddPartitionsToTxn,
            3,
            32,
            &add_partitions,
        ))
        .await
        .unwrap();
    let response: AddPartitionsToTxnResponse =
        decode_response(ApiKey::AddPartitionsToTxn, 3, response);
    assert_eq!(
        response.results_by_topic_v3_and_below[0].results_by_partition[0].partition_error_code,
        NO_ERROR
    );

    let describe = DescribeTransactionsRequest::default().with_transactional_ids(vec![
        transactional_id.clone(),
        TransactionalId::from(StrBytes::from_string("missing-tx".into())),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::DescribeTransactions,
            0,
            32,
            &describe,
        ))
        .await
        .unwrap();
    let response: DescribeTransactionsResponse =
        decode_response(ApiKey::DescribeTransactions, 0, response);
    assert_eq!(response.transaction_states[0].error_code, NO_ERROR);
    assert_eq!(
        response.transaction_states[0].transaction_state.as_str(),
        "Ongoing"
    );
    assert_eq!(response.transaction_states[0].topics[0].partitions, [0]);
    assert_eq!(
        response.transaction_states[1].error_code,
        crate::kafka_error::TRANSACTIONAL_ID_NOT_FOUND
    );

    let list = ListTransactionsRequest::default()
        .with_state_filters(vec![StrBytes::from_string("Ongoing".into())])
        .with_duration_filter(0)
        .with_transactional_id_pattern(Some(StrBytes::from_string("orders-.*".into())));
    let response = broker
        .handle_request(request_frame(ApiKey::ListTransactions, 2, 32, &list))
        .await
        .unwrap();
    let response: ListTransactionsResponse = decode_response(ApiKey::ListTransactions, 2, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.transaction_states.len(), 1);
    assert_eq!(
        response.transaction_states[0].transactional_id,
        transactional_id
    );

    let produce = ProduceRequest::default()
        .with_transactional_id(Some(transactional_id.clone()))
        .with_acks(-1)
        .with_timeout_ms(1_000)
        .with_topic_data(vec![
            TopicProduceData::default()
                .with_name(topic_name("transactions"))
                .with_partition_data(vec![
                    PartitionProduceData::default()
                        .with_index(0)
                        .with_records(Some(producer_records(
                            producer.producer_id.0,
                            producer.producer_epoch,
                            0,
                            true,
                            b"pending",
                        ))),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::Produce, 3, 32, &produce))
        .await
        .unwrap();
    let response: ProduceResponse = decode_response(ApiKey::Produce, 3, response);
    assert_eq!(
        response.responses[0].partition_responses[0].error_code,
        NO_ERROR
    );

    let fetch = |isolation_level| {
        FetchRequest::default()
            .with_replica_id(BrokerId::from(-1))
            .with_max_wait_ms(0)
            .with_min_bytes(1)
            .with_max_bytes(1024 * 1024)
            .with_isolation_level(isolation_level)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(topic_name("transactions"))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_fetch_offset(0)
                            .with_partition_max_bytes(1024 * 1024),
                    ]),
            ])
    };
    let pending = broker
        .handle_request(request_frame(ApiKey::Fetch, 4, 33, &fetch(1)))
        .await
        .unwrap();
    let pending: FetchResponse = decode_response(ApiKey::Fetch, 4, pending);
    assert_eq!(pending.responses[0].partitions[0].last_stable_offset, 0);
    assert!(
        pending.responses[0].partitions[0]
            .records
            .as_ref()
            .is_none_or(Bytes::is_empty)
    );
    let uncommitted = broker
        .handle_request(request_frame(ApiKey::Fetch, 4, 34, &fetch(0)))
        .await
        .unwrap();
    let uncommitted: FetchResponse = decode_response(ApiKey::Fetch, 4, uncommitted);
    assert!(
        uncommitted.responses[0].partitions[0]
            .records
            .as_ref()
            .is_some_and(|records| !records.is_empty())
    );

    let group_id = kafka_protocol::messages::GroupId::from(StrBytes::from_string("workers".into()));
    let add_offsets = AddOffsetsToTxnRequest::default()
        .with_transactional_id(transactional_id.clone())
        .with_producer_id(producer.producer_id)
        .with_producer_epoch(producer.producer_epoch)
        .with_group_id(group_id.clone());
    let response = broker
        .handle_request(request_frame(ApiKey::AddOffsetsToTxn, 3, 35, &add_offsets))
        .await
        .unwrap();
    let response: AddOffsetsToTxnResponse = decode_response(ApiKey::AddOffsetsToTxn, 3, response);
    assert_eq!(response.error_code, NO_ERROR);

    let commit_offsets = TxnOffsetCommitRequest::default()
        .with_transactional_id(transactional_id.clone())
        .with_group_id(group_id.clone())
        .with_producer_id(producer.producer_id)
        .with_producer_epoch(producer.producer_epoch)
        .with_generation_id(-1)
        .with_topics(vec![
            TxnOffsetCommitRequestTopic::default()
                .with_name(topic_name("transactions"))
                .with_partitions(vec![
                    TxnOffsetCommitRequestPartition::default()
                        .with_partition_index(0)
                        .with_committed_offset(1),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::TxnOffsetCommit,
            3,
            36,
            &commit_offsets,
        ))
        .await
        .unwrap();
    let response: TxnOffsetCommitResponse = decode_response(ApiKey::TxnOffsetCommit, 3, response);
    assert_eq!(response.topics[0].partitions[0].error_code, NO_ERROR);

    let end = EndTxnRequest::default()
        .with_transactional_id(transactional_id)
        .with_producer_id(producer.producer_id)
        .with_producer_epoch(producer.producer_epoch)
        .with_committed(true);
    let response = broker
        .handle_request(request_frame(ApiKey::EndTxn, 5, 37, &end))
        .await
        .unwrap();
    let response: EndTxnResponse = decode_response(ApiKey::EndTxn, 5, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.producer_id, producer.producer_id);
    assert_eq!(response.producer_epoch, producer.producer_epoch + 1);

    let retry = broker
        .handle_request(request_frame(ApiKey::EndTxn, 5, 371, &end))
        .await
        .unwrap();
    let retry: EndTxnResponse = decode_response(ApiKey::EndTxn, 5, retry);
    assert_eq!(retry.error_code, NO_ERROR);
    assert_eq!(retry.producer_id, response.producer_id);
    assert_eq!(retry.producer_epoch, response.producer_epoch);

    let committed = broker
        .handle_request(request_frame(ApiKey::Fetch, 4, 38, &fetch(1)))
        .await
        .unwrap();
    let committed: FetchResponse = decode_response(ApiKey::Fetch, 4, committed);
    assert_eq!(committed.responses[0].partitions[0].last_stable_offset, 1);
    assert!(
        committed.responses[0].partitions[0]
            .records
            .as_ref()
            .is_some_and(|records| !records.is_empty())
    );

    let offsets = OffsetFetchRequest::default()
        .with_group_id(group_id)
        .with_topics(Some(vec![
            OffsetFetchRequestTopic::default()
                .with_name(topic_name("transactions"))
                .with_partition_indexes(vec![0]),
        ]));
    let response = broker
        .handle_request(request_frame(ApiKey::OffsetFetch, 1, 39, &offsets))
        .await
        .unwrap();
    let response: OffsetFetchResponse = decode_response(ApiKey::OffsetFetch, 1, response);
    assert_eq!(response.topics[0].partitions[0].committed_offset, 1);
}

#[tokio::test]
async fn topic_configs_round_trip_through_incremental_admin_apis() {
    let broker = broker();
    broker.metadata.create_topic("retained", 1).await.unwrap();
    let alter = IncrementalAlterConfigsRequest::default().with_resources(vec![
        AlterConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(StrBytes::from_string("retained".into()))
            .with_configs(vec![
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("retention.ms".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("2500".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("file.delete.delay.ms".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("700".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("flush.messages".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("7".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("flush.ms".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("11".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("cleanup.policy".into()))
                    .with_config_operation(2)
                    .with_value(Some(StrBytes::from_string("compact".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("compression.type".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("zstd".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("compression.gzip.level".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("9".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("compression.lz4.level".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("17".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("compression.zstd.level".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("22".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("min.compaction.lag.ms".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("100".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("max.compaction.lag.ms".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("1000".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("min.cleanable.dirty.ratio".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("0.75".into()))),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("min.insync.replicas".into()))
                    .with_config_operation(0)
                    .with_value(Some(StrBytes::from_string("2".into()))),
            ]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            50,
            &alter,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);

    let describe = DescribeConfigsRequest::default()
        .with_include_documentation(true)
        .with_resources(vec![
            DescribeConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from_string("retained".into()))
                .with_configuration_keys(Some(vec![
                    StrBytes::from_string("retention.ms".into()),
                    StrBytes::from_string("file.delete.delay.ms".into()),
                    StrBytes::from_string("flush.messages".into()),
                    StrBytes::from_string("flush.ms".into()),
                    StrBytes::from_string("cleanup.policy".into()),
                    StrBytes::from_string("compression.type".into()),
                    StrBytes::from_string("compression.gzip.level".into()),
                    StrBytes::from_string("compression.lz4.level".into()),
                    StrBytes::from_string("compression.zstd.level".into()),
                    StrBytes::from_string("min.compaction.lag.ms".into()),
                    StrBytes::from_string("max.compaction.lag.ms".into()),
                    StrBytes::from_string("min.cleanable.dirty.ratio".into()),
                    StrBytes::from_string("min.insync.replicas".into()),
                ])),
        ]);
    let response = broker
        .handle_request(request_frame(ApiKey::DescribeConfigs, 4, 51, &describe))
        .await
        .unwrap();
    let response: DescribeConfigsResponse = decode_response(ApiKey::DescribeConfigs, 4, response);
    assert_eq!(response.results[0].error_code, NO_ERROR);
    assert_eq!(response.results[0].configs.len(), 13);
    let values = response.results[0]
        .configs
        .iter()
        .map(|config| {
            (
                config.name.as_str(),
                config.value.as_ref().unwrap().as_str(),
            )
        })
        .collect::<HashMap<_, _>>();
    assert_eq!(values["retention.ms"], "2500");
    assert_eq!(values["file.delete.delay.ms"], "700");
    assert_eq!(values["flush.messages"], "7");
    assert_eq!(values["flush.ms"], "11");
    assert_eq!(values["cleanup.policy"], "delete,compact");
    assert_eq!(values["compression.type"], "zstd");
    assert_eq!(values["compression.gzip.level"], "9");
    assert_eq!(values["compression.lz4.level"], "17");
    assert_eq!(values["compression.zstd.level"], "22");
    assert_eq!(values["min.compaction.lag.ms"], "100");
    assert_eq!(values["max.compaction.lag.ms"], "1000");
    assert_eq!(values["min.cleanable.dirty.ratio"], "0.75");
    assert_eq!(values["min.insync.replicas"], "2");
    assert!(
        response.results[0]
            .configs
            .iter()
            .all(|config| config.documentation.is_some())
    );

    let validate_only = IncrementalAlterConfigsRequest::default()
        .with_validate_only(true)
        .with_resources(vec![
            AlterConfigsResource::default()
                .with_resource_type(2)
                .with_resource_name(StrBytes::from_string("retained".into()))
                .with_configs(vec![
                    AlterableConfig::default()
                        .with_name(StrBytes::from_string("retention.ms".into()))
                        .with_config_operation(0)
                        .with_value(Some(StrBytes::from_string("1".into()))),
                ]),
        ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            52,
            &validate_only,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    assert_eq!(
        broker
            .metadata
            .topic_config("retained")
            .await
            .unwrap()
            .retention_ms,
        2500
    );

    let delete_configs = IncrementalAlterConfigsRequest::default().with_resources(vec![
        AlterConfigsResource::default()
            .with_resource_type(2)
            .with_resource_name(StrBytes::from_string("retained".into()))
            .with_configs(vec![
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("compression.type".into()))
                    .with_config_operation(1),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("file.delete.delay.ms".into()))
                    .with_config_operation(1),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("flush.messages".into()))
                    .with_config_operation(1),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("flush.ms".into()))
                    .with_config_operation(1),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("compression.gzip.level".into()))
                    .with_config_operation(1),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("compression.lz4.level".into()))
                    .with_config_operation(1),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("compression.zstd.level".into()))
                    .with_config_operation(1),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("max.compaction.lag.ms".into()))
                    .with_config_operation(1),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("min.cleanable.dirty.ratio".into()))
                    .with_config_operation(1),
                AlterableConfig::default()
                    .with_name(StrBytes::from_string("min.insync.replicas".into()))
                    .with_config_operation(1),
            ]),
    ]);
    let response = broker
        .handle_request(request_frame(
            ApiKey::IncrementalAlterConfigs,
            1,
            53,
            &delete_configs,
        ))
        .await
        .unwrap();
    let response: IncrementalAlterConfigsResponse =
        decode_response(ApiKey::IncrementalAlterConfigs, 1, response);
    assert_eq!(response.responses[0].error_code, NO_ERROR);
    let stored = broker.metadata.topic_config("retained").await.unwrap();
    assert_eq!(stored.file_delete_delay_ms, 60_000);
    assert_eq!(stored.flush_messages, i64::MAX);
    assert_eq!(stored.flush_ms, i64::MAX);
    assert_eq!(stored.compression_type, "producer");
    assert_eq!(stored.compression_gzip_level, -1);
    assert_eq!(stored.compression_lz4_level, 9);
    assert_eq!(stored.compression_zstd_level, 3);
    assert_eq!(stored.max_compaction_lag_ms, i64::MAX);
    assert_eq!(stored.min_cleanable_dirty_ratio, 0.5);
    assert_eq!(stored.min_insync_replicas, 1);
}
