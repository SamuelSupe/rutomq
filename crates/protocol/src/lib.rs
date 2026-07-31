//! Small, dependency-light helpers around the generated Kafka protocol types.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, decode_request_header_from_buffer};
use thiserror::Error;

pub use kafka_protocol::{messages, protocol, records};

pub const KAFKA_BASELINE: &str = "4.0";
pub const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid Kafka frame length {0}")]
    InvalidFrameLength(i32),
    #[error("Kafka frame is incomplete")]
    IncompleteFrame,
    #[error("unknown Kafka API key {0}")]
    UnknownApiKey(i16),
    #[error("Kafka response frame length {0} exceeds the maximum")]
    ResponseFrameTooLarge(usize),
    #[error("protocol codec error: {0}")]
    Codec(#[from] anyhow::Error),
}

#[derive(Debug, Clone)]
pub struct RequestFrame {
    pub header: RequestHeader,
    pub api_key: ApiKey,
    pub version: i16,
    pub size: usize,
    pub body: Bytes,
}

pub fn decode_request(payload: Bytes) -> Result<RequestFrame, ProtocolError> {
    let size = payload.len();
    if payload.remaining() < 4 {
        return Err(ProtocolError::IncompleteFrame);
    }

    let api_key_raw = i16::from_be_bytes([payload[0], payload[1]]);
    let version = i16::from_be_bytes([payload[2], payload[3]]);
    let api_key =
        ApiKey::try_from(api_key_raw).map_err(|_| ProtocolError::UnknownApiKey(api_key_raw))?;
    let mut header_input = payload.clone();
    let header = decode_request_header_from_buffer(&mut header_input)?;

    Ok(RequestFrame {
        header,
        api_key,
        version,
        size,
        body: header_input,
    })
}

pub fn decode_body<T: Decodable>(mut body: Bytes, version: i16) -> Result<T, ProtocolError> {
    Ok(T::decode(&mut body, version)?)
}

/// Returns the generated Kafka 4.3 schema version used on the wire.
pub fn body_version(_api_key: ApiKey, wire_version: i16) -> i16 {
    wire_version
}

pub fn encode_response<T: Encodable>(
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    response: &T,
) -> Result<Bytes, ProtocolError> {
    let header_version = api_key.response_header_version(version);
    let response_version = body_version(api_key, version);
    let header = ResponseHeader::default().with_correlation_id(correlation_id);
    let frame_size = response_frame_size(api_key, version, response)?;
    let payload_size = frame_size - 4;
    let frame_size = i32::try_from(payload_size)
        .map_err(|_| ProtocolError::ResponseFrameTooLarge(payload_size))?;

    let mut frame = BytesMut::with_capacity(4 + payload_size);
    frame.put_i32(frame_size);
    header.encode(&mut frame, header_version)?;
    response.encode(&mut frame, response_version)?;
    Ok(frame.freeze())
}

pub fn response_frame_size<T: Encodable>(
    api_key: ApiKey,
    version: i16,
    response: &T,
) -> Result<usize, ProtocolError> {
    let header_version = api_key.response_header_version(version);
    let response_version = body_version(api_key, version);
    let payload_size = ResponseHeader::default()
        .compute_size(header_version)?
        .checked_add(response.compute_size(response_version)?)
        .ok_or(ProtocolError::ResponseFrameTooLarge(usize::MAX))?;
    if payload_size > MAX_FRAME_SIZE {
        return Err(ProtocolError::ResponseFrameTooLarge(payload_size));
    }
    payload_size
        .checked_add(4)
        .ok_or(ProtocolError::ResponseFrameTooLarge(usize::MAX))
}

pub fn api_versions() -> Vec<(i16, i16, i16)> {
    [
        (ApiKey::Produce as i16, 3, 13),
        (ApiKey::Fetch as i16, 4, 18),
        (ApiKey::ListOffsets as i16, 1, 11),
        (ApiKey::Metadata as i16, 0, 13),
        (ApiKey::OffsetCommit as i16, 2, 10),
        (ApiKey::OffsetFetch as i16, 1, 10),
        (ApiKey::FindCoordinator as i16, 0, 6),
        (ApiKey::JoinGroup as i16, 0, 9),
        (ApiKey::SyncGroup as i16, 0, 5),
        (ApiKey::Heartbeat as i16, 0, 4),
        (ApiKey::LeaveGroup as i16, 0, 5),
        (ApiKey::DescribeGroups as i16, 0, 6),
        (ApiKey::ListGroups as i16, 0, 5),
        (ApiKey::DeleteGroups as i16, 0, 2),
        (ApiKey::CreateTopics as i16, 2, 7),
        (ApiKey::DeleteTopics as i16, 1, 6),
        (ApiKey::DeleteRecords as i16, 0, 2),
        (ApiKey::CreatePartitions as i16, 0, 3),
        (ApiKey::DescribeTopicPartitions as i16, 0, 0),
        (ApiKey::DescribeConfigs as i16, 1, 4),
        (ApiKey::AlterConfigs as i16, 0, 2),
        (ApiKey::IncrementalAlterConfigs as i16, 0, 1),
        (ApiKey::AlterReplicaLogDirs as i16, 1, 2),
        (ApiKey::DescribeLogDirs as i16, 1, 5),
        (ApiKey::ElectLeaders as i16, 0, 2),
        (ApiKey::AlterPartitionReassignments as i16, 0, 1),
        (ApiKey::ListPartitionReassignments as i16, 0, 0),
        (ApiKey::DescribeUserScramCredentials as i16, 0, 0),
        (ApiKey::AlterUserScramCredentials as i16, 0, 0),
        (ApiKey::DescribeQuorum as i16, 0, 2),
        (ApiKey::UpdateFeatures as i16, 0, 2),
        (ApiKey::GetTelemetrySubscriptions as i16, 0, 0),
        (ApiKey::PushTelemetry as i16, 0, 0),
        (ApiKey::ListConfigResources as i16, 0, 1),
        (ApiKey::CreateDelegationToken as i16, 1, 3),
        (ApiKey::RenewDelegationToken as i16, 1, 2),
        (ApiKey::ExpireDelegationToken as i16, 1, 2),
        (ApiKey::DescribeDelegationToken as i16, 1, 3),
        (ApiKey::DescribeClientQuotas as i16, 0, 1),
        (ApiKey::AlterClientQuotas as i16, 0, 1),
        (ApiKey::InitProducerId as i16, 0, 6),
        (ApiKey::OffsetForLeaderEpoch as i16, 2, 4),
        (ApiKey::AddPartitionsToTxn as i16, 0, 5),
        (ApiKey::AddOffsetsToTxn as i16, 0, 4),
        (ApiKey::EndTxn as i16, 0, 5),
        (ApiKey::WriteTxnMarkers as i16, 1, 2),
        (ApiKey::TxnOffsetCommit as i16, 0, 5),
        (ApiKey::DescribeTransactions as i16, 0, 0),
        (ApiKey::ListTransactions as i16, 0, 2),
        (ApiKey::DescribeProducers as i16, 0, 0),
        (ApiKey::OffsetDelete as i16, 0, 0),
        (ApiKey::DescribeAcls as i16, 1, 3),
        (ApiKey::CreateAcls as i16, 1, 3),
        (ApiKey::DeleteAcls as i16, 1, 3),
        (ApiKey::ConsumerGroupHeartbeat as i16, 0, 1),
        (ApiKey::ConsumerGroupDescribe as i16, 0, 1),
        (ApiKey::StreamsGroupHeartbeat as i16, 0, 0),
        (ApiKey::StreamsGroupDescribe as i16, 0, 0),
        (ApiKey::ShareGroupHeartbeat as i16, 1, 1),
        (ApiKey::ShareGroupDescribe as i16, 1, 1),
        (ApiKey::ShareFetch as i16, 1, 2),
        (ApiKey::ShareAcknowledge as i16, 1, 2),
        (ApiKey::DescribeShareGroupOffsets as i16, 0, 1),
        (ApiKey::AlterShareGroupOffsets as i16, 0, 0),
        (ApiKey::DeleteShareGroupOffsets as i16, 0, 0),
        (ApiKey::InitializeShareGroupState as i16, 0, 0),
        (ApiKey::ReadShareGroupState as i16, 0, 0),
        (ApiKey::WriteShareGroupState as i16, 0, 1),
        (ApiKey::DeleteShareGroupState as i16, 0, 0),
        (ApiKey::ReadShareGroupStateSummary as i16, 0, 1),
        (ApiKey::DescribeCluster as i16, 0, 2),
        (ApiKey::SaslHandshake as i16, 0, 1),
        (ApiKey::SaslAuthenticate as i16, 0, 2),
        (ApiKey::ApiVersions as i16, 0, 4),
    ]
    .to_vec()
}

/// Returns the Kafka broker-listener ApiVersions ranges.
///
/// Kafka 4.3 advertises removed Produce versions 0-2 to work around
/// KAFKA-18659 in librdkafka. Dispatch must continue to use [`supports_version`]
/// so those versions are never decoded.
pub fn advertised_api_versions() -> Vec<(i16, i16, i16)> {
    let mut versions = api_versions();
    if let Some((_, min_version, _)) = versions
        .iter_mut()
        .find(|(api_key, _, _)| *api_key == ApiKey::Produce as i16)
    {
        *min_version = 0;
    }
    versions
}

pub fn supports_version(api_key: ApiKey, version: i16) -> bool {
    api_versions()
        .into_iter()
        .any(|(key, min, max)| key == api_key as i16 && version >= min && version <= max)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::describe_log_dirs_response::DescribeLogDirsResult;
    use kafka_protocol::messages::{
        ApiVersionsRequest, DescribeLogDirsResponse, InitProducerIdRequest, TransactionalId,
    };
    use kafka_protocol::protocol::buf::ByteBufMut;
    use kafka_protocol::protocol::{Encodable, StrBytes};
    use std::collections::BTreeMap;

    struct OversizedResponse;

    impl Encodable for OversizedResponse {
        fn encode<B: ByteBufMut>(&self, _buf: &mut B, _version: i16) -> anyhow::Result<()> {
            panic!("oversized response must be rejected before encoding")
        }

        fn compute_size(&self, _version: i16) -> anyhow::Result<usize> {
            Ok(MAX_FRAME_SIZE)
        }
    }

    #[test]
    fn round_trips_request_header_and_body() {
        let request = ApiVersionsRequest::default();
        let mut bytes = BytesMut::new();
        request.encode(&mut bytes, 0).unwrap();
        let mut payload = BytesMut::new();
        RequestHeader::default()
            .with_request_api_key(ApiKey::ApiVersions as i16)
            .with_request_api_version(0)
            .with_correlation_id(42)
            .encode(&mut payload, 1)
            .unwrap();
        payload.extend_from_slice(&bytes);
        let frame = decode_request(payload.freeze()).unwrap();
        assert_eq!(frame.api_key, ApiKey::ApiVersions);
        assert_eq!(frame.header.correlation_id, 42);
        let decoded: ApiVersionsRequest = decode_body(frame.body, 0).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn only_accepts_implemented_api_versions() {
        assert!(!supports_version(ApiKey::Produce, 0));
        assert!(!supports_version(ApiKey::Produce, 1));
        assert!(!supports_version(ApiKey::Produce, 2));
        assert!(supports_version(ApiKey::Produce, 13));
        assert!(supports_version(ApiKey::Fetch, 18));
        assert!(supports_version(ApiKey::ListOffsets, 11));
        assert!(!supports_version(ApiKey::ListOffsets, 12));
        assert_eq!(body_version(ApiKey::ListOffsets, 11), 11);
        assert_eq!(body_version(ApiKey::ListOffsets, 10), 10);
        assert!(supports_version(ApiKey::OffsetCommit, 10));
        assert!(supports_version(ApiKey::OffsetFetch, 10));
        assert!(supports_version(ApiKey::EndTxn, 5));
        assert!(supports_version(ApiKey::WriteTxnMarkers, 1));
        assert!(supports_version(ApiKey::WriteTxnMarkers, 2));
        assert!(!supports_version(ApiKey::WriteTxnMarkers, 0));
        assert!(!supports_version(ApiKey::WriteTxnMarkers, 3));
        assert!(supports_version(ApiKey::ListTransactions, 2));
        assert!(supports_version(ApiKey::DescribeConfigs, 4));
        assert!(supports_version(ApiKey::AlterConfigs, 2));
        assert!(supports_version(ApiKey::AlterReplicaLogDirs, 2));
        assert!(supports_version(ApiKey::DescribeLogDirs, 5));
        assert!(!supports_version(ApiKey::DescribeLogDirs, 6));
        assert!(supports_version(ApiKey::ElectLeaders, 2));
        assert!(supports_version(ApiKey::AlterPartitionReassignments, 1));
        assert!(!supports_version(ApiKey::AlterPartitionReassignments, 2));
        assert!(supports_version(ApiKey::ListPartitionReassignments, 0));
        assert!(supports_version(ApiKey::DescribeUserScramCredentials, 0));
        assert!(supports_version(ApiKey::AlterUserScramCredentials, 0));
        assert!(supports_version(ApiKey::DescribeQuorum, 2));
        assert!(!supports_version(ApiKey::DescribeQuorum, 3));
        assert!(supports_version(ApiKey::UpdateFeatures, 2));
        assert!(!supports_version(ApiKey::UpdateFeatures, 3));
        assert!(supports_version(ApiKey::ListConfigResources, 1));
        assert!(supports_version(ApiKey::GetTelemetrySubscriptions, 0));
        assert!(supports_version(ApiKey::PushTelemetry, 0));
        assert!(supports_version(ApiKey::CreateDelegationToken, 3));
        assert!(!supports_version(ApiKey::CreateDelegationToken, 0));
        assert!(supports_version(ApiKey::RenewDelegationToken, 2));
        assert!(supports_version(ApiKey::ExpireDelegationToken, 2));
        assert!(supports_version(ApiKey::DescribeDelegationToken, 3));
        assert!(supports_version(ApiKey::DescribeClientQuotas, 1));
        assert!(supports_version(ApiKey::AlterClientQuotas, 1));
        assert!(supports_version(ApiKey::InitProducerId, 6));
        assert!(!supports_version(ApiKey::InitProducerId, 7));
        assert!(supports_version(ApiKey::SaslHandshake, 0));
        assert!(supports_version(ApiKey::SaslHandshake, 1));
        assert!(supports_version(ApiKey::SaslAuthenticate, 2));
        assert!(!supports_version(ApiKey::EndTxn, 6));
        assert!(supports_version(ApiKey::DescribeAcls, 3));
        assert!(!supports_version(ApiKey::DescribeAcls, 0));
        assert!(supports_version(ApiKey::ConsumerGroupHeartbeat, 1));
        assert!(supports_version(ApiKey::ConsumerGroupDescribe, 1));
        assert!(supports_version(ApiKey::StreamsGroupHeartbeat, 0));
        assert!(supports_version(ApiKey::StreamsGroupDescribe, 0));
        assert!(!supports_version(ApiKey::StreamsGroupHeartbeat, 1));
        assert!(supports_version(ApiKey::ShareGroupHeartbeat, 1));
        assert!(supports_version(ApiKey::ShareGroupDescribe, 1));
        assert!(supports_version(ApiKey::ShareFetch, 1));
        assert!(supports_version(ApiKey::ShareFetch, 2));
        assert!(supports_version(ApiKey::ShareAcknowledge, 1));
        assert!(supports_version(ApiKey::ShareAcknowledge, 2));
        assert!(supports_version(ApiKey::DescribeShareGroupOffsets, 0));
        assert!(supports_version(ApiKey::DescribeShareGroupOffsets, 1));
        assert!(supports_version(ApiKey::AlterShareGroupOffsets, 0));
        assert!(supports_version(ApiKey::DeleteShareGroupOffsets, 0));
        assert!(supports_version(ApiKey::InitializeShareGroupState, 0));
        assert!(supports_version(ApiKey::ReadShareGroupState, 0));
        assert!(supports_version(ApiKey::WriteShareGroupState, 0));
        assert!(supports_version(ApiKey::WriteShareGroupState, 1));
        assert!(supports_version(ApiKey::DeleteShareGroupState, 0));
        assert!(supports_version(ApiKey::ReadShareGroupStateSummary, 0));
        assert!(supports_version(ApiKey::ReadShareGroupStateSummary, 1));
        assert!(!supports_version(ApiKey::ShareFetch, 0));
        assert!(!supports_version(ApiKey::DescribeShareGroupOffsets, 2));
        assert!(!supports_version(ApiKey::ShareAcknowledge, 3));
        assert!(!supports_version(ApiKey::InitializeShareGroupState, 1));
        assert!(!supports_version(ApiKey::ReadShareGroupState, 1));
        assert!(!supports_version(ApiKey::WriteShareGroupState, 2));
        assert!(!supports_version(ApiKey::DeleteShareGroupState, 1));
        assert!(!supports_version(ApiKey::ReadShareGroupStateSummary, 2));
        assert!(supports_version(ApiKey::DescribeGroups, 6));
        assert!(supports_version(ApiKey::ListGroups, 5));
        assert!(supports_version(ApiKey::DeleteGroups, 2));
        assert!(supports_version(ApiKey::DescribeCluster, 2));
        assert!(supports_version(ApiKey::CreatePartitions, 3));
        assert!(supports_version(ApiKey::DescribeTopicPartitions, 0));
        assert!(supports_version(ApiKey::DeleteRecords, 2));
        assert!(supports_version(ApiKey::OffsetDelete, 0));
        assert!(supports_version(ApiKey::OffsetForLeaderEpoch, 4));
        assert!(supports_version(ApiKey::DescribeProducers, 0));
    }

    #[test]
    fn api_versions_advertises_the_kafka_librdkafka_produce_workaround() {
        let implemented = api_versions()
            .into_iter()
            .map(|(key, min, max)| (key, (min, max)))
            .collect::<BTreeMap<_, _>>();
        let advertised = advertised_api_versions()
            .into_iter()
            .map(|(key, min, max)| (key, (min, max)))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(implemented[&(ApiKey::Produce as i16)], (3, 13));
        assert_eq!(advertised[&(ApiKey::Produce as i16)], (0, 13));
        assert!(implemented.iter().all(|(key, range)| {
            *key == ApiKey::Produce as i16 || advertised.get(key) == Some(range)
        }));
    }

    #[test]
    fn rejects_oversized_response_before_encoding() {
        let error = encode_response(ApiKey::ApiVersions, 0, 42, &OversizedResponse).unwrap_err();
        assert!(matches!(error, ProtocolError::ResponseFrameTooLarge(_)));
    }

    #[test]
    fn generated_api_gaps_are_explicit() {
        let advertised = api_versions()
            .into_iter()
            .map(|(key, min, max)| (key, (min, max)))
            .collect::<BTreeMap<_, _>>();
        let generated = (0i16..=92)
            .filter_map(|key| ApiKey::try_from(key).ok().map(|api| (key, api)))
            .collect::<Vec<_>>();
        let omitted = generated
            .iter()
            .filter_map(|(key, _)| (!advertised.contains_key(key)).then_some(*key))
            .collect::<Vec<_>>();
        assert_eq!(
            omitted,
            vec![52, 53, 54, 56, 58, 59, 62, 63, 64, 67, 70, 73, 80, 81, 82]
        );

        for (key, api) in generated {
            let Some((advertised_min, advertised_max)) = advertised.get(&key).copied() else {
                continue;
            };
            let generated = api.valid_versions();
            assert_eq!(
                (advertised_min, advertised_max),
                (generated.min, generated.max),
                "advertised range differs for {api:?}"
            );
        }
    }

    #[test]
    fn init_producer_id_v6_round_trips_two_phase_fields() {
        let request = InitProducerIdRequest::default()
            .with_transactional_id(Some(TransactionalId::from(StrBytes::from_string(
                "two-phase".to_owned(),
            ))))
            .with_transaction_timeout_ms(1)
            .with_enable_2_pc(true)
            .with_keep_prepared_txn(true);
        let mut encoded = BytesMut::new();
        request.encode(&mut encoded, 6).unwrap();
        let decoded: InitProducerIdRequest = decode_body(encoded.freeze(), 6).unwrap();
        assert!(decoded.enable_2_pc);
        assert!(decoded.keep_prepared_txn);

        let mut unsupported = BytesMut::new();
        assert!(request.encode(&mut unsupported, 5).is_err());
    }

    #[test]
    fn describe_log_dirs_v5_round_trips_cordoned_state() {
        let response = DescribeLogDirsResponse::default().with_results(vec![
            DescribeLogDirsResult::default().with_is_cordoned(true),
        ]);
        let mut encoded = BytesMut::new();
        response.encode(&mut encoded, 5).unwrap();
        let decoded: DescribeLogDirsResponse = decode_body(encoded.freeze(), 5).unwrap();
        assert!(decoded.results[0].is_cordoned);

        let mut unsupported = BytesMut::new();
        assert!(response.encode(&mut unsupported, 6).is_err());
    }
}
