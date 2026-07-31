use self::authorization::{AuthorizationContext, authorization_failure};
use self::client_telemetry_manager::ClientTelemetryManager;
use self::fetch_session::FetchSessionManager;
use crate::assignment_executor::AssignmentExecutor;
use crate::batcher::{PendingObjects, ProduceBatcher};
use crate::client_quota_manager::ClientQuotaManager;
use crate::compaction;
use crate::config::AgentConfig;
use crate::consumer_offset_maintenance;
use crate::delegation_token_maintenance;
use crate::failure_injection::FailureInjection;
use crate::fetch_cache::FetchCache;
use crate::gc;
use crate::health::Metrics;
#[cfg(test)]
use crate::kafka_error::MEMBER_ID_REQUIRED;
use crate::kafka_error::{
    CLUSTER_AUTHORIZATION_FAILED, FENCED_INSTANCE_ID, GROUP_AUTHORIZATION_FAILED, INVALID_REQUEST,
    NO_ERROR, TOPIC_AUTHORIZATION_FAILED, TRANSACTIONAL_ID_AUTHORIZATION_FAILED, UNKNOWN_MEMBER_ID,
    UNKNOWN_SERVER_ERROR, UNKNOWN_TOPIC_OR_PARTITION, UNSUPPORTED_VERSION, control_error_code,
};
use crate::observability;
use crate::observed_store::ObservedObjectStore;
use crate::producer_state_maintenance;
use crate::retention;
use crate::sasl::{AuthenticationStatus, SaslAuthenticator, SaslConnection};
use crate::tls;
use crate::transaction_maintenance;
use crate::transactional_id_maintenance;
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use bytes::Bytes;
#[cfg(test)]
use bytes::BytesMut;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::leave_group_response::MemberResponse;
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, AlterClientQuotasRequest, AlterConfigsRequest,
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse,
    AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse, AlterShareGroupOffsetsRequest,
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse, ApiKey,
    ApiVersionsRequest, BrokerId, ConsumerGroupDescribeRequest, ConsumerGroupHeartbeatRequest,
    CreateAclsRequest, CreateDelegationTokenRequest, CreatePartitionsRequest, CreateTopicsRequest,
    CreateTopicsResponse, DeleteAclsRequest, DeleteGroupsRequest, DeleteRecordsRequest,
    DeleteShareGroupOffsetsRequest, DeleteShareGroupStateRequest, DeleteTopicsRequest,
    DescribeAclsRequest, DescribeClientQuotasRequest, DescribeClusterRequest,
    DescribeConfigsRequest, DescribeDelegationTokenRequest, DescribeGroupsRequest,
    DescribeLogDirsRequest, DescribeLogDirsResponse, DescribeProducersRequest,
    DescribeQuorumRequest, DescribeShareGroupOffsetsRequest, DescribeTopicPartitionsRequest,
    DescribeTransactionsRequest, DescribeTransactionsResponse, DescribeUserScramCredentialsRequest,
    DescribeUserScramCredentialsResponse, ElectLeadersRequest, ElectLeadersResponse, EndTxnRequest,
    EndTxnResponse, ExpireDelegationTokenRequest, FetchRequest, FetchResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, GetTelemetrySubscriptionsRequest,
    HeartbeatRequest, HeartbeatResponse, IncrementalAlterConfigsRequest, InitProducerIdRequest,
    InitProducerIdResponse, InitializeShareGroupStateRequest, JoinGroupRequest, LeaveGroupRequest,
    LeaveGroupResponse, ListConfigResourcesRequest, ListGroupsRequest, ListOffsetsRequest,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse, ListTransactionsRequest,
    ListTransactionsResponse, MetadataRequest, OffsetCommitRequest, OffsetDeleteRequest,
    OffsetFetchRequest, OffsetForLeaderEpochRequest, ProduceRequest, PushTelemetryRequest,
    ReadShareGroupStateRequest, ReadShareGroupStateSummaryRequest, RenewDelegationTokenRequest,
    SaslAuthenticateRequest, SaslHandshakeRequest, ShareAcknowledgeRequest, ShareFetchRequest,
    ShareGroupDescribeRequest, ShareGroupHeartbeatRequest, StreamsGroupDescribeRequest,
    StreamsGroupHeartbeatRequest, SyncGroupRequest, TxnOffsetCommitRequest,
    TxnOffsetCommitResponse, UpdateFeaturesRequest, WriteShareGroupStateRequest,
    WriteTxnMarkersRequest, WriteTxnMarkersResponse,
};
#[cfg(test)]
use kafka_protocol::messages::{OffsetCommitResponse, OffsetFetchResponse};
use kafka_protocol::protocol::{Encodable, StrBytes};
#[cfg(test)]
use rutomq_control::PartitionKey;
use rutomq_control::{
    AclOperation, AclResourceType, CONNECTION_CREATION_RATE, CONSUMER_BYTE_RATE,
    CONTROLLER_MUTATION_RATE, ControlError, GroupMemberIdentity, LeaveGroupMemberError,
    MetadataStore, PRODUCER_BYTE_RATE, REQUEST_PERCENTAGE, TopicConfig, TopicInfo,
    TransactionFilter,
};
use rutomq_protocol::{
    MAX_FRAME_SIZE, ProtocolError, RequestFrame, body_version, decode_body, decode_request,
    encode_response as encode_protocol_response, response_frame_size, supports_version,
};
use rutomq_storage::ObjectStore;
#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant, timeout, timeout_at};
use tracing::{debug, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct Broker {
    metadata: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
    config: AgentConfig,
    metrics: Arc<Metrics>,
    assignment_executor: AssignmentExecutor,
    batcher: ProduceBatcher,
    sasl: SaslAuthenticator,
    quotas: ClientQuotaManager,
    telemetry: ClientTelemetryManager,
    fetch_sessions: FetchSessionManager,
    fetch_cache: FetchCache,
    connection_slots: Arc<Semaphore>,
    request_bytes: Arc<Semaphore>,
    response_bytes: Arc<Semaphore>,
    failure_injection: FailureInjection,
}

struct BudgetedResponse {
    bytes: Bytes,
}

struct BudgetedBytes {
    bytes: Bytes,
    _budget: OwnedSemaphorePermit,
}

impl AsRef<[u8]> for BudgetedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl BudgetedResponse {
    fn empty() -> Self {
        Self {
            bytes: Bytes::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    fn into_bytes(self) -> Bytes {
        self.bytes
    }
}

#[derive(Default)]
struct ClientInformation {
    software_name: Option<String>,
    software_version: Option<String>,
}

impl Broker {
    pub fn new(
        metadata: Arc<dyn MetadataStore>,
        objects: Arc<dyn ObjectStore>,
        config: AgentConfig,
        metrics: Arc<Metrics>,
    ) -> Self {
        let objects: Arc<dyn ObjectStore> =
            Arc::new(ObservedObjectStore::new(objects, metrics.clone()));
        let pending: PendingObjects = Arc::new(Mutex::new(HashSet::new()));
        let batcher = ProduceBatcher::new(
            metadata.clone(),
            objects.clone(),
            config.clone(),
            metrics.clone(),
            pending.clone(),
        );
        let assignment_executor = AssignmentExecutor::new(
            metadata.clone(),
            config.group_coordinator_background_threads,
            metrics.clone(),
        );
        let sasl = SaslAuthenticator::new(&config.security, metadata.clone());
        let quotas = ClientQuotaManager::new(metadata.clone());
        let telemetry = ClientTelemetryManager::new(
            metadata.clone(),
            metrics.clone(),
            config.telemetry_max_bytes,
        );
        let fetch_cache = FetchCache::new(config.fetch_cache_bytes);
        let connection_slots = Arc::new(Semaphore::new(config.max_connections));
        let request_bytes = Arc::new(Semaphore::new(config.max_inflight_request_bytes));
        let response_bytes = Arc::new(Semaphore::new(config.max_inflight_response_bytes));
        gc::spawn(
            metadata.clone(),
            objects.clone(),
            config.cluster_id.clone(),
            pending.clone(),
            config.orphan_gc_interval,
            config.orphan_gc_grace,
            metrics.clone(),
        );
        transaction_maintenance::spawn(
            metadata.clone(),
            metrics.clone(),
            config.transaction_abort_timed_out_cleanup_interval,
        );
        producer_state_maintenance::spawn(
            metadata.clone(),
            config.producer_id_expiration_ms,
            config.producer_id_expiration_check_interval,
        );
        consumer_offset_maintenance::spawn(
            metadata.clone(),
            config.offsets_retention_minutes,
            config.offsets_retention_check_interval,
            metrics.clone(),
        );
        transactional_id_maintenance::spawn(
            metadata.clone(),
            config.transactional_id_expiration_ms,
            config.transactional_id_expiration_check_interval,
            metrics.clone(),
        );
        if config.security.delegation_token_secret.is_some() {
            delegation_token_maintenance::spawn(metadata.clone());
        }
        retention::spawn(
            metadata.clone(),
            objects.clone(),
            config.retention_interval,
            config.object_delete_grace,
            metrics.clone(),
        );
        compaction::spawn(
            metadata.clone(),
            objects.clone(),
            config.clone(),
            pending,
            metrics.clone(),
        );
        observability::spawn(
            metadata.clone(),
            metrics.clone(),
            config.observability_interval,
            config.observability_max_groups,
            config.consumer_lag_max_series,
            config.partition_retention_max_series,
        );
        Self {
            metadata,
            objects,
            config,
            metrics,
            assignment_executor,
            batcher,
            sasl,
            quotas,
            telemetry,
            fetch_sessions: FetchSessionManager::default(),
            fetch_cache,
            connection_slots,
            request_bytes,
            response_bytes,
            failure_injection: FailureInjection::from_env(),
        }
    }

    fn reserve_response_bytes(
        &self,
        size: usize,
    ) -> std::result::Result<OwnedSemaphorePermit, ProtocolError> {
        let size = u32::try_from(size).map_err(|_| {
            ProtocolError::Codec(anyhow!("Kafka response is too large for the memory budget"))
        })?;
        self.response_bytes
            .clone()
            .try_acquire_many_owned(size)
            .map_err(|_| ProtocolError::Codec(anyhow!("Kafka response memory budget exhausted")))
    }

    fn encode_response<T: Encodable>(
        &self,
        api_key: ApiKey,
        version: i16,
        correlation_id: i32,
        response: &T,
    ) -> std::result::Result<BudgetedResponse, ProtocolError> {
        let frame_size = response_frame_size(api_key, version, response)?;
        let budget = self.reserve_response_bytes(frame_size)?;
        let bytes = encode_protocol_response(api_key, version, correlation_id, response)?;
        Ok(BudgetedResponse {
            bytes: Bytes::from_owner(BudgetedBytes {
                bytes,
                _budget: budget,
            }),
        })
    }

    pub async fn serve(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let tls_acceptor = tls::acceptor(&self.config.security)?;
        let listener = TcpListener::bind(self.config.kafka_addr)
            .await
            .with_context(|| format!("bind Kafka listener {}", self.config.kafka_addr))?;
        info!(
            address = ?listener.local_addr()?,
            tls = tls_acceptor.is_some(),
            sasl = self.sasl.enabled(),
            "Kafka listener started"
        );
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    let (stream, peer) = accepted?;
                    let Ok(connection_slot) =
                        self.connection_slots.clone().try_acquire_owned()
                    else {
                        debug!(?peer, "Kafka connection limit reached");
                        continue;
                    };
                    let broker = self.clone();
                    let tls_acceptor = tls_acceptor.clone();
                    let connection_shutdown = shutdown.clone();
                    broker.metrics.active_connections.inc();
                    connections.spawn(async move {
                        let _connection_slot = connection_slot;
                        match broker
                            .quotas
                            .reserve_ip(CONNECTION_CREATION_RATE, &peer.ip().to_string(), 1.0)
                            .await
                        {
                            Ok(reservation) if !reservation.delay.is_zero() => {
                                broker.metrics.record_quota_throttle(reservation.delay);
                                tokio::time::sleep(reservation.delay).await;
                            }
                            Ok(_) => {}
                            Err(error) => {
                                warn!(%error, "failed to load connection creation quota");
                            }
                        }
                        let result = if let Some(acceptor) = tls_acceptor {
                            match timeout(Duration::from_secs(10), acceptor.accept(stream)).await {
                                Ok(Ok(stream)) => {
                                    broker
                                        .serve_connection(stream, peer, connection_shutdown)
                                        .await
                                }
                                Ok(Err(error)) => Err(error.into()),
                                Err(_) => Err(anyhow!("TLS handshake timed out")),
                            }
                        } else {
                            broker
                                .serve_connection(stream, peer, connection_shutdown)
                                .await
                        };
                        if let Err(error) = result {
                            debug!(?peer, %error, "Kafka connection closed");
                        }
                        broker.metrics.active_connections.dec();
                    });
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = completed {
                        warn!(%error, "Kafka connection task failed");
                    }
                }
            }
        }

        self.metrics.set_ready(false);
        self.batcher.stop_accepting();
        drop(listener);
        let deadline = Instant::now() + self.config.shutdown_grace;
        match timeout_at(deadline, self.batcher.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(%error, "Produce batcher shutdown failed"),
            Err(_) => warn!("Produce batcher exceeded shutdown grace period"),
        }
        while !connections.is_empty() {
            match timeout_at(deadline, connections.join_next()).await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(error))) => warn!(%error, "Kafka connection task failed"),
                Ok(None) => break,
                Err(_) => {
                    warn!(
                        connections = connections.len(),
                        "aborting Kafka connections after shutdown grace period"
                    );
                    connections.abort_all();
                    while connections.join_next().await.is_some() {}
                    break;
                }
            }
        }
        info!("Kafka listener stopped");
        Ok(())
    }

    async fn serve_connection<S>(
        &self,
        mut stream: S,
        peer: SocketAddr,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut sasl = self.sasl.connection();
        let mut client_information = ClientInformation::default();
        loop {
            let frame_size = match tokio::select! {
                result = timeout(self.config.connection_io_timeout, stream.read_i32()) => {
                    result.map_err(|_| anyhow!("Kafka frame header read timed out"))?
                }
                _ = shutdown.changed() => return Ok(()),
            } {
                Ok(size) => size,
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            if frame_size <= 0 || frame_size as usize > self.config.max_frame_size {
                return Err(anyhow!("invalid Kafka frame size {frame_size}"));
            }
            let request_bytes = self
                .request_bytes
                .clone()
                .try_acquire_many_owned(frame_size as u32)
                .map_err(|_| anyhow!("Kafka request memory budget exhausted"))?;
            let mut payload = vec![0u8; frame_size as usize];
            tokio::select! {
                result = timeout(
                    self.config.connection_io_timeout,
                    stream.read_exact(&mut payload),
                ) => {
                    result
                        .map_err(|_| anyhow!("Kafka frame body read timed out"))??;
                }
                _ = shutdown.changed() => return Ok(()),
            }
            let payload = Bytes::from(payload);
            let response = if sasl.expects_opaque_token() {
                self.handle_opaque_sasl_token(payload, &mut sasl).await?
            } else {
                self.handle_connection_request(payload, &mut sasl, peer, &mut client_information)
                    .await?
            };
            drop(request_bytes);
            if !response.is_empty() {
                tokio::select! {
                    result = timeout(
                        self.config.connection_io_timeout,
                        stream.write_all(response.as_bytes()),
                    ) => {
                        result
                            .map_err(|_| anyhow!("Kafka response write timed out"))??;
                    }
                    _ = shutdown.changed() => return Ok(()),
                }
            }
            if sasl.is_failed() {
                return Ok(());
            }
            if *shutdown.borrow() {
                return Ok(());
            }
        }
    }

    async fn handle_opaque_sasl_token(
        &self,
        payload: Bytes,
        sasl: &mut SaslConnection,
    ) -> Result<BudgetedResponse> {
        let result = sasl.authenticate_opaque(payload).await;
        match result.status {
            AuthenticationStatus::Complete => {
                self.metrics.sasl_authentications.inc();
                debug!(
                    principal = sasl.principal(),
                    "legacy SASL authentication completed"
                );
            }
            AuthenticationStatus::Failed => {
                self.metrics.sasl_authentication_failures.inc();
                return Err(anyhow!("legacy SASL authentication failed"));
            }
            AuthenticationStatus::Continue => {}
        }
        let auth_bytes = result.response.auth_bytes;
        if auth_bytes.len() > MAX_FRAME_SIZE {
            return Err(anyhow!("legacy SASL response is too large"));
        }
        let size = i32::try_from(auth_bytes.len())
            .map_err(|_| anyhow!("legacy SASL response is too large"))?;
        let frame_size = auth_bytes
            .len()
            .checked_add(4)
            .ok_or_else(|| anyhow!("legacy SASL response is too large"))?;
        let budget = self.reserve_response_bytes(frame_size)?;
        let mut response = Vec::with_capacity(frame_size);
        response.extend_from_slice(&size.to_be_bytes());
        response.extend_from_slice(&auth_bytes);
        Ok(BudgetedResponse {
            bytes: Bytes::from_owner(BudgetedBytes {
                bytes: Bytes::from(response),
                _budget: budget,
            }),
        })
    }

    pub async fn handle_request(&self, payload: Bytes) -> Result<Bytes> {
        let response = match connection_request(payload)? {
            ConnectionRequest::Supported(request) => {
                self.dispatch_request(
                    request,
                    &AuthorizationContext::anonymous(std::net::Ipv4Addr::LOCALHOST.into()),
                )
                .await?
            }
            ConnectionRequest::UnsupportedApiVersions {
                correlation_id,
                requested_version,
            } => {
                self.unsupported_api_versions_response(correlation_id, requested_version)
                    .await?
            }
        };
        Ok(response.into_bytes())
    }

    async fn handle_connection_request(
        &self,
        payload: Bytes,
        sasl: &mut SaslConnection,
        peer: SocketAddr,
        client_information: &mut ClientInformation,
    ) -> Result<BudgetedResponse> {
        let request = match connection_request(payload)? {
            ConnectionRequest::Supported(request) => request,
            ConnectionRequest::UnsupportedApiVersions {
                correlation_id,
                requested_version,
            } => {
                return self
                    .unsupported_api_versions_response(correlation_id, requested_version)
                    .await;
            }
        };
        let correlation_id = request.header.correlation_id;
        match request.api_key {
            ApiKey::SaslHandshake => {
                let version = request.version;
                let request: SaslHandshakeRequest = decode_body(request.body, version)?;
                let reauthenticating = sasl.has_authenticated_session();
                let response = sasl.handshake(request, version);
                if reauthenticating && sasl.is_failed() {
                    self.metrics.sasl_reauthentication_failures.inc();
                }
                Ok(self.encode_response(
                    ApiKey::SaslHandshake,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::SaslAuthenticate => {
                let version = request.version;
                let request: SaslAuthenticateRequest = decode_body(request.body, version)?;
                let reauthenticating = sasl.is_reauthenticating();
                let result = sasl.authenticate(request).await;
                match result.status {
                    AuthenticationStatus::Complete if reauthenticating => {
                        self.metrics.sasl_reauthentications.inc();
                        debug!(
                            principal = sasl.principal(),
                            "SASL re-authentication completed"
                        );
                    }
                    AuthenticationStatus::Complete => {
                        self.metrics.sasl_authentications.inc();
                        debug!(
                            principal = sasl.principal(),
                            "SASL authentication completed"
                        );
                    }
                    AuthenticationStatus::Failed if reauthenticating => {
                        self.metrics.sasl_reauthentication_failures.inc();
                    }
                    AuthenticationStatus::Failed => {
                        self.metrics.sasl_authentication_failures.inc();
                    }
                    AuthenticationStatus::Continue => {}
                }
                Ok(self.encode_response(
                    ApiKey::SaslAuthenticate,
                    version,
                    correlation_id,
                    &result.response,
                )?)
            }
            ApiKey::ApiVersions => {
                if request.version >= 3 {
                    let api_versions: ApiVersionsRequest =
                        decode_body(request.body.clone(), request.version)?;
                    client_information.software_name =
                        nonempty(api_versions.client_software_name.as_str());
                    client_information.software_version =
                        nonempty(api_versions.client_software_version.as_str());
                }
                let context = authorization_context(sasl, peer, client_information);
                self.dispatch_request(request, &context).await
            }
            _ if !sasl.is_authenticated() => Err(anyhow!(
                "SASL authentication is required before Kafka API {:?}",
                request.api_key
            )),
            _ => {
                let context = authorization_context(sasl, peer, client_information);
                self.dispatch_request(request, &context).await
            }
        }
    }

    async fn unsupported_api_versions_response(
        &self,
        correlation_id: i32,
        requested_version: i16,
    ) -> Result<BudgetedResponse> {
        self.metrics
            .record_kafka_request(ApiKey::ApiVersions, requested_version);
        let response = self
            .handle_api_versions(0)
            .await
            .with_error_code(UNSUPPORTED_VERSION);
        Ok(self.encode_response(ApiKey::ApiVersions, 0, correlation_id, &response)?)
    }

    async fn dispatch_request(
        &self,
        request: RequestFrame,
        context: &AuthorizationContext,
    ) -> Result<BudgetedResponse> {
        let api_key = request.api_key;
        self.metrics.record_kafka_request(api_key, request.version);
        let client_id = request
            .header
            .client_id
            .as_ref()
            .map(|value| value.as_str().to_owned())
            .unwrap_or_default();
        let started = tokio::time::Instant::now();
        let response = self
            .dispatch_request_inner(request, context, client_id.clone())
            .await?;
        if !matches!(
            api_key,
            ApiKey::Produce
                | ApiKey::Fetch
                | ApiKey::ApiVersions
                | ApiKey::SaslHandshake
                | ApiKey::SaslAuthenticate
        ) {
            let request_quota = self
                .quotas
                .reserve_user(
                    REQUEST_PERCENTAGE,
                    &context.principal,
                    &client_id,
                    started.elapsed().as_secs_f64() * 100.0,
                )
                .await?;
            let mutation_quota = if is_controller_mutation(api_key) {
                self.quotas
                    .reserve_user(
                        CONTROLLER_MUTATION_RATE,
                        &context.principal,
                        &client_id,
                        1.0,
                    )
                    .await?
            } else {
                crate::client_quota_manager::QuotaReservation::unlimited()
            };
            let delay = request_quota.delay.max(mutation_quota.delay);
            if !delay.is_zero() {
                self.metrics.record_quota_throttle(delay);
                tokio::time::sleep(delay).await;
            }
        }
        Ok(response)
    }

    async fn dispatch_request_inner(
        &self,
        request: RequestFrame,
        context: &AuthorizationContext,
        client_id: String,
    ) -> Result<BudgetedResponse> {
        let correlation_id = request.header.correlation_id;
        match request.api_key {
            ApiKey::SaslHandshake | ApiKey::SaslAuthenticate => {
                Err(anyhow!("SASL APIs require connection state"))
            }
            ApiKey::ApiVersions => {
                let _: ApiVersionsRequest = decode_body(request.body, request.version)?;
                let response = self.handle_api_versions(request.version).await;
                Ok(self.encode_response(
                    request.api_key,
                    request.version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::GetTelemetrySubscriptions => {
                let version = request.version;
                let typed_request: GetTelemetrySubscriptionsRequest =
                    decode_body(request.body, version)?;
                let response = self.telemetry.get(typed_request, &client_id, context).await;
                Ok(self.encode_response(
                    ApiKey::GetTelemetrySubscriptions,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::PushTelemetry => {
                let version = request.version;
                let typed_request: PushTelemetryRequest = decode_body(request.body, version)?;
                let response = self
                    .telemetry
                    .push(typed_request, &client_id, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::PushTelemetry,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::UpdateFeatures => {
                let version = request.version;
                let typed_request: UpdateFeaturesRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_update_features(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::UpdateFeatures,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DescribeAcls => {
                let version = request.version;
                let typed_request: DescribeAclsRequest = decode_body(request.body, version)?;
                let response = self.handle_describe_acls(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::DescribeAcls,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::CreateAcls => {
                let version = request.version;
                let typed_request: CreateAclsRequest = decode_body(request.body, version)?;
                let response = self.handle_create_acls(typed_request, context).await;
                Ok(self.encode_response(ApiKey::CreateAcls, version, correlation_id, &response)?)
            }
            ApiKey::DeleteAcls => {
                let version = request.version;
                let typed_request: DeleteAclsRequest = decode_body(request.body, version)?;
                let response = self.handle_delete_acls(typed_request, context).await;
                Ok(self.encode_response(ApiKey::DeleteAcls, version, correlation_id, &response)?)
            }
            ApiKey::DescribeUserScramCredentials => {
                let version = request.version;
                let typed_request: DescribeUserScramCredentialsRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_describe_user_scram_credentials(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DescribeUserScramCredentials,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::AlterUserScramCredentials => {
                let version = request.version;
                let typed_request: AlterUserScramCredentialsRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_alter_user_scram_credentials(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::AlterUserScramCredentials,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::CreateDelegationToken => {
                let version = request.version;
                let typed_request: CreateDelegationTokenRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_create_delegation_token(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::CreateDelegationToken,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::RenewDelegationToken => {
                let version = request.version;
                let typed_request: RenewDelegationTokenRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_renew_delegation_token(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::RenewDelegationToken,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ExpireDelegationToken => {
                let version = request.version;
                let typed_request: ExpireDelegationTokenRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_expire_delegation_token(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::ExpireDelegationToken,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DescribeDelegationToken => {
                let version = request.version;
                let typed_request: DescribeDelegationTokenRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_describe_delegation_token(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DescribeDelegationToken,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DescribeClientQuotas => {
                let version = request.version;
                let typed_request: DescribeClientQuotasRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_describe_client_quotas(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DescribeClientQuotas,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::AlterClientQuotas => {
                let version = request.version;
                let typed_request: AlterClientQuotasRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_alter_client_quotas(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::AlterClientQuotas,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::Metadata => {
                let version = request.version;
                let typed_request: MetadataRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_metadata(typed_request, version, context)
                    .await?;
                Ok(self.encode_response(ApiKey::Metadata, version, correlation_id, &response)?)
            }
            ApiKey::DescribeCluster => {
                let version = request.version;
                let typed_request: DescribeClusterRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_describe_cluster(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DescribeCluster,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DescribeQuorum => {
                let version = request.version;
                let typed_request: DescribeQuorumRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_describe_quorum(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DescribeQuorum,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ElectLeaders => {
                let version = request.version;
                let typed_request: ElectLeadersRequest = decode_body(request.body, version)?;
                let response = self.handle_elect_leaders(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::ElectLeaders,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::AlterPartitionReassignments => {
                let version = request.version;
                let typed_request: AlterPartitionReassignmentsRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_alter_partition_reassignments(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::AlterPartitionReassignments,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ListPartitionReassignments => {
                let version = request.version;
                let typed_request: ListPartitionReassignmentsRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_list_partition_reassignments(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::ListPartitionReassignments,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::CreateTopics => {
                let version = request.version;
                let typed_request: CreateTopicsRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_create_topics(typed_request, version, context)
                    .await?;
                Ok(self.encode_response(
                    ApiKey::CreateTopics,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DeleteTopics => {
                let version = request.version;
                let typed_request: DeleteTopicsRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_delete_topics(typed_request, version, context)
                    .await?;
                Ok(self.encode_response(
                    ApiKey::DeleteTopics,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DeleteRecords => {
                let version = request.version;
                let typed_request: DeleteRecordsRequest = decode_body(request.body, version)?;
                let response = self.handle_delete_records(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::DeleteRecords,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::CreatePartitions => {
                let version = request.version;
                let typed_request: CreatePartitionsRequest = decode_body(request.body, version)?;
                let response = self.handle_create_partitions(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::CreatePartitions,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DescribeTopicPartitions => {
                let version = request.version;
                let typed_request: DescribeTopicPartitionsRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_describe_topic_partitions(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DescribeTopicPartitions,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DescribeConfigs => {
                let version = request.version;
                let typed_request: DescribeConfigsRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_describe_configs(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DescribeConfigs,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ListConfigResources => {
                let version = request.version;
                let typed_request: ListConfigResourcesRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_list_config_resources(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::ListConfigResources,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::AlterConfigs => {
                let version = request.version;
                let typed_request: AlterConfigsRequest = decode_body(request.body, version)?;
                let response = self.handle_alter_configs(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::AlterConfigs,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::IncrementalAlterConfigs => {
                let version = request.version;
                let typed_request: IncrementalAlterConfigsRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_incremental_alter_configs(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::IncrementalAlterConfigs,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::AlterReplicaLogDirs => {
                let version = request.version;
                let typed_request: AlterReplicaLogDirsRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_alter_replica_log_dirs(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::AlterReplicaLogDirs,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DescribeLogDirs => {
                let version = request.version;
                let typed_request: DescribeLogDirsRequest = decode_body(request.body, version)?;
                let response = self.handle_describe_log_dirs(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::DescribeLogDirs,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::Produce => {
                let version = request.version;
                let request_size = request.size;
                let typed_request: ProduceRequest = decode_body(request.body, version)?;
                let acks = typed_request.acks;
                let started = tokio::time::Instant::now();
                self.metrics.produce_requests.inc();
                let mut response = self.handle_produce(typed_request, version, context).await?;
                let bandwidth_quota = self
                    .quotas
                    .reserve_user(
                        PRODUCER_BYTE_RATE,
                        &context.principal,
                        &client_id,
                        request_size as f64,
                    )
                    .await?;
                let request_quota = if acks == 0 {
                    crate::client_quota_manager::QuotaReservation::unlimited()
                } else {
                    self.quotas
                        .reserve_user(
                            REQUEST_PERCENTAGE,
                            &context.principal,
                            &client_id,
                            started.elapsed().as_secs_f64() * 100.0,
                        )
                        .await?
                };
                let delay = bandwidth_quota.delay.max(request_quota.delay);
                response.throttle_time_ms = bandwidth_quota
                    .throttle_time_ms()
                    .max(request_quota.throttle_time_ms());
                if !delay.is_zero() {
                    self.metrics.record_quota_throttle(delay);
                    tokio::time::sleep(delay).await;
                }
                let committed = response
                    .responses
                    .iter()
                    .flat_map(|topic| topic.partition_responses.iter())
                    .fold(None, |state, partition| {
                        Some(state.unwrap_or(true) && partition.error_code == NO_ERROR)
                    })
                    .unwrap_or(false);
                if acks != 0
                    && committed
                    && self
                        .failure_injection
                        .disconnect_after_committed_produce(&client_id)
                {
                    return Err(anyhow!(
                        "injected test-only disconnect after durable Produce commit"
                    ));
                }
                if acks == 0 {
                    if response.responses.iter().any(|topic| {
                        topic
                            .partition_responses
                            .iter()
                            .any(|partition| partition.error_code != NO_ERROR)
                    }) {
                        return Err(anyhow!("acks=0 Produce failed"));
                    }
                    return Ok(BudgetedResponse::empty());
                }
                Ok(self.encode_response(ApiKey::Produce, version, correlation_id, &response)?)
            }
            ApiKey::InitProducerId => {
                let version = request.version;
                let typed_request: InitProducerIdRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_init_producer_id(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::InitProducerId,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::OffsetForLeaderEpoch => {
                let version = request.version;
                let typed_request: OffsetForLeaderEpochRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_offset_for_leader_epoch(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::OffsetForLeaderEpoch,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::AddPartitionsToTxn => {
                let version = request.version;
                let typed_request: AddPartitionsToTxnRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_add_partitions_to_txn(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::AddPartitionsToTxn,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::AddOffsetsToTxn => {
                let version = request.version;
                let typed_request: AddOffsetsToTxnRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_add_offsets_to_txn(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::AddOffsetsToTxn,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::TxnOffsetCommit => {
                let version = request.version;
                let typed_request: TxnOffsetCommitRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_txn_offset_commit(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::TxnOffsetCommit,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::EndTxn => {
                let version = request.version;
                let typed_request: EndTxnRequest = decode_body(request.body, version)?;
                let response = self.handle_end_txn(typed_request, version, context).await;
                Ok(self.encode_response(ApiKey::EndTxn, version, correlation_id, &response)?)
            }
            ApiKey::WriteTxnMarkers => {
                let version = request.version;
                let typed_request: WriteTxnMarkersRequest = decode_body(request.body, version)?;
                let response = self.handle_write_txn_markers(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::WriteTxnMarkers,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DescribeTransactions => {
                let version = request.version;
                let typed_request: DescribeTransactionsRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_describe_transactions(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DescribeTransactions,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ListTransactions => {
                let version = request.version;
                let typed_request: ListTransactionsRequest = decode_body(request.body, version)?;
                let response = self.handle_list_transactions(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::ListTransactions,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DescribeProducers => {
                let version = request.version;
                let typed_request: DescribeProducersRequest = decode_body(request.body, version)?;
                let response = self.handle_describe_producers(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::DescribeProducers,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::Fetch => {
                let version = request.version;
                let typed_request: FetchRequest = decode_body(request.body, version)?;
                let started = tokio::time::Instant::now();
                self.metrics.fetch_requests.inc();
                let mut fetched = self.handle_fetch(typed_request, version, context).await?;
                let response_size = fetch_response_bytes(&fetched.response);
                let bandwidth_quota = self
                    .quotas
                    .reserve_user(
                        CONSUMER_BYTE_RATE,
                        &context.principal,
                        &client_id,
                        response_size as f64,
                    )
                    .await?;
                let request_quota = self
                    .quotas
                    .reserve_user(
                        REQUEST_PERCENTAGE,
                        &context.principal,
                        &client_id,
                        started.elapsed().as_secs_f64() * 100.0,
                    )
                    .await?;
                let delay = bandwidth_quota.delay.max(request_quota.delay);
                fetched.response.throttle_time_ms = bandwidth_quota
                    .throttle_time_ms()
                    .max(request_quota.throttle_time_ms());
                if !delay.is_zero() {
                    self.quotas.cancel_for_fetch_retry(&bandwidth_quota, delay);
                    self.fetch_sessions
                        .throttle_response(fetched.session, &mut fetched.response);
                    self.metrics.record_quota_throttle(delay);
                    tokio::time::sleep(delay).await;
                } else {
                    self.fetch_sessions
                        .commit_response(fetched.session, &fetched.response);
                }
                Ok(self.encode_response(
                    ApiKey::Fetch,
                    version,
                    correlation_id,
                    &fetched.response,
                )?)
            }
            ApiKey::ListOffsets => {
                let version = request.version;
                let typed_request: ListOffsetsRequest =
                    decode_body(request.body, body_version(ApiKey::ListOffsets, version))?;
                let response = self
                    .handle_list_offsets(typed_request, version, context)
                    .await;
                Ok(self.encode_response(ApiKey::ListOffsets, version, correlation_id, &response)?)
            }
            ApiKey::FindCoordinator => {
                let version = request.version;
                let typed_request: FindCoordinatorRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_find_coordinator(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::FindCoordinator,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::OffsetCommit => {
                let version = request.version;
                let typed_request: OffsetCommitRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_offset_commit(typed_request, version, context)
                    .await?;
                Ok(self.encode_response(
                    ApiKey::OffsetCommit,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::OffsetFetch => {
                let version = request.version;
                let typed_request: OffsetFetchRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_offset_fetch(typed_request, version, context)
                    .await?;
                Ok(self.encode_response(ApiKey::OffsetFetch, version, correlation_id, &response)?)
            }
            ApiKey::OffsetDelete => {
                let version = request.version;
                let typed_request: OffsetDeleteRequest = decode_body(request.body, version)?;
                let response = self.handle_offset_delete(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::OffsetDelete,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::JoinGroup => {
                let version = request.version;
                let typed_request: JoinGroupRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_join_group(typed_request, version, context, &client_id)
                    .await;
                Ok(self.encode_response(ApiKey::JoinGroup, version, correlation_id, &response)?)
            }
            ApiKey::SyncGroup => {
                let version = request.version;
                let typed_request: SyncGroupRequest = decode_body(request.body, version)?;
                let response = self.handle_sync_group(typed_request, context).await;
                Ok(self.encode_response(ApiKey::SyncGroup, version, correlation_id, &response)?)
            }
            ApiKey::Heartbeat => {
                let version = request.version;
                let typed_request: HeartbeatRequest = decode_body(request.body, version)?;
                let response = self.handle_heartbeat(typed_request, context).await;
                Ok(self.encode_response(ApiKey::Heartbeat, version, correlation_id, &response)?)
            }
            ApiKey::LeaveGroup => {
                let version = request.version;
                let typed_request: LeaveGroupRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_leave_group(typed_request, version, context)
                    .await;
                Ok(self.encode_response(ApiKey::LeaveGroup, version, correlation_id, &response)?)
            }
            ApiKey::ListGroups => {
                let version = request.version;
                let typed_request: ListGroupsRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_list_groups(typed_request, version, context)
                    .await;
                Ok(self.encode_response(ApiKey::ListGroups, version, correlation_id, &response)?)
            }
            ApiKey::DescribeGroups => {
                let version = request.version;
                let typed_request: DescribeGroupsRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_describe_groups(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DescribeGroups,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DeleteGroups => {
                let version = request.version;
                let typed_request: DeleteGroupsRequest = decode_body(request.body, version)?;
                let response = self.handle_delete_groups(typed_request, context).await;
                Ok(self.encode_response(
                    ApiKey::DeleteGroups,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ConsumerGroupHeartbeat => {
                let version = request.version;
                let typed_request: ConsumerGroupHeartbeatRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_consumer_group_heartbeat(typed_request, context, client_id)
                    .await;
                Ok(self.encode_response(
                    ApiKey::ConsumerGroupHeartbeat,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ConsumerGroupDescribe => {
                let version = request.version;
                let typed_request: ConsumerGroupDescribeRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_consumer_group_describe(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::ConsumerGroupDescribe,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::StreamsGroupHeartbeat => {
                let version = request.version;
                let typed_request: StreamsGroupHeartbeatRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_streams_group_heartbeat(typed_request, context, client_id)
                    .await;
                Ok(self.encode_response(
                    ApiKey::StreamsGroupHeartbeat,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::StreamsGroupDescribe => {
                let version = request.version;
                let typed_request: StreamsGroupDescribeRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_streams_group_describe(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::StreamsGroupDescribe,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DescribeShareGroupOffsets => {
                let version = request.version;
                let typed_request: DescribeShareGroupOffsetsRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_describe_share_group_offsets(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DescribeShareGroupOffsets,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::AlterShareGroupOffsets => {
                let version = request.version;
                let typed_request: AlterShareGroupOffsetsRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_alter_share_group_offsets(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::AlterShareGroupOffsets,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DeleteShareGroupOffsets => {
                let version = request.version;
                let typed_request: DeleteShareGroupOffsetsRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_delete_share_group_offsets(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DeleteShareGroupOffsets,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ShareGroupHeartbeat => {
                let version = request.version;
                let typed_request: ShareGroupHeartbeatRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_share_group_heartbeat(typed_request, context, client_id)
                    .await;
                Ok(self.encode_response(
                    ApiKey::ShareGroupHeartbeat,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ShareGroupDescribe => {
                let version = request.version;
                let typed_request: ShareGroupDescribeRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_share_group_describe(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::ShareGroupDescribe,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ShareFetch => {
                let version = request.version;
                let typed_request: ShareFetchRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_share_fetch(typed_request, version, context)
                    .await?;
                Ok(self.encode_response(ApiKey::ShareFetch, version, correlation_id, &response)?)
            }
            ApiKey::ShareAcknowledge => {
                let version = request.version;
                let typed_request: ShareAcknowledgeRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_share_acknowledge(typed_request, version, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::ShareAcknowledge,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::InitializeShareGroupState => {
                let version = request.version;
                let typed_request: InitializeShareGroupStateRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_initialize_share_group_state(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::InitializeShareGroupState,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ReadShareGroupState => {
                let version = request.version;
                let typed_request: ReadShareGroupStateRequest = decode_body(request.body, version)?;
                let response = self
                    .handle_read_share_group_state(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::ReadShareGroupState,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::WriteShareGroupState => {
                let version = request.version;
                let typed_request: WriteShareGroupStateRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_write_share_group_state(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::WriteShareGroupState,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::DeleteShareGroupState => {
                let version = request.version;
                let typed_request: DeleteShareGroupStateRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_delete_share_group_state(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::DeleteShareGroupState,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            ApiKey::ReadShareGroupStateSummary => {
                let version = request.version;
                let typed_request: ReadShareGroupStateSummaryRequest =
                    decode_body(request.body, version)?;
                let response = self
                    .handle_read_share_group_state_summary(typed_request, context)
                    .await;
                Ok(self.encode_response(
                    ApiKey::ReadShareGroupStateSummary,
                    version,
                    correlation_id,
                    &response,
                )?)
            }
            api_key => {
                warn!(
                    ?api_key,
                    "Kafka API is not implemented in this vertical slice"
                );
                Err(anyhow!("Kafka API {api_key:?} is not implemented"))
            }
        }
    }

    async fn handle_create_topics(
        &self,
        request: CreateTopicsRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> Result<CreateTopicsResponse> {
        if let Err((error_code, message)) =
            create_topics_validation::validate_batch(&request.topics, self.config.num_partitions)
        {
            let topics = request
                .topics
                .into_iter()
                .map(|topic| {
                    CreatableTopicResult::default()
                        .with_name(topic.name)
                        .with_error_code(error_code)
                        .with_error_message(Some(message.clone().into()))
                })
                .collect();
            return Ok(CreateTopicsResponse::default().with_topics(topics));
        }
        let mut names = HashSet::with_capacity(request.topics.len());
        let mut duplicates = HashSet::new();
        for topic in &request.topics {
            let name = topic.name.as_str().to_owned();
            if !names.insert(name.clone()) {
                duplicates.insert(name);
            }
        }
        let mut topics = Vec::with_capacity(request.topics.len());
        for topic in request.topics {
            let name = topic.name.clone();
            let authorized = self
                .authorized(
                    context,
                    AclResourceType::Cluster,
                    authorization::CLUSTER_RESOURCE_NAME,
                    AclOperation::Create,
                )
                .await?
                || self
                    .authorized(
                        context,
                        AclResourceType::Topic,
                        name.as_str(),
                        AclOperation::Create,
                    )
                    .await?;
            if !authorized {
                topics.push(
                    CreatableTopicResult::default()
                        .with_name(name)
                        .with_error_code(TOPIC_AUTHORIZATION_FAILED)
                        .with_error_message(Some("topic authorization failed".into())),
                );
                continue;
            }
            if duplicates.contains(name.as_str()) {
                topics.push(
                    CreatableTopicResult::default()
                        .with_name(name)
                        .with_error_code(INVALID_REQUEST)
                        .with_error_message(Some(
                            "CreateTopics contains duplicate topic names".into(),
                        )),
                );
                continue;
            }
            if let Err(error) = self.metadata.validate_topic_creation(name.as_str()).await {
                topics.push(
                    CreatableTopicResult::default()
                        .with_name(name)
                        .with_error_code(control_error_code(&error))
                        .with_error_message(Some(error.to_string().into())),
                );
                continue;
            }
            let config = match config_api::create_topic_config(&topic.configs) {
                Ok(config) => config,
                Err(error) => {
                    topics.push(
                        CreatableTopicResult::default()
                            .with_name(name)
                            .with_error_code(control_error_code(&error))
                            .with_error_message(Some(error.to_string().into())),
                    );
                    continue;
                }
            };
            let can_describe_configs = version >= 5
                && self
                    .authorized(
                        context,
                        AclResourceType::Topic,
                        name.as_str(),
                        AclOperation::DescribeConfigs,
                    )
                    .await?;
            let response_configs =
                can_describe_configs.then(|| config_api::create_topic_response_configs(&config));
            let creation = match create_topics_validation::validate(
                &topic,
                self.config.num_partitions,
                self.config.default_replication_factor,
            ) {
                Ok(creation) => creation,
                Err((error_code, message)) => {
                    topics.push(
                        CreatableTopicResult::default()
                            .with_name(name)
                            .with_error_code(error_code)
                            .with_error_message(Some(message.into())),
                    );
                    continue;
                }
            };
            let partitions = creation.partitions;
            let replication_factor = creation.replication_factor;
            let result = if request.validate_only {
                Ok(TopicInfo {
                    id: Uuid::new_v4(),
                    name: name.as_str().to_owned(),
                    partitions,
                })
            } else {
                self.metadata
                    .create_topic_with_config(name.as_str(), partitions, config)
                    .await
            };
            topics.push(match result {
                Ok(info) => {
                    let mut result = CreatableTopicResult::default()
                        .with_name(name)
                        .with_error_code(NO_ERROR)
                        .with_error_message(None);
                    if version >= 7 {
                        result = result.with_topic_id(info.id);
                    }
                    if version >= 5 {
                        result = if can_describe_configs {
                            result
                                .with_num_partitions(info.partitions)
                                .with_replication_factor(replication_factor)
                                .with_configs(response_configs)
                        } else {
                            result.with_topic_config_error_code(TOPIC_AUTHORIZATION_FAILED)
                        };
                    }
                    result
                }
                Err(error) => CreatableTopicResult::default()
                    .with_name(name)
                    .with_error_code(control_error_code(&error))
                    .with_error_message(Some(error.to_string().into())),
            });
        }
        Ok(CreateTopicsResponse::default().with_topics(topics))
    }

    async fn handle_find_coordinator(
        &self,
        request: FindCoordinatorRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> FindCoordinatorResponse {
        if version <= 3 {
            let error_code = self
                .find_coordinator_error(request.key_type, request.key.as_str(), version, context)
                .await
                .unwrap_or(UNKNOWN_SERVER_ERROR);
            let mut response = FindCoordinatorResponse::default()
                .with_error_code(error_code)
                .with_error_message(coordinator_error_message(error_code));
            if error_code == NO_ERROR {
                response = response
                    .with_node_id(BrokerId::from(0))
                    .with_host(self.config.advertise_host.clone().into())
                    .with_port(self.config.advertise_port);
            } else {
                response = response
                    .with_node_id(BrokerId::from(-1))
                    .with_host(StrBytes::from_static_str(""))
                    .with_port(-1);
            }
            return response;
        }
        let keys = request.coordinator_keys;
        let mut coordinators = Vec::with_capacity(keys.len());
        for key in keys {
            let error_code = self
                .find_coordinator_error(request.key_type, key.as_str(), version, context)
                .await
                .unwrap_or(UNKNOWN_SERVER_ERROR);
            let mut coordinator = Coordinator::default()
                .with_key(key)
                .with_error_code(error_code);
            if error_code == NO_ERROR {
                coordinator = coordinator
                    .with_node_id(BrokerId::from(0))
                    .with_host(self.config.advertise_host.clone().into())
                    .with_port(self.config.advertise_port);
            } else {
                coordinator = coordinator
                    .with_node_id(BrokerId::from(-1))
                    .with_host(StrBytes::from_static_str(""))
                    .with_port(-1);
            }
            coordinators.push(coordinator);
        }
        FindCoordinatorResponse::default().with_coordinators(coordinators)
    }

    async fn find_coordinator_error(
        &self,
        key_type: i8,
        key: &str,
        version: i16,
        context: &AuthorizationContext,
    ) -> std::result::Result<i16, ()> {
        let authorization = match key_type {
            0 => self
                .authorized(context, AclResourceType::Group, key, AclOperation::Describe)
                .await
                .map(|authorized| {
                    if authorized {
                        NO_ERROR
                    } else {
                        GROUP_AUTHORIZATION_FAILED
                    }
                }),
            1 => self
                .authorized(
                    context,
                    AclResourceType::TransactionalId,
                    key,
                    AclOperation::Describe,
                )
                .await
                .map(|authorized| {
                    if authorized {
                        NO_ERROR
                    } else {
                        TRANSACTIONAL_ID_AUTHORIZATION_FAILED
                    }
                }),
            2 if version < 6 => return Ok(INVALID_REQUEST),
            2 => {
                let authorized = self
                    .authorized(
                        context,
                        AclResourceType::Cluster,
                        authorization::CLUSTER_RESOURCE_NAME,
                        AclOperation::ClusterAction,
                    )
                    .await
                    .map_err(|_| ())?;
                if !authorized {
                    return Ok(CLUSTER_AUTHORIZATION_FAILED);
                }
                return Ok(if valid_share_coordinator_key(key) {
                    NO_ERROR
                } else {
                    INVALID_REQUEST
                });
            }
            _ => return Ok(INVALID_REQUEST),
        };
        authorization.map_err(|_| ())
    }

    async fn handle_heartbeat(
        &self,
        request: HeartbeatRequest,
        context: &AuthorizationContext,
    ) -> HeartbeatResponse {
        let group_id = request.group_id.as_str().to_owned();
        if let Some((error_code, _)) = authorization_failure(
            self.authorized(
                context,
                AclResourceType::Group,
                &group_id,
                AclOperation::Read,
            )
            .await,
            GROUP_AUTHORIZATION_FAILED,
        ) {
            return HeartbeatResponse::default().with_error_code(error_code);
        }
        let result = self
            .metadata
            .heartbeat_group(
                &group_id,
                request.generation_id,
                request.member_id.as_str(),
                request
                    .group_instance_id
                    .as_ref()
                    .map(|value| value.as_str()),
            )
            .await;
        HeartbeatResponse::default()
            .with_error_code(result.as_ref().err().map_or(NO_ERROR, control_error_code))
    }

    async fn handle_leave_group(
        &self,
        request: LeaveGroupRequest,
        version: i16,
        context: &AuthorizationContext,
    ) -> LeaveGroupResponse {
        let group_id = request.group_id.as_str().to_owned();
        let members = if request.members.is_empty() {
            if request.member_id.is_empty() {
                Vec::new()
            } else {
                vec![GroupMemberIdentity {
                    member_id: request.member_id.as_str().to_owned(),
                    group_instance_id: None,
                }]
            }
        } else {
            request
                .members
                .iter()
                .map(|member| GroupMemberIdentity {
                    member_id: member.member_id.as_str().to_owned(),
                    group_instance_id: member
                        .group_instance_id
                        .as_ref()
                        .map(|value| value.as_str().to_owned()),
                })
                .collect::<Vec<_>>()
        };
        if let Some((error_code, _)) = authorization_failure(
            self.authorized(
                context,
                AclResourceType::Group,
                &group_id,
                AclOperation::Read,
            )
            .await,
            GROUP_AUTHORIZATION_FAILED,
        ) {
            return LeaveGroupResponse::default()
                .with_error_code(error_code)
                .with_members(if version >= 3 {
                    members
                        .into_iter()
                        .map(|identity| {
                            MemberResponse::default()
                                .with_member_id(StrBytes::from_string(identity.member_id))
                                .with_group_instance_id(
                                    identity.group_instance_id.map(StrBytes::from_string),
                                )
                                .with_error_code(error_code)
                        })
                        .collect()
                } else {
                    Vec::new()
                });
        }
        match self.metadata.leave_group(&group_id, &members).await {
            Ok(results) => {
                let first_error = results
                    .first()
                    .and_then(|result| result.error)
                    .map_or(NO_ERROR, leave_group_member_error_code);
                LeaveGroupResponse::default()
                    .with_error_code(if version >= 3 { NO_ERROR } else { first_error })
                    .with_members(if version >= 3 {
                        results
                            .into_iter()
                            .map(|result| {
                                MemberResponse::default()
                                    .with_member_id(StrBytes::from_string(
                                        result.identity.member_id,
                                    ))
                                    .with_group_instance_id(
                                        result
                                            .identity
                                            .group_instance_id
                                            .map(StrBytes::from_string),
                                    )
                                    .with_error_code(
                                        result
                                            .error
                                            .map_or(NO_ERROR, leave_group_member_error_code),
                                    )
                            })
                            .collect()
                    } else {
                        Vec::new()
                    })
            }
            Err(ControlError::GroupNotFound(_)) if version >= 3 => LeaveGroupResponse::default()
                .with_members(
                    members
                        .into_iter()
                        .map(|identity| {
                            MemberResponse::default()
                                .with_member_id(StrBytes::from_string(identity.member_id))
                                .with_group_instance_id(
                                    identity.group_instance_id.map(StrBytes::from_string),
                                )
                                .with_error_code(UNKNOWN_MEMBER_ID)
                        })
                        .collect(),
                ),
            Err(error) => LeaveGroupResponse::default().with_error_code(match error {
                ControlError::GroupNotFound(_) => UNKNOWN_MEMBER_ID,
                _ => control_error_code(&error),
            }),
        }
    }
}

fn valid_share_coordinator_key(key: &str) -> bool {
    let mut tokens = key.split(':');
    let (Some(group_id), Some(topic_id), Some(partition), None) =
        (tokens.next(), tokens.next(), tokens.next(), tokens.next())
    else {
        return false;
    };
    if group_id.trim().is_empty() || topic_id.len() > 24 || partition.parse::<i32>().is_err() {
        return false;
    }
    URL_SAFE_NO_PAD
        .decode(topic_id)
        .or_else(|_| URL_SAFE.decode(topic_id))
        .is_ok_and(|decoded| decoded.len() == 16)
}

fn coordinator_error_message(error_code: i16) -> Option<StrBytes> {
    let message = match error_code {
        NO_ERROR => return None,
        GROUP_AUTHORIZATION_FAILED => "Group authorization failed.",
        CLUSTER_AUTHORIZATION_FAILED => "Cluster authorization failed.",
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED => "Transactional ID authorization failed.",
        INVALID_REQUEST => "The coordinator request is invalid.",
        UNKNOWN_SERVER_ERROR => "The server experienced an unexpected error.",
        _ => "Coordinator lookup failed.",
    };
    Some(StrBytes::from_static_str(message))
}

fn leave_group_member_error_code(error: LeaveGroupMemberError) -> i16 {
    match error {
        LeaveGroupMemberError::UnknownMemberId => UNKNOWN_MEMBER_ID,
        LeaveGroupMemberError::FencedInstanceId => FENCED_INSTANCE_ID,
    }
}

enum ConnectionRequest {
    Supported(RequestFrame),
    UnsupportedApiVersions {
        correlation_id: i32,
        requested_version: i16,
    },
}

fn connection_request(payload: Bytes) -> Result<ConnectionRequest> {
    let request = decode_request(payload).map_err(|error| anyhow!(error))?;
    if !supports_version(request.api_key, request.version) {
        if request.api_key == ApiKey::ApiVersions {
            return Ok(ConnectionRequest::UnsupportedApiVersions {
                correlation_id: request.header.correlation_id,
                requested_version: request.version,
            });
        }
        return Err(anyhow!(
            "Kafka API {:?} version {} is not advertised",
            request.api_key,
            request.version
        ));
    }
    Ok(ConnectionRequest::Supported(request))
}

#[cfg(test)]
fn supported_request(payload: Bytes) -> Result<RequestFrame> {
    match connection_request(payload)? {
        ConnectionRequest::Supported(request) => Ok(request),
        ConnectionRequest::UnsupportedApiVersions {
            requested_version, ..
        } => Err(anyhow!(
            "Kafka API {:?} version {} is not advertised",
            ApiKey::ApiVersions,
            requested_version
        )),
    }
}

fn authorization_context(
    sasl: &SaslConnection,
    peer: SocketAddr,
    client_information: &ClientInformation,
) -> AuthorizationContext {
    let context = sasl.principal().map_or_else(
        || AuthorizationContext::anonymous(peer.ip()),
        |principal| {
            if sasl.token_authenticated() {
                AuthorizationContext::authenticated_token(principal, peer.ip())
            } else {
                AuthorizationContext::authenticated(principal, peer.ip())
            }
        },
    );
    context.with_client_connection(
        peer.port(),
        client_information.software_name.as_deref(),
        client_information.software_version.as_deref(),
    )
}

fn nonempty(value: &str) -> Option<String> {
    (!value.is_empty()).then(|| value.to_owned())
}

pub async fn serve_admin(
    metrics: Arc<Metrics>,
    config: &AgentConfig,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let listener = TcpListener::bind(config.admin_addr)
        .await
        .with_context(|| format!("bind admin listener {}", config.admin_addr))?;
    metrics
        .serve(listener, config.connection_io_timeout, shutdown)
        .await
}

fn topic_name(value: &str) -> kafka_protocol::messages::TopicName {
    kafka_protocol::messages::TopicName::from(StrBytes::from_string(value.to_owned()))
}

#[allow(dead_code)]
fn _max_frame_size() -> usize {
    MAX_FRAME_SIZE
}

fn fetch_response_bytes(response: &FetchResponse) -> usize {
    response
        .responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .filter_map(|partition| partition.records.as_ref())
        .map(Bytes::len)
        .sum()
}

fn is_controller_mutation(api_key: ApiKey) -> bool {
    matches!(
        api_key,
        ApiKey::CreateTopics
            | ApiKey::DeleteTopics
            | ApiKey::CreatePartitions
            | ApiKey::DeleteRecords
            | ApiKey::AlterConfigs
            | ApiKey::IncrementalAlterConfigs
            | ApiKey::AlterReplicaLogDirs
            | ApiKey::ElectLeaders
            | ApiKey::AlterPartitionReassignments
            | ApiKey::AlterUserScramCredentials
            | ApiKey::CreateAcls
            | ApiKey::DeleteAcls
            | ApiKey::AlterClientQuotas
            | ApiKey::UpdateFeatures
            | ApiKey::CreateDelegationToken
            | ApiKey::RenewDelegationToken
            | ApiKey::ExpireDelegationToken
    )
}

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "legacy_alter_configs_tests.rs"]
mod legacy_alter_configs_tests;

#[path = "transaction_api.rs"]
mod transaction_api;

#[path = "metadata_api.rs"]
mod metadata_api;

#[path = "config_api.rs"]
mod config_api;

#[path = "alter_configs_api.rs"]
mod alter_configs_api;

#[path = "describe_configs_api.rs"]
mod describe_configs_api;

#[path = "config_synonyms.rs"]
mod config_synonyms;

#[path = "broker_config.rs"]
mod broker_config;

#[path = "config_resource_api.rs"]
mod config_resource_api;

#[path = "group_config.rs"]
mod group_config;
#[path = "group_offset_reset.rs"]
mod group_offset_reset;

#[path = "client_metric_config.rs"]
mod client_metric_config;

#[path = "client_telemetry_manager.rs"]
mod client_telemetry_manager;

#[path = "log_dir_api.rs"]
mod log_dir_api;

#[path = "authorization.rs"]
mod authorization;

#[path = "acl_api.rs"]
mod acl_api;

#[path = "produce_api.rs"]
mod produce_api;

#[path = "consumer_group_api.rs"]
mod consumer_group_api;

#[path = "streams_group_api.rs"]
mod streams_group_api;
#[path = "streams_group_protocol.rs"]
mod streams_group_protocol;
#[path = "streams_internal_topics.rs"]
mod streams_internal_topics;
#[path = "streams_topology_validation.rs"]
mod streams_topology_validation;

#[path = "share_group_api.rs"]
mod share_group_api;

#[path = "share_state_api.rs"]
mod share_state_api;

#[path = "share_api.rs"]
mod share_api;

#[path = "share_topic_authorization.rs"]
mod share_topic_authorization;

#[path = "share_protocol.rs"]
mod share_protocol;

#[path = "share_fetch_api.rs"]
mod share_fetch_api;

#[path = "share_acknowledge_api.rs"]
mod share_acknowledge_api;

#[path = "share_offset_api.rs"]
mod share_offset_api;

#[path = "share_offset_mutation_api.rs"]
mod share_offset_mutation_api;

#[path = "group_admin_api.rs"]
mod group_admin_api;

#[path = "cluster_api.rs"]
mod cluster_api;

#[path = "classic_group_join.rs"]
mod classic_group_join;

#[path = "classic_group_subscription.rs"]
mod classic_group_subscription;

#[path = "classic_group_sync.rs"]
mod classic_group_sync;

#[path = "topic_admin_api.rs"]
mod topic_admin_api;

#[path = "delete_topics_api.rs"]
mod delete_topics_api;

#[path = "create_topics_validation.rs"]
mod create_topics_validation;

#[path = "offset_admin_api.rs"]
mod offset_admin_api;

#[path = "group_offset_api.rs"]
mod group_offset_api;

#[path = "fetch_api.rs"]
mod fetch_api;

#[path = "fetch_session.rs"]
mod fetch_session;

#[path = "list_offsets_api.rs"]
mod list_offsets_api;

#[path = "partition_state_api.rs"]
pub(crate) mod partition_state_api;

#[path = "leadership_api.rs"]
mod leadership_api;

#[path = "scram_admin_api.rs"]
mod scram_admin_api;

#[path = "delegation_token_api.rs"]
mod delegation_token_api;

#[path = "client_quota_api.rs"]
mod client_quota_api;

#[path = "feature_api.rs"]
mod feature_api;

#[path = "quorum_api.rs"]
mod quorum_api;

#[cfg(test)]
#[path = "acl_tests.rs"]
mod acl_tests;

#[cfg(test)]
#[path = "authorization_filter_tests.rs"]
mod authorization_filter_tests;

#[cfg(test)]
#[path = "create_acls_validation_tests.rs"]
mod create_acls_validation_tests;

#[cfg(test)]
#[path = "consumer_group_tests.rs"]
mod consumer_group_tests;

#[cfg(test)]
#[path = "streams_group_tests.rs"]
mod streams_group_tests;

#[cfg(test)]
#[path = "share_group_tests.rs"]
mod share_group_tests;

#[cfg(test)]
#[path = "share_state_tests.rs"]
mod share_state_tests;

#[cfg(test)]
#[path = "share_protocol_tests.rs"]
mod share_protocol_tests;

#[cfg(test)]
#[path = "share_authorization_tests.rs"]
mod share_authorization_tests;

#[cfg(test)]
#[path = "share_offset_tests.rs"]
mod share_offset_tests;

#[cfg(test)]
#[path = "share_offset_authorization_tests.rs"]
mod share_offset_authorization_tests;

#[cfg(test)]
#[path = "group_admin_tests.rs"]
mod group_admin_tests;

#[cfg(test)]
#[path = "describe_cluster_semantics_tests.rs"]
mod describe_cluster_semantics_tests;

#[cfg(test)]
#[path = "describe_configs_authorization_tests.rs"]
mod describe_configs_authorization_tests;

#[cfg(test)]
#[path = "group_describe_authorization_tests.rs"]
mod group_describe_authorization_tests;

#[cfg(test)]
#[path = "group_membership_authorization_tests.rs"]
mod group_membership_authorization_tests;

#[cfg(test)]
#[path = "classic_group_sync_tests.rs"]
mod classic_group_sync_tests;

#[cfg(test)]
#[path = "classic_group_identity_tests.rs"]
mod classic_group_identity_tests;

#[cfg(test)]
#[path = "classic_group_join_barrier_tests.rs"]
mod classic_group_join_barrier_tests;

#[cfg(test)]
#[path = "topic_admin_tests.rs"]
mod topic_admin_tests;

#[cfg(test)]
#[path = "create_topics_tests.rs"]
mod create_topics_tests;

#[cfg(test)]
#[path = "offset_admin_tests.rs"]
mod offset_admin_tests;

#[cfg(test)]
#[path = "group_offset_tests.rs"]
mod group_offset_tests;

#[cfg(test)]
#[path = "fetch_session_tests.rs"]
mod fetch_session_tests;

#[cfg(test)]
#[path = "fetch_cache_api_tests.rs"]
mod fetch_cache_api_tests;

#[cfg(test)]
#[path = "fetch_authorization_tests.rs"]
mod fetch_authorization_tests;

#[cfg(test)]
#[path = "list_offsets_tests.rs"]
mod list_offsets_tests;

#[cfg(test)]
#[path = "partition_state_tests.rs"]
mod partition_state_tests;

#[cfg(test)]
#[path = "config_admin_tests.rs"]
mod config_admin_tests;

#[cfg(test)]
#[path = "record_admission_tests.rs"]
mod record_admission_tests;

#[cfg(test)]
#[path = "config_resource_tests.rs"]
mod config_resource_tests;

#[cfg(test)]
#[path = "broker_config_tests.rs"]
mod broker_config_tests;

#[cfg(test)]
#[path = "unsupported_topic_config_tests.rs"]
mod unsupported_topic_config_tests;

#[cfg(test)]
#[path = "client_telemetry_tests.rs"]
mod client_telemetry_tests;

#[cfg(test)]
#[path = "log_dir_tests.rs"]
mod log_dir_tests;

#[cfg(test)]
#[path = "leadership_tests.rs"]
mod leadership_tests;

#[cfg(test)]
#[path = "scram_admin_tests.rs"]
mod scram_admin_tests;

#[cfg(test)]
#[path = "delegation_token_tests.rs"]
mod delegation_token_tests;

#[cfg(test)]
#[path = "client_quota_tests.rs"]
mod client_quota_tests;

#[cfg(test)]
#[path = "feature_tests.rs"]
mod feature_tests;

#[cfg(test)]
#[path = "quorum_tests.rs"]
mod quorum_tests;

#[cfg(test)]
#[path = "transaction_v6_tests.rs"]
mod transaction_v6_tests;

#[cfg(test)]
#[path = "transaction_v2_tests.rs"]
mod transaction_v2_tests;

#[cfg(test)]
#[path = "write_txn_markers_tests.rs"]
mod write_txn_markers_tests;

#[cfg(test)]
#[path = "add_partitions_txn_tests.rs"]
mod add_partitions_txn_tests;

#[cfg(test)]
#[path = "delete_topics_tests.rs"]
mod delete_topics_tests;

#[cfg(test)]
#[path = "delete_topics_acl_tests.rs"]
mod delete_topics_acl_tests;

#[cfg(test)]
#[path = "produce_missing_topic_tests.rs"]
mod produce_missing_topic_tests;

#[cfg(test)]
#[path = "metadata_semantics_tests.rs"]
mod metadata_semantics_tests;

#[cfg(test)]
#[path = "find_coordinator_tests.rs"]
mod find_coordinator_tests;

#[cfg(test)]
#[path = "transaction_query_tests.rs"]
mod transaction_query_tests;
