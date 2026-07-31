use super::authorization::AuthorizationContext;
use super::tests::{decode_response, request_frame};
use super::*;
use crate::kafka_error::{
    DELEGATION_TOKEN_AUTHORIZATION_FAILED, DELEGATION_TOKEN_OWNER_MISMATCH,
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, INVALID_PRINCIPAL_TYPE,
};
use kafka_protocol::messages::create_delegation_token_request::CreatableRenewers;
use kafka_protocol::messages::{
    CreateDelegationTokenRequest, CreateDelegationTokenResponse, DescribeDelegationTokenRequest,
    DescribeDelegationTokenResponse, ExpireDelegationTokenRequest, ExpireDelegationTokenResponse,
    RenewDelegationTokenRequest, RenewDelegationTokenResponse,
};
use rutomq_control::{
    AclOperation, AclPatternType, AclPermission, AclResourceType, AclRule, MemoryMetadataStore,
};
use rutomq_storage::OpenDalObjectStore;

fn token_broker(acl_enabled: bool) -> Broker {
    let mut config = AgentConfig::default();
    config.security.delegation_token_secret =
        Some("rutomq-delegation-token-test-secret".to_owned());
    config.security.acl_enabled = acl_enabled;
    if acl_enabled {
        config.security.super_users.insert("User:admin".to_owned());
    }
    Broker::new(
        Arc::new(MemoryMetadataStore::new()),
        Arc::new(OpenDalObjectStore::memory().unwrap()),
        config,
        Arc::new(Metrics::new().unwrap()),
    )
}

async fn handle_as(
    broker: &Broker,
    username: &str,
    token_authenticated: bool,
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    body: &impl kafka_protocol::protocol::Encodable,
) -> Bytes {
    let request = supported_request(request_frame(api_key, version, correlation_id, body)).unwrap();
    let host = std::net::Ipv4Addr::LOCALHOST.into();
    let context = if token_authenticated {
        AuthorizationContext::authenticated_token(username, host)
    } else {
        AuthorizationContext::authenticated(username, host)
    };
    broker
        .dispatch_request(request, &context)
        .await
        .unwrap()
        .into_bytes()
}

fn renewer(principal_type: &str, principal_name: &str) -> CreatableRenewers {
    CreatableRenewers::default()
        .with_principal_type(StrBytes::from_string(principal_type.to_owned()))
        .with_principal_name(StrBytes::from_string(principal_name.to_owned()))
}

fn user_rule(principal: &str, owner: &str, operation: AclOperation) -> AclRule {
    AclRule {
        resource_type: AclResourceType::User,
        resource_name: owner.to_owned(),
        pattern_type: AclPatternType::Literal,
        principal: principal.to_owned(),
        host: "*".to_owned(),
        operation,
        permission: AclPermission::Allow,
    }
}

#[tokio::test]
async fn delegation_token_wire_lifecycle_supports_owner_and_renewer() {
    let broker = token_broker(false);
    let create = CreateDelegationTokenRequest::default()
        .with_renewers(vec![
            renewer("User", ""),
            renewer("User", "bob"),
            renewer("User", "bob"),
        ])
        .with_max_lifetime_ms(60_000);
    let response = handle_as(
        &broker,
        "alice",
        false,
        ApiKey::CreateDelegationToken,
        3,
        200,
        &create,
    )
    .await;
    let created: CreateDelegationTokenResponse =
        decode_response(ApiKey::CreateDelegationToken, 3, response);
    assert_eq!(created.error_code, NO_ERROR);
    assert_eq!(created.principal_name.as_str(), "alice");
    assert_eq!(created.token_requester_principal_name.as_str(), "alice");
    assert_eq!(created.token_id.len(), 22);
    assert_eq!(created.hmac.len(), 64);

    let renew = RenewDelegationTokenRequest::default()
        .with_hmac(created.hmac.clone())
        .with_renew_period_ms(30_000);
    let response = handle_as(
        &broker,
        "bob",
        false,
        ApiKey::RenewDelegationToken,
        2,
        201,
        &renew,
    )
    .await;
    let renewed: RenewDelegationTokenResponse =
        decode_response(ApiKey::RenewDelegationToken, 2, response);
    assert_eq!(renewed.error_code, NO_ERROR);
    assert!(renewed.expiry_timestamp_ms <= created.max_timestamp_ms);

    let response = handle_as(
        &broker,
        "mallory",
        false,
        ApiKey::RenewDelegationToken,
        2,
        202,
        &renew,
    )
    .await;
    let denied: RenewDelegationTokenResponse =
        decode_response(ApiKey::RenewDelegationToken, 2, response);
    assert_eq!(denied.error_code, DELEGATION_TOKEN_OWNER_MISMATCH);

    let describe = DescribeDelegationTokenRequest::default().with_owners(None);
    let response = handle_as(
        &broker,
        "alice",
        false,
        ApiKey::DescribeDelegationToken,
        3,
        203,
        &describe,
    )
    .await;
    let described: DescribeDelegationTokenResponse =
        decode_response(ApiKey::DescribeDelegationToken, 3, response);
    assert_eq!(described.error_code, NO_ERROR);
    assert_eq!(described.tokens.len(), 1);
    assert_eq!(described.tokens[0].renewers.len(), 3);
    assert_eq!(described.tokens[0].renewers[0].principal_name.as_str(), "");
    assert_eq!(
        described.tokens[0].renewers[1].principal_name.as_str(),
        "bob"
    );
    assert_eq!(
        described.tokens[0].renewers[2].principal_name.as_str(),
        "bob"
    );

    let expire = ExpireDelegationTokenRequest::default()
        .with_hmac(created.hmac)
        .with_expiry_time_period_ms(-1);
    let response = handle_as(
        &broker,
        "alice",
        false,
        ApiKey::ExpireDelegationToken,
        2,
        204,
        &expire,
    )
    .await;
    let expired: ExpireDelegationTokenResponse =
        decode_response(ApiKey::ExpireDelegationToken, 2, response);
    assert_eq!(expired.error_code, NO_ERROR);

    let response = handle_as(
        &broker,
        "alice",
        false,
        ApiKey::DescribeDelegationToken,
        3,
        205,
        &describe,
    )
    .await;
    let described: DescribeDelegationTokenResponse =
        decode_response(ApiKey::DescribeDelegationToken, 3, response);
    assert!(described.tokens.is_empty());
}

#[tokio::test]
async fn delegation_token_owner_name_controls_requester_defaulting() {
    let broker = token_broker(false);
    for (correlation_id, owner_name) in [(206, None), (207, Some(StrBytes::from_static_str("")))] {
        let request = CreateDelegationTokenRequest::default()
            .with_owner_principal_type(Some(StrBytes::from_static_str("Service")))
            .with_owner_principal_name(owner_name)
            .with_max_lifetime_ms(60_000);
        let response = handle_as(
            &broker,
            "alice",
            false,
            ApiKey::CreateDelegationToken,
            3,
            correlation_id,
            &request,
        )
        .await;
        let response: CreateDelegationTokenResponse =
            decode_response(ApiKey::CreateDelegationToken, 3, response);
        assert_eq!(response.error_code, NO_ERROR);
        assert_eq!(response.principal_type.as_str(), "User");
        assert_eq!(response.principal_name.as_str(), "alice");
        assert_eq!(response.token_requester_principal_type.as_str(), "User");
        assert_eq!(response.token_requester_principal_name.as_str(), "alice");
    }

    let explicit = CreateDelegationTokenRequest::default()
        .with_owner_principal_type(Some(StrBytes::from_static_str("Service")))
        .with_owner_principal_name(Some(StrBytes::from_static_str("worker")))
        .with_max_lifetime_ms(60_000);
    let response = handle_as(
        &broker,
        "alice",
        false,
        ApiKey::CreateDelegationToken,
        3,
        208,
        &explicit,
    )
    .await;
    let response: CreateDelegationTokenResponse =
        decode_response(ApiKey::CreateDelegationToken, 3, response);
    assert_eq!(response.error_code, NO_ERROR);
    assert_eq!(response.principal_type.as_str(), "Service");
    assert_eq!(response.principal_name.as_str(), "worker");
    assert_eq!(response.token_requester_principal_name.as_str(), "alice");
}

#[tokio::test]
async fn delegation_token_enforces_request_and_owner_authorization_rules() {
    let broker = token_broker(true);
    let create_for_alice = CreateDelegationTokenRequest::default()
        .with_owner_principal_type(Some(StrBytes::from_static_str("User")))
        .with_owner_principal_name(Some(StrBytes::from_static_str("alice")))
        .with_max_lifetime_ms(60_000);

    let response = handle_as(
        &broker,
        "mallory",
        false,
        ApiKey::CreateDelegationToken,
        3,
        210,
        &create_for_alice,
    )
    .await;
    let denied: CreateDelegationTokenResponse =
        decode_response(ApiKey::CreateDelegationToken, 3, response);
    assert_eq!(denied.error_code, DELEGATION_TOKEN_AUTHORIZATION_FAILED);
    assert_eq!(denied.principal_type.as_str(), "User");
    assert_eq!(denied.principal_name.as_str(), "alice");
    assert_eq!(denied.token_requester_principal_type.as_str(), "User");
    assert_eq!(denied.token_requester_principal_name.as_str(), "mallory");
    assert_eq!(denied.issue_timestamp_ms, -1);
    assert_eq!(denied.expiry_timestamp_ms, -1);
    assert_eq!(denied.max_timestamp_ms, -1);

    broker
        .metadata
        .create_acl(user_rule(
            "User:mallory",
            "User:alice",
            AclOperation::CreateTokens,
        ))
        .await
        .unwrap();
    let response = handle_as(
        &broker,
        "mallory",
        false,
        ApiKey::CreateDelegationToken,
        3,
        211,
        &create_for_alice,
    )
    .await;
    let created: CreateDelegationTokenResponse =
        decode_response(ApiKey::CreateDelegationToken, 3, response);
    assert_eq!(created.error_code, NO_ERROR);
    assert_eq!(created.principal_name.as_str(), "alice");
    assert_eq!(created.token_requester_principal_name.as_str(), "mallory");

    let describe = DescribeDelegationTokenRequest::default().with_owners(None);
    let response = handle_as(
        &broker,
        "mallory",
        false,
        ApiKey::DescribeDelegationToken,
        3,
        212,
        &describe,
    )
    .await;
    let described: DescribeDelegationTokenResponse =
        decode_response(ApiKey::DescribeDelegationToken, 3, response);
    assert_eq!(described.tokens.len(), 1);

    broker
        .metadata
        .create_acl(user_rule(
            "User:viewer",
            "User:alice",
            AclOperation::DescribeTokens,
        ))
        .await
        .unwrap();
    let response = handle_as(
        &broker,
        "viewer",
        false,
        ApiKey::DescribeDelegationToken,
        3,
        213,
        &describe,
    )
    .await;
    let described: DescribeDelegationTokenResponse =
        decode_response(ApiKey::DescribeDelegationToken, 3, response);
    assert_eq!(described.tokens.len(), 1);

    let renew = RenewDelegationTokenRequest::default()
        .with_hmac(created.hmac.clone())
        .with_renew_period_ms(30_000);
    let response = handle_as(
        &broker,
        "mallory",
        false,
        ApiKey::RenewDelegationToken,
        2,
        214,
        &renew,
    )
    .await;
    let renewed: RenewDelegationTokenResponse =
        decode_response(ApiKey::RenewDelegationToken, 2, response);
    assert_eq!(renewed.error_code, NO_ERROR);

    let expire = ExpireDelegationTokenRequest::default()
        .with_hmac(created.hmac)
        .with_expiry_time_period_ms(-1);
    let response = handle_as(
        &broker,
        "mallory",
        false,
        ApiKey::ExpireDelegationToken,
        2,
        215,
        &expire,
    )
    .await;
    let expired: ExpireDelegationTokenResponse =
        decode_response(ApiKey::ExpireDelegationToken, 2, response);
    assert_eq!(expired.error_code, NO_ERROR);

    let invalid =
        CreateDelegationTokenRequest::default().with_renewers(vec![renewer("Service", "worker")]);
    let response = handle_as(
        &broker,
        "admin",
        false,
        ApiKey::CreateDelegationToken,
        3,
        216,
        &invalid,
    )
    .await;
    let invalid: CreateDelegationTokenResponse =
        decode_response(ApiKey::CreateDelegationToken, 3, response);
    assert_eq!(invalid.error_code, INVALID_PRINCIPAL_TYPE);
    assert_eq!(invalid.principal_name.as_str(), "admin");
    assert_eq!(invalid.token_requester_principal_name.as_str(), "admin");
    assert_eq!(invalid.issue_timestamp_ms, -1);
    assert_eq!(invalid.expiry_timestamp_ms, -1);
    assert_eq!(invalid.max_timestamp_ms, -1);

    let response = handle_as(
        &broker,
        "admin",
        true,
        ApiKey::CreateDelegationToken,
        3,
        217,
        &CreateDelegationTokenRequest::default(),
    )
    .await;
    let token_request: CreateDelegationTokenResponse =
        decode_response(ApiKey::CreateDelegationToken, 3, response);
    assert_eq!(
        token_request.error_code,
        DELEGATION_TOKEN_REQUEST_NOT_ALLOWED
    );
    assert_eq!(token_request.principal_name.as_str(), "admin");
    assert_eq!(
        token_request.token_requester_principal_name.as_str(),
        "admin"
    );
    assert_eq!(token_request.issue_timestamp_ms, -1);
    assert_eq!(token_request.expiry_timestamp_ms, -1);
    assert_eq!(token_request.max_timestamp_ms, -1);

    let renew = RenewDelegationTokenRequest::default()
        .with_hmac(Bytes::from_static(b"ignored"))
        .with_renew_period_ms(1_000);
    let response = handle_as(
        &broker,
        "admin",
        true,
        ApiKey::RenewDelegationToken,
        2,
        218,
        &renew,
    )
    .await;
    let renew: RenewDelegationTokenResponse =
        decode_response(ApiKey::RenewDelegationToken, 2, response);
    assert_eq!(renew.error_code, DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    assert_eq!(renew.expiry_timestamp_ms, -1);

    let expire = ExpireDelegationTokenRequest::default()
        .with_hmac(Bytes::from_static(b"ignored"))
        .with_expiry_time_period_ms(-1);
    let response = handle_as(
        &broker,
        "admin",
        true,
        ApiKey::ExpireDelegationToken,
        2,
        219,
        &expire,
    )
    .await;
    let expire: ExpireDelegationTokenResponse =
        decode_response(ApiKey::ExpireDelegationToken, 2, response);
    assert_eq!(expire.error_code, DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    assert_eq!(expire.expiry_timestamp_ms, -1);

    let response = broker
        .handle_request(request_frame(
            ApiKey::CreateDelegationToken,
            3,
            220,
            &CreateDelegationTokenRequest::default(),
        ))
        .await
        .unwrap();
    let anonymous: CreateDelegationTokenResponse =
        decode_response(ApiKey::CreateDelegationToken, 3, response);
    assert_eq!(anonymous.error_code, DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    assert_eq!(anonymous.principal_name.as_str(), "ANONYMOUS");
    assert_eq!(
        anonymous.token_requester_principal_name.as_str(),
        "ANONYMOUS"
    );
    assert_eq!(anonymous.issue_timestamp_ms, -1);
    assert_eq!(anonymous.expiry_timestamp_ms, -1);
    assert_eq!(anonymous.max_timestamp_ms, -1);
}
