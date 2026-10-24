//! transport-api 服务端：消费 `tb_transport.api.requests`，校验设备凭证并回响应到
//! `tb_transport.api.responses.<node_id>`（node_id 取请求消息 key，request_id 原样带回）。

use bytes::Bytes;
use prost::Message as _;
use rustls_pki_types::{CertificateDer, UnixTime};
use tb_common::{EntityType, TbError, TbResult};
use tb_dao::models::{
    CredentialsType, Device, DeviceCredentials, DeviceProfile, EntityRelation, OtaPackage,
};
use tb_proto::transport as pb;
use tb_queue::topics::TbTopic;
use tb_queue::{KafkaQueueProducer, QueueConsumer, QueueProducer as _, StreamQueueConsumer};
use uuid::Uuid;
use webpki::{EndEntityCert, KeyUsage, anchor_from_trusted_cert};

use crate::AppState;

const REQUESTS_PARTITIONS: i32 = 10;

pub async fn start_transport_api(state: AppState) -> tokio::task::JoinHandle<()> {
    start_transport_api_for_group(state, "tb-core-transport-api").await
}

/// group_id 可定制：集成测试用独立 group 避免幽灵成员拖慢 rebalance。
pub async fn start_transport_api_for_group(
    state: AppState,
    group_id: &str,
) -> tokio::task::JoinHandle<()> {
    let brokers = std::env::var("TB_KAFKA_BROKERS").unwrap_or_else(|_| "localhost:29092".into());
    if let Err(e) = tb_queue::admin::ensure_topic(
        &brokers,
        TbTopic::TRANSPORT_API_REQUESTS,
        REQUESTS_PARTITIONS,
    )
    .await
    {
        tracing::warn!("ensure transport-api requests topic failed: {e}");
    }
    if let Err(e) =
        tb_queue::admin::ensure_topic(&brokers, &TbTopic::rule_engine("Main"), REQUESTS_PARTITIONS)
            .await
    {
        tracing::warn!("ensure rule engine topic failed: {e}");
    }
    let group_id = group_id.to_string();
    tokio::spawn(async move {
        let producer = match KafkaQueueProducer::new(&brokers, &group_id) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("transport-api producer init failed: {e}");
                return;
            }
        };
        let mut consumer = match StreamQueueConsumer::new_manual_commit(
            &brokers,
            &group_id,
            &[TbTopic::TRANSPORT_API_REQUESTS],
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!("transport-api consumer init failed: {e}");
                return;
            }
        };
        tracing::info!("transport-api server started");
        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    let node_id = msg
                        .key
                        .as_ref()
                        .map(|k| String::from_utf8_lossy(k).into_owned())
                        .unwrap_or_default();
                    if node_id.is_empty() {
                        tracing::warn!("transport-api request without node_id key, dropped");
                        continue;
                    }
                    let Ok(req) = pb::TransportApiRequestMsg::decode(msg.payload.clone()) else {
                        tracing::warn!("undecodable transport-api request");
                        continue;
                    };
                    let response = handle_request(&state, req.request).await;
                    let out = pb::TransportApiResponseMsg {
                        request_id: req.request_id,
                        response: Some(response),
                    };
                    let topic = TbTopic::transport_api_responses(&node_id);
                    match producer
                        .send(&topic, &node_id, Bytes::from(out.encode_to_vec()))
                        .await
                    {
                        Ok(()) => {
                            if let Err(e) = consumer.commit(&msg).await {
                                tracing::error!("transport-api commit failed: {e}");
                            }
                        }
                        Err(e) => {
                            tracing::error!("transport-api response send failed: {e}");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("transport-api consumer error: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    })
}

async fn handle_request(
    state: &AppState,
    request: Option<pb::transport_api_request_msg::Request>,
) -> pb::transport_api_response_msg::Response {
    use pb::transport_api_request_msg::Request;
    match request {
        Some(Request::ValidateToken(r)) => {
            pb::transport_api_response_msg::Response::ValidateCredentials(
                validate_token(state, &r.token).await,
            )
        }
        Some(Request::ValidateMqttBasic(r)) => {
            pb::transport_api_response_msg::Response::ValidateCredentials(
                validate_mqtt_basic(state, &r).await,
            )
        }
        Some(Request::ValidateX509(r)) => {
            pb::transport_api_response_msg::Response::ValidateCredentials(
                validate_x509(state, &r.cert_hash).await,
            )
        }
        Some(Request::ValidateOrCreateX509(r)) => {
            pb::transport_api_response_msg::Response::ValidateCredentials(
                validate_or_create_x509(state, &r).await,
            )
        }
        Some(Request::ValidateLwm2m(r)) => {
            pb::transport_api_response_msg::Response::ValidateCredentials(
                validate_lwm2m(state, &r.endpoint).await,
            )
        }
        Some(Request::GetDeviceProfile(r)) => match get_device_profile(state, &r).await {
            Ok(Some(profile)) => pb::transport_api_response_msg::Response::DeviceProfile(profile),
            Ok(None) => pb::transport_api_response_msg::Response::Error(pb::TransportApiError {
                code: 404,
                message: "device profile not found".into(),
            }),
            Err(e) => {
                tracing::warn!("get_device_profile failed: {e}");
                pb::transport_api_response_msg::Response::Error(pb::TransportApiError {
                    code: 500,
                    message: e.to_string(),
                })
            }
        },
        Some(Request::GetDevicesByTransport(r)) => {
            match get_devices_by_transport(state, &r).await {
                Ok(devices) => {
                    pb::transport_api_response_msg::Response::GetDevicesByTransportResponse(devices)
                }
                Err(e) => {
                    tracing::warn!("get_devices_by_transport failed: {e}");
                    pb::transport_api_response_msg::Response::Error(pb::TransportApiError {
                        code: 500,
                        message: e.to_string(),
                    })
                }
            }
        }
        Some(Request::GetAttributes(r)) => {
            pb::transport_api_response_msg::Response::GetAttributesResponse(
                get_attributes(state, &r).await,
            )
        }
        Some(Request::GetOrCreateGatewayDevice(r)) => {
            pb::transport_api_response_msg::Response::GatewayDevice(
                match get_or_create_gateway_device(state, &r).await {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!("get_or_create_gateway_device failed: {e}");
                        return pb::transport_api_response_msg::Response::Error(
                            pb::TransportApiError {
                                code: 500,
                                message: e.to_string(),
                            },
                        );
                    }
                },
            )
        }
        Some(Request::GetOtaPackage(r)) => match get_ota_package(state, &r).await {
            Ok(Some(p)) => pb::transport_api_response_msg::Response::OtaPackage(p),
            Ok(None) => pb::transport_api_response_msg::Response::Error(pb::TransportApiError {
                code: 404,
                message: "no ota package".into(),
            }),
            Err(e) => {
                tracing::warn!("get_ota_package failed: {e}");
                pb::transport_api_response_msg::Response::Error(pb::TransportApiError {
                    code: 500,
                    message: e.to_string(),
                })
            }
        },
        Some(Request::GetOtaChunk(r)) => {
            pb::transport_api_response_msg::Response::OtaChunk(get_ota_chunk(state, &r).await)
        }
        Some(Request::ProvisionDevice(r)) => {
            pb::transport_api_response_msg::Response::ProvisionResponse(
                provision_device(state, &r).await,
            )
        }
        other => {
            tracing::debug!(?other, "unsupported transport-api request");
            pb::transport_api_response_msg::Response::Error(pb::TransportApiError {
                code: 501,
                message: "not implemented".into(),
            })
        }
    }
}

fn invalid_credentials() -> pb::ValidateCredentialsResponse {
    pb::ValidateCredentialsResponse {
        valid: false,
        device: None,
        profile: None,
        credentials_body: String::new(),
    }
}

/// 设备属性同步查询（HTTP transport GET attributes 用）：client/shared 两组，带 key 字符串。
async fn get_attributes(
    state: &AppState,
    r: &pb::GetAttributesRequest,
) -> pb::GetAttributesResponse {
    let empty = pb::GetAttributesResponse::default();
    let Ok(device_id) = uuid::Uuid::parse_str(&r.device_id) else {
        tracing::warn!(device_id = r.device_id, "get_attributes with bad device_id");
        return empty;
    };
    let filter = |keys: &[String]| (!keys.is_empty()).then(|| keys.to_vec());
    let client = state
        .attributes_dao
        .find(
            device_id,
            tb_common::AttributeScope::ClientScope,
            filter(&r.client_keys).as_deref(),
        )
        .await;
    let shared = state
        .attributes_dao
        .find(
            device_id,
            tb_common::AttributeScope::SharedScope,
            filter(&r.shared_keys).as_deref(),
        )
        .await;
    let (Ok(client), Ok(shared)) = (client, shared) else {
        tracing::warn!("get_attributes dao failed");
        return empty;
    };
    let to_proto = |entries: Vec<tb_common::KvEntry>| -> Vec<tb_proto::common::KeyValueProto> {
        entries
            .into_iter()
            .map(|e| tb_proto::convert::kv_to_proto_named(0, &e.key, &e.value))
            .collect()
    };
    pb::GetAttributesResponse {
        client_attributes: to_proto(client),
        shared_attributes: to_proto(shared),
    }
}

/// 凭证命中后组装设备+档案；任一环节缺失即无效。
async fn build_valid_response(
    state: &AppState,
    credentials: &DeviceCredentials,
) -> pb::ValidateCredentialsResponse {
    let device = state.device_dao.find_by_id(credentials.device_id).await;
    let device = match device {
        Ok(Some(d)) => d,
        _ => return invalid_credentials(),
    };
    let profile = state
        .device_profile_dao
        .find_by_id(device.device_profile_id)
        .await;
    let profile = match profile {
        Ok(Some(p)) => p,
        _ => return invalid_credentials(),
    };
    pb::ValidateCredentialsResponse {
        valid: true,
        device: Some(device_proto(&device)),
        profile: Some(profile_proto(&profile)),
        credentials_body: credentials.credentials_value.clone().unwrap_or_default(),
    }
}

fn device_proto(d: &Device) -> pb::DeviceInfoProto {
    pb::DeviceInfoProto {
        device_id: d.id.to_string(),
        tenant_id: d.tenant_id.to_string(),
        customer_id: d.customer_id.map(|c| c.to_string()).unwrap_or_default(),
        device_profile_id: d.device_profile_id.to_string(),
        device_name: d.name.clone(),
        device_type: d.r#type.clone().unwrap_or_else(|| "default".into()),
        additional_info_json: d
            .additional_info
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_default(),
        device_data_json: d.device_data.to_string(),
    }
}

fn profile_proto(p: &DeviceProfile) -> pb::DeviceProfileProto {
    pb::DeviceProfileProto {
        device_profile_id: p.id.to_string(),
        tenant_id: p.tenant_id.to_string(),
        name: p.name.clone(),
        transport_type: p.transport_type.clone(),
        provision_enabled: p.provision_type != "DISABLED",
        profile_data_json: p.profile_data.to_string(),
    }
}

pub async fn get_device_profile(
    state: &AppState,
    request: &pb::GetDeviceProfileRequest,
) -> TbResult<Option<pb::DeviceProfileProto>> {
    let profile_id = Uuid::parse_str(&request.device_profile_id)
        .map_err(|_| TbError::InvalidInput("bad device_profile_id".into()))?;
    Ok(state
        .device_profile_dao
        .find_by_id(profile_id)
        .await?
        .as_ref()
        .map(profile_proto))
}

pub async fn get_devices_by_transport(
    state: &AppState,
    request: &pb::GetDevicesByTransportRequest,
) -> TbResult<pb::GetDevicesByTransportResponse> {
    let tenant_id = if request.tenant_id.is_empty() {
        None
    } else {
        Some(
            Uuid::parse_str(&request.tenant_id)
                .map_err(|_| TbError::InvalidInput("bad tenant_id".into()))?,
        )
    };
    let page_link = tb_dao::PageLink::new(i64::from(request.page), i64::from(request.page_size));
    let page = state
        .device_dao
        .find_by_transport_type(tenant_id, &request.transport_type, &page_link)
        .await?;
    Ok(pb::GetDevicesByTransportResponse {
        devices: page.data.iter().map(device_proto).collect(),
        total_elements: page.total_elements,
        total_pages: i32::try_from(page.total_pages).unwrap_or(i32::MAX),
        has_next: page.has_next,
    })
}

pub async fn validate_lwm2m(state: &AppState, endpoint: &str) -> pb::ValidateCredentialsResponse {
    let Some(credentials) = find_credentials(state, endpoint).await else {
        return invalid_credentials();
    };
    if credentials.credentials_type != CredentialsType::Lwm2m {
        return invalid_credentials();
    }
    let endpoint_matches = credentials
        .credentials_value
        .as_deref()
        .and_then(|body| serde_json::from_str::<serde_json::Value>(body).ok())
        .is_some_and(|body| {
            body["endpoint"].as_str() == Some(endpoint)
                && body["securityConfigClientMode"].as_str() == Some("NO_SEC")
        });
    if !endpoint_matches {
        return invalid_credentials();
    }
    build_valid_response(state, &credentials).await
}

async fn find_credentials(state: &AppState, credentials_id: &str) -> Option<DeviceCredentials> {
    match state
        .device_credentials_dao
        .find_by_credentials_id(credentials_id)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("credentials lookup failed: {e}");
            None
        }
    }
}

async fn validate_token(state: &AppState, token: &str) -> pb::ValidateCredentialsResponse {
    let Some(credentials) = find_credentials(state, token).await else {
        return invalid_credentials();
    };
    if credentials.credentials_type != CredentialsType::AccessToken {
        return invalid_credentials();
    }
    build_valid_response(state, &credentials).await
}

async fn validate_x509(state: &AppState, cert_hash: &str) -> pb::ValidateCredentialsResponse {
    let Some(credentials) = find_credentials(state, cert_hash).await else {
        return invalid_credentials();
    };
    if credentials.credentials_type != CredentialsType::X509Certificate {
        return invalid_credentials();
    }
    build_valid_response(state, &credentials).await
}

fn verify_x509_client_chain(
    chain: &[Vec<u8>],
    trusted_certificate: &[u8],
    time: UnixTime,
) -> Result<(), webpki::Error> {
    if chain.len() < 2 {
        return Err(webpki::Error::UnknownIssuer);
    }
    let leaf_der = CertificateDer::from(chain[0].as_slice());
    let leaf = EndEntityCert::try_from(&leaf_der)?;
    let trusted_der = CertificateDer::from(trusted_certificate);
    let trust_anchor = anchor_from_trusted_cert(&trusted_der)?;
    let intermediates = chain[1..chain.len() - 1]
        .iter()
        .map(|certificate| CertificateDer::from(certificate.as_slice()))
        .collect::<Vec<_>>();
    leaf.verify_for_usage(
        webpki::ALL_VERIFICATION_ALGS,
        std::slice::from_ref(&trust_anchor),
        &intermediates,
        time,
        KeyUsage::client_auth(),
        None,
        Some(&|path: &webpki::VerifiedPath<'_>| {
            if path.intermediate_certificates().count() == intermediates.len() {
                Ok(())
            } else {
                Err(webpki::Error::UnknownIssuer)
            }
        }),
    )
    .map(|_| ())
}

fn x509_device_name(leaf_der: &[u8], pattern: &regex::Regex) -> TbResult<String> {
    let (remaining, certificate) = x509_parser::parse_x509_certificate(leaf_der)
        .map_err(|_| TbError::InvalidInput("invalid X509 leaf certificate".into()))?;
    if !remaining.is_empty() {
        return Err(TbError::InvalidInput(
            "invalid X509 leaf certificate".into(),
        ));
    }
    let common_name = certificate
        .subject()
        .iter_common_name()
        .next()
        .ok_or_else(|| TbError::InvalidInput("X509 leaf CN is missing".into()))?
        .as_str()
        .map_err(|_| TbError::InvalidInput("X509 leaf CN is not valid UTF-8".into()))?;
    let name = pattern
        .captures(common_name)
        .and_then(|captures| captures.get(1))
        .map(|name| name.as_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            TbError::InvalidInput("X509 leaf CN does not match capture group 1".into())
        })?;
    Ok(name.to_string())
}

async fn x509_existing_device_response(
    state: &AppState,
    device: Device,
    profile: &DeviceProfile,
    leaf: &tb_common::x509::X509Credential,
) -> pb::ValidateCredentialsResponse {
    if device.device_profile_id != profile.id {
        return invalid_credentials();
    }
    let credentials = match state
        .device_credentials_dao
        .find_by_device_id(device.id)
        .await
    {
        Ok(Some(credentials)) => credentials,
        _ => return invalid_credentials(),
    };
    if credentials.credentials_type != CredentialsType::X509Certificate {
        return invalid_credentials();
    }
    let rotated = DeviceCredentials {
        id: Uuid::now_v7(),
        created_time: 0,
        device_id: device.id,
        credentials_type: CredentialsType::X509Certificate,
        credentials_id: Some(leaf.credentials_id.clone()),
        credentials_value: Some(leaf.certificate_base64.clone()),
    };
    match state.device_credentials_dao.save(&rotated).await {
        Ok(credentials) => build_valid_response(state, &credentials).await,
        Err(e) => {
            tracing::warn!(device_id = %device.id, "X509 credential rotation failed: {e}");
            invalid_credentials()
        }
    }
}

pub async fn validate_or_create_x509(
    state: &AppState,
    request: &pb::ValidateOrCreateX509Request,
) -> pb::ValidateCredentialsResponse {
    let chain = &request.certificate_chain;
    if tb_common::x509::validate_x509_chain_limits(chain).is_err() {
        return invalid_credentials();
    }
    let leaf = match tb_common::x509::x509_credential_from_der(&chain[0]) {
        Ok(leaf) => leaf,
        Err(_) => return invalid_credentials(),
    };
    if let Some(credentials) = find_credentials(state, &leaf.credentials_id).await {
        if credentials.credentials_type != CredentialsType::X509Certificate {
            return invalid_credentials();
        }
        return build_valid_response(state, &credentials).await;
    }
    let chain_tail_hash = match tb_common::x509::x509_cert_hash_from_der(&chain[chain.len() - 1]) {
        Ok(hash) => hash,
        Err(_) => return invalid_credentials(),
    };
    let profiles = match state
        .device_profile_dao
        .find_x509_chain_by_certificate_hash(&chain_tail_hash)
        .await
    {
        Ok(profiles) => profiles,
        Err(e) => {
            tracing::warn!("X509 chain profile lookup failed: {e}");
            return invalid_credentials();
        }
    };
    let [profile] = profiles.as_slice() else {
        return invalid_credentials();
    };
    let configuration =
        match tb_common::x509::x509_chain_provision_configuration(&profile.profile_data) {
            Ok(configuration) if configuration.provision_device_key == chain_tail_hash => {
                configuration
            }
            _ => return invalid_credentials(),
        };
    if let Err(e) = verify_x509_client_chain(chain, &configuration.certificate_der, UnixTime::now())
    {
        tracing::debug!(profile_id = %profile.id, "X509 client path rejected: {e}");
        return invalid_credentials();
    }
    let device_name = match x509_device_name(&chain[0], &configuration.certificate_regex) {
        Ok(name) => name,
        Err(_) => return invalid_credentials(),
    };

    for _ in 0..2 {
        match state
            .device_dao
            .find_by_name(profile.tenant_id, &device_name)
            .await
        {
            Ok(Some(device)) => {
                return x509_existing_device_response(state, device, profile, &leaf).await;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("X509 provision device lookup failed: {e}");
                return invalid_credentials();
            }
        }
        if !configuration.allow_create_new_devices {
            return invalid_credentials();
        }
        let device = Device {
            id: Uuid::now_v7(),
            created_time: 0,
            tenant_id: profile.tenant_id,
            customer_id: None,
            device_profile_id: profile.id,
            name: device_name.clone(),
            r#type: None,
            label: None,
            external_id: None,
            firmware_id: None,
            software_id: None,
            additional_info: None,
            device_data: serde_json::json!({}),
        };
        let credentials = DeviceCredentials {
            id: Uuid::now_v7(),
            created_time: 0,
            device_id: device.id,
            credentials_type: CredentialsType::X509Certificate,
            credentials_id: Some(leaf.credentials_id.clone()),
            credentials_value: Some(leaf.certificate_base64.clone()),
        };
        match state
            .device_credentials_dao
            .save_device_with_credentials(&device, &credentials)
            .await
        {
            Ok((_, credentials)) => return build_valid_response(state, &credentials).await,
            Err(e) => {
                tracing::debug!("X509 provision create conflict or failure, rereading: {e}");
            }
        }
    }
    invalid_credentials()
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    let mut difference = left.len() ^ right.len();
    for i in 0..left.len().max(right.len()) {
        difference |=
            usize::from(left.get(i).copied().unwrap_or(0) ^ right.get(i).copied().unwrap_or(0));
    }
    difference == 0
}

/// Java 兼容语义：先按 MQTT_BASIC 的 credentials_id = "{clientId}|{userName}" 查并比对
/// credentials_value JSON 里的 password；查不到再按 userName 当 ACCESS_TOKEN 查。
async fn validate_mqtt_basic(
    state: &AppState,
    r: &pb::ValidateMqttBasicRequest,
) -> pb::ValidateCredentialsResponse {
    let key = format!("{}|{}", r.client_id, r.username);
    if let Some(credentials) = find_credentials(state, &key).await
        && credentials.credentials_type == CredentialsType::MqttBasic
    {
        let ok = credentials
            .credentials_value
            .as_deref()
            .and_then(|v| serde_json::from_str::<serde_json::Value>(v).ok())
            .is_some_and(|body| {
                body["clientId"].as_str() == Some(r.client_id.as_str())
                    && body["userName"].as_str() == Some(r.username.as_str())
                    && body["password"]
                        .as_str()
                        .is_some_and(|password| constant_time_eq(password, &r.password))
            });
        if ok {
            return build_valid_response(state, &credentials).await;
        }
        return invalid_credentials();
    }
    if let Some(credentials) = find_credentials(state, &r.username).await
        && credentials.credentials_type == CredentialsType::AccessToken
    {
        return build_valid_response(state, &credentials).await;
    }
    invalid_credentials()
}

/// 网关子设备 get-or-create：按 (tenant, name) 查；无则以默认档案 + 随机 ACCESS_TOKEN 创建，
/// 并建 gateway→device "Created" relation（失败仅告警，不阻塞）。
async fn get_or_create_gateway_device(
    state: &AppState,
    r: &pb::GetOrCreateGatewayDeviceRequest,
) -> TbResult<pb::DeviceInfoProto> {
    let gateway_id = Uuid::parse_str(&r.gateway_id)
        .map_err(|_| TbError::InvalidInput("bad gateway_id".into()))?;
    let tenant_id =
        Uuid::parse_str(&r.tenant_id).map_err(|_| TbError::InvalidInput("bad tenant_id".into()))?;
    if let Some(device) = state
        .device_dao
        .find_by_name(tenant_id, &r.device_name)
        .await?
    {
        return Ok(device_proto(&device));
    }
    let profile = crate::routes::device::ensure_default_device_profile(state, tenant_id).await?;
    let device = Device {
        id: Uuid::now_v7(),
        created_time: 0,
        tenant_id,
        customer_id: None,
        device_profile_id: profile.id,
        name: r.device_name.clone(),
        r#type: (!r.device_type.is_empty()).then(|| r.device_type.clone()),
        label: None,
        external_id: None,
        firmware_id: None,
        software_id: None,
        additional_info: None,
        device_data: serde_json::json!({}),
    };
    let credentials = DeviceCredentials {
        id: Uuid::now_v7(),
        created_time: 0,
        device_id: device.id,
        credentials_type: CredentialsType::AccessToken,
        credentials_id: Some(Uuid::now_v7().to_string()),
        credentials_value: None,
    };
    let (saved, _) = match state
        .device_credentials_dao
        .save_device_with_credentials(&device, &credentials)
        .await
    {
        Ok(saved) => saved,
        Err(e) => {
            if let Some(existing) = state
                .device_dao
                .find_by_name(tenant_id, &r.device_name)
                .await?
            {
                return Ok(device_proto(&existing));
            }
            return Err(e);
        }
    };
    let relation = EntityRelation {
        from_id: gateway_id,
        from_type: EntityType::Device,
        to_id: saved.id,
        to_type: EntityType::Device,
        relation_type_group: "COMMON".into(),
        relation_type: "Created".into(),
        additional_info: None,
    };
    if let Err(e) = state.relation_dao.save(&relation).await {
        tracing::warn!("save gateway relation failed: {e}");
    }
    Ok(device_proto(&saved))
}

pub(crate) fn ota_proto(p: &OtaPackage) -> pb::OtaPackageProto {
    pb::OtaPackageProto {
        ota_package_id: p.id.to_string(),
        title: p.title.clone(),
        version: p.version.clone(),
        tag: p.tag.clone().unwrap_or_default(),
        file_name: p.file_name.clone().unwrap_or_default(),
        content_type: p.content_type.clone().unwrap_or_default(),
        checksum: p.checksum.clone().unwrap_or_default(),
        size: p.size.unwrap_or(0),
    }
}

/// 该档案下指定类型、与设备当前版本不同的最新 OTA 包（不含 data）。
async fn get_ota_package(
    state: &AppState,
    r: &pb::GetOtaPackageRequest,
) -> TbResult<Option<pb::OtaPackageProto>> {
    use tb_proto::common::OtaPackageType;
    let tenant_id =
        Uuid::parse_str(&r.tenant_id).map_err(|_| TbError::InvalidInput("bad tenant_id".into()))?;
    let profile_id = Uuid::parse_str(&r.device_profile_id)
        .map_err(|_| TbError::InvalidInput("bad device_profile_id".into()))?;
    let ota_type = match OtaPackageType::try_from(r.r#type) {
        Ok(OtaPackageType::Firmware) => "FIRMWARE",
        Ok(OtaPackageType::Software) => "SOFTWARE",
        _ => return Ok(None),
    };
    let pkg = state
        .ota_package_dao
        .find_latest(
            tenant_id,
            profile_id,
            ota_type,
            &r.current_title,
            &r.current_version,
        )
        .await?;
    Ok(pkg.as_ref().map(ota_proto))
}

const MAX_OTA_CHUNK_SIZE: i32 = 1024 * 1024;

fn ota_chunk_location(chunk_index: i32, chunk_size: i32) -> Option<(i32, i32)> {
    let chunk_size = chunk_size.clamp(1, MAX_OTA_CHUNK_SIZE);
    let start = i64::from(chunk_index.max(0)) * i64::from(chunk_size);
    let sql_start = i32::try_from(start.saturating_add(1)).ok()?;
    Some((sql_start, chunk_size))
}

/// OTA 分块：越界/包不存在返回空 data。
async fn get_ota_chunk(state: &AppState, r: &pb::GetOtaChunkRequest) -> pb::GetOtaChunkResponse {
    let empty = pb::GetOtaChunkResponse { data: Vec::new() };
    let Ok(id) = Uuid::parse_str(&r.ota_package_id) else {
        return empty;
    };
    let Some((sql_start, chunk_size)) = ota_chunk_location(r.chunk_index, r.chunk_size) else {
        return empty;
    };
    let data = state
        .ota_package_dao
        .find_data_chunk(id, sql_start, chunk_size)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(%id, "read OTA chunk failed: {e}");
            None
        })
        .unwrap_or_default();
    pb::GetOtaChunkResponse { data }
}

/// 设备 provisioning：profile_data.provisionConfiguration 校验 key/secret；
/// ALLOW_CREATE_NEW_DEVICES 可就地建设备，CHECK_PRE_PROVISIONED_DEVICES 要求设备已存在。
async fn provision_device(
    state: &AppState,
    r: &pb::ProvisionDeviceRequest,
) -> pb::ProvisionDeviceResponse {
    let failure = |msg: &str| pb::ProvisionDeviceResponse {
        status: "FAILURE".into(),
        credentials_type: String::new(),
        credentials_value: String::new(),
        device_name: r.device_name.clone(),
        error_msg: msg.into(),
    };
    let tenant_id = if r.tenant_id.is_empty() {
        None
    } else {
        match Uuid::parse_str(&r.tenant_id) {
            Ok(id) => Some(id),
            Err(_) => return failure("bad tenant_id"),
        }
    };
    let profile = match state
        .device_profile_dao
        .find_by_provision_key(tenant_id, &r.provision_device_key)
        .await
    {
        Ok(Some(p)) => p,
        Ok(None) => return failure("provision key not found"),
        Err(e) => {
            tracing::warn!("provision profile lookup failed: {e}");
            return failure("internal error");
        }
    };
    let configured_secret = profile
        .profile_data
        .get("provisionConfiguration")
        .and_then(|c| c.get("provisionDeviceSecret"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if configured_secret != r.provision_device_secret {
        return failure("invalid provision secret");
    }
    let device = match state
        .device_dao
        .find_by_name(profile.tenant_id, &r.device_name)
        .await
    {
        Ok(Some(d)) => d,
        Ok(None) => {
            if profile.provision_type != "ALLOW_CREATE_NEW_DEVICES" {
                return failure("device not pre-provisioned");
            }
            let device = Device {
                id: Uuid::now_v7(),
                created_time: 0,
                tenant_id: profile.tenant_id,
                customer_id: None,
                device_profile_id: profile.id,
                name: r.device_name.clone(),
                r#type: None,
                label: None,
                external_id: None,
                firmware_id: None,
                software_id: None,
                additional_info: None,
                device_data: serde_json::json!({}),
            };
            let credentials = DeviceCredentials {
                id: Uuid::now_v7(),
                created_time: 0,
                device_id: device.id,
                credentials_type: CredentialsType::AccessToken,
                credentials_id: Some(Uuid::now_v7().to_string()),
                credentials_value: None,
            };
            match state
                .device_credentials_dao
                .save_device_with_credentials(&device, &credentials)
                .await
            {
                Ok((d, _)) => d,
                Err(e) => {
                    tracing::warn!("provision create device failed: {e}");
                    return failure("internal error");
                }
            }
        }
        Err(e) => {
            tracing::warn!("provision device lookup failed: {e}");
            return failure("internal error");
        }
    };
    match state
        .device_credentials_dao
        .find_by_device_id(device.id)
        .await
    {
        Ok(Some(c)) => pb::ProvisionDeviceResponse {
            status: "SUCCESS".into(),
            credentials_type: c.credentials_type.as_str().into(),
            credentials_value: c.credentials_id.clone().unwrap_or_default(),
            device_name: device.name.clone(),
            error_msg: String::new(),
        },
        _ => failure("credentials missing"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mqtt_password_comparison_handles_content_and_length() {
        assert!(constant_time_eq("secret", "secret"));
        assert!(constant_time_eq("密码", "密码"));
        assert!(!constant_time_eq("secret", "secreu"));
        assert!(!constant_time_eq("secret", "secret2"));
        assert!(!constant_time_eq("", "non-empty"));
    }

    #[test]
    fn ota_chunk_size_is_bounded_and_offset_is_checked() {
        assert_eq!(ota_chunk_location(1, 4096), Some((4097, 4096)));
        assert_eq!(
            ota_chunk_location(0, MAX_OTA_CHUNK_SIZE * 2),
            Some((1, MAX_OTA_CHUNK_SIZE))
        );
        assert_eq!(ota_chunk_location(-1, -1), Some((1, 1)));
        assert_eq!(ota_chunk_location(i32::MAX, MAX_OTA_CHUNK_SIZE), None);
    }

    struct TestCa {
        certificate: rcgen::Certificate,
        key: rcgen::KeyPair,
    }

    fn test_ca(common_name: &str) -> TestCa {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap();
        TestCa { certificate, key }
    }

    fn test_leaf(ca: &TestCa, common_name: &str, expired: bool) -> Vec<u8> {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ClientAuth];
        if expired {
            params.not_before = rcgen::date_time_ymd(2020, 1, 1);
            params.not_after = rcgen::date_time_ymd(2021, 1, 1);
        }
        let key = rcgen::KeyPair::generate().unwrap();
        params
            .signed_by(&key, &ca.certificate, &ca.key)
            .unwrap()
            .der()
            .to_vec()
    }

    #[test]
    fn webpki_client_auth_path_rejects_wrong_missing_expired_and_tampered_certificates() {
        let ca = test_ca("root");
        let other_ca = test_ca("other-root");
        let leaf = test_leaf(&ca, "device-1", false);
        let chain = vec![leaf.clone(), ca.certificate.der().to_vec()];
        assert!(
            verify_x509_client_chain(&chain, ca.certificate.der().as_ref(), UnixTime::now())
                .is_ok()
        );
        assert!(
            verify_x509_client_chain(&chain, other_ca.certificate.der().as_ref(), UnixTime::now())
                .is_err()
        );
        let unrelated = vec![
            leaf.clone(),
            other_ca.certificate.der().to_vec(),
            ca.certificate.der().to_vec(),
        ];
        assert!(
            verify_x509_client_chain(&unrelated, ca.certificate.der().as_ref(), UnixTime::now())
                .is_err()
        );
        assert!(
            verify_x509_client_chain(
                std::slice::from_ref(&leaf),
                ca.certificate.der().as_ref(),
                UnixTime::now()
            )
            .is_err()
        );

        let expired = vec![
            test_leaf(&ca, "expired", true),
            ca.certificate.der().to_vec(),
        ];
        assert!(
            verify_x509_client_chain(&expired, ca.certificate.der().as_ref(), UnixTime::now())
                .is_err()
        );

        let mut tampered = leaf;
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        let tampered = vec![tampered, ca.certificate.der().to_vec()];
        assert!(
            verify_x509_client_chain(&tampered, ca.certificate.der().as_ref(), UnixTime::now())
                .is_err()
        );
    }

    #[test]
    fn common_name_requires_regex_capture_group_one() {
        let ca = test_ca("root");
        let leaf = test_leaf(&ca, "prefix-device-42", false);
        let pattern = regex::Regex::new("^prefix-(.*)$").unwrap();
        assert_eq!(x509_device_name(&leaf, &pattern).unwrap(), "device-42");
        let mismatch = regex::Regex::new("^other-(.*)$").unwrap();
        assert!(x509_device_name(&leaf, &mismatch).is_err());
        let empty = regex::Regex::new("^prefix-(.*)-42$").unwrap();
        assert_eq!(x509_device_name(&leaf, &empty).unwrap(), "device");
    }
}
