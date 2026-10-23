//! transport-api 服务端：消费 `tb_transport.api.requests`，校验设备凭证并回响应到
//! `tb_transport.api.responses.<node_id>`（node_id 取请求消息 key，request_id 原样带回）。

use bytes::Bytes;
use prost::Message as _;
use tb_common::{EntityType, TbError, TbResult};
use tb_dao::models::{
    CredentialsType, Device, DeviceCredentials, DeviceProfile, EntityRelation, OtaPackage,
};
use tb_proto::transport as pb;
use tb_queue::topics::TbTopic;
use tb_queue::{KafkaQueueProducer, QueueConsumer, QueueProducer as _, StreamQueueConsumer};
use uuid::Uuid;

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
        let mut consumer =
            match StreamQueueConsumer::new(&brokers, &group_id, &[TbTopic::TRANSPORT_API_REQUESTS])
            {
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
                    let Ok(req) = pb::TransportApiRequestMsg::decode(msg.payload) else {
                        tracing::warn!("undecodable transport-api request");
                        continue;
                    };
                    let response = handle_request(&state, req.request).await;
                    let out = pb::TransportApiResponseMsg {
                        request_id: req.request_id,
                        response: Some(response),
                    };
                    let topic = TbTopic::transport_api_responses(&node_id);
                    if let Err(e) = producer
                        .send(&topic, &node_id, Bytes::from(out.encode_to_vec()))
                        .await
                    {
                        tracing::error!("transport-api response send failed: {e}");
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
        Some(Request::ValidateX509(_)) => {
            pb::transport_api_response_msg::Response::ValidateCredentials(
                pb::ValidateCredentialsResponse {
                    valid: false,
                    device: None,
                    profile: None,
                    credentials_body: String::new(),
                },
            )
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
                    && body["password"].as_str() == Some(r.password.as_str())
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
    let (saved, _) = state
        .device_credentials_dao
        .save_device_with_credentials(&device, &credentials)
        .await?;
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

/// OTA 分块：越界/包不存在返回空 data。
async fn get_ota_chunk(state: &AppState, r: &pb::GetOtaChunkRequest) -> pb::GetOtaChunkResponse {
    let empty = pb::GetOtaChunkResponse { data: Vec::new() };
    let Ok(id) = Uuid::parse_str(&r.ota_package_id) else {
        return empty;
    };
    let Ok(Some(pkg)) = state.ota_package_dao.find_by_id(id).await else {
        return empty;
    };
    let Some(data) = pkg.data else { return empty };
    let chunk_size = r.chunk_size.max(1) as usize;
    let start = r.chunk_index.max(0) as usize * chunk_size;
    if start >= data.len() {
        return empty;
    }
    let end = (start + chunk_size).min(data.len());
    pb::GetOtaChunkResponse {
        data: data[start..end].to_vec(),
    }
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
