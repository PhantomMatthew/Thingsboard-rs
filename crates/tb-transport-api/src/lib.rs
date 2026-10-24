//! tb-transport-api：transport 节点侧共享客户端。
//!
//! - 同步请求/响应：validate_* 通过 `tb_transport.api.requests` 发往 tb-core，
//!   响应走 `tb_transport.api.responses.<node_id>`，后台 task 按 request_id 路由到 oneshot。
//! - 异步上行：publish_* 把遥测/属性/RPC 封装为 ToRuleEngineMsg 发到 `tb_rule_engine.Main`
//!   （key=device_id，保证同设备分区有序）。
//! - 租户级限流：`TB_TRANSPORT_RATE_LIMIT_PER_TENANT`（msg/s，默认 0=不限），
//!   超限返回 TbError::RateLimitExceeded。

pub mod rate_limit;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use tb_common::msg::msg_type;
use tb_common::{KvEntry, TbError, TbResult, TsKvEntry};
use tb_proto::ruleengine::{TbMsgProto, ToRuleEngineMsg};
use tb_proto::transport as pb;
use tb_queue::{
    KafkaQueueProducer, QueueConsumer as _, QueueProducer as _, StreamQueueConsumer, TbTopic,
};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::rate_limit::RateLimiter;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CACHE_TTL: Duration = Duration::from_secs(60);

type PendingMap = Arc<Mutex<HashMap<String, oneshot::Sender<pb::TransportApiResponseMsg>>>>;

fn mutex_lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn remove_pending(pending: &PendingMap, request_id: &str) {
    let _ = mutex_lock(pending).remove(request_id);
}

fn handle_send_result(
    pending: &PendingMap,
    request_id: &str,
    result: Result<(), tb_queue::QueueError>,
) -> TbResult<()> {
    match result {
        Ok(()) => Ok(()),
        Err(e) => {
            remove_pending(pending, request_id);
            Err(TbError::internal(e))
        }
    }
}

struct Inner {
    node_id: String,
    producer: KafkaQueueProducer,
    pending: PendingMap,
    cache: moka::future::Cache<String, Arc<(pb::DeviceInfoProto, pb::DeviceProfileProto)>>,
    rate_limiter: Mutex<Option<RateLimiter>>,
    router_task: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
pub struct TransportApiClient {
    inner: Arc<Inner>,
}

impl TransportApiClient {
    pub fn new(brokers: &str, node_id: &str) -> Result<Self, tb_queue::QueueError> {
        let producer = KafkaQueueProducer::new(brokers, &format!("transport-api-{node_id}"))?;
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let responses_topic = TbTopic::transport_api_responses(node_id);
        let router_task = {
            let brokers = brokers.to_string();
            let pending = pending.clone();
            let responses_topic = responses_topic.clone();
            let group_id = format!("transport-api-{node_id}");
            tokio::spawn(async move {
                if let Err(e) = tb_queue::admin::ensure_topic(&brokers, &responses_topic, 1).await {
                    tracing::warn!("ensure responses topic failed: {e}");
                }
                let mut consumer =
                    match StreamQueueConsumer::new(&brokers, &group_id, &[&responses_topic]) {
                        Ok(c) => c,
                        Err(e) => {
                            tracing::error!("transport-api response consumer failed to start: {e}");
                            return;
                        }
                    };
                loop {
                    match consumer.recv().await {
                        Ok(msg) => {
                            let Ok(resp) = pb::TransportApiResponseMsg::decode(msg.payload) else {
                                tracing::warn!("undecodable transport-api response");
                                continue;
                            };
                            let tx = mutex_lock(&pending).remove(&resp.request_id);
                            if let Some(tx) = tx {
                                let _ = tx.send(resp);
                            } else {
                                tracing::debug!(request_id = resp.request_id, "late response");
                            }
                        }
                        Err(e) => {
                            tracing::error!("transport-api response consumer error: {e}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            })
        };
        let cache = moka::future::Cache::builder()
            .max_capacity(100_000)
            .time_to_live(CACHE_TTL)
            .build();
        let rate_limiter = std::env::var("TB_TRANSPORT_RATE_LIMIT_PER_TENANT")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|&n| n > 0)
            .map(RateLimiter::new);
        Ok(Self {
            inner: Arc::new(Inner {
                node_id: node_id.to_string(),
                producer,
                pending,
                cache,
                rate_limiter: Mutex::new(rate_limiter),
                router_task,
            }),
        })
    }

    /// 测试/定制用：覆盖环境变量配置的限流阈值（0 = 不限）。
    pub fn set_rate_limit(&self, max_msgs_per_sec_per_tenant: u32) {
        let limiter = (max_msgs_per_sec_per_tenant > 0)
            .then(|| RateLimiter::new(max_msgs_per_sec_per_tenant));
        *mutex_lock(&self.inner.rate_limiter) = limiter;
    }

    /// 租户令牌桶取 msg_count 枚；未配置限流时恒 true。
    fn rate_limit_check(&self, tenant_id: &str, msg_count: u32) -> bool {
        match &*mutex_lock(&self.inner.rate_limiter) {
            Some(limiter) => limiter.check_n(tenant_id, msg_count),
            None => true,
        }
    }

    pub fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    /// 发请求并等响应（10s 超时）。失败时清理 pending 槽位。
    async fn request(
        &self,
        request: pb::transport_api_request_msg::Request,
    ) -> TbResult<pb::TransportApiResponseMsg> {
        let request_id = Uuid::now_v7().to_string();
        let msg = pb::TransportApiRequestMsg {
            request_id: request_id.clone(),
            request: Some(request),
        };
        let (tx, rx) = oneshot::channel();
        let _ = mutex_lock(&self.inner.pending).insert(request_id.clone(), tx);
        let send_result = self
            .inner
            .producer
            .send(
                TbTopic::TRANSPORT_API_REQUESTS,
                &self.inner.node_id,
                Bytes::from(msg.encode_to_vec()),
            )
            .await;
        handle_send_result(&self.inner.pending, &request_id, send_result)?;
        match tokio::time::timeout(REQUEST_TIMEOUT, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(TbError::Internal("response channel closed".into())),
            Err(_) => {
                remove_pending(&self.inner.pending, &request_id);
                Err(TbError::Internal("transport-api request timeout".into()))
            }
        }
    }

    pub async fn validate_token(&self, token: &str) -> TbResult<pb::ValidateCredentialsResponse> {
        if let Some(cached) = self.inner.cache.get(token).await {
            let (device, profile) = &*cached;
            return Ok(pb::ValidateCredentialsResponse {
                valid: true,
                device: Some(device.clone()),
                profile: Some(profile.clone()),
                credentials_body: String::new(),
            });
        }
        let resp = self
            .request(pb::transport_api_request_msg::Request::ValidateToken(
                pb::ValidateTokenRequest {
                    token: token.to_string(),
                },
            ))
            .await?;
        let out = match resp.response {
            Some(pb::transport_api_response_msg::Response::ValidateCredentials(v)) => Ok(v),
            Some(pb::transport_api_response_msg::Response::Error(e)) => Err(TbError::Internal(
                format!("transport-api error {}: {}", e.code, e.message),
            )),
            other => {
                tracing::warn!(?other, "unexpected transport-api response");
                Err(TbError::Internal(
                    "unexpected transport-api response".into(),
                ))
            }
        }?;
        if out.valid
            && let (Some(device), Some(profile)) = (out.device.clone(), out.profile.clone())
        {
            self.inner
                .cache
                .insert(token.to_string(), Arc::new((device, profile)))
                .await;
        }
        Ok(out)
    }

    pub async fn validate_mqtt_basic(
        &self,
        client_id: &str,
        username: &str,
        password: &str,
    ) -> TbResult<pb::ValidateCredentialsResponse> {
        let resp = self
            .request(pb::transport_api_request_msg::Request::ValidateMqttBasic(
                pb::ValidateMqttBasicRequest {
                    client_id: client_id.to_string(),
                    username: username.to_string(),
                    password: password.to_string(),
                },
            ))
            .await?;
        match resp.response {
            Some(pb::transport_api_response_msg::Response::ValidateCredentials(v)) => Ok(v),
            Some(pb::transport_api_response_msg::Response::Error(e)) => Err(TbError::Internal(
                format!("transport-api error {}: {}", e.code, e.message),
            )),
            other => {
                tracing::warn!(?other, "unexpected transport-api response");
                Err(TbError::Internal(
                    "unexpected transport-api response".into(),
                ))
            }
        }
    }

    pub async fn validate_x509(
        &self,
        cert_hash: &str,
    ) -> TbResult<pb::ValidateCredentialsResponse> {
        let resp = self
            .request(pb::transport_api_request_msg::Request::ValidateX509(
                pb::ValidateX509Request {
                    cert_hash: cert_hash.to_string(),
                },
            ))
            .await?;
        match resp.response {
            Some(pb::transport_api_response_msg::Response::ValidateCredentials(v)) => Ok(v),
            Some(pb::transport_api_response_msg::Response::Error(e)) => Err(TbError::Internal(
                format!("transport-api error {}: {}", e.code, e.message),
            )),
            other => {
                tracing::warn!(?other, "unexpected transport-api response");
                Err(TbError::Internal(
                    "unexpected transport-api response".into(),
                ))
            }
        }
    }

    /// 设备属性同步查询（HTTP GET attributes 用）。
    pub async fn get_attributes(
        &self,
        device_id: &str,
        client_keys: Vec<String>,
        shared_keys: Vec<String>,
    ) -> TbResult<pb::GetAttributesResponse> {
        let resp = self
            .request(pb::transport_api_request_msg::Request::GetAttributes(
                pb::GetAttributesRequest {
                    device_id: device_id.to_string(),
                    client_keys,
                    shared_keys,
                },
            ))
            .await?;
        match resp.response {
            Some(pb::transport_api_response_msg::Response::GetAttributesResponse(v)) => Ok(v),
            Some(pb::transport_api_response_msg::Response::Error(e)) => Err(TbError::Internal(
                format!("transport-api error {}: {}", e.code, e.message),
            )),
            other => {
                tracing::warn!(?other, "unexpected transport-api response");
                Err(TbError::Internal(
                    "unexpected transport-api response".into(),
                ))
            }
        }
    }

    /// 遥测上行：每个 ts 分组一条消息，data_json 为 {"ts":ms,"values":{...}}。
    /// 限流按分组后的 Kafka 消息数一次预扣。
    pub async fn publish_telemetry(
        &self,
        device: &pb::DeviceInfoProto,
        entries: Vec<TsKvEntry>,
    ) -> TbResult<()> {
        let mut by_ts: HashMap<i64, serde_json::Map<String, serde_json::Value>> = HashMap::new();
        let mut order: Vec<i64> = Vec::new();
        for e in entries {
            let group = by_ts.entry(e.ts).or_insert_with(|| {
                order.push(e.ts);
                serde_json::Map::new()
            });
            group.insert(e.key, serde_json::to_value(&e.value).unwrap_or_default());
        }
        let msg_count = u32::try_from(order.len()).unwrap_or(u32::MAX);
        if !self.rate_limit_check(&device.tenant_id, msg_count) {
            return Err(TbError::RateLimitExceeded(format!(
                "tenant {} telemetry rate limit exceeded",
                device.tenant_id
            )));
        }
        for ts in order {
            let data = serde_json::json!({"ts": ts, "values": by_ts[&ts]});
            self.publish(device, msg_type::POST_TELEMETRY_REQUEST, data)
                .await?;
        }
        Ok(())
    }

    /// 属性上行：data_json 为 {"scope":"CLIENT_SCOPE","kv":{...}}。
    pub async fn publish_attributes(
        &self,
        device: &pb::DeviceInfoProto,
        entries: Vec<KvEntry>,
    ) -> TbResult<()> {
        if !self.rate_limit_check(&device.tenant_id, 1) {
            return Err(TbError::RateLimitExceeded(format!(
                "tenant {} attributes rate limit exceeded",
                device.tenant_id
            )));
        }
        let kv: serde_json::Map<String, serde_json::Value> = entries
            .into_iter()
            .map(|e| (e.key, serde_json::to_value(&e.value).unwrap_or_default()))
            .collect();
        let data = serde_json::json!({"scope": "CLIENT_SCOPE", "kv": kv});
        self.publish(device, msg_type::POST_ATTRIBUTES_REQUEST, data)
            .await
    }

    /// 服务端 RPC 上行（无响应等待）：data_json 为 {"method":..,"params":..,"timeout":..}。
    pub async fn publish_to_server_rpc(
        &self,
        device: &pb::DeviceInfoProto,
        method: &str,
        params: serde_json::Value,
        timeout_ms: i64,
    ) -> TbResult<()> {
        if !self.rate_limit_check(&device.tenant_id, 1) {
            return Err(TbError::RateLimitExceeded(format!(
                "tenant {} to-server RPC rate limit exceeded",
                device.tenant_id
            )));
        }
        let data = serde_json::json!({
            "method": method,
            "params": params,
            "timeout": timeout_ms,
        });
        self.publish(device, msg_type::TO_SERVER_RPC_REQUEST, data)
            .await
    }

    /// 网关子设备 get-or-create。
    pub async fn get_or_create_gateway_device(
        &self,
        gateway: &pb::DeviceInfoProto,
        device_name: &str,
        device_type: &str,
    ) -> TbResult<pb::DeviceInfoProto> {
        let resp = self
            .request(
                pb::transport_api_request_msg::Request::GetOrCreateGatewayDevice(
                    pb::GetOrCreateGatewayDeviceRequest {
                        gateway_id: gateway.device_id.clone(),
                        tenant_id: gateway.tenant_id.clone(),
                        device_name: device_name.to_string(),
                        device_type: device_type.to_string(),
                    },
                ),
            )
            .await?;
        match resp.response {
            Some(pb::transport_api_response_msg::Response::GatewayDevice(d)) => Ok(d),
            Some(pb::transport_api_response_msg::Response::Error(e)) => Err(TbError::Internal(
                format!("transport-api error {}: {}", e.code, e.message),
            )),
            other => {
                tracing::warn!(?other, "unexpected transport-api response");
                Err(TbError::Internal(
                    "unexpected transport-api response".into(),
                ))
            }
        }
    }

    /// 查该档案下可更新的 OTA 包元数据；404 → Ok(None)。
    pub async fn get_ota_package(
        &self,
        tenant_id: &str,
        device_profile_id: &str,
        ota_type: tb_proto::common::OtaPackageType,
        current_title: &str,
        current_version: &str,
    ) -> TbResult<Option<pb::OtaPackageProto>> {
        let resp = self
            .request(pb::transport_api_request_msg::Request::GetOtaPackage(
                pb::GetOtaPackageRequest {
                    tenant_id: tenant_id.to_string(),
                    device_profile_id: device_profile_id.to_string(),
                    r#type: ota_type as i32,
                    current_title: current_title.to_string(),
                    current_version: current_version.to_string(),
                },
            ))
            .await?;
        match resp.response {
            Some(pb::transport_api_response_msg::Response::OtaPackage(p)) => Ok(Some(p)),
            Some(pb::transport_api_response_msg::Response::Error(e)) if e.code == 404 => Ok(None),
            Some(pb::transport_api_response_msg::Response::Error(e)) => Err(TbError::Internal(
                format!("transport-api error {}: {}", e.code, e.message),
            )),
            other => {
                tracing::warn!(?other, "unexpected transport-api response");
                Err(TbError::Internal(
                    "unexpected transport-api response".into(),
                ))
            }
        }
    }

    /// 拉取 OTA 数据分块；越界返回空。
    pub async fn get_ota_chunk(
        &self,
        ota_package_id: &str,
        chunk_index: i32,
        chunk_size: i32,
    ) -> TbResult<Bytes> {
        let resp = self
            .request(pb::transport_api_request_msg::Request::GetOtaChunk(
                pb::GetOtaChunkRequest {
                    ota_package_id: ota_package_id.to_string(),
                    chunk_index,
                    chunk_size,
                },
            ))
            .await?;
        match resp.response {
            Some(pb::transport_api_response_msg::Response::OtaChunk(c)) => Ok(c.data.into()),
            Some(pb::transport_api_response_msg::Response::Error(e)) => Err(TbError::Internal(
                format!("transport-api error {}: {}", e.code, e.message),
            )),
            other => {
                tracing::warn!(?other, "unexpected transport-api response");
                Err(TbError::Internal(
                    "unexpected transport-api response".into(),
                ))
            }
        }
    }

    /// 设备 provisioning：返回 SUCCESS/FAILURE 及凭证。
    pub async fn provision_device(
        &self,
        tenant_id: &str,
        device_name: &str,
        provision_device_key: &str,
        provision_device_secret: &str,
    ) -> TbResult<pb::ProvisionDeviceResponse> {
        let resp = self
            .request(pb::transport_api_request_msg::Request::ProvisionDevice(
                pb::ProvisionDeviceRequest {
                    tenant_id: tenant_id.to_string(),
                    device_name: device_name.to_string(),
                    provision_device_key: provision_device_key.to_string(),
                    provision_device_secret: provision_device_secret.to_string(),
                },
            ))
            .await?;
        match resp.response {
            Some(pb::transport_api_response_msg::Response::ProvisionResponse(r)) => Ok(r),
            Some(pb::transport_api_response_msg::Response::Error(e)) => Err(TbError::Internal(
                format!("transport-api error {}: {}", e.code, e.message),
            )),
            other => {
                tracing::warn!(?other, "unexpected transport-api response");
                Err(TbError::Internal(
                    "unexpected transport-api response".into(),
                ))
            }
        }
    }

    async fn publish(
        &self,
        device: &pb::DeviceInfoProto,
        msg_type: &str,
        data: serde_json::Value,
    ) -> TbResult<()> {
        let metadata: HashMap<String, String> = [
            ("deviceName".to_string(), device.device_name.clone()),
            ("deviceType".to_string(), device.device_type.clone()),
        ]
        .into_iter()
        .collect();
        let msg = ToRuleEngineMsg {
            tenant_id: device.tenant_id.clone(),
            tb_msg: Some(TbMsgProto {
                id: Uuid::now_v7().to_string(),
                queue_name: "Main".to_string(),
                r#type: msg_type.to_string(),
                originator_id: device.device_id.clone(),
                originator_type: tb_proto::common::EntityType::Device as i32,
                tenant_id: device.tenant_id.clone(),
                customer_id: device.customer_id.clone(),
                data_json: data.to_string(),
                metadata,
                rule_chain_id: String::new(),
                rule_node_id: String::new(),
                ts: chrono::Utc::now().timestamp_millis(),
            }),
        };
        self.inner
            .producer
            .send(
                &TbTopic::rule_engine("Main"),
                &device.device_id,
                Bytes::from(msg.encode_to_vec()),
            )
            .await
            .map_err(TbError::internal)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.router_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tb_common::KvValue;

    fn test_client(rate: u32) -> TransportApiClient {
        TransportApiClient {
            inner: Arc::new(Inner {
                node_id: "test-node".into(),
                producer: KafkaQueueProducer::new("localhost:1", "transport-api-test").unwrap(),
                pending: Arc::new(Mutex::new(HashMap::new())),
                cache: moka::future::Cache::builder()
                    .max_capacity(100_000)
                    .time_to_live(CACHE_TTL)
                    .build(),
                rate_limiter: Mutex::new((rate > 0).then(|| RateLimiter::new(rate))),
                router_task: tokio::spawn(async {}),
            }),
        }
    }

    fn device() -> pb::DeviceInfoProto {
        pb::DeviceInfoProto {
            tenant_id: "tenant-1".into(),
            ..Default::default()
        }
    }

    fn telemetry(ts: i64, key: &str) -> TsKvEntry {
        TsKvEntry {
            ts,
            key: key.into(),
            value: KvValue::Long(1),
        }
    }

    #[test]
    fn send_failure_removes_pending_slot() {
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let (tx, _rx) = oneshot::channel();
        let _ = mutex_lock(&pending).insert("request-1".into(), tx);

        let result = handle_send_result(
            &pending,
            "request-1",
            Err(tb_queue::QueueError::Serialization("send failed".into())),
        );

        assert!(matches!(result, Err(TbError::Internal(_))));
        assert!(!mutex_lock(&pending).contains_key("request-1"));
    }

    #[tokio::test]
    async fn telemetry_fanout_uses_message_count_for_rate_limit() {
        let client = test_client(1);
        let result = client
            .publish_telemetry(
                &device(),
                vec![telemetry(1, "a"), telemetry(1, "b"), telemetry(2, "c")],
            )
            .await;

        assert!(matches!(result, Err(TbError::RateLimitExceeded(_))));
    }

    #[tokio::test]
    async fn to_server_rpc_is_rate_limited() {
        let client = test_client(1);
        let device = device();
        assert!(client.rate_limit_check(&device.tenant_id, 1));

        let result = client
            .publish_to_server_rpc(&device, "ping", serde_json::json!({}), 1_000)
            .await;

        assert!(matches!(result, Err(TbError::RateLimitExceeded(_))));
    }
}
