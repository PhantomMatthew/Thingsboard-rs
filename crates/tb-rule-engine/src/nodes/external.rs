//! external.*：外部系统调用节点（REST/Kafka/MQTT）。

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde::Deserialize;
use tb_common::msg::TbMsg;
use tb_common::{TbError, TbResult};

use crate::context::NodeContext;
use crate::node::Node;
use crate::nodes::parse_config;

/// `{{metadata.x}}` / `{{msg.a.b}}` 模板替换；取不到的值替换为空串。
fn render_template(template: &str, msg: &TbMsg) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find("}}") else {
            out.push_str(&rest[start..]);
            return out;
        };
        let expr = after[..end].trim();
        let value = if let Some(path) = expr.strip_prefix("metadata.") {
            msg.metadata.get(path).cloned().unwrap_or_default()
        } else if let Some(path) = expr.strip_prefix("msg.") {
            let mut v = &msg.data;
            for seg in path.split('.') {
                v = v.get(seg).unwrap_or(&serde_json::Value::Null);
            }
            match v {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Null => String::new(),
                other => other.to_string(),
            }
        } else {
            String::new()
        };
        out.push_str(&value);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    out
}

/// external.rest_call：HTTP 调用，2xx → "Success"，其余/错误 → "Failure"。
/// 禁止跟随重定向并固定已校验地址；TB_RE_ALLOWED_HOSTS 默认空，未显式放行即拒绝。
pub struct RestCallNode {
    url: String,
    method: reqwest::Method,
    headers: HashMap<String, String>,
    body_script: Option<String>,
    allowed_hosts: HashSet<String>,
}

fn allowed_hosts_from_env() -> HashSet<String> {
    std::env::var("TB_RE_ALLOWED_HOSTS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

fn host_allowed(allowed_hosts: &HashSet<String>, host: &str) -> bool {
    allowed_hosts.contains(&host.to_ascii_lowercase())
}

fn allow_private_network() -> bool {
    std::env::var("TB_RE_ALLOW_PRIVATE_NETWORK")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(false)
}

fn is_non_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.octets()[0] == 0
                || (ip.octets()[0] == 100 && (64..=127).contains(&ip.octets()[1]))
                || (ip.octets()[0] == 198 && (18..=19).contains(&ip.octets()[1]))
                || ip.octets()[0] >= 240
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || (ip.segments()[0] == 0x2001 && ip.segments()[1] == 0x0db8)
                || ip
                    .to_ipv4_mapped()
                    .is_some_and(|ip| is_non_public_ip(IpAddr::V4(ip)))
        }
    }
}

async fn resolve_outbound_addresses(host: &str, port: u16) -> TbResult<Vec<SocketAddr>> {
    let addresses: Vec<_> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|e| TbError::InvalidInput(format!("outbound host resolution failed: {e}")))?
        .collect();
    if addresses.is_empty() {
        return Err(TbError::InvalidInput(
            "outbound host resolution returned no addresses".into(),
        ));
    }
    if !allow_private_network()
        && addresses
            .iter()
            .any(|address| is_non_public_ip(address.ip()))
    {
        return Err(TbError::PermissionDenied(
            "outbound private, loopback, link-local, and reserved addresses are disabled".into(),
        ));
    }
    Ok(addresses)
}

async fn pinned_http_client(url: &reqwest::Url) -> TbResult<reqwest::Client> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(TbError::InvalidInput(
            "external.rest_call only supports HTTP(S)".into(),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| TbError::InvalidInput("external.rest_call URL has no host".into()))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| TbError::InvalidInput("external.rest_call URL has no port".into()))?;
    let addresses = resolve_outbound_addresses(host, port).await?;
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .resolve_to_addrs(host, &addresses)
        .build()
        .map_err(TbError::internal)
}

impl RestCallNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            url: String,
            #[serde(default = "default_method")]
            method: String,
            #[serde(default)]
            headers: HashMap<String, String>,
            body_script: Option<String>,
        }
        fn default_method() -> String {
            "POST".into()
        }
        let c: Config = parse_config(config)?;
        let method = match c.method.as_str() {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            "PUT" => reqwest::Method::PUT,
            "DELETE" => reqwest::Method::DELETE,
            other => {
                return Err(TbError::InvalidInput(format!(
                    "unsupported http method: {other}"
                )));
            }
        };
        Ok(Self {
            url: c.url,
            method,
            headers: c.headers,
            body_script: c.body_script,
            allowed_hosts: allowed_hosts_from_env(),
        })
    }
}

#[async_trait]
impl Node for RestCallNode {
    async fn on_msg(&self, ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        let url = render_template(&self.url, &msg);
        let parsed_url = match reqwest::Url::parse(&url) {
            Ok(url) => url,
            Err(e) => {
                tracing::warn!(url = %url, "rest_call URL invalid: {e}");
                return vec![("Failure".into(), msg)];
            }
        };
        if parsed_url
            .host_str()
            .is_none_or(|host| !host_allowed(&self.allowed_hosts, host))
        {
            tracing::warn!(url = %url, "rest_call host is not allowed");
            return vec![("Failure".into(), msg)];
        }
        let client = match pinned_http_client(&parsed_url).await {
            Ok(client) => client,
            Err(e) => {
                tracing::warn!(url = %url, "rest_call outbound validation failed: {e}");
                return vec![("Failure".into(), msg)];
            }
        };
        let mut req = client.request(self.method.clone(), parsed_url);
        for (k, v) in &self.headers {
            req = req.header(k, render_template(v, &msg));
        }
        if self.method != reqwest::Method::GET && self.method != reqwest::Method::DELETE {
            let body = match &self.body_script {
                Some(script) => match ctx.script_engine.eval_map(
                    script,
                    &msg.data,
                    &msg.metadata,
                    &msg.msg_type,
                ) {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("rest_call body script failed: {e}");
                        return vec![("Failure".into(), msg)];
                    }
                },
                None => msg.data.clone(),
            };
            req = req.json(&body);
        }
        match req.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                msg.metadata
                    .insert("rest_call_status".into(), status.to_string());
                let rel = if resp.status().is_success() {
                    "Success"
                } else {
                    "Failure"
                };
                vec![(rel.into(), msg)]
            }
            Err(e) => {
                tracing::warn!(url = %url, "rest_call request failed: {e}");
                vec![("Failure".into(), msg)]
            }
        }
    }
}

/// external.kafka：把整条 TbMsg JSON 发到白名单前缀内的 topic；key_script 决定分区 key，
/// 缺省用 originator id，未配置 TB_RE_KAFKA_ALLOWED_TOPIC_PREFIXES 时全部拒绝。
pub struct KafkaNode {
    topic: String,
    key_script: Option<String>,
    allowed_topic_prefixes: Vec<String>,
}

fn allowed_topic_prefixes_from_env() -> Vec<String> {
    std::env::var("TB_RE_KAFKA_ALLOWED_TOPIC_PREFIXES")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|prefix| !prefix.is_empty())
        .map(str::to_string)
        .collect()
}

fn kafka_topic_allowed(prefixes: &[String], topic: &str) -> bool {
    !topic.is_empty()
        && prefixes
            .iter()
            .any(|prefix| !prefix.is_empty() && topic.starts_with(prefix))
}

impl KafkaNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            topic: String,
            key_script: Option<String>,
        }
        let c: Config = parse_config(config)?;
        Ok(Self {
            topic: c.topic,
            key_script: c.key_script,
            allowed_topic_prefixes: allowed_topic_prefixes_from_env(),
        })
    }
}

#[async_trait]
impl Node for KafkaNode {
    async fn on_msg(&self, ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let rel = match self.send(ctx, &msg).await {
            Ok(()) => "Success",
            Err(e) => {
                tracing::warn!("external.kafka failed: {e}");
                "Failure"
            }
        };
        vec![(rel.into(), msg)]
    }
}

impl KafkaNode {
    async fn send(&self, ctx: &NodeContext, msg: &TbMsg) -> TbResult<()> {
        if !kafka_topic_allowed(&self.allowed_topic_prefixes, &self.topic) {
            return Err(TbError::PermissionDenied(format!(
                "external.kafka topic is not allowed: {}",
                self.topic
            )));
        }
        let key = match &self.key_script {
            Some(script) => ctx
                .script_engine
                .eval_string(script, &msg.data, &msg.metadata, &msg.msg_type)
                .map_err(|e| TbError::InvalidInput(format!("key script: {e}")))?,
            None => msg.originator.id.to_string(),
        };
        let payload = serde_json::to_vec(msg).map_err(TbError::internal)?;
        ctx.kafka_producer
            .send(&self.topic, &key, Bytes::from(payload))
            .await
            .map_err(|e| TbError::Internal(format!("kafka send: {e}")))
    }
}

/// external.mqtt：rumqttc 发布；client 懒建连，后台 task 驱动 eventloop。
pub struct MqttNode {
    client_id: String,
    host: String,
    port: u16,
    credentials: Option<(String, String)>,
    allowed_hosts: HashSet<String>,
    topic: String,
    qos: rumqttc::QoS,
    client: tokio::sync::OnceCell<rumqttc::AsyncClient>,
    eventloop_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

fn is_terminal_mqtt_error(error: &rumqttc::ConnectionError) -> bool {
    matches!(error, rumqttc::ConnectionError::RequestsDone)
}

async fn drive_mqtt_eventloop(mut eventloop: rumqttc::EventLoop) {
    loop {
        match eventloop.poll().await {
            Ok(_) => {}
            Err(e) if is_terminal_mqtt_error(&e) => {
                tracing::debug!("mqtt request channel closed; eventloop stopped");
                break;
            }
            Err(e) => {
                tracing::warn!("mqtt eventloop error: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

impl MqttNode {
    pub fn new(config: &serde_json::Value) -> TbResult<Self> {
        #[derive(Deserialize)]
        struct Config {
            host: String,
            #[serde(default = "default_port")]
            port: u16,
            topic: String,
            #[serde(default)]
            qos: u8,
            client_id: Option<String>,
            username: Option<String>,
            password: Option<String>,
        }
        fn default_port() -> u16 {
            1883
        }
        let c: Config = parse_config(config)?;
        let qos = match c.qos {
            0 => rumqttc::QoS::AtMostOnce,
            1 => rumqttc::QoS::AtLeastOnce,
            2 => rumqttc::QoS::ExactlyOnce,
            other => {
                return Err(TbError::InvalidInput(format!("bad mqtt qos: {other}")));
            }
        };
        let client_id = c
            .client_id
            .unwrap_or_else(|| format!("tb-rule-{}", uuid::Uuid::now_v7().simple()));
        Ok(Self {
            client_id,
            host: c.host,
            port: c.port,
            credentials: c.username.zip(c.password),
            allowed_hosts: allowed_hosts_from_env(),
            topic: c.topic,
            qos,
            client: tokio::sync::OnceCell::new(),
            eventloop_task: std::sync::Mutex::new(None),
        })
    }

    async fn client(&self) -> TbResult<&rumqttc::AsyncClient> {
        self.client
            .get_or_try_init(|| async {
                if !host_allowed(&self.allowed_hosts, &self.host) {
                    return Err(TbError::PermissionDenied(format!(
                        "external.mqtt host is not allowed: {}",
                        self.host
                    )));
                }
                let addresses = resolve_outbound_addresses(&self.host, self.port).await?;
                let address = addresses[0];
                let mut options = rumqttc::MqttOptions::new(
                    self.client_id.clone(),
                    address.ip().to_string(),
                    self.port,
                );
                if let Some((username, password)) = &self.credentials {
                    options.set_credentials(username.clone(), password.clone());
                }
                let (client, eventloop) = rumqttc::AsyncClient::new(options, 16);
                let task = tokio::spawn(drive_mqtt_eventloop(eventloop));
                let mut slot = self
                    .eventloop_task
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *slot = Some(task);
                Ok(client)
            })
            .await
    }
}

impl Drop for MqttNode {
    fn drop(&mut self) {
        let slot = self
            .eventloop_task
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(task) = slot.take() {
            task.abort();
        }
    }
}

#[async_trait]
impl Node for MqttNode {
    async fn on_msg(&self, _ctx: &NodeContext, msg: TbMsg) -> Vec<(String, TbMsg)> {
        let payload = match serde_json::to_vec(&msg) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("external.mqtt serialize failed: {e}");
                return vec![("Failure".into(), msg)];
            }
        };
        let topic = render_template(&self.topic, &msg);
        let client = match self.client().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("external.mqtt client init failed: {e}");
                return vec![("Failure".into(), msg)];
            }
        };
        let rel = match client.publish(&topic, self.qos, false, payload).await {
            Ok(()) => "Success",
            Err(e) => {
                tracing::warn!(topic = %topic, "external.mqtt publish failed: {e}");
                "Failure"
            }
        };
        vec![(rel.into(), msg)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct EnvGuard {
        name: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvGuard {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(name);
            unsafe { std::env::set_var(name, value) };
            Self { name, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            unsafe {
                if let Some(value) = &self.previous {
                    std::env::set_var(self.name, value);
                } else {
                    std::env::remove_var(self.name);
                }
            }
        }
    }

    #[test]
    fn outbound_allowlists_default_to_deny() {
        let allowed: HashSet<String> = ["example.com".into()].into_iter().collect();
        assert!(host_allowed(&allowed, "Example.COM"));
        assert!(!host_allowed(&allowed, "example.com.evil.test"));
        assert!(!host_allowed(&HashSet::new(), "anywhere.test"));

        let prefixes = vec!["tenant-events.".to_string()];
        assert!(kafka_topic_allowed(&prefixes, "tenant-events.eu"));
        assert!(!kafka_topic_allowed(&prefixes, "tb_core"));
        assert!(!kafka_topic_allowed(&[], "tenant-events.eu"));
        assert!(is_non_public_ip("127.0.0.1".parse().unwrap()));
        assert!(is_non_public_ip("169.254.169.254".parse().unwrap()));
        assert!(!is_non_public_ip("8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn mqtt_requests_done_is_terminal() {
        assert!(is_terminal_mqtt_error(
            &rumqttc::ConnectionError::RequestsDone
        ));
        assert!(!is_terminal_mqtt_error(
            &rumqttc::ConnectionError::NetworkTimeout
        ));
    }

    #[tokio::test]
    async fn mqtt_drop_aborts_eventloop_task() {
        let _lock = ENV_LOCK.lock().await;
        let _allowed = EnvGuard::set("TB_RE_ALLOWED_HOSTS", "127.0.0.1");
        let _private = EnvGuard::set("TB_RE_ALLOW_PRIVATE_NETWORK", "true");
        let node = MqttNode::new(&serde_json::json!({
            "host": "127.0.0.1", "port": 1, "topic": "tb/test"
        }))
        .unwrap();
        node.client().await.unwrap();
        let abort_handle = node
            .eventloop_task
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .abort_handle();
        drop(node);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(abort_handle.is_finished());
    }

    #[tokio::test]
    async fn outbound_connections_reject_unlisted_and_private_targets() {
        let _lock = ENV_LOCK.lock().await;
        let _allowed = EnvGuard::set("TB_RE_ALLOWED_HOSTS", "");
        let _private = EnvGuard::set("TB_RE_ALLOW_PRIVATE_NETWORK", "false");
        let node = MqttNode::new(&serde_json::json!({
            "host": "127.0.0.1", "port": 1883, "topic": "tb/test"
        }))
        .unwrap();
        assert!(matches!(
            node.client().await,
            Err(TbError::PermissionDenied(_))
        ));
        let url = reqwest::Url::parse("http://127.0.0.1:8080/internal").unwrap();
        assert!(matches!(
            pinned_http_client(&url).await,
            Err(TbError::PermissionDenied(_))
        ));
    }
}
