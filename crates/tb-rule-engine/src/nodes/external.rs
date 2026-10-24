//! external.*：外部系统调用节点（REST/Kafka/MQTT）。

use std::collections::HashMap;
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
/// 禁止跟随重定向；TB_RE_ALLOWED_HOSTS 可配置逗号分隔的精确主机白名单。
pub struct RestCallNode {
    url: String,
    method: reqwest::Method,
    headers: HashMap<String, String>,
    body_script: Option<String>,
    client: reqwest::Client,
    allowed_hosts: Option<std::collections::HashSet<String>>,
}

fn host_allowed(
    allowed_hosts: Option<&std::collections::HashSet<String>>,
    url: &reqwest::Url,
) -> bool {
    let Some(allowed_hosts) = allowed_hosts else {
        return true;
    };
    url.host_str()
        .is_some_and(|host| allowed_hosts.contains(&host.to_ascii_lowercase()))
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
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(TbError::internal)?;
        let allowed_hosts = std::env::var("TB_RE_ALLOWED_HOSTS")
            .ok()
            .map(|hosts| {
                hosts
                    .split(',')
                    .map(str::trim)
                    .filter(|host| !host.is_empty())
                    .map(str::to_ascii_lowercase)
                    .collect::<std::collections::HashSet<_>>()
            })
            .filter(|hosts| !hosts.is_empty());
        Ok(Self {
            url: c.url,
            method,
            headers: c.headers,
            body_script: c.body_script,
            client,
            allowed_hosts,
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
        if !host_allowed(self.allowed_hosts.as_ref(), &parsed_url) {
            tracing::warn!(url = %url, "rest_call host is not allowed");
            return vec![("Failure".into(), msg)];
        }
        let mut req = self.client.request(self.method.clone(), parsed_url);
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

/// external.kafka：把整条 TbMsg JSON 发到任意 topic；key_script（Rhai 返回字符串）决定分区 key，
/// 缺省用 originator id。
pub struct KafkaNode {
    topic: String,
    key_script: Option<String>,
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
    options: rumqttc::MqttOptions,
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
        let mut options = rumqttc::MqttOptions::new(client_id, c.host, c.port);
        if let (Some(u), Some(p)) = (c.username, c.password) {
            options.set_credentials(u, p);
        }
        Ok(Self {
            options,
            topic: c.topic,
            qos,
            client: tokio::sync::OnceCell::new(),
            eventloop_task: std::sync::Mutex::new(None),
        })
    }

    async fn client(&self) -> TbResult<&rumqttc::AsyncClient> {
        self.client
            .get_or_init(|| async {
                let (client, eventloop) = rumqttc::AsyncClient::new(self.options.clone(), 16);
                let task = tokio::spawn(drive_mqtt_eventloop(eventloop));
                let mut slot = self
                    .eventloop_task
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *slot = Some(task);
                client
            })
            .await;
        self.client
            .get()
            .ok_or_else(|| TbError::Internal("mqtt client initialization failed".into()))
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

    #[test]
    fn rest_allowlist_matches_exact_host() {
        let allowed = ["Example.COM".to_ascii_lowercase()].into_iter().collect();
        assert!(host_allowed(
            Some(&allowed),
            &reqwest::Url::parse("http://example.com:8080/path").unwrap()
        ));
        assert!(!host_allowed(
            Some(&allowed),
            &reqwest::Url::parse("http://example.com.evil.test/path").unwrap()
        ));
        assert!(host_allowed(
            None,
            &reqwest::Url::parse("http://anywhere.test/path").unwrap()
        ));
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
}
