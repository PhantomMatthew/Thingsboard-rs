//! external.*：外部系统调用节点（REST/Kafka/MQTT）。

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde::Deserialize;
use tb_common::msg::TbMsg;
use tb_common::{TbError, TbResult};
use tb_queue::QueueProducer;

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

/// external.rest_call：HTTP 调用，2xx → "Success"，其余/错误 → "Failure"；
/// 响应状态码写 metadata.rest_call_status。超时 10s。
pub struct RestCallNode {
    url: String,
    method: reqwest::Method,
    headers: HashMap<String, String>,
    body_script: Option<String>,
    client: reqwest::Client,
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
            .build()
            .map_err(TbError::internal)?;
        Ok(Self {
            url: c.url,
            method,
            headers: c.headers,
            body_script: c.body_script,
            client,
        })
    }
}

#[async_trait]
impl Node for RestCallNode {
    async fn on_msg(&self, ctx: &NodeContext, mut msg: TbMsg) -> Vec<(String, TbMsg)> {
        let url = render_template(&self.url, &msg);
        let mut req = self.client.request(self.method.clone(), &url);
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
        })
    }

    async fn client(&self) -> TbResult<&rumqttc::AsyncClient> {
        self.client
            .get_or_init(|| async {
                let (client, mut eventloop) = rumqttc::AsyncClient::new(self.options.clone(), 16);
                tokio::spawn(async move {
                    loop {
                        if let Err(e) = eventloop.poll().await {
                            tracing::warn!("mqtt eventloop error: {e}");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                });
                client
            })
            .await;
        Ok(self.client.get().expect("initialized above"))
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
