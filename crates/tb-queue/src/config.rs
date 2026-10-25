//! Kafka 客户端公共配置：bootstrap 地址加认证/TLS 环境变量。
//!
//! - `TB_KAFKA_SECURITY_PROTOCOL`：`plaintext`（默认）/ `sasl_plaintext` / `ssl` / `sasl_ssl`
//! - `TB_KAFKA_SASL_MECHANISM`：SASL 协议下默认 `PLAIN`
//! - `TB_KAFKA_SASL_USERNAME` / `TB_KAFKA_SASL_PASSWORD`
//! - `TB_KAFKA_SSL_CA_LOCATION` / `TB_KAFKA_SSL_CERTIFICATE_LOCATION` / `TB_KAFKA_SSL_KEY_LOCATION`
//!
//! `ssl` / `sasl_ssl` 及 SCRAM 需要以 `kafka-tls` feature 构建（生产镜像默认开启）。

use std::sync::Once;

use rdkafka::config::ClientConfig;

const ENV_PROPERTIES: &[(&str, &str)] = &[
    ("TB_KAFKA_SECURITY_PROTOCOL", "security.protocol"),
    ("TB_KAFKA_SASL_MECHANISM", "sasl.mechanism"),
    ("TB_KAFKA_SASL_USERNAME", "sasl.username"),
    ("TB_KAFKA_SASL_PASSWORD", "sasl.password"),
    ("TB_KAFKA_SSL_CA_LOCATION", "ssl.ca.location"),
    (
        "TB_KAFKA_SSL_CERTIFICATE_LOCATION",
        "ssl.certificate.location",
    ),
    ("TB_KAFKA_SSL_KEY_LOCATION", "ssl.key.location"),
];

static PLAINTEXT_WARNING: Once = Once::new();

pub fn client_config(brokers: &str) -> ClientConfig {
    let config = client_config_from(brokers, |key| std::env::var(key).ok());
    let production =
        std::env::var("TB_ENV").is_ok_and(|value| value.eq_ignore_ascii_case("production"));
    if production
        && config
            .get("security.protocol")
            .is_none_or(|protocol| protocol.eq_ignore_ascii_case("plaintext"))
    {
        PLAINTEXT_WARNING.call_once(|| {
            tracing::warn!(
                "Kafka uses PLAINTEXT without authentication; set TB_KAFKA_SECURITY_PROTOCOL"
            );
        });
    }
    config
}

fn client_config_from(brokers: &str, lookup: impl Fn(&str) -> Option<String>) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    for (env, property) in ENV_PROPERTIES {
        if let Some(value) = lookup(env).filter(|value| !value.is_empty()) {
            config.set(*property, value);
        }
    }
    let sasl = config
        .get("security.protocol")
        .is_some_and(|protocol| protocol.to_ascii_lowercase().starts_with("sasl_"));
    if sasl && config.get("sasl.mechanism").is_none() {
        config.set("sasl.mechanism", "PLAIN");
    }
    config
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rdkafka::producer::FutureProducer;

    use super::*;

    fn config(vars: &[(&str, &str)]) -> ClientConfig {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        client_config_from("broker:9092", |key| vars.get(key).cloned())
    }

    #[test]
    fn plaintext_by_default() {
        let config = config(&[]);
        assert_eq!(config.get("bootstrap.servers"), Some("broker:9092"));
        assert_eq!(config.get("security.protocol"), None);
        assert_eq!(config.get("sasl.mechanism"), None);
    }

    #[test]
    fn sasl_settings_are_applied_with_plain_default() {
        let config = config(&[
            ("TB_KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
            ("TB_KAFKA_SASL_USERNAME", "tb"),
            ("TB_KAFKA_SASL_PASSWORD", "p a$s"),
            ("TB_KAFKA_SSL_CA_LOCATION", ""),
        ]);
        assert_eq!(config.get("security.protocol"), Some("SASL_PLAINTEXT"));
        assert_eq!(config.get("sasl.mechanism"), Some("PLAIN"));
        assert_eq!(config.get("sasl.username"), Some("tb"));
        assert_eq!(config.get("sasl.password"), Some("p a$s"));
        assert_eq!(config.get("ssl.ca.location"), None);
        assert!(config.create::<FutureProducer>().is_ok());
    }

    #[test]
    fn explicit_mechanism_is_kept() {
        let config = config(&[
            ("TB_KAFKA_SECURITY_PROTOCOL", "sasl_ssl"),
            ("TB_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512"),
        ]);
        assert_eq!(config.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
    }

    #[test]
    fn tls_support_matches_build_feature() {
        let created = config(&[("TB_KAFKA_SECURITY_PROTOCOL", "ssl")])
            .create::<FutureProducer>()
            .is_ok();
        assert_eq!(created, cfg!(feature = "kafka-tls"));
    }
}
