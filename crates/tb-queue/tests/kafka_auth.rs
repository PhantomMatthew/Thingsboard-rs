//! 需要启用 SASL 且关闭自动建 topic 的 Kafka（生产 compose 的 kafka 服务）。
//! 环境变量：`TB_TEST_KAFKA_BROKERS`，以及 `TB_KAFKA_SECURITY_PROTOCOL` /
//! `TB_KAFKA_SASL_USERNAME` / `TB_KAFKA_SASL_PASSWORD`。默认忽略，显式运行：
//! `cargo test -p tb-queue --test kafka_auth -- --ignored`

use std::time::Duration;

use bytes::Bytes;
use tb_queue::{KafkaQueueProducer, QueueConsumer as _, QueueProducer as _, StreamQueueConsumer};
use uuid::Uuid;

#[tokio::test]
#[ignore]
async fn sasl_broker_requires_credentials_and_explicit_topics() {
    let brokers = std::env::var("TB_TEST_KAFKA_BROKERS").expect("TB_TEST_KAFKA_BROKERS");
    let topic = format!("tb_test.sasl.{}", Uuid::now_v7());
    tb_queue::admin::ensure_topic(&brokers, &topic, 1)
        .await
        .unwrap();

    let producer = KafkaQueueProducer::new(&brokers, "tb-test-sasl").unwrap();
    producer
        .send(&topic, "k", Bytes::from_static(b"hello"))
        .await
        .unwrap();
    let group = format!("tb-test-sasl-{}", Uuid::now_v7());
    let mut consumer =
        StreamQueueConsumer::new_from_beginning_manual(&brokers, &group, &[&topic]).unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
        .await
        .expect("consume timeout")
        .unwrap();
    assert_eq!(&msg.payload[..], b"hello");

    let missing = format!("tb_test.missing.{}", Uuid::now_v7());
    assert!(
        producer
            .send(&missing, "k", Bytes::from_static(b"x"))
            .await
            .is_err(),
        "broker must not auto-create topics"
    );

    // SAFETY: 本测试二进制只有这一个测试，修改环境变量时没有并发读取者。
    unsafe { std::env::set_var("TB_KAFKA_SASL_PASSWORD", "wrong-password") };
    let rejected = KafkaQueueProducer::new(&brokers, "tb-test-sasl-wrong").unwrap();
    assert!(
        rejected
            .send(&topic, "k", Bytes::from_static(b"x"))
            .await
            .is_err(),
        "wrong SASL password must be rejected"
    );
}
