//! tb-bench：Thingsboard-rs 压测工具。
//!
//! 用法：
//!   tb-bench mqtt [--host localhost:1883] [--devices 100] [--messages 10]
//!                 [--tps-target 0] [--samples 10]
//!                 [--core-url http://localhost:8080]
//!                 [--admin-user sysadmin@thingsboard.org] [--admin-password sysadmin]
//!
//! 流程：REST 建租户/租户管理员/设备（accessToken=bench-{run}-{i}）→
//! 并发 MQTT 连接发布 QoS0 遥测 → 抽样轮询 latest 验证端到端延迟 → 打印报告。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use futures::StreamExt as _;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde_json::{Value, json};

#[derive(Debug)]
struct BenchArgs {
    host: String,
    devices: usize,
    messages: usize,
    tps_target: f64,
    samples: usize,
    core_url: String,
    admin_user: String,
    admin_password: String,
}

fn parse_args() -> Result<BenchArgs> {
    let mut args = std::env::args().skip(1);
    let sub = args.next().unwrap_or_default();
    if sub != "mqtt" {
        bail!(
            "用法: tb-bench mqtt [--host H] [--devices N] [--messages M] [--tps-target T] \
             [--samples N] [--core-url U] [--admin-user U] [--admin-password P]"
        );
    }
    let mut a = BenchArgs {
        host: "localhost:1883".into(),
        devices: 100,
        messages: 10,
        tps_target: 0.0,
        samples: 10,
        core_url: std::env::var("TB_CORE_URL").unwrap_or_else(|_| "http://localhost:8080".into()),
        admin_user: "sysadmin@thingsboard.org".into(),
        admin_password: "sysadmin".into(),
    };
    let rest: Vec<String> = args.collect();
    let mut i = 0;
    while i < rest.len() {
        let val =
            |i: usize| -> Result<String> { rest.get(i + 1).cloned().context("缺少参数值") };
        match rest[i].as_str() {
            "--host" => a.host = val(i)?,
            "--devices" => a.devices = val(i)?.parse().context("--devices")?,
            "--messages" => a.messages = val(i)?.parse().context("--messages")?,
            "--tps-target" => a.tps_target = val(i)?.parse().context("--tps-target")?,
            "--samples" => a.samples = val(i)?.parse().context("--samples")?,
            "--core-url" => a.core_url = val(i)?,
            "--admin-user" => a.admin_user = val(i)?,
            "--admin-password" => a.admin_password = val(i)?,
            other => bail!("未知参数: {other}"),
        }
        i += 2;
    }
    if a.devices == 0 || a.messages == 0 {
        bail!("--devices 与 --messages 必须 > 0");
    }
    Ok(a)
}

async fn login(client: &reqwest::Client, core_url: &str, user: &str, pass: &str) -> Result<String> {
    let resp = client
        .post(format!("{core_url}/api/auth/login"))
        .json(&json!({"username": user, "password": pass}))
        .send()
        .await?;
    if resp.status() != 200 {
        bail!("login {user} failed: {}", resp.status());
    }
    Ok(resp.json::<Value>().await?["token"]
        .as_str()
        .context("token")?
        .to_string())
}

/// REST 准备：租户 + 租户管理员 + N 台设备。返回 (管理员 token, [(device_id, access_token)])。
async fn provision(
    client: &reqwest::Client,
    a: &BenchArgs,
    run_id: &str,
) -> Result<(String, Vec<(String, String)>)> {
    let sys = login(client, &a.core_url, &a.admin_user, &a.admin_password).await?;
    let resp = client
        .post(format!("{}/api/tenant", a.core_url))
        .bearer_auth(&sys)
        .json(&json!({"title": format!("bench-{run_id}")}))
        .send()
        .await?;
    if resp.status() != 200 {
        bail!("create tenant: {}", resp.status());
    }
    let tenant_id = resp.json::<Value>().await?["id"]
        .as_str()
        .context("tenant id")?
        .to_string();
    let email = format!("bench-{run_id}@tb.io");
    let resp = client
        .post(format!("{}/api/user", a.core_url))
        .bearer_auth(&sys)
        .json(&json!({
            "tenantId": tenant_id, "email": email,
            "authority": "TENANT_ADMIN", "password": "bench",
        }))
        .send()
        .await?;
    if resp.status() != 200 {
        bail!("create tenant admin: {}", resp.status());
    }
    let admin = login(client, &a.core_url, &email, "bench").await?;

    let created = AtomicU64::new(0);
    let devices = futures::stream::iter(0..a.devices)
        .map(|i| {
            let client = client.clone();
            let admin = admin.clone();
            let created = &created;
            async move {
                let token = format!("bench-{run_id}-{i}");
                let resp = client
                    .post(format!("{}/api/device", a.core_url))
                    .bearer_auth(&admin)
                    .json(&json!({
                        "name": format!("bench-{run_id}-{i}"),
                        "accessToken": token,
                    }))
                    .send()
                    .await?;
                if resp.status() != 200 {
                    bail!("create device {i}: {}", resp.status());
                }
                let id = resp.json::<Value>().await?["id"]
                    .as_str()
                    .context("device id")?
                    .to_string();
                created.fetch_add(1, Ordering::Relaxed);
                Ok((id, token))
            }
        })
        .buffer_unordered(32)
        .collect::<Vec<Result<(String, String)>>>()
        .await;
    let mut ok = Vec::new();
    let mut failed = 0usize;
    for d in devices {
        match d {
            Ok(d) => ok.push(d),
            Err(e) => {
                failed += 1;
                if failed <= 3 {
                    eprintln!("[bench] 设备创建失败: {e:#}");
                }
            }
        }
    }
    println!(
        "[bench] 设备创建: {} 成功 / {} 失败",
        created.load(Ordering::Relaxed),
        failed
    );
    Ok((admin, ok))
}

struct DeviceOutcome {
    index: usize,
    device_id: String,
    connected: bool,
    published: u64,
    errors: u64,
    last_publish: Option<Instant>,
}

/// 单设备：连接 → 发布 messages 条 QoS0 遥测 → 断开。
async fn run_device(
    index: usize,
    device_id: String,
    access_token: String,
    host: &str,
    port: u16,
    messages: usize,
    interval: Option<Duration>,
) -> DeviceOutcome {
    let mut outcome = DeviceOutcome {
        index,
        device_id,
        connected: false,
        published: 0,
        errors: 0,
        last_publish: None,
    };
    let mut opts = MqttOptions::new(format!("bench-dev-{index}"), host, port);
    opts.set_credentials(&access_token, "");
    opts.set_keep_alive(Duration::from_secs(30));
    let (client, mut el) = AsyncClient::new(opts, 64);

    // 等 ConnAck（15s 超时），之后后台驱动事件循环刷 QoS0 发送缓冲。
    let conn = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match el.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => return true,
                Ok(_) => {}
                Err(_) => return false,
            }
        }
    })
    .await;
    if !matches!(conn, Ok(true)) {
        outcome.errors += 1;
        return outcome;
    }
    outcome.connected = true;
    tokio::spawn(async move { while el.poll().await.is_ok() {} });

    for seq in 0..messages {
        if let Some(iv) = interval {
            tokio::time::sleep(iv).await;
        }
        let payload = json!({"seq": seq, "temperature": 20 + (seq % 10)}).to_string();
        match client
            .publish("v1/devices/me/telemetry", QoS::AtMostOnce, false, payload)
            .await
        {
            Ok(()) => {
                outcome.published += 1;
                outcome.last_publish = Some(Instant::now());
            }
            Err(_) => outcome.errors += 1,
        }
    }
    // 给 QoS0 发送缓冲留 flush 时间。
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = client.disconnect().await;
    outcome
}

/// 端到端延迟：轮询 latest 直至最后一跳可见（deadline 30s）。
async fn measure_latency(
    client: &reqwest::Client,
    core_url: &str,
    admin: &str,
    device_id: &str,
    last_seq: i64,
    published_at: Instant,
) -> Option<Duration> {
    let url = format!("{core_url}/api/telemetry/DEVICE/{device_id}/values/timeseries?keys=seq");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(resp) = client.get(&url).bearer_auth(admin).send().await
            && resp.status() == 200
            && let Ok(body) = resp.json::<Value>().await
            && body["seq"][0]["value"].as_i64() == Some(last_seq)
        {
            return Some(published_at.elapsed());
        }
        if Instant::now() > deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn bench_mqtt(a: BenchArgs) -> Result<()> {
    // v7 前段是毫秒时间戳会撞，取头 4 位 + 尾 4 位随机段。
    let s = uuid::Uuid::now_v7().simple().to_string();
    let run_id = format!("{}{}", &s[..4], &s[28..]);
    let run_id = run_id.as_str();
    let client = reqwest::Client::builder().no_proxy().build()?;
    let total_start = Instant::now();

    println!(
        "[bench] run={run_id} devices={} messages={} tps_target={}",
        a.devices, a.messages, a.tps_target
    );
    let (admin, devices) = provision(&client, &a, run_id).await?;
    if devices.is_empty() {
        bail!("没有可用设备，终止");
    }

    let (host, port) = a.host.split_once(':').context("--host 须为 host:port")?;
    let host = host.to_string();
    let port: u16 = port.parse().context("--host 端口")?;
    // 不限速时每设备尽快发；限速时按 devices/tps 均摊到每设备发送间隔。
    let interval = if a.tps_target > 0.0 {
        Some(Duration::from_secs_f64(devices.len() as f64 / a.tps_target))
    } else {
        None
    };

    // 采样集合：均匀取至多 samples 台。
    let sample_count = a.samples.min(devices.len());
    let step = (devices.len() / sample_count.max(1)).max(1);
    let sampled: Vec<usize> = (0..devices.len())
        .step_by(step)
        .take(sample_count)
        .collect();

    println!(
        "[bench] 开始连接+发布（采样 {} 台做端到端延迟验证）…",
        sampled.len()
    );
    let pub_start = Instant::now();
    let outcomes: Vec<DeviceOutcome> = futures::stream::iter(devices.into_iter().enumerate())
        .map(|(i, (device_id, token))| {
            run_device(i, device_id, token, &host, port, a.messages, interval)
        })
        .buffer_unordered(usize::MAX)
        .collect()
        .await;
    let pub_elapsed = pub_start.elapsed();

    let connected = outcomes.iter().filter(|o| o.connected).count();
    let published: u64 = outcomes.iter().map(|o| o.published).sum();
    let errors: u64 = outcomes.iter().map(|o| o.errors).sum();

    // 端到端延迟抽样。
    let admin = Arc::new(admin);
    let mut lat_tasks = Vec::new();
    for o in &outcomes {
        if sampled.contains(&o.index)
            && let Some(at) = o.last_publish
        {
            let client = client.clone();
            let admin = admin.clone();
            let core_url = a.core_url.clone();
            let device_id = o.device_id.clone();
            let last_seq = (a.messages - 1) as i64;
            lat_tasks.push(tokio::spawn(async move {
                measure_latency(&client, &core_url, &admin, &device_id, last_seq, at).await
            }));
        }
    }
    let mut latencies = Vec::new();
    for t in lat_tasks {
        if let Ok(Some(d)) = t.await {
            latencies.push(d);
        }
    }

    // ---- 报告 ----
    let total = total_start.elapsed();
    let tps = published as f64 / pub_elapsed.as_secs_f64();
    println!("\n========== tb-bench 报告 ==========");
    println!("运行 ID           : {run_id}");
    println!("设备创建          : {} 台", outcomes.len());
    println!(
        "连接成功率        : {}/{} ({:.1}%)",
        connected,
        outcomes.len(),
        100.0 * connected as f64 / outcomes.len() as f64
    );
    println!("发布消息          : {published} 条（错误 {errors}）");
    println!("发布阶段耗时      : {:.2}s", pub_elapsed.as_secs_f64());
    println!("发布吞吐          : {tps:.1} msg/s");
    if !latencies.is_empty() {
        let min = latencies.iter().min().unwrap();
        let max = latencies.iter().max().unwrap();
        let avg = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        println!(
            "端到端延迟(抽样{}) : min={}ms avg={}ms max={}ms（{} 台超时未计入）",
            latencies.len(),
            min.as_millis(),
            avg.as_millis(),
            max.as_millis(),
            sampled.len() - latencies.len()
        );
    } else {
        println!("端到端延迟        : 无样本（全部超时或未发布）");
    }
    println!("总耗时            : {:.2}s", total.as_secs_f64());
    println!("===================================");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    bench_mqtt(args).await
}
