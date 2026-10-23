# 性能与压测

## 工具

`tb-bench`（`tools/bench`，workspace 成员）：

```bash
# 前置：基础设施 + tb-core + tb-rule-engine + tb-transport-mqtt 已启动
cargo run -p tb-bench --release -- mqtt \
  --host localhost:1883 --devices 100 --messages 10 --tps-target 0 \
  --samples 10 \
  --core-url http://localhost:8080 \
  --admin-user sysadmin@thingsboard.org --admin-password sysadmin
```

流程与指标：

1. REST 建租户 → 租户管理员 → 批量建设备（accessToken=`bench-{run}-{i}`，32 并发）。
2. 并发 rumqttc 连接（15s ConnAck 超时），统计**连接成功率**。
3. 每设备向 `v1/devices/me/telemetry` 发 `--messages` 条 QoS0 遥测；
   `--tps-target 0` 为不限速，>0 时按 `devices/tps` 均摊为每设备发送间隔。
   统计**发布吞吐（msg/s）** 与发布错误数。
4. **端到端延迟**：均匀抽样 `--samples` 台设备，从最后一跳发布时刻起，
   以 200ms 间隔轮询 REST latest（`/api/telemetry/DEVICE/{id}/values/timeseries?keys=seq`），
   直到最后一条可见（30s 超时）。该链路覆盖 MQTT → Kafka → tb-core → 规则引擎 →
   PostgreSQL/Redis 完整路径；轮询粒度使读数含最多 ~200ms 高估。

## 实测数据

环境：macOS（Docker Desktop 跑 postgres/kafka/redis），服务为 `cargo build` debug 二进制，
单机全栈（tb-core ×1、tb-rule-engine ×1、tb-transport-mqtt ×1）。发布客户端为 release 构建。
日期：2026-10-24。

| 规模 | 连接成功率 | 发布吞吐 | 端到端延迟（抽样 avg / max） | 备注 |
|---|---|---|---|---|
| 10 设备 × 20 条（200 条） | 10/10 = 100% | 338 msg/s | 534ms / 578ms | 小批量 |
| 100 设备 × 10 条（1000 条） | 100/100 = 100% | 795 msg/s | 940ms / 1181ms | 突发脉冲 |
| 100 设备 × 50 条（5000 条） | 100/100 = 100% | 4070 msg/s | 4705ms / 5643ms | 持续突发，含积压追赶 |

解读：

- 接入层（MQTT transport + Kafka 上行）在单机可承受 ~4k msg/s 突发写入，无错误。
- 端到端延迟随突发规模增长：瓶颈在下游「规则引擎消费 + ts_kv 落库」追赶 Kafka 积压，
  而非接入层。稳态（非突发）下延迟回到亚秒级（第一组 534ms 即接近稳态基线，
  其中含 ~200ms 轮询粒度与 debug 构建开销）。

## 规模外推方法

- **横向扩接入层**：transport 服务无状态，按 `TB_NODE_ID` 区分节点；
  N 个 MQTT 节点吞吐近似线性叠加（前提是 Kafka 分区数 ≥ 设备并行度）。
- **横向扩规则引擎**：`tb_rule_engine.{queue}` 按实体 ID 分区，增开 tb-rule-engine
  实例（同 consumer group）即可分摊落库压力；分区数应 ≥ 实例数。
- **瓶颈定位顺序**：先看发布吞吐是否达标（接入层）→ 再看端到端延迟漂移（下游追赶速度）
  → 用 `--tps-target` 限速找稳态吞吐（延迟不再增长的最大 tps）。
- **建议测量法**：固定 devices，递增 `--messages` 或 `--tps-target`，观察
  「抽样延迟 avg」何时开始随运行时长线性增长——拐点即单机全栈稳态上限；
  之后每加一个 rule-engine 实例重测，验证线性度。

注意：以上数字来自 debug 构建服务 + 单机 Docker 基础设施，仅用于回归对比与量级参考；
生产容量评估请用 `docker/Dockerfile` 的 release 镜像在目标硬件上重测。
