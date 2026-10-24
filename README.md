# Thingsboard-rs

ThingsBoard IoT 平台的 Rust 重写版（微服务架构，非兼容重设计）。

## 架构

```
                        ┌──────────────────────────── 设备 ────────────────────────────┐
                        │  MQTT :1883        HTTP :8088         CoAP :5683/udp         │
                        └───────┬────────────────┬──────────────────┬─────────────────┘
                                ▼                ▼                  ▼
                    ┌──────────────────────────────────────────────────────┐
                    │  transport 层（tb-transport-mqtt / -http / -coap）    │
                    └───────┬─────────────────────────────────┬────────────┘
              transport-api │ 请求/响应（凭证校验/配置拉取）   │ ToCoreMsg（遥测/属性/会话事件）
                            ▼                                 ▼
                    ┌───────────────┐   REST :8080   ┌──────────────────┐
                    │    tb-core    │◀──────────────▶│   PostgreSQL     │
                    │ （实体/会话/  │                └──────────────────┘
                    │  RPC/审计）    │── notifications ──▶ transport（下行）
                    └──────┬────────┘
                           │ tb_rule_engine.Main
                           ▼
                    ┌───────────────┐   规则链执行（Rhai 脚本/遥测落库/告警）
                    │ tb-rule-engine│──────────▶ ts_kv / attribute_kv / alarm
                    └──────┬────────┘
                           │ tb_edqs.events（按租户分区）
                           ▼
                    ┌───────────────┐   ┌──────────────┐   ┌────────────────┐
                    │    tb-edqs    │   │   tb-auth    │   │ tb-housekeeper │
                    │ 读模型 :8090  │   │ JWT :8081    │   │ 周期清理       │
                    └───────────────┘   └──────────────┘   └────────────────┘
                        底层消息总线：Kafka    缓存：Redis    存储：PostgreSQL
```

## 服务清单

| 服务 | 二进制 | 默认端口 | 职责 |
|---|---|---|---|
| tb-core | `tb-core` | 8080 (HTTP) | REST API、实体管理、会话路由、RPC、审计、用量统计 |
| tb-auth | `tb-auth` | 8081 (HTTP) | JWT 签发/刷新/校验、改密、OAuth2 登录 |
| tb-rule-engine | `tb-rule-engine` | — | 规则链消费执行、遥测落库、告警、计算字段 |
| tb-transport-mqtt | `tb-transport-mqtt` | 1883 (TCP) | 设备 MQTT 接入（含网关、OTA、provision） |
| tb-transport-http | `tb-transport-http` | 8088 (HTTP) | 设备 HTTP 接入 |
| tb-transport-coap | `tb-transport-coap` | 5683 (UDP) | 设备 CoAP 接入 |
| tb-edqs | `tb-edqs` | 8090 (HTTP) | 实体数据查询服务（内存读模型） |
| tb-housekeeper | `tb-housekeeper` | — | 周期清理过期 ts_kv / 调试事件 / 已清除告警 |

## 环境变量

通用：`TB_DATABASE_URL`（默认 `postgres://thingsboard:thingsboard@localhost:5433/thingsboard`）、
`TB_KAFKA_BROKERS`（默认 `localhost:29092`）、`RUST_LOG`。

| 变量 | 服务 | 默认 | 说明 |
|---|---|---|---|
| `TB_HTTP_PORT` | tb-core / tb-auth | 8080 / 8081 | HTTP 监听端口 |
| `TB_JWT_SECRET` | tb-core / tb-auth | 内置开发值 | JWT 签名密钥，生产必须覆盖且两服务一致 |
| `TB_JWT_ACCESS_EXPIRATION_SEC` / `TB_JWT_REFRESH_EXPIRATION_SEC` | tb-auth | 900 / 604800 | token 有效期 |
| `TB_REDIS_URL` | tb-core | `redis://localhost:16379` | latest 遥测缓存 |
| `TB_EDQS_URL` | tb-core | `http://localhost:8090` | EDQS 代理目标 |
| `TB_EDQS_API_KEY` | tb-core / tb-edqs | 空 | 内部 API 密钥，生产必须配置且两服务一致 |
| `TB_INGEST_ENABLED` | tb-core | false | 开启旧直通遥测落库（Phase 4 起由 rule-engine 接管，默认关） |
| `TB_RE_QUEUE` | tb-rule-engine | `Main` | 消费的规则引擎队列名 |
| `TB_MQTT_BIND` | tb-transport-mqtt | `0.0.0.0:1883` | MQTT 监听地址 |
| `TB_TRANSPORT_HTTP_PORT` | tb-transport-http | 8088 | HTTP transport 端口 |
| `TB_COAP_BIND` | tb-transport-coap | `0.0.0.0:5683` | CoAP 监听地址 |
| `TB_NODE_ID` | 三个 transport | 自动生成 | 节点 ID（Kafka 下行 topic 路由键） |
| `TB_EDQS_PORT` / `TB_EDQS_GROUP` | tb-edqs | 8090 / 默认组 | 查询端口 / 消费组 |
| `TB_HOUSEKEEPER_INTERVAL_SEC` | tb-housekeeper | 3600 | 清理周期 |
| `TB_TS_TTL_DAYS` / `TB_DEBUG_EVENT_TTL_DAYS` / `TB_ALARM_TTL_DAYS` / `TB_DEDUP_WINDOW_TTL_DAYS` | tb-housekeeper | 0 / 7 / 0 / 7 | 各类数据保留天数（0=不清理；去重仅清终态窗口） |

## 快速开始（开发）

```bash
just up        # 启动 postgres(:5433) / kafka(:29092) / redis(:16379)
just migrate   # 应用数据库迁移
just build     # 构建 workspace
just test      # 运行测试
just verify    # up + migrate + test

# 本地跑全栈（各一个终端）
cargo run -p tb-core
cargo run -p tb-rule-engine-server
cargo run -p tb-transport-mqtt
```

> 端口有意避开本机 5432/9092/6379 上可能已运行的 Java 版 ThingsBoard 栈。

## 生产部署

```bash
docker build -f docker/Dockerfile -t thingsboard-rs:latest .   # 单镜像含全部服务
cp .env.prod.example .env.prod                                  # 填入 JWT_SECRET 等
docker compose -f docker-compose.prod.yml --env-file .env.prod up -d
```

单一镜像通过 `--entrypoint`（compose 中为 `entrypoint:`）区分服务角色，默认入口 `tb-core`。
详见 [docker-compose.prod.yml](docker-compose.prod.yml)。

## 压测

```bash
# 需 tb-core + tb-rule-engine + tb-transport-mqtt 已启动
cargo run -p tb-bench --release -- mqtt \
  --host localhost:1883 --devices 100 --messages 10 --tps-target 0
```

输出连接成功率 / 发布吞吐 / 端到端延迟（REST latest 抽样验证）文字报告。
实测数据与规模外推方法见 [docs/PERFORMANCE.md](docs/PERFORMANCE.md)。

## 文档

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)：服务划分、Kafka 消息流、数据模型、与 Java 版差异
- [docs/API.md](docs/API.md)：REST 端点速查表
- [docs/PERFORMANCE.md](docs/PERFORMANCE.md)：压测方法与实测数据

## 仓库结构

```
crates/
├── tb-common         # 共享基础类型：EntityId/EntityType、KvValue、TbMsg、错误
├── tb-proto          # 服务间 protobuf 契约（proto/ 目录，prost 生成）
├── tb-queue          # Kafka 抽象：producer/consumer/admin、按实体 ID murmur3 分区
├── tb-dao            # PostgreSQL 数据访问层（sqlx）
├── tb-script         # Rhai 脚本沙箱（规则引擎用）
├── tb-rule-engine    # 规则链框架与节点库（库 crate）
├── tb-calculated     # 计算字段
└── tb-transport-api  # transport ↔ core 请求/响应客户端与服务端
services/             # 8 个可执行服务（见上表）
tools/bench           # tb-bench 压测工具
proto/                # 服务间消息契约定义
migrations/           # SQL 迁移（编译期嵌入 tb-dao）
docker/Dockerfile     # 全服务单镜像多阶段构建
docker-compose.yml    # 开发用基础设施（postgres/kafka/redis）
docker-compose.prod.yml # 生产部署模板（完整栈）
```
