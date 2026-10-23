# 架构

Thingsboard-rs 是 ThingsBoard IoT 平台的 Rust 重写：微服务拆分 + Kafka 消息总线 +
PostgreSQL 持久化 + Redis 缓存。协议层复用 ThingsBoard 设备接入协议（topic/路径兼容），
服务间契约为自定义 protobuf（`proto/` 目录，prost 生成）。

## 微服务划分与职责

| 服务 | 职责 | 关键依赖 |
|---|---|---|
| tb-core | REST API、实体 CRUD（租户/客户/用户/设备/资产/规则链/OTA/计算字段）、设备会话注册表、RPC 路由、审计日志、用量统计、latest 遥测缓存 | PG、Kafka、Redis、tb-edqs |
| tb-auth | JWT 签发/刷新/校验、改密、OAuth2 客户端与登录 | PG |
| tb-rule-engine | 消费规则引擎队列、执行规则链（Rhai 脚本节点）、遥测落库、告警生命周期、计算字段触发、DLQ | PG、Kafka |
| tb-transport-mqtt | 设备 MQTT 接入：凭证校验、遥测/属性上行、RPC/属性订阅下行、网关模式、OTA 分发、设备 provision | Kafka（经 transport-api 到 core） |
| tb-transport-http | 设备 HTTP 接入（同上语义的 HTTP 形态） | Kafka |
| tb-transport-coap | 设备 CoAP 接入（UDP） | Kafka |
| tb-edqs | 实体数据查询服务：启动时全量快照 + Kafka 增量事件维护内存读模型，供 tb-core 代理查询 | PG（快照）、Kafka |
| tb-housekeeper | 周期清理过期 ts_kv、规则节点调试事件、已清除告警 | PG |

## Kafka topic 清单

| Topic | 分区键 | 生产者 → 消费者 | 载荷 |
|---|---|---|---|
| `tb_transport.api.requests` | tenant_id | transport → tb-core | `TransportApiRequestMsg`（凭证校验/配置/OTA/provision 请求） |
| `tb_transport.api.responses.{nodeId}` | — | tb-core → transport（按节点） | `TransportApiResponseMsg` |
| `tb_core` | device_id | transport → tb-core | `ToCoreMsg`（遥测/属性上行、会话开闭、订阅、RPC 响应、设备活性） |
| `tb_core.notifications` | device_id | tb-core → 内部消费 | `UplinkNotificationMsg` 等上行通知 |
| `tb_transport.notifications.{nodeId}` | device_id | tb-core → transport（按节点） | `ToTransportMsg`（RPC 下行、属性更新、会话关闭、OTA 通知） |
| `tb_rule_engine.{queueName}` | entity_id | tb-core/rule-engine → tb-rule-engine | `ToRuleEngineMsg`（`TbMsgProto`：遥测/属性/活性/告警事件） |
| `tb_rule_engine.dlq` | entity_id | tb-rule-engine → （留存） | 处理失败消息（死信） |
| `tb_edqs.events` | tenant_id | tb-core → tb-edqs | `EdqsEventMsg`（实体增删、属性更新、latest 遥测） |
| `tb_core.notifications` / `tb_rule_engine.notifications` / `tb_housekeeper` | — | 已在 `tb-queue` 定义、当前未启用（预留） | — |

实体分区：设备/实体消息按实体 ID murmur3 分区，保证同实体消息有序；
EDQS 事件按 tenant_id 分区，保证同租户读模型更新有序。

## 消息流

### 上行（设备 → 存储）

```
设备 ──MQTT/HTTP/CoAP──▶ tb-transport-*
                            │ 1) ValidateToken/... ──▶ tb_transport.api.requests ──▶ tb-core ──▶ responses.{node}
                            │ 2) ToCoreMsg(telemetry/attributes/session) ──▶ tb_core
                            ▼
                        tb-core（core_consumer）
                            │ 会话注册/活性属性/用量统计
                            │ 遥测、属性 ──▶ tb_rule_engine.Main
                            ▼
                        tb-rule-engine（规则链）
                            │ script(Rhai)/filter/action 节点
                            │ save timeseries → ts_kv     save attributes → attribute_kv
                            │ create/clear alarm → alarm  失败 → tb_rule_engine.dlq
                            ▼
                        tb-core 同步写 tb_edqs.events（latest/属性/实体变更）→ tb-edqs 读模型
```

### 下行（服务端 → 设备）

```
REST /api/rpc/twoway/{device} / 属性写 SHARED_SCOPE
   → tb-core 查会话注册表得 node_id
   → tb_transport.notifications.{nodeId}
   → 该节点 transport 下行（MQTT publish / HTTP 长轮询 / CoAP）
   → 设备 RPC 响应 → tb_core → RPC 等待方
```

### EDQS

```
tb-core（实体 CRUD / latest 写入）──▶ tb_edqs.events ──▶ tb-edqs 内存读模型
查询方 ──▶ tb-core /api/edqs/query/entityData ──(TB_EDQS_URL 代理)──▶ tb-edqs /api/query/entityData
```

## 数据模型要点（PostgreSQL）

- `key_dictionary`：遥测/属性 key → int 字典，ts_kv/attribute_kv 只存 int 外键，节省存储并加速按 key 查询。
- `ts_kv`：`(entity_id, key, ts)` 三元组时序点，值分 bool/long/double/string/json 列。
- `ts_kv_latest`：每实体每 key 最新值（Redis 缓存前置，miss 回源此表）。
- `attribute_kv`：`(entity_id, attribute_type, key)`，scope = SERVER/SHARED/CLIENT。
- `relation`：`(from_id, from_type, relation_type, to_id, to_type)` 实体关系边（Contains/Manages 等）。
- `alarm`：`(originator, type)` 唯一活跃告警；状态机 ACTIVE_* → CLEARED_*/ACK。
- `device` / `device_credentials`：凭证类型 ACCESS_TOKEN / X509 / MQTT_BASIC，transport 经 transport-api 校验。
- `rule_chain` / `rule_node`：规则链元数据 + 节点配置（JSONB），脚本节点内嵌 Rhai 源码。
- `api_usage_state`：按租户聚合的消息/遥测点数用量计数。
- `audit_log`：实体写操作审计（谁、何时、对什么、做了什么）。
- `rule_node_debug_event`：规则节点调试事件（默认保留 7 天，housekeeper 清理）。

ID 一律 UUIDv7（时间有序，索引友好）。

## 与 Java 版 ThingsBoard 的设计差异

- **Rhai 替代 JS/Nashorn**：规则链脚本节点用 Rhai 沙箱执行，无 JVM 脚本引擎开销。
- **无 Actor 框架**：Java 版的 TbActorSystem（设备 actor/租户 actor 邮箱模型）被
  「无状态服务 + Kafka 分区保序 + Redis 会话注册表」替代；并发控制交给 Kafka 分区与数据库。
- **REST 端点重设计**：遥测/属性读写收敛到 `/api/telemetry/{entityType}/{entityId}/...`，
  不逐端点兼容 Java 版；设备侧协议（MQTT topic、HTTP 路径）保持兼容。
- **UUIDv7**：替代 Java 版的时间基 UUID 实现，单调性更好。
- **EDQS 简化**：Java 版 EDQS 是独立查询引擎进程组；这里实现为单进程内存读模型
  （快照 + Kafka 增量事件），由 tb-core 代理暴露。
- **传输层无 Netty**：MQTT 用 tokio-util codec 手写编解码，HTTP 用 axum，CoAP 用 coap-lite。
- **单镜像部署**：所有服务一个镜像，`--entrypoint` 区分角色（见 docker/Dockerfile）。
