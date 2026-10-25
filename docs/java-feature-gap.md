# 与 Java 版 ThingsBoard 的功能差距

整理日期：2026-10-25  
对照范围：ThingsBoard Java 社区版约 3.6 及之后的产品面。专业版能力单独列在文末。  
依据：审查当时 `main`（`aae9e01`）的服务、REST、规则节点注册表和数据模型。`Dashboard`、`Edge` 只出现在实体枚举或 DAO 中，没有对应服务。

> 本文是该时点的差距记录。其后 `main` 已接入 LwM2M NoSec（注册、Read、SINGLE Observe）、SNMP v2c 周期采集，以及基于租户私有 CA 的证书链验证与设备自动开通。下文「没有 LwM2M/SNMP」「只做证书钉扎」描述的是 `aae9e01`，不是写入本文时的最新 `HEAD`。

这是一套后端重写。设备侧 MQTT、HTTP、CoAP 的主路径大体对齐 Java 版。缺的是社区版里日常使用的管理面、查询面，以及一批传输和规则能力。

## 已经覆盖的主路径

- 设备上报遥测和属性，默认根规则链把这两类消息落到 PostgreSQL。
- REST 查询遥测、属性和告警；在线单向/双向 RPC。
- MQTT、HTTP、CoAP 接入，access token、MQTT basic、证书钉扎、租户私有 CA 证书链认证与设备自动注册。
- LwM2M NoSec 注册、Read、TLV、SINGLE Observe；SNMP v2c UDP GET 周期采集。
- 网关的 connect、disconnect、telemetry、attributes、attributes request、rpc。
- MQTT/HTTP firmware/software OTA 分块（显式绑定选包与状态回写）、MQTT/HTTP provision、设备认领；CoAP OTA/provision 尚未覆盖。
- 简化版 EDQS、简化版计算字段、审计日志、housekeeper 清理。

默认根规则链只连接「消息类型开关 → 保存遥测 / 保存属性」，不处理连接、断开、RPC 和告警规则。

## 使用入口

没有 Web UI、部件库和仪表盘渲染，也没有 WebSocket 订阅。Java 的实时遥测、属性、告警和通知都走 WebSocket；这里只能轮询 REST。

`dashboard` 表和 `DashboardDao` 已经接进 `AppState`，但没有仪表盘 API，也没有公开链接。

REST 是重新设计的，遥测和属性集中在 `/api/telemetry/...`。Java 版 UI、官方移动 App，以及按 Java REST 编写的脚本不能直接接上。设备侧 topic 和 `/api/v1/{token}/...` 按兼容实现。

## 实体和查询

已有租户、客户、用户、设备、资产、设备/资产 Profile、规则链、OTA、计算字段。

没有实体视图（Entity View），也没有通用关系增删查 REST API。关系数据已用于规则链关系过滤、关联实体属性富化、关联 originator，以及网关自动创建的 `Created` 边。

EDQS 只有四种过滤：全部、按类型、名称包含、按 ID。可读实体字段、最新遥测和 CLIENT/SHARED/SERVER 属性；POST、GET、直连查询和 PostgreSQL fallback 均已强制 customer 隔离。Java 的 EDQS 还有关系过滤、设备/资产类型、单实体、实体计数、available keys、告警查询、告警计数和 API 用量查询。

计算字段只做「若干遥测或属性参数 + 一条 Rhai 表达式 + 一个遥测或服务端属性输出」。Java 还有关联实体参数、向关联实体传播、多输出、调试、告警规则和 TBEL。

## 设备接入

已有 MQTT、HTTP、CoAP，以及 access token、MQTT basic、证书钉扎和证书链认证、上述网关子集、部分 OTA/provision、认领、在线 RPC。

Java 社区版还有、这里未完整覆盖的接入：

| 能力 | 当前主分支现状 |
| --- | --- |
| LwM2M | 已有 UDP NoSec 注册、Read、TLV、SINGLE Observe；缺 DTLS、Bootstrap、Composite、Write/Execute/Discover/Create/Delete、下行 RPC、Queue Mode 和 OTA |
| SNMP | 已有 v2c UDP GET 周期采集；缺 v1/v3、TCP、SET、Trap、RPC 和运行期设备刷新；启动发现仅取第一页最多 1000 台 |
| CoAP | 缺 DTLS、provision 和 firmware/software OTA |
| OTA | MQTT/HTTP 已覆盖 firmware 与 software 分块分发；选包只看设备级、其次 Profile 级显式绑定（校验租户、类型、Profile 与数据），生效包变化时回写 `target_*`、`{fw,sw}_state`（QUEUED/INITIATED）和 `{fw,sw}_*` shared 属性，解绑删除；缺 CoAP/LwM2M 分发、按 Profile 批量限速推送和下载进度类服务端状态 |
| MQTT/HTTP 兼容项 | 已实现路径保持 topic/path 兼容；缺 MQTT v2 短 topic、JSON/Protobuf、MQTT over WebSocket，HTTP attributes 长轮询未实现（Java 已弃用） |
| 离线持久 RPC | RPC 要求设备在线且已订阅，超时即失败 |
| Profile 不活动超时 | 消息活动会节流更新 `active`、`lastActivityTime`，连接/断开也会写活动属性；没有「超过 N 分钟无消息则离线」的 Profile 状态机 |
| 网关认领、Sparkplug | 无 `v1/gateway/claim`，无 Sparkplug |
| 证书链认证和自动注册 | 基线 `aae9e01` 仅有证书哈希钉扎；其后 `dc57200` 增加了租户私有 CA 的证书链验证与设备自动开通，不提供 CA 证书签发服务 |

## 告警、规则和通知

规则节点可以创建、确认、清除告警，同一来源、同一类型同时只有一条活跃告警；REST 只有查询、ack、clear，没有 Java 的告警创建接口。没有告警评论、告警指派、owner/tenant/关系传播，也没有设备 Profile 上的阈值规则。Java 的 `TbDeviceProfileNode` 不存在。Profile 的 `profile_data` 可以存 JSON，但不会据此自动产生告警。

规则节点注册表共 30 个：过滤、富化、改 key、改 originator、保存遥测/属性、创建/清除告警、延迟、去重、数学、单向 RPC、REST、Kafka、MQTT、子规则链。

Java 社区版常见、这里没有的节点包括：脚本 switch、设备/资产 Profile switch、告警状态过滤、读取原始遥测、计算 delta、客户/租户属性、双向 RPC 调用、发邮件、拆分数组、生成器、把设备分配给客户、创建或删除关系、同步节点、RabbitMQ、AWS SNS/SQS、GCP Pub/Sub、Azure IoT Hub、通知中心节点、推送到 Edge。

脚本引擎只有 Rhai。Java 的 JS 和 TBEL 规则链不能直接导入。

没有邮件、短信、Slack，也没有通知中心（模板、接收目标、投递规则）。没有用户激活邮件和重置密码邮件。创建用户时带密码会直接激活；不带密码只留下未启用的 `activate_token`。

## 账号和租户治理

只有 `SYS_ADMIN`、`TENANT_ADMIN`、`CUSTOMER_USER` 三种角色。没有两步验证，没有用户 API Key。

OAuth2 是简化的授权码换邮箱，没有 Java 的 mapper（按域名归入租户或客户、属性映射、已有用户匹配策略）。

租户 Profile 只有种子数据中的一条默认记录，创建租户时会挂上。没有 Profile 管理 API，也不按 Profile 限制设备数、消息数或数据点。用量接口只累计 transport 消息、规则执行和 REST 请求，超限不会拒绝。

规则引擎默认消费 `Main`，进程级 `TB_RE_QUEUE` 可改名；没有按租户配置多队列路由、隔离以及提交/处理策略。

审计日志只覆盖一部分写操作。没有 Java 的系统设置面（邮件服务器、JWT、短信等存在管理设置中）。

## 存储和运行形态

时序只在 PostgreSQL，没有 Cassandra，也没有 TimescaleDB。队列只有 Kafka，没有 RabbitMQ、AWS SQS、Google Pub/Sub、Azure Service Bus。

没有 Java 的 Actor 邮箱。会话在 Redis，同实体顺序靠 Kafka 分区。因此也没有按设备 Actor 做的本地缓存和邮箱背压。

## 专业版有、这里也没有

以下能力属于 Java 专业版，当前代码没有对应模块：

- 集成与转换器（OPC-UA、TCP、HTTP 等）
- 实体分组和自定义 RBAC
- 客户层级
- 白标
- 调度器
- 报表
- 版本控制
- 平台集成

## 差距摘要

缺的主要是管理界面、实时订阅、完整 LwM2M/SNMP/OTA 协议面、Profile 告警、离线 RPC、通知和邮件，以及 Java 规则链和 REST 的直接兼容。设备上报、落库、REST 查询、在线 RPC、LwM2M/SNMP 有限采集、MQTT/HTTP firmware/software OTA、MQTT/HTTP provision 和认领已经有主路径。
