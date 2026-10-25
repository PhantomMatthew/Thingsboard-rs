# Thingsboard-rs 后续开发计划

计划基线：`main@df71464`  
依据：`docs/code-review-2026-10-25.md`、`docs/java-feature-gap.md` 及当前源码复核。

## 1. 目标与边界

目标按以下顺序推进：

1. 消除跨租户、跨客户、身份接管和默认凭证风险。
2. 保证生产模板可启动，消息确认、Kafka 提交、最新遥测和 EDQS 的数据语义可靠。
3. 补齐已有协议主路径，避免把“能连接”误当成“协议已完整实现”。
4. 补齐 Java 社区版常用后端管理能力。
5. 在稳定后端 API 后建设实时订阅、Dashboard 和 Web UI。
6. 根据明确部署需求再评估其他存储、队列和专业版能力。

以下属于既定架构选择，不以“对齐 Java”为理由返工：

- Rhai 替代 JavaScript/Nashorn；新增 TBEL 或 JS 兼容只作为独立迁移工具需求评估。
- 无 Actor 框架，继续使用无状态服务、Kafka 分区和 Redis 会话注册表。
- 管理 REST 已重新设计，不追求逐端点复制 Java REST。
- EDQS 保持当前单进程读模型，先修 correctness 和权限，再决定是否拆分进程组。
- 单镜像、多 entrypoint 的部署方式保持不变。
- Java CE 本身不支持 MQTT QoS2，因此 QoS2 不列入 Java 差距任务。

## 2. 执行规则与统一验收标准

### 2.1 实施规则

- 每个任务先补能复现问题的测试，再修改实现；安全测试必须覆盖攻击者路径，而不只覆盖管理员正常路径。
- 数据库变更使用新 migration，不修改已发布 migration；提供升级和必要的回滚/清理步骤。
- 配置变更同时更新开发、测试、生产 compose 和文档。生产安全默认值不能依赖开发人员记得额外配置。
- 跨服务消息格式变更必须考虑滚动升级：先兼容读写，再切换生产者，最后移除旧格式。
- 安全修复优先保持开发测试可运行；测试需要的不安全配置必须显式开启，不能继续作为生产默认值。
- 不把“编译通过”当作完成。每个阶段都要执行对应集成测试和端到端场景。

### 2.2 每个阶段的最低检查

```bash
CARGO_NET_OFFLINE=true cargo test --workspace
CARGO_NET_OFFLINE=true cargo clippy --workspace --all-targets -- -D warnings
CARGO_NET_OFFLINE=true cargo check --workspace --all-targets
```

涉及生产模板时执行：

```bash
POSTGRES_PASSWORD=validation-only JWT_SECRET=validation-only TB_EDQS_API_KEY=validation-only \
TB_SYSADMIN_PASSWORD=validation-only \
  docker compose -f docker-compose.prod.yml config --quiet
```

涉及镜像时，使用 release 镜像启动完整服务，检查所有 entrypoint、动态链接和迁移。全量测试与 Kafka 集成测试串行执行，避免多个测试任务争用同一基础设施。

## 3. 阶段 0：基线修正与可重复验收

### T0.1 修正文档事实

- 将规则节点数量修正为 30。
- 将关系能力修正为“无通用关系 REST API”，保留规则节点使用关系的说明。
- 将设备活动描述修正为“有活动更新时间，但无 Profile 不活动超时状态机”。
- 将告警 REST 能力限定为查询、ack、clear；创建来自规则节点。
- 将“证书链签发”改为“证书链认证与设备自动注册”。
- 将 LwM2M/SNMP 摘要改为“有限主路径已覆盖，完整协议仍有差距”。
- 在 review 文档中区分“基线问题”和“当前仍存在问题”，删除当前语境下“未提交认领接入”的歧义。

完成标准：文档中的每项状态都能链接到当前源码或测试；历史基线描述不会被误读为当前功能清单。

### T0.2 建立验收矩阵

建立以下固定场景，后续每个发布里程碑复用：

- SYS_ADMIN、TENANT_ADMIN、CUSTOMER_USER 的允许/拒绝矩阵。
- 两租户、同租户两客户、同客户多用户的隔离矩阵。
- 禁用用户、删除用户、改密、角色变化、customer 变化后的 token 行为。
- Kafka、Redis、PostgreSQL、EDQS 短暂不可用及恢复。
- 服务重启、双实例、重复消息、乱序消息和 poison message。
- MQTT/HTTP/CoAP/LwM2M/SNMP 的真实设备或真实协议客户端测试。

## 4. 阶段 1：生产安全阻断项

本阶段完成前，不把当前版本标记为多客户或公网生产可用。

### T1.1 修复生产启动配置

- 给 `tb-rule-engine` 配置容器内 `TB_REDIS_URL=redis://redis:6379` 和 Redis 健康依赖。
- 检查所有服务的必用环境变量，禁止用开发地址悄悄连接容器自身。
- 为缺失 Redis、Kafka、PostgreSQL 的启动失败提供明确日志。
- 用生产 compose 实际启动并验证一条 MQTT 遥测最终落库，而不只验证 YAML。

涉及：`docker-compose.prod.yml`、`services/tb-rule-engine/src/main.rs`。

### T1.2 修复 OAuth2 身份签发与 SSRF

- 已有用户必须匹配 OAuth2 客户端允许的租户、authority 和 customer 策略。
- 每次登录核对用户存在且凭证启用；禁止仅凭全局邮箱签发其他用户 token。
- 平台级客户端不得把新用户默认写入“最早租户”；租户归属必须显式配置。
- 校验 `email_verified`（按 provider 能力配置）和必要的 ID Token 签名/issuer/audience/nonce。
- token/userinfo 请求禁止自动重定向；生产默认仅允许 HTTPS，并限制内网、环回和链路本地目标。
- 对 token/userinfo 端点使用超时、响应大小限制和明确的错误映射。

验收：构造返回 sysadmin 或其他租户邮箱的 IdP，不能获得对应 token；开放重定向不能访问内网。

涉及：`services/tb-auth/src/oauth2.rs`、`services/tb-auth/src/service.rs`、`services/tb-core/src/routes/oauth2.rs`。

### T1.3 让 access token 受用户状态约束

- core/auth 的受保护请求核对用户存在、凭证启用，并校验 token 的租户、角色、customer 与当前身份一致。
- access token 的 `iat` 不得早于 `password_changed_ts`。
- 管理员设置/重置密码必须更新 `password_changed_ts`，禁止 upsert 将其清空。
- refresh 与 access 使用同一失效水位；删除用户后所有 token 立即失效。
- 身份回源失败时关闭受保护请求，不允许降级为仅信任 JWT。

验收：禁用、删除、改密、管理员重置、角色变化和 customer 变化后，旧 token 均不能继续调用 API；新旧 refresh token 行为一致。

涉及：`services/tb-auth/src/extract.rs`、`services/tb-auth/src/service.rs`、`services/tb-core/src/routes/user.rs`、`crates/tb-dao/src/user.rs`。

### T1.4 EDQS 强制客户隔离

- 在服务端根据认证身份注入 customer 范围，忽略客户端伪造的 tenant/customer 条件。
- CUSTOMER_USER 只能查询分配给其 customer 的实体；无 customer 的 CUSTOMER_USER 默认无结果或拒绝。
- TENANT_ADMIN 保持租户范围；SYS_ADMIN 必须显式指定 tenant。
- POST、GET、EDQS 直连和 PostgreSQL fallback 使用同一权限语义。
- 内部 EDQS API 默认要求密钥，不接受空密钥鉴权。
- 增加字段、latest、三种属性 scope 和 keyFilters 的越权测试。

涉及：`services/tb-core/src/routes/edqs.rs`、`services/tb-edqs/src/query.rs`、EDQS HTTP API 和 `services/tb-core/tests/edqs_proxy.rs`。

### T1.5 默认管理员和 JWT 密钥安全

- 生产模式要求显式配置高强度 sysadmin 初始密码，禁止固定 `sysadmin` 密码。
- 初始管理员首次登录强制改密；已有开发环境通过显式 development 配置保留便利。
- 非显式开发模式下缺少 `TB_JWT_SECRET` 时拒绝启动；校验密钥最低强度。
- 生产 compose 和启动文档同步更新，不提交真实密码或密钥。

涉及：`services/tb-core/src/seed.rs`、`services/tb-auth/src/token.rs`、生产部署文件。

### T1.6 网关、Provision 与 Claim 权限边界

网关：

- 同名设备只能绑定到创建它的网关或管理员显式授权的网关，不能按名称直接接管。
- 修正子设备会话的 gateway 标记和 RPC 会话选择；直连/网关冲突要有明确优先级。
- 增加跨网关、同租户、重复名称和断线重连测试。

Provision：

- provision key 增加唯一约束和冲突迁移策略。
- 缺失 secret 直接失败，使用常量时间比较，并增加跨连接/跨实例失败限流。
- 已存在同名设备不得仅因 provision 密钥正确就返回现有 access token；单独定义安全授权流程。

Claim：

- 默认关闭；空密钥、错误密钥和过期请求不能破坏有效登记或 `claimingAllowed`。
- 限制 duration 上限；允许设备在有效窗口内安全更新自己的登记。
- 明确“已入队”和“已登记”的响应语义，入队失败不能返回成功。

涉及：`services/tb-core/src/transport_api.rs`、`services/tb-core/src/claim.rs`、`crates/tb-dao/src/device_profile.rs`、MQTT/HTTP/CoAP 传输。

### T1.7 规则引擎出站隔离

- REST/MQTT 默认拒绝外部目标，仅允许显式白名单；重定向后重新校验目标。
- 阻止环回、内网、链路本地和云元数据地址，处理 DNS rebinding。
- Kafka topic 使用显式允许列表/前缀，禁止写入内部队列和任意平台 topic。
- 生产模板提供安全默认值；测试环境显式配置允许目标。
- 增加租户管理员恶意规则链的 SSRF、DNS、重定向和内部 topic 测试。

涉及：`crates/tb-rule-engine/src/nodes/external.rs`、`docker-compose.prod.yml`。

### 阶段 1 发布门槛

- OAuth2、EDQS、token、网关、provision、claim、出站攻击测试全部通过。
- 双实例下身份失效与限流语义一致。
- 生产 compose 从零启动后完整遥测链路成功，无默认管理员密码和默认 JWT 密钥。

## 5. 阶段 2：消息与数据可靠性

### T2.1 修复传输确认语义

- MQTT telemetry、attributes、RPC、claim 和 gateway 消息只有在满足既定投递保证后才 PUBACK。
- Kafka 发送失败、限流或 core 拒绝时不得确认成功；明确客户端重试和会话断开策略。
- 检查 HTTP/CoAP/LwM2M 的同类“只记录错误但返回成功”路径。
- 测试 broker 故障、发送超时、队列满、重连和重复投递。

### T2.2 Kafka poison message、失败提交与 DLQ

- core 对不可解码消息定义有界重试、DLQ、错误上下文和 offset 提交策略，避免单条消息永久堵分区。
- 默认根规则链不能把保存节点的 `Failure` 当作处理成功；数据库失败不得提交 offset。
- DLQ 包含原始消息、来源 topic/partition/offset、错误和时间，并限制敏感信息。
- 提供 DLQ 查看、重放和重放幂等策略；重放不能绕过租户与权限校验。
- 测试连续 poison、处理失败、服务崩溃、DLQ 满及恢复。

涉及：`services/tb-core/src/core_consumer.rs`、`crates/tb-rule-engine/src/engine.rs`、`services/tb-rule-engine/src/lib.rs`、保存节点。

### T2.3 遥测 latest 与删除一致性

- 范围删除后在事务中重算受影响 key 的 latest；无剩余点时删除 latest。
- 全 key 删除同步 PostgreSQL、Redis 和 EDQS。
- 明确删除起点/终点边界、并发写入冲突和时间范围为空的行为。
- 增加“删除旧点、删除最新点、删除全部、删除期间并发写入”的测试。

涉及：`crates/tb-dao/src/timeseries.rs`、`services/tb-core/src/routes/telemetry.rs`、`services/tb-edqs/src/state.rs`。

### T2.4 EDQS 增量可靠性

- 属性删除、遥测删除、实体删除、customer 变更都发布对应读模型事件。
- 消除 fire-and-forget 满队列/Kafka 故障造成的永久丢更新；采用可重试发送、持久 outbox 或定期对账。
- 快照与增量切换需要版本/offset 水位，避免中间空洞。
- 提供滞后、失败和最后应用 offset 的可观测性。
- 故障测试必须证明最终一致，而非只检查正常路径。

### T2.5 OTA 正确性

- 优先使用设备显式 firmware/software 绑定，其次 Profile 显式绑定；不能简单选择最新上传包。
- 绑定校验包类型、租户和 Profile 兼容性。
- 统一版本选择、哈希、分块、断点/重复请求和错误语义。
- 增加设备绑定与 Profile 绑定冲突、上传新包不改变既有绑定等测试。

涉及：`crates/tb-dao/src/ota.rs`、`services/tb-core/src/routes/ota.rs`、`services/tb-core/src/transport_api.rs`、各传输 OTA 模块。

### T2.6 生命周期清理与分布式控制

- 删除租户、客户、设备、Profile 时清理或级联处理遥测、属性、告警、关系、会话、凭证和 EDQS 状态，避免孤儿数据。
- 登录失败计数与锁定迁移到 PostgreSQL/Redis 原子操作，覆盖并发和跨实例。
- 传输限流提供生产默认值，并迁移到共享限流或明确分布式算法；保留每实例限制作为补充。
- 回归现有分布式持久化规则去重：Redis 故障、重启、TTL、同 key 并发和重复投递不得退回单机内存语义。

### T2.7 中低优先级加固

- 生产 Redis/Kafka 支持认证和 TLS，关闭 Kafka 自动建 topic，提供受控初始化。
- auth/core 使用一致 CORS 策略。
- 搜索转义 `%`、`_` 和转义符，同时保留分页与排序白名单。
- 限制 Rhai print/debug 日志速率、长度和敏感内容；补齐表达式深度限制。

### 阶段 2 发布门槛

- 基础设施故障下不错误确认、不永久堵分区、不静默丢 EDQS 更新。
- 删除、OTA、去重和生命周期场景通过双实例及重启测试。
- 全量回归通过，并记录与基线相比的吞吐和延迟变化。

## 6. 阶段 3：现有设备协议补全

协议任务可以按实际需求调整顺序，但必须先完成阶段 1/2 的共享认证、OTA 和消息可靠性基础。

### T3.1 OTA 全链路

- MQTT 增加 software topic 和状态回写。
- HTTP 增加 software 下载与状态回写。
- CoAP 增加 firmware/software 分发。
- 统一 `fw_state`/`sw_state`、进度、错误和版本状态模型。
- 用真实客户端验证 firmware 与 software 的完整升级流程。

### T3.2 LwM2M

按依赖顺序实施：

1. 安全：PSK，再评估 RPK/X509；DTLS 身份与设备凭证绑定。
2. 完整下行操作：Write、Execute、Discover、Create、Delete、Write-Attributes。
3. Composite Read/Write/Observe 和非 SINGLE 策略。
4. Bootstrap、注册生命周期、Queue Mode、离线下行和地址迁移回归。
5. LwM2M OTA、RPC 映射和完整错误码。

每一步继续使用真实 Leshan 客户端，覆盖安全、并发、乱序通知和独立响应，不以单元编解码测试替代互操作测试。

### T3.3 SNMP

- 先修启动发现只取第一页 1000 台的问题：完整分页、周期刷新和实体/Profile 变更处理。
- 增加 v1/v3 和 USM 认证加密，密钥只走安全配置。
- 增加 TCP、共享属性 SET、设备 RPC、Trap/to-server RPC。
- 验证设备上下线、配置热更新、超时、重试和重复 Trap。

### T3.4 CoAP、HTTP 和 MQTT 兼容项

- CoAP：DTLS、provision、OTA；明确 session 与确认重试语义。
- HTTP：HTTPS 部署/终止方案；attributes 长轮询仅在确有兼容需求时实现，因为 Java 已标记 deprecated。
- MQTT：按客户端需求补 v2 短 topic、JSON/Protobuf、Sparkplug、网关 claim 和 MQTT over WebSocket。
- 保持 Java 已兼容 topic/path 的回归向量；QoS2 不作为 Java 对齐目标。

### 阶段 3 发布门槛

- 文档按协议列出支持矩阵，不再用单一“支持 LwM2M/SNMP/OTA”概括子集。
- 每个声明支持的能力都有真实客户端端到端测试和故障测试。

## 7. 阶段 4：Java CE 常用后端能力

### T4.1 EDQS 查询面

- 实体 count、available keys。
- 单实体、设备/资产类型、关系过滤。
- 告警查询、告警计数和 API usage 查询。
- 所有新增过滤器复用统一 tenant/customer 权限范围。
- 为 PostgreSQL fallback 与 EDQS 建立同输入同结果测试。

### T4.2 关系、Entity View 与 Dashboard 后端

- 通用关系增删查 API、权限校验、分页和级联语义。
- Entity View 数据模型、API、遥测/属性可见范围及传播语义。
- Dashboard CRUD、分配、公开链接和权限；不能仅因已有 DAO 就标记完成。
- 增加客户越权、公开链接撤销和关系环路/无效端点测试。

### T4.3 告警完整状态机

- 设备 Profile 阈值规则与不活动超时，覆盖持续值、恢复、ack/clear 和 Profile 热更新。
- 告警 REST 创建、评论、指派及权限。
- create/clear 节点的 owner、tenant、relation propagation。
- 计算字段告警规则和多输出；明确去重、升级、清除和事件顺序。

### T4.4 RPC 与会话

- 持久 RPC、离线排队、过期、重试、取消和设备上线投递。
- 双向 RPC 规则节点，关联请求/响应并处理超时。
- 网关与直连会话冲突、持久会话和重启恢复测试。

### T4.5 规则节点与计算字段

按使用频率分批实现，不为追求数量一次堆叠未测试节点：

1. 脚本/profile switch、告警状态过滤、读取原始遥测、delta。
2. customer/tenant attributes、拆分数组、生成器、分配客户、关系创建/删除。
3. 双向 RPC、同步节点和通知中心节点。
4. RabbitMQ、SNS/SQS、Pub/Sub、Azure IoT Hub 等外部节点按实际部署需求实现。

计算字段补关联实体参数、向关联实体传播、多输出和调试。Java JS/TBEL 规则链导入只提供验证/迁移报告，不假装 Rhai 可直接兼容。

### T4.6 通知、账号和租户治理

- 邮件基础设置、模板、通知中心、接收目标和投递规则。
- 用户激活、重置密码邮件；随后按需增加 SMS/Slack。
- OAuth2 mapper、两步验证、用户 API Key。
- 租户 Profile 管理、设备/消息/数据点配额和超限拒绝策略。
- 按租户多队列路由、隔离、提交和处理策略；不是仅允许修改 `TB_RE_QUEUE` 名称。
- 补齐审计覆盖和系统设置面。

### 阶段 4 发布门槛

- 每个管理 API 有完整 RBAC/tenant/customer 测试、审计和分页测试。
- Profile 告警、持久 RPC、通知和队列都有重启/故障恢复场景。
- Java CE 功能差距文档逐项更新，区分“不支持”“部分支持”“完整支持”。

## 8. 阶段 5：实时订阅、Dashboard 与 Web UI

该阶段依赖阶段 4 的权限与 API 稳定，避免 UI 建立在仍会变化的接口上。

### T5.1 WebSocket 与实时推送

- 遥测、属性、告警和通知订阅。
- 连接认证、token 失效、订阅授权、实体权限变更后的撤订。
- 背压、慢消费者、心跳、重连、集群节点间事件路由。
- 双实例下跨节点订阅和断线恢复测试。

### T5.2 Web UI 与 Dashboard

- 登录、用户/租户/客户、设备/资产、Profile、规则链、告警和 OTA 管理。
- Dashboard/部件渲染、实时数据绑定、公开链接。
- 先定义 Rust REST 对应的前端契约，不承诺直接复用 Java UI 或官方移动 App。
- 增加浏览器端到端测试、权限可见性测试和 XSS/CSRF/CORS 检查。

### 阶段 5 发布门槛

- 核心管理工作不依赖手工 REST 调用。
- 实时订阅不错发其他 tenant/customer 数据，服务重启和 token 撤销行为正确。

## 9. 阶段 6：可观测性、扩展与按需能力

### T6.1 生产可观测性

- 所有服务提供健康、存活和就绪检查。
- Prometheus 指标：Kafka lag、处理失败/DLQ、EDQS 滞后、消息确认失败、RPC/OTA、数据库/Redis 延迟。
- 结构化日志、trace/correlation ID、告警建议和容量仪表盘。
- 扩展 `tb-bench` 覆盖 QoS1、规则失败、双实例、WebSocket 和协议混合负载。

### T6.2 可选基础设施

仅在有明确规模或许可要求时实施：

- Cassandra/TimescaleDB 时序存储。
- RabbitMQ、AWS SQS、Google Pub/Sub、Azure Service Bus。
- 多 EDQS 进程/分片及独立扩缩容。

每项都需要与 PostgreSQL/Kafka 基线做功能等价、迁移、备份恢复和性能验收。

### T6.3 专业版能力单独立项

以下不阻塞 Java CE 后端对齐，必须根据商业/用户需求单独立项：

- managed integrations 与转换器（OPC-UA、TCP、HTTP 等）。
- 实体分组、自定义 RBAC、客户层级。
- 白标、调度器、报表、版本控制、平台集成。

## 10. 里程碑与依赖

- **M0 可核对基线**：T0 完成，文档、支持矩阵和验收场景一致。
- **M1 安全可部署**：T1 完成；是多客户/公网部署的最低门槛。
- **M2 可靠数据面**：T2 完成；故障下不虚假确认、不堵分区、不静默残留旧数据。
- **M3 协议主路径完整**：按选定协议完成 T3，并通过真实客户端互操作。
- **M4 CE 常用后端可用**：T4 完成，覆盖 EDQS、关系、告警、RPC、通知和治理。
- **M5 可用产品面**：T5 完成，提供实时订阅和核心 Web 管理。
- **M6 可运营与扩展**：T6.1 完成；其余按实际需求选做。

硬性依赖：`M1 → M2 → M3/M4 → M5`。T6.1 可在 M2 后并行推进，但发布前不得用监控代替正确性修复。

## 11. 建议的首批执行任务

按顺序直接开始，不等待 UI 或完整协议项目：

1. 修复生产 rule-engine Redis 配置并做 compose 端到端启动。
2. 修复 OAuth2 身份边界与出站 HTTP 安全。
3. 修复 access/refresh token 失效和管理员重置密码水位。
4. 修复 EDQS customer 隔离的所有查询路径。
5. 修复 sysadmin/JWT 生产默认值。
6. 修复网关、provision、claim 和规则引擎出站隔离。
7. 修复 MQTT 确认、Kafka poison/失败提交、latest/EDQS 一致性。
8. 修复 OTA 绑定，再扩展各协议 OTA 与 SNMP/LwM2M 剩余能力。

每个任务独立提交，包含回归测试、配置/文档更新和对应验收结果；不要用一个大规模提交同时改变认证、消息协议和数据库语义。
