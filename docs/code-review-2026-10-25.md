# Code Review — Thingsboard-rs

审查日期：2026-10-25  
基线：`main` @ `aae9e01`，相对当时的 `origin/main`（`60ed865`）超前 25 个提交，约 217 个文件、47333 行新增。  
工作区另有未提交改动：MQTT / HTTP / CoAP 的设备认领接入。

> 本文是该时点的审查记录。其后 `main` 已继续提交设备认领的 MQTT/CoAP 入口、租户私有 CA 证书链、SNMP v2c 与 LwM2M NoSec。这些后续变更不在本次核对范围内。
>
> 后续修复记录（当前工作区）：OAuth2 已增加租户/角色/启用状态检查、禁止重定向和默认 HTTPS/公网地址限制；受保护请求已回源检查用户、凭证和改密水位，管理员重置密码不再清空该水位；生产 rule-engine 已补 Redis 地址；EDQS 的 POST、GET、直连与 PostgreSQL fallback 已强制 customer 隔离。上述改动已有定向回归，但不代表本文其他问题已修复，也不改变下文作为 `aae9e01` 历史审查记录的含义。

结论：单租户、管理员即运维的实验部署可以继续用。多客户租户或对外生产环境，在修掉下面的严重项之前不能上线。静态阅读覆盖认证、租户边界、设备接入、规则引擎出站、队列提交和部署模板；没有在本次审查中重跑集成测试。

严重级别：

| 级别 | 含义 |
| --- | --- |
| 严重 | 可跨租户或跨客户读取/接管身份，或默认配置即可伪造管理员 |
| 高 | 在已启用的产品路径上可绕过锁定、认领、会话失效或出站隔离 |
| 中 | 可靠性、权限范围或加固缺口，需要特定前置条件 |
| 低 | 加固与一致性问题 |

## 严重

### 1. OAuth2 按全局邮箱签发已有用户的 JWT

`services/tb-auth/src/oauth2.rs` 的 `oauth2_login` 用 IdP userinfo 里的 `email` 做 `find_by_email`。命中任意已有用户后直接 `issue_tokens`，不核对：

- 用户是否属于该 OAuth 客户端的租户
- 凭证是否 `enabled`
- `authority` 是否允许被这个 IdP 登录
- userinfo 是否声明 `email_verified`
- access token 是否对应这个客户端签发的身份（没有校验 ID Token 签名，只信任 `user_info_uri` 返回的 JSON）

因此，任一已启用的 OAuth2 客户端，只要其 userinfo 能返回 `sysadmin@thingsboard.org` 或其他租户用户的邮箱，调用方就得到该用户的 access/refresh token。平台级客户端（`tenant_id` 为空）还会把新用户落到最早的非 System 租户，并做成 `customer_id = None` 的 `CUSTOMER_USER`。

出站 HTTP 客户端使用 reqwest 默认重定向策略。`token_uri` / `user_info_uri` 只限制为 `http`/`https`，测试明确允许 `http://127.0.0.1/token`。管理端保存客户端虽限于 `SYS_ADMIN`，但登录端点是公开的，IdP 上的开放重定向会把换票请求带到内网。

### 2. 客户用户可以通过 EDQS 读取整个租户

`POST/GET /api/edqs/query/entityData` 只要求任意有效 access token，并用 JWT 里的 `tenant_id` 覆盖查询租户。查询引擎的 `EntityFilter::All` / `EntityType` 不按 `customer_id` 过滤。`fields` 和 `keyFilters` 可以读取实体字段、`LATEST` 遥测以及 CLIENT/SHARED/SERVER 属性。

客户用户因此能列出同租户其他客户的设备与资产，并读出最新遥测和属性。设备、遥测、告警、RPC 路由有客户归属检查，这条查询路径没有。`services/tb-core/tests/edqs_proxy.rs` 也没有客户用户被拒绝的用例。EDQS 进程本身在 `TB_EDQS_API_KEY` 为空时同样不鉴权；生产 compose 要求该密钥，但 core 代理已经把这个缺口暴露给所有登录用户。

## 高

### 3. 改密和禁用用户不能收回 access token；管理员重置密码会重新启用旧 refresh token

`AuthUser` 只校验 JWT 签名、过期时间和 token 类型，不读数据库。结果是：

- 禁用凭证、删除用户、修改 `authority` / `customer_id` 之后，已发出的 access token 在过期前仍然有效（默认 15 分钟）。
- `password_changed_ts` 只在 `AuthService::refresh` 里比较。access token 与 `get_user_from_token` 都不看这个字段。

`POST /api/user` 在带 `password` 时写入的 `password_changed_ts` 是 `None`。`user_credentials` 的 `ON CONFLICT (user_id)` 会把该列更新成 NULL。一次管理员重置密码会清掉此前 `change_password` 写下的失效水位，已经作废的 refresh token（默认 7 天）重新可用。

### 4. 登录锁定只存在于单个进程，并且会把数据库计数清零

连续 5 次失败后，`AuthService` 把用户放进进程内 `moka` 缓存，同时把 `failed_login_attempts` 写回 0。其他 `tb-auth` 实例看不到这把锁，计数也从 0 重新开始。水平扩展部署下，把登录打到另一实例即可绕过锁定。同一实例上的并发失败也会丢更新，因为读-改-写没有原子条件。

### 5. 设备认领默认开启，错误密钥会毁掉登记

`TB_CLAIM_ENABLED` 默认 `true`。设备不需要 `claimingAllowed` 属性。HTTP、MQTT、CoAP 都把空白 body 解成 `secret_key = ""`、`duration_ms = 0`，core 再把时长补成默认 24 小时。知道未分配设备名称的同租户客户用户可以用空 `secretKey` 完成认领。

`claim` 在密钥不匹配或已过期时调用 `clear_registration(..., clear_allowed = true)`，删除 Redis 登记和 `claimingData` / `claimingAllowed`。客户用户不需要正确密钥，只要知道设备名，就可以把正在进行的认领窗口拆掉。`TB_CLAIM_ENABLED=false` 时，这还会把管理员打开的 `claimingAllowed` 一并清掉。

登记使用 `SET NX`。空密钥或错误的第一次登记会占住 TTL，设备随后发布的正式密钥不会覆盖。

设备还可以自选任意正的 `duration_ms`，没有上限。

### 6. 规则引擎出站默认可以访问任意主机和任意 Kafka topic

`external.rest_call` 仅在设置了非空的 `TB_RE_ALLOWED_HOSTS` 时限制主机；未设置时 `host_allowed` 返回 true。重定向已关闭，但 `http://127.0.0.1`、链路本地地址和云元数据地址默认可达。`external.mqtt` 的 host/port 同样没有白名单。`external.kafka` 把租户管理员配置的 topic 交给平台 Kafka producer，payload 是 `TbMsg` JSON，可写入规则队列或其他内部 topic。

生产 compose 的 `tb-rule-engine` 没有设置 `TB_RE_ALLOWED_HOSTS`。租户管理员保存规则链是已开放的 API。

### 7. 网关凭据可以按名称接管同租户任意设备

`get_or_create_gateway_device` 在租户内按名称查到设备后直接返回该设备，不检查它是否由这台网关创建、是否未分配、是否已有独立凭据。网关随后为它注册会话、上报遥测，并因为子会话把 `is_gateway` 设为 `true` 而进入 RPC 订阅路径。

一台被标记 `additional_info.gateway=true` 的设备，其接入凭据等价于该租户全部设备名称空间的控制权。`rpc_session` 又优先选择 `sub_rpc && !is_gateway` 的会话，所以脏的直连会话在 Redis 租约（5 分钟）内会压过网关上仍活着的子设备会话，RPC 会发到已经断开的连接。

### 8. Provision 会交出现有设备的 access token，并且密钥是全局查找

未认证的 `POST /api/v1/provision` 和 MQTT `username=provision` 调用 `find_by_provision_key(None, key)`。SQL 是 `LIMIT 1` 且没有 `ORDER BY`，`provisionDeviceKey` 也没有唯一约束。两个租户使用同一个 key 时，命中哪条 profile 不确定。

`provisionDeviceSecret` 缺失时按空字符串比较，比较本身不是常量时间。密钥通过后，若该租户已有同名设备，响应返回该设备当前的 `credentials_id`。持有 profile provision 密钥的调用方可以取回管理员手工创建的设备 token，不限于本次新建的设备。单连接有 20 次尝试和 5 次连续失败上限，没有全局尝试限制。

### 9. 种子管理员使用公开的默认口令

首次启动在用户不存在时写入 `sysadmin@thingsboard.org` / `sysadmin`，没有强制改密，也没有启动失败开关。生产 compose 要求替换 `JWT_SECRET` 和数据库口令，不要求替换这个账号。

未设置 `TB_JWT_SECRET` 时，`JwtTokenFactory::from_env` 使用源码中的 `tb-rs-dev-only-secret-do-not-use-in-production`。生产 compose 用 `${JWT_SECRET:?...}` 挡住了这条路径；直接运行二进制则可以伪造任意 `authority`、`tenant_id` 和 `customer_id`，因为请求路径不回源核对用户。

## 中

### 10. 未提交的认领接入：三条传输的失败语义不一致

审查时的工作区让三条传输都能登记认领，并拒绝 JSON 数组。HTTP 与 CoAP 在 Kafka 发送失败时返回错误；MQTT `v1/devices/me/claim` 调用不返回 `Result` 的 `send_core`，失败只打日志，QoS 1 仍然 PUBACK。认领是一次性登记，设备无法从 PUBACK 判断 core 是否收到。

CoAP 每次认领使用新的 `Uuid::now_v7()` 作为 `session_id`。`claim::register` 只用其中的 tenant/device id，不会打开会话，这一点是安全的。成功响应只表示消息已入队，不表示 Redis 登记已经写入。

### 11. 传输限流默认关闭，且只在单个进程内

`TB_TRANSPORT_RATE_LIMIT_PER_TENANT` 未设置时，`rate_limit_check` 恒为 true。令牌桶在进程内存里，多实例之间不共享。CoAP 另有按源地址的内存限流，换地址即可绕过。

### 12. 生产模板里的 Redis 和 Kafka 没有认证

`docker-compose.prod.yml` 中 Redis 无口令，Kafka 为 `PLAINTEXT`，且 `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`。端口没有映射到宿主机，风险在容器网络内。能访问该网络的调用方可以读会话与认领密钥，也可以向 `tb_core`、规则队列投递消息。core 把传输上报的 `SessionInfo` 当作已认证会话处理。

### 13. MQTTS 客户端证书只做钉扎，不校验 CA 与吊销

`PinnedClientCertVerifier::verify_client_cert` 只检查证书能解析且在有效期内，然后返回 `ClientCertVerified`。握手签名校验能证明客户端持有该证书私钥，再按证书哈希查找设备。这个模型可以工作，但被盗的私钥在凭据删除之前一直有效，过期以外没有吊销。`client_auth_mandatory` 为 false，同一 TLS 端口仍接受用户名口令。呈现了客户端证书时，认证不再回退到 access token。

> 其后提交 `dc57200` 增加了基于租户私有 CA 的证书链验证与设备自动开通。本节描述的是 `aae9e01` 时的钉扎行为。

## 低

### 14. 认证服务的 CORS 与 core 不一致

`tb-auth` 使用 `CorsLayer::permissive()`。`tb-core` 默认只允许 `http://localhost:3000`，并用 `TB_CORS_ALLOWED_ORIGINS` 收紧。当前会话放在 `Authorization` 头里，浏览器不会自动带上，现有风险有限。两套策略应该一致。

### 15. 搜索通配符未转义

`PageLink::search_pattern` 把用户输入包进 `%...%` 后绑定到 `ILIKE`。这不是 SQL 注入，排序列也有白名单。用户输入的 `%` 和 `_` 会改变匹配范围。

### 16. Rhai 的 `print`/`debug` 进入平台日志

脚本引擎限制了操作数、字符串长度和容器大小，也没有注册 IO。`on_print` 以 `info` 写日志，规则脚本可以刷日志。JSON 转换有 64 层深度限制，引擎表达式深度使用 Rhai 默认值。

## 本次认领改动里做对的部分

- HTTP、MQTT、CoAP 都先把 body 解成 JSON object，再映射到 `ClaimPayload`。`[]` 不再被当成合法认领体。
- 空白 body 的处理在三条传输上一致。
- CoAP 把原来的 `send_core` 拆成返回 `Result` 的发送函数，入队失败回 `5.00`，不再假装成功。
- MQTT 与 CoAP 都补了端到端测试：设备登记后，客户用户用同一密钥完成认领。
- core 侧 `assign_customer_if_unassigned` 仍是认领落库的原子条件，重复认领不会把已分配设备改派。

## 其余已经成立的保护

- 设备、遥测、告警、RPC 在租户管理员与客户用户之间有归属检查。租户管理员不能把用户提升为 `SYS_ADMIN`。
- DAO 查询使用绑定参数。告警与审计日志里的 `format!` 只拼接固定条件片段和白名单排序列。
- 口令使用 bcrypt 默认 cost。MQTT basic 的口令比较是常量时间。access token 与 refresh token 类型不能混用。
- 规则引擎和 core 消费者使用手动提交；处理失败不提交 offset，规则侧失败进入 DLQ。
- 告警 ack/clear 用条件更新，状态竞争返回冲突而不是覆盖。
- X.509 凭据入库前做 DER 解析，拒绝空证书和多证书拼接，哈希与 Java 向量对齐。
- 生产 compose 不把 PostgreSQL 端口暴露到宿主机，并要求 `JWT_SECRET` 与 `TB_EDQS_API_KEY`。

## 建议的修复顺序

1. OAuth2 登录只允许客户端所属租户内、凭证仍启用的用户；拒绝用邮箱冒充其他 authority。校验 ID Token，或至少要求 userinfo 来自已登记且禁止重定向的 HTTPS 端点。
2. EDQS 对 `CUSTOMER_USER` 强制 `customer_id` 过滤；没有客户归属的用户不能查询 `All`。
3. 每个受保护请求核对用户仍存在、凭证启用，且 `iat` 不早于 `password_changed_ts`。管理员设置密码时写入当前时间，禁止把该列更新成 NULL。
4. 登录失败计数和锁定放进 PostgreSQL 或 Redis，用原子更新。
5. 认领默认关闭。错误密钥只返回失败，不删除登记，也不清除 `claimingAllowed`。空密钥在默认策略下拒绝。给 `duration_ms` 加上限。MQTT 在入队失败时不要 PUBACK。`SET` 允许设备在窗口内更新自己的密钥。
6. 规则引擎出站主机默认拒绝，Kafka topic 限制在显式允许的前缀之外的内部 topic 不能写。
7. 网关只能绑定自己创建或显式授权的子设备。子设备会话不要标记成 `is_gateway`，避免压过或被直连会话错误优先。
8. provision key 全局唯一；缺失 secret 时直接失败；对已存在设备返回凭据要单独授权。密钥比较使用常量时间，并加全局失败限流。
9. 首次启动若仍是默认 sysadmin 口令则拒绝对外监听，或要求 `TB_SYSADMIN_PASSWORD`。未设置 `TB_JWT_SECRET` 时进程退出。
10. 生产 Redis 设置口令，Kafka 关闭自动建 topic 并启用认证；传输限流给出生产默认值。
