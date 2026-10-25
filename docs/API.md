# REST API 速查表

端点与 `services/tb-core/src/routes/`、`services/tb-auth/src/`、`services/tb-edqs/src/api.rs`、
`services/tb-transport-http/src/lib.rs` 当前代码一致。

权限缩写：**SA** = SYS_ADMIN，**TA** = TENANT_ADMIN，**CU** = CUSTOMER_USER。
除注明「公开」外，均需 `Authorization: Bearer <JWT>`。分页参数统一为
`page/pageSize/textSearch/sortProperty/sortOrder`。

## 认证（tb-auth :8081，tb-core 同路径亦可用）

| 方法 | 路径 | 权限 | 说明 |
|---|---|---|---|
| POST | /api/auth/login | 公开 | 用户名密码登录，返回 token/refreshToken |
| POST | /api/auth/token/refresh | 公开 | 刷新 access token |
| POST | /api/auth/changePassword | 已登录 | 修改当前用户密码 |
| GET | /api/auth/user | 已登录 | 当前用户信息 |
| GET | /api/oauth2/clients | 公开 | OAuth2 客户端列表（登录页用） |
| POST | /api/oauth2/login | 公开 | OAuth2 授权码换 JWT |

## 租户 / 客户 / 用户

| 方法 | 路径 | 权限 | 说明 |
|---|---|---|---|
| POST | /api/tenant | SA | 创建/更新租户 |
| GET/DELETE | /api/tenant/{id} | SA | 查/删租户 |
| GET | /api/tenants | SA | 租户分页 |
| POST | /api/customer | TA | 创建/更新客户 |
| GET/DELETE | /api/customer/{id} | TA | 查/删客户 |
| GET | /api/customers | TA | 客户分页 |
| POST | /api/user | SA/TA | 创建/更新用户（带 password 直接激活） |
| GET/DELETE | /api/user/{id} | SA/TA（CU 可查自己） | 查/删用户 |
| GET | /api/users | TA | 租户用户分页 |
| GET | /api/customer/{customerId}/users | TA | 客户用户分页 |

## 设备 / 资产 / 配置

| 方法 | 路径 | 权限 | 说明 |
|---|---|---|---|
| POST | /api/device | TA | 创建/更新设备（可带 accessToken 建凭证） |
| GET/DELETE | /api/device/{id} | TA | 查/删设备 |
| GET | /api/devices | TA | 设备分页 |
| GET | /api/device/types | TA | 设备类型列表 |
| GET | /api/device/{id}/credentials | TA | 查设备凭证 |
| POST | /api/device/credentials | TA | 更新设备凭证 |
| GET | /api/customer/{customerId}/devices | TA/CU | 客户设备分页 |
| POST/DELETE | /api/device/{deviceId}/assign/{customerId} | TA | 分配/回收设备 |
| POST | /api/customer/device/{deviceName}/claim | CU | 客户使用 secretKey 认领设备 |
| POST | /api/asset | TA | 创建/更新资产 |
| GET/DELETE | /api/asset/{id} | TA | 查/删资产 |
| GET | /api/assets | TA | 资产分页 |
| GET | /api/customer/{customerId}/assets | TA/CU | 客户资产分页 |
| POST/DELETE | /api/asset/{assetId}/assign/{customerId} | TA | 分配/回收资产 |
| POST | /api/deviceProfile / /api/assetProfile | TA | 创建/更新配置 |
| GET | /api/deviceProfile/default / /api/assetProfile/default | TA | 默认配置 |
| GET/DELETE | /api/deviceProfile/{id} / /api/assetProfile/{id} | TA | 查/删配置 |
| GET | /api/deviceProfiles / /api/assetProfiles | TA | 配置分页 |

## 遥测与属性

`{entityType}` 为 DEVICE/ASSET/...；写遥测体支持 `{k:v}` 映射或 `[{ts, values}]` 数组。

| 方法 | 路径 | 权限 | 说明 |
|---|---|---|---|
| POST/DELETE | /api/telemetry/{entityType}/{entityId}/timeseries | TA/CU | 写/删遥测 |
| GET | /api/telemetry/{entityType}/{entityId}/values/timeseries | TA/CU | 无 startTs=latest；带 startTs/endTs/keys/interval/agg/orderBy=历史与聚合 |
| GET | /api/telemetry/{entityType}/{entityId}/keys/timeseries | TA/CU | 遥测 key 列表 |
| POST/DELETE | /api/telemetry/{entityType}/{entityId}/attributes/{scope} | TA/CU | 写/删属性（SERVER/SHARED/CLIENT） |
| GET | /api/telemetry/{entityType}/{entityId}/values/attributes/{scope} | TA/CU | 按 scope 读属性（?keys=） |
| GET | /api/telemetry/{entityType}/{entityId}/values/attributes | TA/CU | 全 scope 属性 |

## 告警 / RPC

| 方法 | 路径 | 权限 | 说明 |
|---|---|---|---|
| GET | /api/alarm/{alarmId} | TA/CU（归属校验） | 告警详情 |
| POST | /api/alarm/{alarmId}/ack / clear | TA/CU | 确认/清除 |
| GET | /api/alarms/{entityType}/{entityId} | TA/CU | 实体告警分页（?status=&startTs=&endTs=） |
| GET | /api/alarms | TA | 租户告警分页 |
| POST | /api/rpc/twoway/{deviceId} | TA/CU | 双向 RPC（等待设备响应，timeout ms） |
| POST | /api/rpc/oneway/{deviceId} | TA/CU | 单向 RPC |

## 规则链 / 计算字段

| 方法 | 路径 | 权限 | 说明 |
|---|---|---|---|
| POST | /api/ruleChain | TA | 创建/更新规则链 |
| GET/DELETE | /api/ruleChain/{id} | TA | 查/删规则链 |
| GET/PUT | /api/ruleChain/{id}/nodes | TA | 查/整链保存节点 |
| GET | /api/ruleChains | TA | 规则链分页 |
| GET | /api/ruleNode/{ruleNodeId}/events | TA | 节点调试事件分页 |
| POST | /api/calculatedField | TA | 创建/更新计算字段 |
| GET/DELETE | /api/calculatedField/{id} | TA | 查/删 |
| GET | /api/calculatedFields | TA | 分页 |
| GET | /api/entity/{entityType}/{entityId}/calculatedFields | TA | 按实体查 |

## OTA / OAuth2 管理 / 审计 / 用量 / EDQS

| 方法 | 路径 | 权限 | 说明 |
|---|---|---|---|
| POST | /api/otaPackage | TA | 创建 OTA 包（multipart 上传） |
| GET/DELETE | /api/otaPackage/{id} | TA | 查/删（仍被设备或档案绑定时删除返回 409） |
| GET | /api/otaPackage/{id}/download | TA | 下载固件 |
| GET | /api/otaPackages | TA | 分页（?type=FIRMWARE\|SOFTWARE） |
| POST | /api/device/{deviceId}/firmware/{otaId} / software/{otaId} | TA | 给设备指派固件/软件（设备级优先于档案级） |
| DELETE | /api/device/{deviceId}/firmware / software | TA | 解除设备级绑定，回落到档案绑定 |
| POST | /api/deviceProfile/{profileId}/firmware/{otaId} / software/{otaId} | TA | 给配置指派固件/软件 |
| DELETE | /api/deviceProfile/{profileId}/firmware / software | TA | 解除档案级绑定 |
| GET | /api/oauth2/clients | SA/TA | 客户端分页 |
| POST | /api/oauth2/client | SA | 创建/更新客户端 |
| GET | /api/oauth2/client/{id} | SA/TA | 查客户端（TA 限本租户） |
| DELETE | /api/oauth2/client/{id} | SA | 删客户端 |
| GET | /api/audit/logs | SA/TA | 审计日志分页（?entityType=&entityId=） |
| GET | /api/usage/{tenantId} | SA/TA（TA 限本租户） | 用量统计 |
| POST/GET | /api/edqs/query/entityData | SA/TA/CU | 实体数据查询（代理到 tb-edqs；TA/CU 强制本租户） |

## tb-edqs 直连（:8090，内部服务）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST/GET | /api/query/entityData | 实体数据查询（body/query 带 tenantId） |
| GET | /health | 健康检查 |

## 设备侧 HTTP（tb-transport-http :8088，路径与 Java 版兼容）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | /api/v1/health | 健康检查 |
| POST | /api/v1/{token}/telemetry | 发布遥测 |
| POST/GET | /api/v1/{token}/attributes | 发布/请求属性 |
| POST | /api/v1/{token}/claim | 设备认领 |
| POST/GET | /api/v1/{token}/rpc | 上行 RPC / 长轮询下行 RPC |
| POST | /api/v1/{token}/rpc/{requestId} | RPC 响应 |
| GET | /api/v1/{token}/firmware、/software | OTA 分块下载（?title=&version=&chunk=&size=；title/version 为 fw_*/sw_* 属性给出的目标包，未绑定 404，不一致 400） |
| POST | /api/v1/provision | 设备自注册 |

设备侧 MQTT（:1883）topic：`v1/devices/me/telemetry`、`v1/devices/me/attributes`、
`v1/devices/me/rpc/request/+` / `response/+`、`v1/devices/me/attributes/response/+`、
`v1/gateway/*`、`v2/{fw,sw}/response/+`（订阅时推目标包元信息）、
`v2/{fw,sw}/request/{requestId}/chunk/{n}`（回 `v2/{fw,sw}/response/{requestId}/chunk/{n}`，
未绑定回 `v2/{fw,sw}/error`）、`/provision`，用户名 = accessToken。
设备侧 CoAP（:5683/udp）语义同 HTTP（OTA 下载除外）。

OTA 状态回写：生效包（设备级绑定优先，其次档案级，无绑定即无包）变化时，core 写入遥测
`target_{fw,sw}_title/version/tag/ts` 与 `{fw,sw}_state`（QUEUED → INITIATED），并保存、推送
shared 属性 `{fw,sw}_title/version/tag/size/checksum_algorithm/checksum`；解绑时删除这些属性。
设备随后自行上报 `current_{fw,sw}_*`、`{fw,sw}_state`、`{fw,sw}_error` 遥测。

## MQTTS X509 证书链 provisioning

设备档案使用 `provisionType=X509_CERTIFICATE_CHAIN`，并在
`profileData.provisionConfiguration` 配置 `certificate`（PEM/Base64 CA 证书）、
`certificateRegExPattern`（必须含 group 1，用于从 leaf CN 提取设备名）及
`allowCreateNewDevicesByX509Certificate`（默认 `false`）。保存时服务端根据配置证书的
SHA3-256 自动生成 `provisionDeviceKey`，调用方提供的同名字段不会被信任。

MQTTS 客户端须发送 leaf 在前、CA 在尾的完整 DER 链（含中间 CA），最多 8 张、合计
64 KiB。已有 pinned leaf 凭证优先；否则仅在链尾唯一匹配 chain profile、完整
`client_auth` 路径验证及 CN 正则均通过时认证。自动创建开启时才允许新设备；同 profile
可轮换 X509 leaf，跨 profile 或设备已有其他类型凭证会被拒绝。
