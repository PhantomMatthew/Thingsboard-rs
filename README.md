# Thingsboard-rs

ThingsBoard IoT 平台的 Rust 重写版（微服务架构，非兼容重设计）。

## 架构

```
crates/
├── tb-common    # 共享基础类型：EntityId/EntityType、KvValue、TbMsg、错误
├── tb-proto     # 服务间 protobuf 契约（proto/ 目录，prost 生成）
├── tb-queue     # Kafka 抽象：producer/consumer/admin、按实体 ID murmur3 分区
├── tb-dao       # PostgreSQL 数据访问层（sqlx）
└── tb-script    # Rhai 脚本沙箱（规则引擎用）
services/
├── tb-auth      # 用户认证：JWT 签发/刷新/校验（库 + 可执行）
└── tb-core      # 核心服务：REST API、实体管理、会话路由、RPC
proto/           # 服务间消息契约定义
migrations/      # SQL 迁移（sqlx-cli）
tests/           # 黑盒集成测试
```

## 快速开始

```bash
just up        # 启动 postgres(:5433) / kafka(:29092) / redis(:16379)
just migrate   # 应用数据库迁移
just build     # 构建 workspace
just test      # 运行测试
just verify    # up + migrate + test
```

> 端口有意避开本机 5432/9092/6379 上可能已运行的 Java 版 ThingsBoard 栈。

## 路线图

见项目规划：Phase 0 脚手架 → Phase 1 实体/认证 → Phase 2 遥测属性 →
Phase 3 设备接入（MQTT/HTTP/CoAP）→ Phase 4 规则引擎 → …→ Phase 10 加固发布。
