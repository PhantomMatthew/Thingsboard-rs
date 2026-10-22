-- Thingsboard-rs 初始 schema（重新设计，简化自 ThingsBoard Java 版）
-- 约定：所有 id 为 UUIDv7；时间戳为 epoch millis (bigint)。

CREATE TABLE tenant (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    title           VARCHAR NOT NULL,
    region          VARCHAR,
    country         VARCHAR,
    state           VARCHAR,
    city            VARCHAR,
    address         VARCHAR,
    address2        VARCHAR,
    zip             VARCHAR,
    phone           VARCHAR,
    email           VARCHAR,
    additional_info JSONB,
    tenant_profile_id UUID
);

CREATE TABLE tenant_profile (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    name            VARCHAR NOT NULL,
    description     VARCHAR,
    is_default      BOOLEAN NOT NULL DEFAULT FALSE,
    profile_data    JSONB NOT NULL DEFAULT '{}'
);
ALTER TABLE tenant ADD CONSTRAINT fk_tenant_profile
    FOREIGN KEY (tenant_profile_id) REFERENCES tenant_profile(id);

CREATE TABLE customer (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    title           VARCHAR NOT NULL,
    country         VARCHAR,
    state           VARCHAR,
    city            VARCHAR,
    address         VARCHAR,
    address2        VARCHAR,
    zip             VARCHAR,
    phone           VARCHAR,
    email           VARCHAR,
    additional_info JSONB
);
CREATE INDEX idx_customer_tenant ON customer(tenant_id);

CREATE TABLE tb_user (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    customer_id     UUID REFERENCES customer(id) ON DELETE CASCADE,
    authority       VARCHAR NOT NULL,  -- SYS_ADMIN / TENANT_ADMIN / CUSTOMER_USER
    email           VARCHAR NOT NULL UNIQUE,
    first_name      VARCHAR,
    last_name       VARCHAR,
    phone           VARCHAR,
    additional_info JSONB
);
CREATE INDEX idx_user_tenant ON tb_user(tenant_id);

CREATE TABLE user_credentials (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    user_id         UUID NOT NULL UNIQUE REFERENCES tb_user(id) ON DELETE CASCADE,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    password        VARCHAR,           -- bcrypt hash
    activate_token  VARCHAR UNIQUE,
    reset_token     VARCHAR UNIQUE,
    failed_login_attempts INT NOT NULL DEFAULT 0,
    last_login_ts   BIGINT
);

CREATE TABLE device_profile (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    name            VARCHAR NOT NULL,
    description     VARCHAR,
    is_default      BOOLEAN NOT NULL DEFAULT FALSE,
    transport_type  VARCHAR NOT NULL DEFAULT 'DEFAULT', -- DEFAULT/MQTT/HTTP/COAP/LWM2M/SNMP
    provision_type  VARCHAR NOT NULL DEFAULT 'DISABLED',
    profile_data    JSONB NOT NULL DEFAULT '{}',        -- 告警规则、传输配置、provision 配置
    firmware_id     UUID,
    software_id     UUID,
    UNIQUE (tenant_id, name)
);

CREATE TABLE asset_profile (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    name            VARCHAR NOT NULL,
    description     VARCHAR,
    is_default      BOOLEAN NOT NULL DEFAULT FALSE,
    profile_data    JSONB NOT NULL DEFAULT '{}',
    UNIQUE (tenant_id, name)
);

CREATE TABLE device (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    customer_id     UUID REFERENCES customer(id) ON DELETE SET NULL,
    device_profile_id UUID NOT NULL REFERENCES device_profile(id),
    name            VARCHAR NOT NULL,
    type            VARCHAR,
    label           VARCHAR,
    external_id     UUID,
    firmware_id     UUID,
    software_id     UUID,
    additional_info JSONB,
    device_data     JSONB NOT NULL DEFAULT '{}',  -- 传输配置覆盖
    UNIQUE (tenant_id, name)
);
CREATE INDEX idx_device_tenant ON device(tenant_id);
CREATE INDEX idx_device_customer ON device(customer_id);
CREATE INDEX idx_device_profile ON device(device_profile_id);

CREATE TABLE device_credentials (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    device_id       UUID NOT NULL UNIQUE REFERENCES device(id) ON DELETE CASCADE,
    credentials_type   VARCHAR NOT NULL, -- ACCESS_TOKEN / X509_CERTIFICATE / MQTT_BASIC
    credentials_id     VARCHAR UNIQUE,   -- 查找键：token / cert hash / mqtt clientId|userName
    credentials_value  VARCHAR           -- 凭证主体（X509 PEM / MQTT_BASIC JSON）
);

CREATE TABLE asset (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    customer_id     UUID REFERENCES customer(id) ON DELETE SET NULL,
    asset_profile_id UUID NOT NULL REFERENCES asset_profile(id),
    name            VARCHAR NOT NULL,
    label           VARCHAR,
    external_id     UUID,
    additional_info JSONB,
    UNIQUE (tenant_id, name)
);
CREATE INDEX idx_asset_tenant ON asset(tenant_id);

CREATE TABLE dashboard (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    title           VARCHAR NOT NULL,
    configuration   JSONB NOT NULL DEFAULT '{}',
    assigned_customers JSONB,
    mobile_hide     BOOLEAN NOT NULL DEFAULT FALSE,
    mobile_order    INT,
    image           VARCHAR,
    additional_info JSONB
);

CREATE TABLE rule_chain (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    name            VARCHAR NOT NULL,
    first_rule_node_id UUID,
    root            BOOLEAN NOT NULL DEFAULT FALSE,
    debug_mode      BOOLEAN NOT NULL DEFAULT FALSE,
    configuration   JSONB,
    additional_info JSONB,
    UNIQUE (tenant_id, name)
);

CREATE TABLE rule_node (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    rule_chain_id   UUID NOT NULL REFERENCES rule_chain(id) ON DELETE CASCADE,
    type            VARCHAR NOT NULL,   -- 节点类型标识（类名风格字符串）
    name            VARCHAR NOT NULL,
    debug_mode      BOOLEAN NOT NULL DEFAULT FALSE,
    singleton_mode  BOOLEAN NOT NULL DEFAULT FALSE,
    queue_name      VARCHAR,
    configuration   JSONB,
    additional_info JSONB
);
CREATE INDEX idx_rule_node_chain ON rule_node(rule_chain_id);

-- 通用实体关系表（替代外键表达图关系）
CREATE TABLE relation (
    from_id         UUID NOT NULL,
    from_type       VARCHAR NOT NULL,
    to_id           UUID NOT NULL,
    to_type         VARCHAR NOT NULL,
    relation_type_group VARCHAR NOT NULL DEFAULT 'COMMON',
    relation_type   VARCHAR NOT NULL,
    additional_info JSONB,
    PRIMARY KEY (from_id, from_type, relation_type_group, relation_type, to_id, to_type)
);
CREATE INDEX idx_relation_to ON relation(to_id, to_type, relation_type_group, relation_type);

CREATE TABLE ota_package (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL REFERENCES tenant(id) ON DELETE CASCADE,
    device_profile_id UUID NOT NULL REFERENCES device_profile(id),
    type            VARCHAR NOT NULL,  -- FIRMWARE / SOFTWARE
    title           VARCHAR NOT NULL,
    version         VARCHAR NOT NULL,
    tag             VARCHAR,
    file_name       VARCHAR,
    content_type    VARCHAR,
    checksum_algorithm VARCHAR,
    checksum        VARCHAR,
    size            BIGINT,
    data            BYTEA,
    additional_info JSONB,
    UNIQUE (tenant_id, device_profile_id, type, title, version)
);

CREATE TABLE alarm (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL,
    originator_id   UUID NOT NULL,
    originator_type VARCHAR NOT NULL,
    type            VARCHAR NOT NULL,
    severity        VARCHAR NOT NULL,  -- CRITICAL/MAJOR/MINOR/WARNING/INDETERMINATE
    status          VARCHAR NOT NULL,  -- ACTIVE_UNACK/ACTIVE_ACK/CLEARED_UNACK/CLEARED_ACK
    start_ts        BIGINT NOT NULL,
    end_ts          BIGINT NOT NULL,
    ack_ts          BIGINT,
    clear_ts        BIGINT,
    details         JSONB,
    propagate       BOOLEAN NOT NULL DEFAULT FALSE,
    propagate_to_owner BOOLEAN NOT NULL DEFAULT FALSE,
    propagate_to_tenant BOOLEAN NOT NULL DEFAULT FALSE,
    propagate_relation_types JSONB,
    customer_id     UUID,
    assignee_id     UUID,
    assign_ts       BIGINT
);
CREATE INDEX idx_alarm_originator ON alarm(originator_id, created_time DESC);
CREATE INDEX idx_alarm_tenant ON alarm(tenant_id, created_time DESC);

CREATE TABLE api_usage_state (
    id              UUID PRIMARY KEY,
    created_time    BIGINT NOT NULL,
    tenant_id       UUID NOT NULL UNIQUE REFERENCES tenant(id) ON DELETE CASCADE,
    transport_msgs  BIGINT NOT NULL DEFAULT 0,
    re_msgs         BIGINT NOT NULL DEFAULT 0,
    db_storage      BIGINT NOT NULL DEFAULT 0,
    js_exec         BIGINT NOT NULL DEFAULT 0,
    updated_at      BIGINT NOT NULL DEFAULT 0
);

-- 遥测 key 字典（字符串 → int interning）
CREATE TABLE key_dictionary (
    key_id  SERIAL PRIMARY KEY,
    key     VARCHAR NOT NULL UNIQUE
);

-- Phase 2 建表：ts_kv / ts_kv_latest / attribute_kv（此处预留）
