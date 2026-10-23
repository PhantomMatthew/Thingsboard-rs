-- Phase 9b：OAuth2 客户端注册与计算字段。

CREATE TABLE oauth2_client (
    id                    UUID PRIMARY KEY,
    created_time          BIGINT NOT NULL,
    tenant_id             UUID,               -- 空 = 平台级（仅 SYS_ADMIN 管理）
    title                 VARCHAR NOT NULL,
    client_id             VARCHAR NOT NULL,
    client_secret         VARCHAR,
    authorization_uri     VARCHAR NOT NULL,
    token_uri             VARCHAR NOT NULL,
    user_info_uri         VARCHAR NOT NULL,
    redirect_uri_template VARCHAR,
    scope                 VARCHAR,
    enabled               BOOLEAN NOT NULL DEFAULT true,
    UNIQUE(client_id)
);
CREATE INDEX idx_oauth2_client_tenant ON oauth2_client(tenant_id);

CREATE TABLE calculated_field (
    id            UUID PRIMARY KEY,
    created_time  BIGINT NOT NULL,
    tenant_id     UUID NOT NULL,
    entity_id     UUID NOT NULL,
    name          VARCHAR NOT NULL,
    configuration JSONB NOT NULL,
    UNIQUE(tenant_id, entity_id, name)
);
CREATE INDEX idx_calculated_field_entity ON calculated_field(entity_id);
