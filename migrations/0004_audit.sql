-- Phase 8a：审计日志。

CREATE TABLE audit_log (
    id            UUID PRIMARY KEY,
    tenant_id     UUID NOT NULL,
    entity_id     UUID,
    entity_type   VARCHAR,
    entity_name   VARCHAR,
    user_id       UUID,
    user_email    VARCHAR,
    action_type   VARCHAR NOT NULL,
    action_data   JSONB,
    action_status VARCHAR NOT NULL DEFAULT 'SUCCESS',
    created_time  BIGINT NOT NULL
);
CREATE INDEX idx_audit_log_tenant_ts ON audit_log(tenant_id, created_time DESC);
CREATE INDEX idx_audit_log_entity ON audit_log(entity_id);
CREATE INDEX idx_audit_log_user ON audit_log(user_id);
