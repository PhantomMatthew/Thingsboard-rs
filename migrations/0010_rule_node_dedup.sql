CREATE TABLE rule_node_dedup_window (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    rule_chain_id   UUID NOT NULL,
    rule_node_id    UUID NOT NULL,
    originator_id   UUID NOT NULL,
    strategy        VARCHAR NOT NULL,
    out_msg_type    VARCHAR NOT NULL,
    queue_name      VARCHAR NOT NULL,
    max_retries     INTEGER NOT NULL,
    pending_count   BIGINT NOT NULL DEFAULT 0,
    state           VARCHAR NOT NULL,
    output_msg      JSONB,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    next_attempt_at BIGINT,
    lease_owner     VARCHAR,
    lease_until     BIGINT,
    expires_at      BIGINT NOT NULL,
    updated_at      BIGINT NOT NULL
);

CREATE UNIQUE INDEX idx_rule_node_dedup_window_open
    ON rule_node_dedup_window(tenant_id, rule_chain_id, rule_node_id, originator_id)
    WHERE state = 'OPEN';
CREATE INDEX idx_rule_node_dedup_window_due
    ON rule_node_dedup_window(state, expires_at, next_attempt_at, lease_until);

CREATE TABLE rule_node_dedup_msg (
    id              UUID PRIMARY KEY,
    window_id       UUID NOT NULL REFERENCES rule_node_dedup_window(id) ON DELETE CASCADE,
    rule_node_id    UUID NOT NULL,
    msg_id          UUID NOT NULL,
    seq             BIGSERIAL NOT NULL,
    msg             JSONB NOT NULL,
    UNIQUE(rule_node_id, msg_id)
);

CREATE INDEX idx_rule_node_dedup_msg_window_seq
    ON rule_node_dedup_msg(window_id, seq);
