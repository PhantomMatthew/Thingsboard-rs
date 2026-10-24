CREATE TABLE rule_node_delay (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL,
    rule_chain_id   UUID NOT NULL,
    rule_node_id    UUID NOT NULL,
    next_node_id    UUID NOT NULL,
    msg_id          UUID NOT NULL,
    hop_count       INTEGER NOT NULL,
    msg             JSONB NOT NULL,
    execute_at      BIGINT NOT NULL,
    state           VARCHAR NOT NULL CHECK (state IN ('PENDING', 'EMITTING', 'DONE', 'DEAD')),
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    next_attempt_at BIGINT,
    lease_owner     VARCHAR,
    lease_until     BIGINT,
    last_error      VARCHAR,
    updated_at      BIGINT NOT NULL,
    UNIQUE(rule_node_id, next_node_id, msg_id, hop_count)
);

CREATE INDEX idx_rule_node_delay_due
    ON rule_node_delay(state, execute_at, next_attempt_at, lease_until);
CREATE INDEX idx_rule_node_delay_terminal
    ON rule_node_delay(state, updated_at)
    WHERE state IN ('DONE', 'DEAD');
