-- Phase 7：规则节点 debug 事件（rule node debug_mode=true 时的 IN/OUT 记录）。

CREATE TABLE rule_node_debug_event (
    id            UUID PRIMARY KEY,
    ts            BIGINT NOT NULL,
    tenant_id     UUID NOT NULL,
    rule_chain_id UUID NOT NULL,
    rule_node_id  UUID NOT NULL,
    msg_id        UUID NOT NULL,
    event_type    VARCHAR NOT NULL,  -- IN / OUT
    data          JSONB,
    metadata      JSONB,
    error         VARCHAR
);
CREATE INDEX idx_rule_node_debug_event_chain_ts
    ON rule_node_debug_event(rule_chain_id, ts DESC);
CREATE INDEX idx_rule_node_debug_event_node_ts
    ON rule_node_debug_event(rule_node_id, ts DESC);
