CREATE INDEX IF NOT EXISTS idx_ts_kv_ts ON ts_kv(ts);
CREATE INDEX IF NOT EXISTS idx_debug_event_ts ON rule_node_debug_event(ts);
