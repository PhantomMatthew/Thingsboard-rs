-- Phase 2：遥测（timeseries）与属性（attributes）存储。
-- 值列五选一：bool_v/str_v/long_v/dbl_v/json_v，恰好一个非空。
-- 普通 PG 表；分区/TTL 留给 housekeeper 阶段。

CREATE TABLE ts_kv (
    entity_id UUID NOT NULL,
    key       INT NOT NULL REFERENCES key_dictionary(key_id),
    ts        BIGINT NOT NULL,
    bool_v    BOOLEAN,
    str_v     VARCHAR,
    long_v    BIGINT,
    dbl_v     DOUBLE PRECISION,
    json_v    JSONB,
    PRIMARY KEY (entity_id, key, ts)
);
CREATE INDEX idx_ts_kv_key ON ts_kv(key);
CREATE INDEX idx_ts_kv_entity_ts ON ts_kv(entity_id, ts);

CREATE TABLE ts_kv_latest (
    entity_id UUID NOT NULL,
    key       INT NOT NULL REFERENCES key_dictionary(key_id),
    ts        BIGINT NOT NULL,
    bool_v    BOOLEAN,
    str_v     VARCHAR,
    long_v    BIGINT,
    dbl_v     DOUBLE PRECISION,
    json_v    JSONB,
    version   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (entity_id, key)
);

CREATE TABLE attribute_kv (
    entity_id      UUID NOT NULL,
    attribute_type SMALLINT NOT NULL,  -- 0=CLIENT_SCOPE 1=SERVER_SCOPE 2=SHARED_SCOPE
    attribute_key  INT NOT NULL REFERENCES key_dictionary(key_id),
    bool_v         BOOLEAN,
    str_v          VARCHAR,
    long_v         BIGINT,
    dbl_v          DOUBLE PRECISION,
    json_v         JSONB,
    version        BIGINT NOT NULL DEFAULT 0,
    last_update_ts BIGINT NOT NULL,
    PRIMARY KEY (entity_id, attribute_type, attribute_key)
);
