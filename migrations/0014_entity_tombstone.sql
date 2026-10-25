-- 已删除实体墓碑：删除事务内写入，housekeeper 在保留期内反复清理删除后迟到的写入
-- （传输凭证缓存、队列积压中的消息），过期后删除墓碑。

CREATE TABLE entity_tombstone (
    entity_id  UUID PRIMARY KEY,
    tenant_id  UUID NOT NULL,
    deleted_at BIGINT NOT NULL
);
CREATE INDEX idx_entity_tombstone_deleted_at ON entity_tombstone(deleted_at);
