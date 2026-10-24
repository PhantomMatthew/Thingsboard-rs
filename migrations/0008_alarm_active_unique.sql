-- 每个 originator + type 最多保留一条 ACTIVE 告警；历史重复保留最新一条，其余清理。
WITH ranked_active AS (
    SELECT id,
           ROW_NUMBER() OVER (
               PARTITION BY originator_id, type
               ORDER BY created_time DESC, start_ts DESC, id DESC
           ) AS position
    FROM alarm
    WHERE status IN ('ACTIVE_UNACK', 'ACTIVE_ACK')
)
UPDATE alarm AS a
SET status = 'CLEARED_UNACK',
    clear_ts = COALESCE(
        a.clear_ts,
        CAST(EXTRACT(EPOCH FROM clock_timestamp()) * 1000 AS BIGINT)
    )
FROM ranked_active AS r
WHERE a.id = r.id
  AND r.position > 1;

CREATE UNIQUE INDEX alarm_active_unique
    ON alarm(originator_id, type)
    WHERE status IN ('ACTIVE_UNACK', 'ACTIVE_ACK');
