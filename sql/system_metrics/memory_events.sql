-- memory_events.sql
-- Queries that exceeded memory limits or used unusually large amounts of memory.
-- exception_code 241 = MEMORY_LIMIT_EXCEEDED (OOM kill).
-- Also captures queries that consumed > 1 GiB even if they finished normally.
SELECT
    event_time,
    hostname,
    user,
    query_duration_ms,
    formatReadableSize(memory_usage)        AS memory_used,
    toString(type)                          AS finish_type,
    CASE
        WHEN exception_code = 241           THEN 'CRITICAL - Query killed: memory limit exceeded'
        WHEN memory_usage > 10737418240     THEN 'CRITICAL - Query used >10 GiB memory'
        WHEN memory_usage > 5368709120      THEN 'WARNING  - Query used >5 GiB memory'
        WHEN memory_usage > 1073741824      THEN 'CAUTION  - Query used >1 GiB memory'
        ELSE                                     'OK       - Normal memory usage'
    END                                     AS memory_status,
    left(exception, 300)                    AS exception,
    left(query, 200)                        AS query_preview
    -- ALERT: exception_code = 241 → query was killed; increase max_memory_usage for the user
    --        or optimise the query (add WHERE on primary key columns, reduce GROUP BY cardinality)
    -- ACTION: per-query limit:  SET max_memory_usage = 20000000000  (20 GiB)
    -- ACTION: per-user limit:   ALTER USER <user> SETTINGS max_memory_usage = 20000000000
    -- ACTION: server-wide:      max_server_memory_usage_to_ram_ratio in config.xml (default 0.9)
FROM clusterAllReplicas({cluster:String}, system.query_log)
WHERE event_time >= now() - toIntervalHour({lookback_hours:UInt32})
  AND (
      exception_code = 241
      OR memory_usage > 1073741824
  )
  AND toString(type) IN ('QueryFinish', 'ExceptionWhileProcessing', 'ExceptionBeforeStart')
ORDER BY event_time DESC
LIMIT 100;
