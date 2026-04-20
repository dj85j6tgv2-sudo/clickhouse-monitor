-- async_inserts.sql
-- Pending async insert queue across the cluster.
-- Large queue or old entries may indicate async insert backpressure.
-- Column name in 24.8: total_bytes (not bytes), first_update is DateTime64(6).
SELECT
    hostName()                                                              AS hostname,
    database,
    table,
    count()                                                                 AS pending_entries,
    formatReadableSize(sum(total_bytes))                                    AS total_bytes_queued,
    max(toUnixTimestamp(toDateTime(first_update)) - toUnixTimestamp(now())) AS oldest_entry_age_seconds,
    min(first_update)                                                       AS oldest_entry_time
FROM clusterAllReplicas({cluster:String}, system.asynchronous_inserts)
GROUP BY hostname, database, table
ORDER BY pending_entries DESC;
