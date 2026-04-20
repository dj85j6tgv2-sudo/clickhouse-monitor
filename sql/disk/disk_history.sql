-- disk_history.sql
-- Per-minute disk usage history from asynchronous_metric_log.
-- Tracks the main filesystem path where ClickHouse data is stored.
SELECT
    toStartOfMinute(event_time)                                                        AS minute,
    hostname,
    round(maxIf(value, metric = 'FilesystemMainPathTotalBytes')     / 1073741824.0, 2) AS total_gb,
    round(avgIf(value, metric = 'FilesystemMainPathAvailableBytes') / 1073741824.0, 2) AS available_gb,
    round(
        (1 - avgIf(value, metric = 'FilesystemMainPathAvailableBytes')
               / nullIf(maxIf(value, metric = 'FilesystemMainPathTotalBytes'), 0)
        ) * 100, 1
    )                                                                                   AS used_pct
    -- used_pct: fraction of main filesystem consumed (OS-wide — includes all processes, not just ClickHouse)
    -- When used_pct approaches 100%, ClickHouse writes will start failing
    -- ACTION: identify largest tables with: SELECT database, table, formatReadableSize(sum(bytes_on_disk))
    --         FROM system.parts GROUP BY database, table ORDER BY sum(bytes_on_disk) DESC LIMIT 20
    -- ACTION: check TTL policies — ALTER TABLE <t> MODIFY TTL <col> + INTERVAL 30 DAY
FROM clusterAllReplicas({cluster:String}, system.asynchronous_metric_log)
WHERE event_time >= now() - toIntervalHour({lookback_hours:UInt32})
  AND metric IN ('FilesystemMainPathTotalBytes', 'FilesystemMainPathAvailableBytes')
GROUP BY minute, hostname
ORDER BY hostname, minute;
