-- memory_history.sql
-- Per-minute memory usage history from asynchronous_metric_log.
-- Pivots key memory metrics into columns and computes OS-level used % and
-- ClickHouse process % for easy charting.
SELECT
    toStartOfMinute(event_time)                                                       AS minute,
    hostname,
    round(maxIf(value, metric = 'OSMemoryTotal')        / 1073741824.0, 2)            AS total_gb,
    round(avgIf(value, metric = 'MemoryResident')       / 1073741824.0, 2)            AS resident_gb,
    round(avgIf(value, metric = 'OSMemoryAvailable')    / 1073741824.0, 2)            AS available_gb,
    round(avgIf(value, metric = 'TrackedMemory')        / 1073741824.0, 2)            AS tracked_gb,
    round(avgIf(value, metric = 'QueriesPeakMemoryUsage') / 1073741824.0, 3)          AS queries_peak_gb,
    round(
        (1 - avgIf(value, metric = 'OSMemoryAvailable')
               / nullIf(maxIf(value, metric = 'OSMemoryTotal'), 0)
        ) * 100, 1
    )                                                                                  AS os_used_pct,
    round(
        avgIf(value, metric = 'MemoryResident')
        / nullIf(maxIf(value, metric = 'OSMemoryTotal'), 0) * 100, 1
    )                                                                                  AS ch_pct
    -- os_used_pct: fraction of total RAM consumed (OS-wide view — catches all processes)
    -- ch_pct:      fraction of total RAM used by the ClickHouse process RSS
    -- When os_used_pct ≈ 100 and ch_pct is high → ClickHouse is the cause
    -- When os_used_pct ≈ 100 but ch_pct is low  → another process is consuming RAM
FROM clusterAllReplicas({cluster:String}, system.asynchronous_metric_log)
WHERE event_time >= now() - toIntervalHour({lookback_hours:UInt32})
  AND metric IN (
      'MemoryResident',
      'OSMemoryTotal',
      'OSMemoryAvailable',
      'TrackedMemory',
      'QueriesPeakMemoryUsage'
  )
GROUP BY minute, hostname
ORDER BY hostname, minute;
