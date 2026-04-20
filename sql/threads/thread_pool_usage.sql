-- thread_pool_usage.sql
-- Background thread pool utilization: active tasks vs configured pool size.
-- High saturation (>80%) means new background work will be queued and delayed.
-- Note: queries system.metrics directly on the connected node (CTEs cannot be passed to clusterAllReplicas).
SELECT
    hostName()                                   AS hostname,
    m.metric                                     AS pool_metric,
    m.value                                      AS active_tasks,
    toUInt64OrZero(s.value)                      AS pool_max,
    round(m.value * 100.0 / greatest(toUInt64OrZero(s.value), 1), 1) AS utilization_pct,
    CASE
        WHEN m.value * 100.0 / greatest(toUInt64OrZero(s.value), 1) >= 95 THEN 'CRITICAL - Pool exhausted (>=95%)'
        WHEN m.value * 100.0 / greatest(toUInt64OrZero(s.value), 1) >= 80 THEN 'WARNING  - Pool near capacity (>=80%)'
        WHEN m.value * 100.0 / greatest(toUInt64OrZero(s.value), 1) >= 60 THEN 'CAUTION  - Pool moderately loaded (>=60%)'
        ELSE                                                                    'OK       - Pool has headroom'
    END AS pool_status
    -- ALERT: BackgroundMergesAndMutationsPoolTask >= 95% → merges are queuing
    -- ACTION: Increase background_pool_size in config.xml (default 16)
    -- ALERT: BackgroundFetchesPoolTask >= 95% → replica catch-up is blocked
    -- ACTION: Increase background_fetches_pool_size (default 8)
FROM system.metrics m
LEFT JOIN system.server_settings s
    ON s.name = CASE m.metric
        WHEN 'BackgroundMergesAndMutationsPoolTask' THEN 'background_pool_size'
        WHEN 'BackgroundFetchesPoolTask'            THEN 'background_fetches_pool_size'
        WHEN 'BackgroundCommonPoolTask'             THEN 'background_common_pool_size'
        WHEN 'BackgroundMovePoolTask'               THEN 'background_move_pool_size'
        WHEN 'BackgroundSchedulePoolTask'           THEN 'background_schedule_pool_size'
        ELSE ''
    END
WHERE m.metric IN (
    'BackgroundMergesAndMutationsPoolTask',
    'BackgroundFetchesPoolTask',
    'BackgroundCommonPoolTask',
    'BackgroundMovePoolTask',
    'BackgroundSchedulePoolTask'
)
ORDER BY utilization_pct DESC;
