-- broken_parts.sql
-- Parts currently sitting in the detached/ directory across all nodes.
-- A detached part indicates ClickHouse moved it aside due to corruption or manual intervention.
-- Note: system.part_log's BrokenPart event type is not available in 24.8; use system.detached_parts.
SELECT
    hostName()                              AS hostname,
    database,
    table,
    name                                    AS part_name,
    partition_id,
    formatReadableSize(bytes_on_disk)       AS size,
    reason,
    modification_time
    -- ALERT: reason LIKE '%broken%' → data corruption, requires investigation
    -- ACTION (step 1): Check if table is replicated:
    --         SELECT * FROM system.replicas WHERE database = '<db>' AND table = '<table>'
    -- ACTION (step 2): If replicated, fetch healthy copy from another replica:
    --         SYSTEM SYNC REPLICA <db>.<table>
    -- ACTION (step 3): If standalone, restore from backup or DROP the broken detached part:
    --         ALTER TABLE <db>.<table> DROP DETACHED PART '<part_name>'
    -- ACTION (step 4): Run CHECK TABLE to assess remaining data health:
    --         CHECK TABLE <db>.<table>
    -- DOCS: system.detached_parts — https://clickhouse.com/docs/en/operations/system-tables/detached_parts
FROM clusterAllReplicas({cluster:String}, system.detached_parts)
WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
ORDER BY modification_time DESC
LIMIT 50;
