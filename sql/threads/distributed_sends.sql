-- distributed_sends.sql
-- Distributed table send queue: data buffered for delivery to remote shards.
-- Large queues or high error counts mean data is stuck and not reaching its shard.
-- Column names use 24.8 schema: data_compressed_bytes, data_files (rows_to_insert removed).
SELECT
    hostName()                                          AS hostname,
    database,
    table,
    data_path,
    is_blocked,
    error_count,
    data_files,
    data_compressed_bytes,
    formatReadableSize(data_compressed_bytes)           AS size_queued,
    broken_data_files,
    last_exception,
    dateDiff('second', last_exception_time, now())      AS seconds_since_last_error,
    multiIf(
        is_blocked = 1 AND error_count > 10, 'CRITICAL - Send queue blocked with repeated errors',
        is_blocked = 1,                      'WARNING  - Send queue is blocked',
        error_count > 0,                     'CAUTION  - Send errors present, retrying',
                                             'OK       - Sending normally'
    ) AS send_status
    -- ALERT: is_blocked = 1 → data accumulating locally, not reaching remote shard
    -- ACTION (check): Verify destination shard is reachable:
    --         SELECT * FROM remote('<shard_host>', system.one)
    -- ACTION (flush): Force flush all pending data:
    --         SYSTEM FLUSH DISTRIBUTED <db>.<table>
    -- ACTION (reset): If permanently stuck, you can drop the queue (data loss risk!):
    --         SYSTEM DROP DISTRIBUTED SEND QUEUE <db>.<table>
    -- DOCS: system.distribution_queue — https://clickhouse.com/docs/en/operations/system-tables/distribution_queue
FROM clusterAllReplicas({cluster:String}, system.distribution_queue)
ORDER BY is_blocked DESC, error_count DESC, data_compressed_bytes DESC;
