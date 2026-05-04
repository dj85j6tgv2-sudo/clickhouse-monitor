# Backfill Script — Instructions

## Overview

`backfill_pair_stats.py` backfills `ecredal_db.pair_stats_daily` over a date
range. For each trading day it:

1. **Drops the day's partition** (idempotent — wipes any orphan rows from a
   previous failed run)
2. **Inserts split 0** — followers where `cityHash64(ISIN) % 2 = 0`
3. **Inserts split 1** — followers where `cityHash64(ISIN) % 2 = 1`
4. **Validates** — compares `count()` vs `countDistinct(leader, follower)` and
   warns if they differ

Weekends are skipped by default (no bond data). The script continues on failure
unless `--stop-on-error` is passed, and prints a summary table at the end.

---

## Requirements

### Python version

Python 3.9 or higher.

```bash
python --version
```

### Install dependencies

```bash
pip install clickhouse-connect
```

Or in a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install clickhouse-connect
```

---

## Configuration

Before running, open `backfill_pair_stats.py` and update the constants at the
top of the file:

```python
DEFAULT_CLUSTER = "your_cluster_name"    # from: SELECT cluster FROM system.clusters
DEFAULT_HOST    = "your-lb.example.com"  # load balancer hostname or IP
DEFAULT_PORT    = 8123                   # HTTP port (8443 for HTTPS)
```

Everything else can be passed on the command line.

### Find your cluster name

```sql
SELECT cluster FROM system.clusters GROUP BY cluster;
```

---

## Basic usage

```bash
python backfill_pair_stats.py \
  --start 2026-03-20 \
  --end   2026-04-19 \
  --host  your-lb.example.com \
  --user  your_user \
  --password your_password \
  --cluster  your_cluster_name
```

Both `--start` and `--end` are **inclusive**. Dates must be `YYYY-MM-DD`.

---

## All options

| Option | Default | Description |
|--------|---------|-------------|
| `--start` | required | Start date `YYYY-MM-DD` (inclusive) |
| `--end` | required | End date `YYYY-MM-DD` (inclusive) |
| `--host` | value in script | Load balancer hostname or IP |
| `--port` | `8123` | HTTP port (`8443` for HTTPS) |
| `--user` | `default` | ClickHouse username |
| `--password` | `""` | ClickHouse password |
| `--database` | `ecredal_db` | Default database |
| `--cluster` | value in script | Cluster name for `ON CLUSTER` |
| `--secure` | off | Enable HTTPS / TLS |
| `--stop-on-error` | off | Abort on first failed day |
| `--include-weekends` | off | Process Saturdays and Sundays too |

---

## Common examples

### Test on a single day first

Always test one day before running the full backfill:

```bash
python backfill_pair_stats.py \
  --start 2026-04-15 \
  --end   2026-04-15 \
  --host  your-lb.example.com \
  --user  your_user \
  --password your_password
```

Expected output:

```
2026-04-15 03:00:01 INFO === Processing 2026-04-15 ===
2026-04-15 03:00:01 INFO DROP PARTITION 2026-04-15
2026-04-15 03:00:02 INFO   INSERT split 0 for 2026-04-15
2026-04-15 03:00:14 INFO   INSERT split 0 done in 12.3s
2026-04-15 03:00:14 INFO   INSERT split 1 for 2026-04-15
2026-04-15 03:00:26 INFO   INSERT split 1 done in 11.8s
2026-04-15 03:00:26 INFO   ✓ 2026-04-15: 121,335,892 pairs in 25.1s

========================================================================
Date         Status              Pairs   Duration  Notes
------------------------------------------------------------------------
2026-04-15   OK          121,335,892      25.1s
------------------------------------------------------------------------
OK: 1   DIRTY: 0   FAIL: 0   Total: 0.4 min
========================================================================
```

### Full 30-day backfill

```bash
python backfill_pair_stats.py \
  --start 2026-03-20 \
  --end   2026-04-19 \
  --host  your-lb.example.com \
  --user  your_user \
  --password your_password \
  --cluster  your_cluster_name
```

### Stop immediately if any day fails

```bash
python backfill_pair_stats.py \
  --start 2026-03-20 \
  --end   2026-04-19 \
  --stop-on-error \
  ...
```

### HTTPS connection

```bash
python backfill_pair_stats.py \
  --host  your-lb.example.com \
  --port  8443 \
  --secure \
  ...
```

### Save logs to a file

```bash
python backfill_pair_stats.py \
  --start 2026-03-20 \
  --end   2026-04-19 \
  ... \
  2>&1 | tee backfill_20260320_20260419.log
```

---

## Resuming after a failure

The script is **fully idempotent** — `DROP PARTITION` runs before every
`INSERT`, so re-running the same date range is always safe. Days that
previously succeeded will be wiped and re-inserted cleanly.

If a day fails partway through (e.g., split 0 succeeded but split 1 timed
out), re-run from that day:

```bash
# Failed on 2026-04-03 — re-run from that day
python backfill_pair_stats.py \
  --start 2026-04-03 \
  --end   2026-04-19 \
  ...
```

The DROP PARTITION on `2026-04-03` will wipe the orphan rows from split 0
before re-inserting both splits cleanly.

---

## Parallelizing the backfill

The script processes days sequentially. To speed up the backfill, run two
instances in parallel over non-overlapping date ranges:

```bash
# Terminal 1 — first half
python backfill_pair_stats.py \
  --start 2026-03-20 --end 2026-04-04 \
  --host ... --user ... --password ... &

# Terminal 2 — second half
python backfill_pair_stats.py \
  --start 2026-04-05 --end 2026-04-19 \
  --host ... --user ... --password ... &

wait
echo "Backfill complete"
```

**Do not run more than 2–3 parallel instances** — each INSERT already uses
16 threads internally. Beyond 3 concurrent jobs you risk saturating the
cluster's RAM and causing OOM kills.

---

## Validation

### After the backfill completes

Run this in DBeaver or any SQL client to verify every day is clean:

```sql
SELECT
    TradeDate,
    count()                         AS raw_rows,
    countDistinct(leader, follower) AS distinct_pairs,
    raw_rows = distinct_pairs       AS is_clean
FROM ecredal_db.pair_stats_daily
GROUP BY TradeDate
ORDER BY TradeDate;
```

Every row should show `is_clean = 1`. Small discrepancies (raw_rows slightly
above distinct_pairs) are normal while ClickHouse background merges are still
running — wait a few minutes and re-check.

### Check replication is healthy

```sql
SELECT
    hostName()       AS replica,
    absolute_delay,
    queue_size,
    inserts_in_queue
FROM clusterAllReplicas('{cluster_name}', system.replicas)
WHERE database = 'ecredal_db' AND table = 'pair_stats_daily';
```

`absolute_delay` should be near 0 on all three replicas once replication
catches up.

### Check row counts per replica match

```sql
SELECT
    hostName() AS replica,
    count()    AS rows
FROM clusterAllReplicas('{cluster_name}', ecredal_db.pair_stats_daily)
GROUP BY replica
ORDER BY replica;
```

All three should show the same number.

---

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | All days succeeded and validated clean |
| `1` | One or more days failed or returned DIRTY counts |
| `2` | Bad arguments (e.g., `--start` > `--end`) |

Use the exit code in scripts:

```bash
python backfill_pair_stats.py ... || echo "Backfill failed — check logs"
```

Or in Jenkins:

```groovy
sh 'python backfill_pair_stats.py ...'
// Jenkins marks the build FAILED automatically if exit code != 0
```

---

## Tuning for larger ISIN sets (30k future scale)

When scaling from 11k to 30k ISINs, adjust these constants in the script:

```python
INSERT_SETTINGS = {
    "max_execution_time": 7200,            # 2h — INSERTs will take longer
    "max_memory_usage": 120_000_000_000,   # 120 GB if your nodes have it
    "max_threads": 16,                     # keep at vCPU count
    ...
}
```

Also consider splitting into 4 groups instead of 2 (`% 4 = 0, 1, 2, 3`) by
changing the loop in the script:

```python
# In process_day(), change:
for split_id in (0, 1):

# To:
for split_id in (0, 1, 2, 3):
```

And update the INSERT SQL to use `% 4` instead of `% 2`. This halves the
memory per INSERT at the cost of 4 INSERT passes per day instead of 2.

---

## Troubleshooting

### `clickhouse_connect` not found

```bash
pip install clickhouse-connect
```

If you have multiple Python versions:

```bash
python3 -m pip install clickhouse-connect
```

### Connection timeout / refused

- Verify the host and port are reachable: `curl http://your-lb:8123/ping`
- If the cluster uses HTTPS: add `--secure --port 8443`
- Check firewall rules between your machine and the load balancer

### INSERT times out mid-run

Increase `max_execution_time` in `INSERT_SETTINGS`:

```python
"max_execution_time": 7200,   # 2 hours
```

And increase the client-level timeout too:

```python
send_receive_timeout=14400,   # 4 hours
```

### `DIRTY` rows in validation

Raw row count > distinct pair count — orphan parts from a previous partial
insert. Re-run the script for that day; the `DROP PARTITION` will clean them
up:

```bash
python backfill_pair_stats.py --start <dirty_date> --end <dirty_date> ...
```

### `Replica already exists` error on DROP PARTITION

This occasionally happens if ZooKeeper has stale state. Wait 30 seconds and
retry, or see the ClickHouse section of the main solution doc for the
`SYSTEM DROP REPLICA` cleanup procedure.

### Memory issues (OOM killed)

Reduce memory per INSERT by splitting into 4 groups instead of 2 (see
**Tuning** section above). Also verify no other heavy queries are running on
the cluster at the same time.

---

## Nightly job (D-2 recurring)

Once the backfill is complete, the same script can be used as the nightly
recurring job. For a single day (D-2):

```bash
# In cron or Jenkins, run at 03:00 every day
python backfill_pair_stats.py \
  --start $(date -d '2 days ago' +%Y-%m-%d) \
  --end   $(date -d '2 days ago' +%Y-%m-%d) \
  --host  your-lb.example.com \
  --user  your_user \
  --password your_password \
  --cluster  your_cluster_name \
  --stop-on-error
```

`--stop-on-error` is appropriate for the nightly job so failures surface
immediately via exit code.

Example cron entry:

```cron
0 3 * * 1-5 /path/to/.venv/bin/python /path/to/backfill_pair_stats.py \
  --start $(date -d '2 days ago' +\%Y-\%m-\%d) \
  --end   $(date -d '2 days ago' +\%Y-\%m-\%d) \
  --host  your-lb.example.com \
  --user  your_user \
  --password your_password \
  --stop-on-error \
  >> /var/log/pair_stats_daily.log 2>&1
```

`1-5` limits execution to Monday–Friday (same as the weekend-skip logic
inside the script).
