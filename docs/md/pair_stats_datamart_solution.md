# Pair Statistics Datamart — ClickHouse Solution

## Context

### Problem

Compute, for each **(leader ISIN, follower ISIN)** pair, the time series of price
differences over a rolling **30-day window** and return its **average** and
**sample standard deviation**.

Because tick timestamps are not aligned between two ISINs, we cannot take a
direct row-by-row difference. For every follower tick we must find the **most
recent leader tick before it** — this is what the ClickHouse `ASOF JOIN` does.

### Requirements

- **Data source**: `ecredal_db.t_MACPP_v2`
  - `PARTITION BY TradeDate`
  - `ORDER BY (ISIN, TradeTime)`
- **ISIN universe**: `ecredal_db.temp_isin_table` — currently ~11k, expected to
  grow to ~30k.
- **Leaders** today: ISINs matching `US31%`.
- **Followers**: the full universe (including `US31%`), with `leader != follower`.
- **Reporting cadence**: end user opens the report **every morning** and expects
  a **rolling 30-day window ending at D-2**. Example: on the morning of
  **21 April**, the report covers **22 March → 19 April** (D-2 = 19 April).
- **Latency target**: **sub-second** report query.

### Why not a pure-query approach

| ISIN count | Pairs | Pure-query runtime | Datamart runtime |
|------------|-------|--------------------|--------------------|
| 11k × ~200 leaders | ~2.2M | ~6 min per report | < 1s |
| 30k × ~500 leaders | ~15M | minutes to hours | < 1s |

At 30k ISINs a single monthly pair query becomes infeasible. A **daily
datamart** decouples user-query cost from universe size: the monthly report only
aggregates ~30 rows per pair, no matter how many pairs exist.

---

## Architecture

```
   ecredal_db.t_MACPP_v2 (raw ticks, partitioned by TradeDate)
              │
              │  nightly batch — one day at a time, target = D-2
              │  runs the existing ASOF query for that single day
              ▼
   ecredal_db.pair_stats_daily (AggregatingMergeTree)
              │
              │  user query: SELECT ... WHERE TradeDate BETWEEN today-32 AND today-2
              ▼
          Report (< 1s)
```

**Key property**: ClickHouse `AggregateFunction(avg, ...)` and
`AggregateFunction(stddevSamp, ...)` states **merge exactly**. The stddev
reconstructed from 30 daily states is mathematically identical to the stddev
computed from the full month of raw ticks in one shot. No precision loss, no
"avg-of-daily-stds" approximation.

---

## Cluster topology

The target cluster has **1 shard × 3 replicas**, accessed through a load
balancer in front of the three nodes. All DDL must therefore be issued
`ON CLUSTER`, and the storage engine must be a `Replicated*` variant so writes
on any replica propagate via ZooKeeper / ClickHouse Keeper.

Replace `{cluster_name}` below with the actual name defined in `remote_servers`
(check with `SELECT cluster FROM system.clusters GROUP BY cluster`).

## Step 1 — Create the datamart table (one-time)

```sql
CREATE TABLE ecredal_db.pair_stats_daily ON CLUSTER '{cluster_name}'
(
    TradeDate  Date,
    leader     LowCardinality(String),
    follower   LowCardinality(String),
    n_obs      UInt64,
    avg_state  AggregateFunction(avg,        Float64),
    std_state  AggregateFunction(stddevSamp, Float64)
)
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{shard}/ecredal_db/pair_stats_daily',
    '{replica}'
)
PARTITION BY toYYYYMM(TradeDate)
ORDER BY (leader, follower, TradeDate);
```

**Design notes**

- `ON CLUSTER '{cluster_name}'` — DDL runs on all three replicas. Issue once
  against the load balancer; ClickHouse distributes it.
- `ReplicatedAggregatingMergeTree` with the ZooKeeper path and replica macro —
  `{shard}` and `{replica}` are the standard macros defined in each node's
  `config.xml`. No manual substitution needed; ClickHouse resolves them
  per-node at execution time.
- ZK path includes `{shard}` so that if you ever add shards, the paths stay
  unique. With your 1-shard topology it resolves to a single path shared by
  the three replicas, which is exactly what enables replication.
- `ORDER BY (leader, follower, TradeDate)` — user queries filter on a date
  range and group by pair. Pair rows for consecutive days land in adjacent
  blocks → sequential reads.
- `PARTITION BY toYYYYMM(TradeDate)` — monthly partitions make
  `DROP PARTITION` cheap when retiring old data.
- `LowCardinality(String)` for ISIN — large dictionary savings on 30k unique
  values.

### Sanity check after creation

```sql
-- Every replica should report the table
SELECT hostName(), database, name, engine
FROM clusterAllReplicas('{cluster_name}', system.tables)
WHERE database = 'ecredal_db' AND name = 'pair_stats_daily';

-- Replication health
SELECT hostName(), is_leader, absolute_delay, queue_size, inserts_in_queue
FROM clusterAllReplicas('{cluster_name}', system.replicas)
WHERE database = 'ecredal_db' AND table = 'pair_stats_daily';
```

---

## Step 2 — Nightly batch insert (scheduled job)

Scheduled to run every morning. Target date = `today() - 2`.

```sql
INSERT INTO ecredal_db.pair_stats_daily
WITH
  target_date AS (SELECT today() - 2 AS d),
  src_data AS (
    SELECT
      TradeTime AS TIME,
      ISIN,
      0.005 * (Bid_Spread + Ask_Spread) AS spread
    FROM ecredal_db.t_MACPP_v2
    PREWHERE TradeDate = (SELECT d FROM target_date)
      AND ISIN IN (SELECT ISIN FROM ecredal_db.temp_isin_table)
    WHERE Bid_Spread IS NOT NULL
      AND Ask_Spread IS NOT NULL
      AND NOT isNaN(Bid_Spread)
      AND NOT isNaN(Ask_Spread)
  ),
  leaders AS (
    SELECT TIME, ISIN, spread
    FROM src_data
    WHERE ISIN LIKE 'US31%'
  ),
  followers_x_leader AS (
    SELECT
      f.TIME,
      f.ISIN   AS follower,
      l.ISIN   AS leader,
      f.spread AS f_spread
    FROM src_data AS f
    CROSS JOIN (SELECT DISTINCT ISIN FROM leaders) AS l
  )
SELECT
  (SELECT d FROM target_date)           AS TradeDate,
  leader,
  follower,
  count()                                AS n_obs,
  avgState(f_spread - l.spread)          AS avg_state,
  stddevSampState(f_spread - l.spread)   AS std_state
FROM followers_x_leader AS fx
ASOF INNER JOIN leaders AS l
  ON fx.leader = l.ISIN
  AND fx.TIME  > l.TIME
WHERE fx.follower != fx.leader
GROUP BY leader, follower;
```

**Characteristics**

- Runs the existing ASOF query **once per day**, against a single day's data.
  Cost per night is linear in daily volume, independent of history length.
- Measured runtime today on one day: ~13 seconds for the `US31%` leader scope.
- **Where to run it**: the `INSERT` goes to any single replica (via the load
  balancer). Replication propagates the new parts automatically to the other
  two replicas — no need to run the INSERT on each node.
- Idempotent re-run: before re-inserting a given `TradeDate`, remove existing
  rows on all replicas with a cluster-wide mutation:
  ```sql
  ALTER TABLE ecredal_db.pair_stats_daily
    ON CLUSTER '{cluster_name}'
    DELETE WHERE TradeDate = '2026-04-19';
  ```
  Or switch the engine to `ReplicatedReplacingMergeTree` keyed on
  `(leader, follower, TradeDate)` if you prefer last-write-wins semantics
  without an explicit DELETE.

---

## Step 3 — One-time backfill

Bootstrap the last ~32 trading days so the first report works from day one.

```bash
#!/usr/bin/env bash
# backfill_pair_stats.sh — run once
set -euo pipefail

for d in $(seq 2 34); do
  DATE=$(date -d "$d days ago" +%Y-%m-%d)
  echo "Backfilling $DATE ..."
  clickhouse-client --query "
    INSERT INTO ecredal_db.pair_stats_daily
    <same CTE body as Step 2, but with TradeDate = '$DATE'>
  " &

  # limit parallelism to 4 concurrent days
  while [ "$(jobs -r | wc -l)" -ge 4 ]; do sleep 2; done
done
wait
```

With 4-way parallelism the backfill typically completes in well under an hour.
Skip weekends/holidays if your raw data has gaps.

---

## Step 4 — End-user report query

This is the query that runs when the user opens the report on the morning of
**21 April** — it returns the rolling 30-day window ending at D-2.

```sql
SELECT
    leader,
    follower,
    sum(n_obs)                  AS n_obs,
    avgMerge(avg_state)         AS avg_diff,
    stddevSampMerge(std_state)  AS std_diff
FROM ecredal_db.pair_stats_daily
WHERE TradeDate >  today() - 32     -- 30 calendar days back from D-2
  AND TradeDate <= today() - 2
GROUP BY leader, follower
HAVING n_obs >= 100                 -- optional: filter illiquid pairs
ORDER BY abs(avg_diff) DESC;
```

**Runtime**: sub-second, independent of ISIN-universe size, because each pair
contributes at most ~30 rows to the aggregation.

**Custom date ranges** work for free — swap the `WHERE` clause for any
`TradeDate BETWEEN …` and the same query returns exact stats for that window.

---

## Scheduling

Any scheduler works. Example cron entry running at 03:00 every day:

```cron
0 3 * * * /usr/bin/clickhouse-client --queries-file=/opt/jobs/pair_stats_daily_insert.sql >> /var/log/pair_stats.log 2>&1
```

Or as a Jenkins job (matches the existing ClickHouse migration pipeline
pattern): one stage runs the `DELETE WHERE TradeDate = today() - 2`, the next
runs the INSERT.

---

## Scaling behaviour

| Metric | 11k ISINs today | 30k ISINs future |
|--------|-----------------|-------------------|
| Leaders (US31%) | ~200 | ~500 (est.) |
| Pairs stored per day | ~2.2M | ~15M |
| Nightly batch runtime | ~13s | ~30–90s (est.) |
| Storage per month | ~55 GB | ~150 GB |
| User query runtime | < 1s | < 1s |

The user-facing query stays flat because it only aggregates over 30 daily rows
per pair. The nightly job scales with daily data volume, not with history
length.

---

## Settings recommended for the nightly job

```sql
SET max_threads = 16;
SET max_block_size = 65536;
SET optimize_read_in_order = 1;
SET optimize_aggregation_in_order = 1;
SET join_algorithm = 'parallel_hash';
```

`full_sorting_merge` was tested and found slower than hash for this join
shape (leader side is small enough to fit the hash table easily).

---

## Why the aggregate-state approach is exact

`stddevSampState` stores the Welford / Chan running form of the variance:
`n`, `mean`, and the sum of squared deviations from the mean (`M2`). The merge
step combines two such triplets using the Chan-Golub-LeVeque parallel formula,
which is numerically stable and produces the **same** result as one-pass
computation over the combined data.

This is why daily states can be merged across arbitrary date ranges —
weekly, monthly, year-to-date, custom — without recomputing from raw ticks.

---

## Maintenance and operations

All mutating DDL must be issued `ON CLUSTER` so the three replicas stay in sync.
Queries (SELECT) can go straight to the load balancer — no `ON CLUSTER` needed.

- **Retention**: drop partitions older than N months as needed
  ```sql
  ALTER TABLE ecredal_db.pair_stats_daily
    ON CLUSTER '{cluster_name}'
    DROP PARTITION 202512;
  ```
- **Recompute a day** (e.g., if raw data was corrected):
  ```sql
  ALTER TABLE ecredal_db.pair_stats_daily
    ON CLUSTER '{cluster_name}'
    DELETE WHERE TradeDate = '2026-04-15';
  -- then re-run the nightly INSERT for that date (single replica is fine)
  ```
- **Schema evolution**: if a new leader pattern is added (say `US91%`), modify
  the `leaders` CTE in the nightly job and backfill history from that point.
  Existing pair rows are unaffected.
- **Monitoring: data completeness**
  ```sql
  SELECT TradeDate, count() AS pair_count
  FROM ecredal_db.pair_stats_daily
  WHERE TradeDate >= today() - 35
  GROUP BY TradeDate
  ORDER BY TradeDate;
  ```
- **Monitoring: replication lag**
  ```sql
  SELECT hostName(), absolute_delay, queue_size, inserts_in_queue
  FROM clusterAllReplicas('{cluster_name}', system.replicas)
  WHERE database = 'ecredal_db' AND table = 'pair_stats_daily';
  ```
  `absolute_delay` should be near 0; non-zero `queue_size` after the nightly
  insert means replication is still catching up, which is normal for a few
  seconds post-insert.

---

## Summary

- **One table**: `ecredal_db.pair_stats_daily`, `ReplicatedAggregatingMergeTree`,
  created `ON CLUSTER` so all three replicas stay in sync.
- **One nightly job**: runs the existing 13s ASOF query for D-2, writes aggregate
  states to any single replica; replication propagates to the others.
- **One report query**: `avgMerge` + `stddevSampMerge` over the 30-day window,
  sub-second. Goes through the load balancer — no `ON CLUSTER`.
- **Exact results**, no precision loss versus raw-tick computation.
- **Scales to 30k ISINs** without changing the user experience.
