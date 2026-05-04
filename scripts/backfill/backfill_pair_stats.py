#!/usr/bin/env python3
"""
Backfill ecredal_db.pair_stats_daily over a date range.

For each day:
  1. DROP PARTITION (idempotent — wipes any orphan rows from previous failed run)
  2. INSERT partition 0 (followers with cityHash64(ISIN) % 2 = 0)
  3. INSERT partition 1 (followers with cityHash64(ISIN) % 2 = 1)
  4. Validate row count vs distinct pairs

Splitting in two halves keeps memory usage manageable and lets us recover
from a single split failure without redoing the whole day.

Requires: clickhouse-connect (pip install clickhouse-connect)
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterator

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_CLUSTER = "your_cluster_name"
DEFAULT_HOST = "your-load-balancer.example.com"
DEFAULT_PORT = 8123

TABLE = "ecredal_db.pair_stats_daily"
ISIN_TABLE = "ecredal_db.temp_isin_table"
SOURCE_TABLE = "ecredal_db.t_MACPP_v2"

# Per-INSERT settings — bound runtime/memory so a stuck query fails loudly
# rather than dragging on for hours
INSERT_SETTINGS = {
    "max_execution_time": 3600,           # 1h hard cap per split
    "max_memory_usage": 80_000_000_000,   # 80 GB per query — adjust to your cluster
    "max_threads": 16,
    "max_block_size": 65536,
    "join_algorithm": "parallel_hash",
    "optimize_read_in_order": 1,
    "optimize_aggregation_in_order": 1,
    "insert_deduplicate": 1,              # block-level dedup safety net
}


# ---------------------------------------------------------------------------
# SQL templates
# ---------------------------------------------------------------------------

DROP_PARTITION_SQL = """
ALTER TABLE {table}
  ON CLUSTER '{cluster}'
  DROP PARTITION %(trade_date)s
"""

INSERT_SQL = """
INSERT INTO {table}
WITH
  src_data AS (
    SELECT
      TradeTime AS TIME,
      ISIN,
      0.005 * (Bid_Spread + Ask_Spread) AS spread
    FROM {source_table}
    PREWHERE TradeDate = %(trade_date)s
      AND ISIN IN (
        SELECT DISTINCT ISIN FROM {isin_table}
        WHERE cityHash64(ISIN) %% 2 = %(split_id)s
           OR ISIN LIKE 'US31%%'
      )
    WHERE Bid_Spread IS NOT NULL
      AND Ask_Spread IS NOT NULL
      AND NOT isNaN(Bid_Spread)
      AND NOT isNaN(Ask_Spread)
  ),
  leaders AS (
    SELECT TIME, ISIN, spread
    FROM src_data
    WHERE ISIN LIKE 'US31%%'
  ),
  followers AS (
    SELECT TIME, ISIN, spread
    FROM src_data
    WHERE ISIN IN (
      SELECT DISTINCT ISIN FROM {isin_table}
      WHERE cityHash64(ISIN) %% 2 = %(split_id)s
    )
  ),
  followers_x_leader AS (
    SELECT
      f.TIME,
      f.ISIN   AS follower,
      l.ISIN   AS leader,
      f.spread AS f_spread
    FROM followers AS f
    CROSS JOIN (SELECT DISTINCT ISIN FROM leaders) AS l
  )
SELECT
  toDate(%(trade_date)s)               AS TradeDate,
  leader,
  follower,
  count()                              AS n_obs,
  avgState(f_spread - l.spread)        AS avg_state,
  stddevSampState(f_spread - l.spread) AS std_state
FROM followers_x_leader AS fx
ASOF INNER JOIN leaders AS l
  ON fx.leader = l.ISIN
  AND fx.TIME  > l.TIME
WHERE fx.follower != fx.leader
GROUP BY leader, follower
"""

VALIDATE_SQL = """
SELECT
    count()                          AS raw_rows,
    countDistinct(leader, follower)  AS distinct_pairs
FROM {table}
WHERE TradeDate = %(trade_date)s
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class DayResult:
    trade_date: date
    rows: int
    distinct_pairs: int
    duration_s: float
    success: bool
    error: str | None = None

    @property
    def is_clean(self) -> bool:
        # Allow tiny tolerance for in-flight merges of AggregatingMergeTree
        return self.rows == self.distinct_pairs


def daterange(start: date, end: date) -> Iterator[date]:
    """Iterate dates inclusive [start, end], skipping weekends."""
    d = start
    while d <= end:
        if d.weekday() < 5:  # Mon–Fri
            yield d
        d += timedelta(days=1)


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def fmt_partition(d: date) -> str:
    # ClickHouse expects partition values for Date columns as 'YYYY-MM-DD'
    return d.isoformat()


# ---------------------------------------------------------------------------
# Per-day processing
# ---------------------------------------------------------------------------

def drop_partition(client: Client, trade_date: date, cluster: str) -> None:
    """Wipe the day's partition. Instant + atomic, idempotent."""
    sql = DROP_PARTITION_SQL.format(table=TABLE, cluster=cluster)
    logging.info("DROP PARTITION %s", trade_date)
    client.command(sql, parameters={"trade_date": fmt_partition(trade_date)})


def insert_split(client: Client, trade_date: date, split_id: int) -> None:
    """Insert one half of the followers (split_id ∈ {0, 1})."""
    sql = INSERT_SQL.format(
        table=TABLE,
        source_table=SOURCE_TABLE,
        isin_table=ISIN_TABLE,
    )
    logging.info("  INSERT split %d for %s", split_id, trade_date)
    t0 = time.monotonic()
    client.command(
        sql,
        parameters={
            "trade_date": trade_date.isoformat(),
            "split_id": split_id,
        },
        settings=INSERT_SETTINGS,
    )
    logging.info("  INSERT split %d done in %.1fs", split_id, time.monotonic() - t0)


def validate_day(client: Client, trade_date: date) -> tuple[int, int]:
    """Return (raw_rows, distinct_pairs) for sanity check."""
    sql = VALIDATE_SQL.format(table=TABLE)
    rows = client.query(sql, parameters={"trade_date": fmt_partition(trade_date)}).result_rows
    raw, distinct = rows[0]
    return int(raw), int(distinct)


def process_day(client: Client, trade_date: date, cluster: str) -> DayResult:
    """Process one trading day end-to-end."""
    t0 = time.monotonic()
    logging.info("=== Processing %s ===", trade_date)

    try:
        drop_partition(client, trade_date, cluster)

        # Two splits to bound memory per query
        for split_id in (0, 1):
            insert_split(client, trade_date, split_id)

        raw_rows, distinct_pairs = validate_day(client, trade_date)
        duration = time.monotonic() - t0

        result = DayResult(
            trade_date=trade_date,
            rows=raw_rows,
            distinct_pairs=distinct_pairs,
            duration_s=duration,
            success=True,
        )

        if not result.is_clean:
            logging.warning(
                "  ⚠ %s: %d rows but %d distinct pairs — possible duplicates "
                "(may resolve after background merges)",
                trade_date, raw_rows, distinct_pairs,
            )
        else:
            logging.info(
                "  ✓ %s: %d pairs in %.1fs",
                trade_date, distinct_pairs, duration,
            )
        return result

    except ClickHouseError as e:
        duration = time.monotonic() - t0
        logging.error("  ✗ %s failed after %.1fs: %s", trade_date, duration, e)
        return DayResult(
            trade_date=trade_date,
            rows=0,
            distinct_pairs=0,
            duration_s=duration,
            success=False,
            error=str(e),
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill ecredal_db.pair_stats_daily",
    )
    parser.add_argument("--start", type=parse_date, required=True,
                        help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", type=parse_date, required=True,
                        help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--user", default="default")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="ecredal_db")
    parser.add_argument("--cluster", default=DEFAULT_CLUSTER)
    parser.add_argument("--secure", action="store_true",
                        help="Use HTTPS / TLS")
    parser.add_argument("--stop-on-error", action="store_true",
                        help="Abort the whole backfill on first failure")
    parser.add_argument("--include-weekends", action="store_true",
                        help="Process Saturdays and Sundays too")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.start > args.end:
        logging.error("--start must be <= --end")
        return 2

    client = clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        database=args.database,
        secure=args.secure,
        # Generous timeouts because each INSERT can take many minutes
        connect_timeout=30,
        send_receive_timeout=7200,
    )

    if args.include_weekends:
        days = [args.start + timedelta(days=i)
                for i in range((args.end - args.start).days + 1)]
    else:
        days = list(daterange(args.start, args.end))

    logging.info("Backfilling %d days from %s to %s",
                 len(days), args.start, args.end)

    results: list[DayResult] = []
    for d in days:
        result = process_day(client, d, args.cluster)
        results.append(result)
        if not result.success and args.stop_on_error:
            logging.error("Stopping due to --stop-on-error")
            break

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print()
    print("=" * 72)
    print(f"{'Date':<12} {'Status':<8} {'Pairs':>14} {'Duration':>10}  Notes")
    print("-" * 72)
    total_duration = 0.0
    n_ok = n_fail = n_dirty = 0
    for r in results:
        total_duration += r.duration_s
        if not r.success:
            n_fail += 1
            status = "FAIL"
            notes = (r.error or "")[:30]
        elif not r.is_clean:
            n_dirty += 1
            status = "DIRTY"
            notes = f"rows={r.rows} != pairs={r.distinct_pairs}"
        else:
            n_ok += 1
            status = "OK"
            notes = ""
        print(f"{r.trade_date.isoformat():<12} {status:<8} "
              f"{r.distinct_pairs:>14,} {r.duration_s:>9.1f}s  {notes}")
    print("-" * 72)
    print(f"OK: {n_ok}   DIRTY: {n_dirty}   FAIL: {n_fail}   "
          f"Total: {total_duration/60:.1f} min")
    print("=" * 72)

    return 0 if (n_fail == 0 and n_dirty == 0) else 1


if __name__ == "__main__":
    sys.exit(main())
