# ClickHouse Monitoring Dashboard — Design Spec

**Date:** 2026-04-20
**Status:** Draft
**Approach:** Layered Architecture with Role Views (Approach B)

---

## 1. Purpose

A Streamlit dashboard to monitor a ClickHouse cluster (`ecredal_cluster`, 1 shard / 3 replicas / 3 keepers). It serves two audiences:

- **DBAs** — deep metrics across all domains (cluster health, queries, disk, merges, connections, threads, system metrics, inserts, dictionaries)
- **Developers/analysts** — query activity, errors, and table usage for any ClickHouse user

Primary pain points to surface prominently:
1. Users running massive queries that consume all memory
2. Disk filling up unexpectedly

## 2. Infrastructure Context

- **Cluster:** `ecredal_cluster` — 1 shard, 3 replicas, 3 ClickHouse Keeper nodes
- **Access:** Single load balancer endpoint (least_connection algorithm), user/password authentication
- **Users:** ~5-10 human users, ~5 service accounts
- **Databases:** Multiple databases within the single cluster

## 3. Project Structure

```
clickhouse-monitor/
├── config.yaml                    # Connection + SMTP + thresholds
├── config.example.yaml            # Template for version control
├── app.py                         # Streamlit entry point
├── sql/                           # SQL monitoring queries (existing files)
│   ├── cluster/
│   │   ├── fetch_queue.sql
│   │   ├── node_status.sql
│   │   ├── replica_consistency.sql
│   │   ├── replication_lag.sql
│   │   └── zookeeper_health.sql
│   ├── queries/
│   │   ├── full_table_scans.sql
│   │   ├── memory_heavy.sql
│   │   ├── running_now.sql
│   │   ├── slow_queries.sql
│   │   └── top_query_patterns.sql
│   ├── disk/
│   │   ├── broken_parts.sql
│   │   ├── detached_parts.sql
│   │   ├── free_space.sql
│   │   ├── parts_health.sql
│   │   ├── table_sizes.sql
│   │   └── ttl_progress.sql
│   ├── merges/
│   │   ├── active_merges.sql
│   │   ├── mutations.sql
│   │   └── queue_depth.sql
│   ├── connections/
│   │   └── session_stats.sql
│   ├── threads/
│   │   ├── background_tasks.sql
│   │   ├── distributed_sends.sql
│   │   └── thread_pool_usage.sql
│   ├── users/
│   │   ├── activity.sql
│   │   ├── errors.sql
│   │   └── top_tables.sql
│   ├── system_metrics/
│   │   ├── current_metrics.sql
│   │   └── events_summary.sql
│   ├── inserts/
│   │   ├── async_inserts.sql
│   │   └── insert_rates.sql
│   └── dictionaries/
│       ├── memory_usage.sql
│       └── status.sql
├── src/
│   ├── __init__.py
│   ├── config.py                  # Load/validate config.yaml
│   ├── connection.py              # ClickHouse client (clickhouse-connect)
│   ├── query_executor.py          # Load SQL files, execute with parameters
│   ├── alerts/
│   │   ├── __init__.py
│   │   ├── evaluator.py           # Parse CRITICAL/WARNING from query results
│   │   └── email_sender.py        # SMTP email notifications
│   └── ui/
│       ├── __init__.py
│       ├── components.py          # Shared UI widgets (status badges, metric cards)
│       └── formatters.py          # DataFrame display formatting
├── pages/
│   ├── 1_Overview.py
│   ├── 2_Cluster.py
│   ├── 3_Queries.py
│   ├── 4_Disk.py
│   ├── 5_Merges.py
│   ├── 6_Connections.py
│   ├── 7_Threads.py
│   ├── 8_System_Metrics.py
│   ├── 9_Inserts.py
│   ├── 10_Dictionaries.py
│   └── 11_User_Dashboard.py
└── requirements.txt

```

## 4. Configuration

File: `config.yaml`

```yaml
clickhouse:
  host: "loadbalancer.example.com"
  port: 8123                          # HTTP interface
  user: "monitoring"
  password: "changeme"
  cluster: "ecredal_cluster"
  connect_timeout: 10
  query_timeout: 30

lookback:
  default_hours: 6
  default_days: 7

refresh:
  auto_enabled: false
  interval_seconds: 60

alerts:
  enabled: true
  check_interval_seconds: 300
  cooldown_minutes: 30
  severity_levels:
    - CRITICAL
    - WARNING

  smtp:
    host: "smtp.example.com"
    port: 587
    use_tls: true
    user: "alerts@example.com"
    password: "changeme"
    from_address: "clickhouse-monitor@example.com"
    recipients:
      - "dba-team@example.com"

thresholds:
  disk_used_pct_warning: 75
  disk_used_pct_critical: 90
  memory_usage_warning_gb: 10
  memory_usage_critical_gb: 20
  replication_delay_warning: 60
  replication_delay_critical: 300
```

- HTTP interface (8123) chosen because it works cleanly through the load balancer's least_connection algorithm
- Alert cooldown prevents email storms for persistent issues
- Configurable thresholds override hardcoded SQL values without editing queries
- Severity filter controls which levels trigger email notifications

## 5. Data Layer

### Connection (`src/connection.py`)

- Uses `clickhouse-connect` library — HTTP-based, works well behind load balancers
- Single shared client instance per Streamlit session via `st.session_state`
- Connection health check on startup (`SELECT 1`)

### Query Executor (`src/query_executor.py`)

- Loads `.sql` files from `sql/` directory by domain/name (e.g., `load_query("cluster", "node_status")`)
- Injects parameters:
  - `{cluster:String}` → config value
  - `{lookback_hours:UInt32}` → from UI time selector
  - `{lookback_days:UInt32}` → `ceil(hours / 24)`
- Returns pandas DataFrames
- Caches results using `st.cache_data` with TTL matching the refresh interval — switching tabs doesn't re-execute the same query
- Error handling: connection failures and query timeouts return an empty DataFrame + error message in the UI, rather than crashing the page
- Only substitutes parameters that exist in the SQL text — queries without time windows (e.g., `running_now.sql`, `free_space.sql`) are unaffected

## 6. Alert Engine

### Background Thread

- Started by `app.py` on launch as a `threading.Thread` with `daemon=True`
- Runs on a configurable cycle (`check_interval_seconds`, default 5 minutes)
- Independent of page navigation — alerts fire even if the dashboard is sitting on a different page

### Monitored Queries

The alert engine evaluates a subset of queries each cycle:

| Query | What it catches |
|---|---|
| `disk/free_space.sql` | Disk usage approaching capacity |
| `cluster/replica_consistency.sql` | Replica read-only, session expired, delay |
| `cluster/zookeeper_health.sql` | ZK session loss |
| `queries/memory_heavy.sql` | Memory-hogging query patterns |
| `queries/running_now.sql` | Long-running queries with high memory (live) |
| `disk/broken_parts.sql` | Data corruption events |
| `merges/mutations.sql` | Stuck mutations |
| `dictionaries/status.sql` | Failed dictionary loads |

### Alert Evaluation (`src/alerts/evaluator.py`)

- Parses status/assessment columns from query results — the SQL already outputs `CRITICAL`, `WARNING`, `CAUTION`, `OK` strings as prefixes
- Extracts severity by matching the prefix pattern
- Builds alert objects: `{severity, domain, message, timestamp, details_row}`
- Maintains an in-memory alert log (last 100 alerts) accessible from the UI

### Cooldown Logic

- Tracks `(domain, severity, key)` tuples — e.g., `("disk", "CRITICAL", "hostname=node1")`
- Suppresses re-send within `cooldown_minutes`
- Severity escalation (WARNING → CRITICAL) bypasses cooldown

### Email Sender (`src/alerts/email_sender.py`)

- Standard `smtplib` with TLS
- Email body includes: severity, domain, summary, affected host/table, and action steps extracted from SQL comments
- Batches multiple alerts from the same cycle into a single email

## 7. UI Layer

### Global Controls (Sidebar — all pages)

- **Time window selector:** preset buttons (1h, 6h, 24h, 7d) + custom input
- **Auto-refresh toggle** + interval selector (30s, 1m, 5m)
- **Manual refresh button**
- **Cluster name display** (from config)
- **Last refresh timestamp**

### Alert Banner (all pages)

- Persistent bar at the top across all pages
- Shows count of active CRITICAL and WARNING alerts (e.g., "2 CRITICAL · 5 WARNING")
- Dismissable per session, reappears on new alerts

### Overview Page (`pages/1_Overview.py`)

The landing page. "Glance and know."

**Top priority panels (always visible, prominent):**

1. **Memory Alert Panel:**
   - Currently running queries sorted by memory usage (from `running_now.sql`)
   - Queries exceeding `memory_usage_warning_gb` highlighted amber, `critical_gb` in red
   - Shows user, elapsed time, memory, query preview
   - Green "All clear" state when nothing is alarming

2. **Disk Alert Panel:**
   - Per-node disk usage bars (from `free_space.sql`)
   - Color transitions: green → amber at warning → red at critical threshold
   - Shows free space remaining in human-readable format

**Health grid below:**
- Grid of status cards, one per domain
- Each card shows: domain name, worst severity badge (colored green/yellow/amber/red), one-line summary
- Clicking a card navigates to that domain's detail page

### DBA Domain Pages

Each domain page follows a consistent layout:

- Page title + last refresh time
- Time window selector (synced with sidebar, overridable per-page)
- Status summary bar — count of CRITICAL/WARNING/CAUTION/OK items
- Data tables with conditional row coloring based on severity
- Expandable detail rows for long fields (query preview, error messages)

**Page content:**

| Page | Queries Used | Key Display |
|---|---|---|
| **Cluster** | node_status, replica_consistency, replication_lag, fetch_queue, zookeeper_health | Node list with uptime/version, replica health table with delay indicators, ZK session status |
| **Queries** | running_now, slow_queries, memory_heavy, full_table_scans, top_query_patterns | Live processes table (no time filter), then tabbed sub-sections for slow/heavy/scans/patterns |
| **Disk** | free_space, table_sizes, parts_health, broken_parts, detached_parts, ttl_progress | Disk usage bars per node, then tables for sizes/parts/TTL grouped by severity |
| **Merges** | active_merges, mutations, queue_depth | Active merges table, stuck mutations highlighted, queue backlog per table |
| **Connections** | session_stats | Connection counts vs limits, protocol breakdown |
| **Threads** | background_tasks, thread_pool_usage, distributed_sends | Pool utilization gauges, active background work list, distributed send queue |
| **System Metrics** | current_metrics, events_summary | Key metrics as metric cards (MemoryTracking, merge pool), full searchable table |
| **Inserts** | insert_rates, async_inserts | Insert volume per table bar chart, async queue status |
| **Dictionaries** | status, memory_usage | Dictionary health table, memory per dictionary |

**Row coloring convention:**
- Red background: CRITICAL
- Amber background: WARNING
- Yellow background: CAUTION
- No highlight: OK

### User Dashboard (`pages/11_User_Dashboard.py`)

Accessible to everyone. No login — user selects a ClickHouse username from a dropdown populated by `users/activity.sql`. Dropdown shows all users (satisfies viewing other users' activity).

**Tab 1: My Activity**
- Metric cards: query count, total duration, total memory in the time window
- Comparison vs. cluster average (e.g., "Your avg query: 1.2s — Cluster avg: 0.8s")
- Table of recent queries: time, duration, memory, tables accessed, query preview
- Source: `users/activity.sql`, post-query DataFrame filter on `user` column (SQL returns all users; Python filters to selection)

**Tab 2: My Errors**
- Error count and breakdown by exception type
- Table of recent failed queries with error messages
- Source: `users/errors.sql`, post-query DataFrame filter on `user` column

**Tab 3: Table Usage**
- Bar chart + table of most-queried tables
- Source: `users/top_tables.sql`, post-query DataFrame filter on `user` column

**Key differences from DBA pages:**
- No severity badges or alert-level language
- Friendly labels (e.g., "Heavy queries" not "CRITICAL - Memory usage exceeds threshold")
- Guidance text where relevant (e.g., "Queries reading >1M rows with few results may benefit from better filtering")

## 8. Tech Stack

```
requirements.txt:
  streamlit>=1.35.0          # UI framework
  clickhouse-connect>=0.7.0  # HTTP ClickHouse client
  pandas>=2.0.0              # DataFrame handling
  pyyaml>=6.0                # Config file parsing
```

Standard library covers the rest:
- `smtplib` + `email` — SMTP alerts
- `threading` — background alert engine
- `pathlib` — SQL file loading
- `re` — severity parsing

**Python version:** 3.10+

**Why `clickhouse-connect`:** HTTP protocol works cleanly through the load balancer (least_connection works at HTTP level), no persistent TCP connections, native pandas output, supports ClickHouse's `{param:Type}` parameterized query syntax.

## 9. Out of Scope

- Query kill capability
- Historical trend storage / persistence
- Custom SQL editor
- CSV/Excel export
- Docker / containerized deployment
- Authentication / login system within the dashboard
- Multiple cluster support
