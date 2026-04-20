# ClickHouse Monitoring Dashboard — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Streamlit dashboard that monitors a ClickHouse cluster using existing SQL queries, with role-based views (DBA + User) and SMTP email alerts.

**Architecture:** Three-layer design — data layer (config, connection, query executor), alert engine (evaluator, email sender, background thread), and UI layer (Streamlit pages). SQL files are loaded from disk and parameterized at runtime.

**Tech Stack:** Python 3.10+, Streamlit, clickhouse-connect, pandas, PyYAML

**Spec:** `docs/superpowers/specs/2026-04-20-clickhouse-dashboard-design.md`

---

## File Map

### New Files

| File | Responsibility |
|---|---|
| `config.example.yaml` | Template configuration with placeholder values |
| `requirements.txt` | Python dependencies |
| `app.py` | Streamlit entry point, sidebar, alert thread launcher |
| `src/__init__.py` | Package init |
| `src/config.py` | Load and validate `config.yaml` |
| `src/connection.py` | ClickHouse client via `clickhouse-connect` |
| `src/query_executor.py` | Load SQL files, inject parameters, execute, return DataFrames |
| `src/alerts/__init__.py` | Package init |
| `src/alerts/evaluator.py` | Parse severity from query result columns, build alert objects, cooldown logic |
| `src/alerts/email_sender.py` | Format and send SMTP emails |
| `src/ui/__init__.py` | Package init |
| `src/ui/components.py` | Reusable widgets: status badges, metric cards, alert banner |
| `src/ui/formatters.py` | DataFrame row coloring, severity-to-color mapping |
| `pages/1_Overview.py` | Landing page: memory panel, disk panel, health grid |
| `pages/2_Cluster.py` | Node status, replication, ZK health |
| `pages/3_Queries.py` | Running, slow, heavy, scans, patterns |
| `pages/4_Disk.py` | Free space, sizes, parts, broken, detached, TTL |
| `pages/5_Merges.py` | Active merges, mutations, queue |
| `pages/6_Connections.py` | Session stats |
| `pages/7_Threads.py` | Background tasks, pools, distributed sends |
| `pages/8_System_Metrics.py` | Current metrics, events summary |
| `pages/9_Inserts.py` | Insert rates, async inserts |
| `pages/10_Dictionaries.py` | Dictionary status, memory |
| `pages/11_User_Dashboard.py` | User activity, errors, table usage |
| `tests/__init__.py` | Test package |
| `tests/test_config.py` | Config loading tests |
| `tests/test_query_executor.py` | SQL loading and parameter injection tests |
| `tests/test_evaluator.py` | Severity parsing and cooldown tests |
| `tests/test_email_sender.py` | Email formatting tests |
| `tests/test_formatters.py` | Row coloring tests |

### Moved Files

The existing SQL files at the project root (`cluster/`, `queries/`, `disk/`, etc.) will be moved into `sql/` to match the spec structure.

---

## Task 1: Project Scaffolding

**Files:**
- Create: `requirements.txt`
- Create: `config.example.yaml`
- Create: `src/__init__.py`
- Create: `src/alerts/__init__.py`
- Create: `src/ui/__init__.py`
- Create: `tests/__init__.py`
- Move: existing SQL directories into `sql/`

- [ ] **Step 1: Create requirements.txt**

```
streamlit>=1.35.0
clickhouse-connect>=0.7.0
pandas>=2.0.0
pyyaml>=6.0
pytest>=8.0.0
```

- [ ] **Step 2: Create config.example.yaml**

```yaml
clickhouse:
  host: "loadbalancer.example.com"
  port: 8123
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

- [ ] **Step 3: Move SQL directories into sql/**

```bash
mkdir -p sql
for dir in cluster queries disk merges connections threads users system_metrics inserts dictionaries; do
  mv "$dir" sql/
done
```

- [ ] **Step 4: Create package init files**

Create empty `__init__.py` files:
- `src/__init__.py`
- `src/alerts/__init__.py`
- `src/ui/__init__.py`
- `tests/__init__.py`

- [ ] **Step 5: Install dependencies**

```bash
pip install -r requirements.txt
```

- [ ] **Step 6: Commit**

```bash
git init
git add requirements.txt config.example.yaml sql/ src/ tests/
git commit -m "chore: scaffold project structure, move SQL files to sql/"
```

---

## Task 2: Config Loader

**Files:**
- Create: `tests/test_config.py`
- Create: `src/config.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_config.py
import pytest
from pathlib import Path
from src.config import load_config, ConfigError


VALID_YAML = """
clickhouse:
  host: "localhost"
  port: 8123
  user: "default"
  password: "pass"
  cluster: "test_cluster"
  connect_timeout: 10
  query_timeout: 30

lookback:
  default_hours: 6
  default_days: 7

refresh:
  auto_enabled: false
  interval_seconds: 60

alerts:
  enabled: false
  check_interval_seconds: 300
  cooldown_minutes: 30
  severity_levels:
    - CRITICAL
  smtp:
    host: "smtp.test.com"
    port: 587
    use_tls: true
    user: "test@test.com"
    password: "pass"
    from_address: "test@test.com"
    recipients:
      - "admin@test.com"

thresholds:
  disk_used_pct_warning: 75
  disk_used_pct_critical: 90
  memory_usage_warning_gb: 10
  memory_usage_critical_gb: 20
  replication_delay_warning: 60
  replication_delay_critical: 300
"""


def test_load_valid_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(VALID_YAML)
    cfg = load_config(config_file)
    assert cfg["clickhouse"]["host"] == "localhost"
    assert cfg["clickhouse"]["cluster"] == "test_cluster"
    assert cfg["lookback"]["default_hours"] == 6
    assert cfg["alerts"]["enabled"] is False
    assert cfg["thresholds"]["disk_used_pct_warning"] == 75


def test_load_config_missing_file():
    with pytest.raises(ConfigError, match="not found"):
        load_config(Path("/nonexistent/config.yaml"))


def test_load_config_missing_clickhouse_section(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("lookback:\n  default_hours: 6\n")
    with pytest.raises(ConfigError, match="clickhouse"):
        load_config(config_file)


def test_load_config_missing_required_field(tmp_path):
    config_file = tmp_path / "config.yaml"
    # Missing 'host' in clickhouse section
    config_file.write_text("""
clickhouse:
  port: 8123
  user: "default"
  password: "pass"
  cluster: "c"
  connect_timeout: 10
  query_timeout: 30
lookback:
  default_hours: 6
  default_days: 7
refresh:
  auto_enabled: false
  interval_seconds: 60
alerts:
  enabled: false
  check_interval_seconds: 300
  cooldown_minutes: 30
  severity_levels: []
  smtp:
    host: "s"
    port: 587
    use_tls: true
    user: "u"
    password: "p"
    from_address: "f"
    recipients: []
thresholds:
  disk_used_pct_warning: 75
  disk_used_pct_critical: 90
  memory_usage_warning_gb: 10
  memory_usage_critical_gb: 20
  replication_delay_warning: 60
  replication_delay_critical: 300
""")
    with pytest.raises(ConfigError, match="host"):
        load_config(config_file)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_config.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'src.config'`

- [ ] **Step 3: Implement config loader**

```python
# src/config.py
from pathlib import Path
import yaml


class ConfigError(Exception):
    pass


_REQUIRED_SECTIONS = ["clickhouse", "lookback", "refresh", "alerts", "thresholds"]
_REQUIRED_CLICKHOUSE = ["host", "port", "user", "password", "cluster", "connect_timeout", "query_timeout"]
_REQUIRED_SMTP = ["host", "port", "use_tls", "user", "password", "from_address", "recipients"]


def load_config(path: Path) -> dict:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    with open(path) as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict):
        raise ConfigError("Config file is empty or invalid YAML")

    for section in _REQUIRED_SECTIONS:
        if section not in cfg:
            raise ConfigError(f"Missing required section: {section}")

    for field in _REQUIRED_CLICKHOUSE:
        if field not in cfg["clickhouse"]:
            raise ConfigError(f"Missing required clickhouse field: {field}")

    if cfg["alerts"].get("enabled"):
        smtp = cfg["alerts"].get("smtp", {})
        for field in _REQUIRED_SMTP:
            if field not in smtp:
                raise ConfigError(f"Missing required smtp field: {field}")

    return cfg
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_config.py -v
```

Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/config.py tests/test_config.py
git commit -m "feat: add config loader with validation"
```

---

## Task 3: Query Executor

**Files:**
- Create: `tests/test_query_executor.py`
- Create: `src/query_executor.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_query_executor.py
import pytest
import pandas as pd
from pathlib import Path
from src.query_executor import load_sql, inject_parameters


def test_load_sql_reads_file(tmp_path):
    sql_dir = tmp_path / "sql" / "cluster"
    sql_dir.mkdir(parents=True)
    sql_file = sql_dir / "node_status.sql"
    sql_file.write_text("SELECT hostName() FROM system.one;")
    result = load_sql(tmp_path / "sql", "cluster", "node_status")
    assert result == "SELECT hostName() FROM system.one;"


def test_load_sql_strips_comments(tmp_path):
    sql_dir = tmp_path / "sql" / "disk"
    sql_dir.mkdir(parents=True)
    sql_file = sql_dir / "free_space.sql"
    sql_file.write_text(
        "-- free_space.sql\n"
        "-- Disk space per disk.\n"
        "SELECT name FROM system.disks;\n"
        "    -- ALERT: something\n"
        "    -- ACTION: do something\n"
    )
    result = load_sql(tmp_path / "sql", "disk", "free_space")
    assert "-- free_space.sql" not in result
    assert "-- ALERT:" not in result
    assert "SELECT name FROM system.disks;" in result


def test_load_sql_missing_file(tmp_path):
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        load_sql(sql_dir, "cluster", "nonexistent")


def test_inject_parameters_cluster():
    sql = "SELECT * FROM clusterAllReplicas({cluster:String}, system.one)"
    result = inject_parameters(sql, cluster="my_cluster", lookback_hours=6, lookback_days=1)
    assert result == "SELECT * FROM clusterAllReplicas('my_cluster', system.one)"


def test_inject_parameters_lookback():
    sql = "WHERE event_time >= now() - toIntervalHour({lookback_hours:UInt32})"
    result = inject_parameters(sql, cluster="c", lookback_hours=24, lookback_days=1)
    assert result == "WHERE event_time >= now() - toIntervalHour(24)"


def test_inject_parameters_lookback_days():
    sql = "WHERE event_time >= now() - toIntervalDay({lookback_days:UInt32})"
    result = inject_parameters(sql, cluster="c", lookback_hours=48, lookback_days=2)
    assert result == "WHERE event_time >= now() - toIntervalDay(2)"


def test_inject_parameters_skips_missing():
    sql = "SELECT * FROM system.processes"
    result = inject_parameters(sql, cluster="c", lookback_hours=6, lookback_days=1)
    assert result == "SELECT * FROM system.processes"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_query_executor.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'src.query_executor'`

- [ ] **Step 3: Implement query executor**

```python
# src/query_executor.py
import re
from pathlib import Path
import pandas as pd
import clickhouse_connect


def load_sql(sql_dir: Path, domain: str, name: str) -> str:
    sql_file = sql_dir / domain / f"{name}.sql"
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    text = sql_file.read_text()
    # Strip standalone comment lines (lines that are only comments)
    # Keep inline SQL — only remove lines where the entire line is a comment
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def inject_parameters(
    sql: str,
    cluster: str,
    lookback_hours: int,
    lookback_days: int,
) -> str:
    replacements = {
        "{cluster:String}": f"'{cluster}'",
        "{lookback_hours:UInt32}": str(lookback_hours),
        "{lookback_days:UInt32}": str(lookback_days),
    }
    result = sql
    for placeholder, value in replacements.items():
        result = result.replace(placeholder, value)
    return result


def execute_query(
    client: clickhouse_connect.driver.Client,
    sql_dir: Path,
    domain: str,
    name: str,
    cluster: str,
    lookback_hours: int,
    lookback_days: int,
) -> pd.DataFrame:
    sql = load_sql(sql_dir, domain, name)
    sql = inject_parameters(sql, cluster, lookback_hours, lookback_days)
    try:
        result = client.query_df(sql)
        return result
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_query_executor.py -v
```

Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/query_executor.py tests/test_query_executor.py
git commit -m "feat: add query executor with SQL loading and parameter injection"
```

---

## Task 4: Connection Manager

**Files:**
- Create: `src/connection.py`

- [ ] **Step 1: Implement connection manager**

No TDD here — this is a thin wrapper around `clickhouse-connect` that requires a live server to test.

```python
# src/connection.py
import clickhouse_connect
from src.config import load_config
from pathlib import Path


def create_client(config: dict) -> clickhouse_connect.driver.Client:
    ch = config["clickhouse"]
    client = clickhouse_connect.get_client(
        host=ch["host"],
        port=ch["port"],
        username=ch["user"],
        password=ch["password"],
        connect_timeout=ch["connect_timeout"],
        query_limit=0,
        send_receive_timeout=ch["query_timeout"],
    )
    return client


def check_connection(client: clickhouse_connect.driver.Client) -> bool:
    try:
        result = client.query("SELECT 1")
        return result.result_rows == [(1,)]
    except Exception:
        return False
```

- [ ] **Step 2: Commit**

```bash
git add src/connection.py
git commit -m "feat: add ClickHouse connection manager"
```

---

## Task 5: Alert Evaluator

**Files:**
- Create: `tests/test_evaluator.py`
- Create: `src/alerts/evaluator.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_evaluator.py
import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.alerts.evaluator import extract_severity, Alert, AlertLog


def test_extract_severity_critical():
    assert extract_severity("CRITICAL - Replica is read-only (ZK issue or disk full)") == "CRITICAL"


def test_extract_severity_warning():
    assert extract_severity("WARNING  - Some replicas are offline") == "WARNING"


def test_extract_severity_caution():
    assert extract_severity("CAUTION  - Replica is >1 min behind leader") == "CAUTION"


def test_extract_severity_ok():
    assert extract_severity("OK       - Replica is healthy") == "OK"


def test_extract_severity_none():
    assert extract_severity("some random text") is None


def test_extract_severity_handles_none_value():
    assert extract_severity(None) is None


def test_alert_log_add_and_get():
    log = AlertLog(max_size=5)
    alert = Alert(
        severity="CRITICAL",
        domain="disk",
        message="Disk usage >90%",
        timestamp=datetime.now(),
        key="hostname=node1",
        details={"hostname": "node1", "used_pct": 95.0},
    )
    log.add(alert)
    assert len(log.get_all()) == 1
    assert log.get_all()[0].severity == "CRITICAL"


def test_alert_log_max_size():
    log = AlertLog(max_size=3)
    for i in range(5):
        log.add(Alert(
            severity="WARNING",
            domain="disk",
            message=f"Alert {i}",
            timestamp=datetime.now(),
            key=f"key-{i}",
            details={},
        ))
    assert len(log.get_all()) == 3


def test_alert_log_counts():
    log = AlertLog(max_size=100)
    log.add(Alert("CRITICAL", "disk", "msg1", datetime.now(), "k1", {}))
    log.add(Alert("CRITICAL", "cluster", "msg2", datetime.now(), "k2", {}))
    log.add(Alert("WARNING", "queries", "msg3", datetime.now(), "k3", {}))
    counts = log.get_counts()
    assert counts["CRITICAL"] == 2
    assert counts["WARNING"] == 1


def test_cooldown_suppresses_duplicate():
    log = AlertLog(max_size=100)
    alert1 = Alert("CRITICAL", "disk", "msg", datetime.now(), "hostname=node1", {})
    assert log.should_send(alert1, cooldown_minutes=30) is True
    log.record_sent(alert1)
    alert2 = Alert("CRITICAL", "disk", "msg", datetime.now(), "hostname=node1", {})
    assert log.should_send(alert2, cooldown_minutes=30) is False


def test_cooldown_allows_after_expiry():
    log = AlertLog(max_size=100)
    alert1 = Alert("CRITICAL", "disk", "msg", datetime.now() - timedelta(minutes=31), "hostname=node1", {})
    log.record_sent(alert1)
    # Manually backdate the sent time
    log._sent_times[("disk", "CRITICAL", "hostname=node1")] = datetime.now() - timedelta(minutes=31)
    alert2 = Alert("CRITICAL", "disk", "msg", datetime.now(), "hostname=node1", {})
    assert log.should_send(alert2, cooldown_minutes=30) is True


def test_cooldown_escalation_bypasses():
    log = AlertLog(max_size=100)
    alert1 = Alert("WARNING", "disk", "msg", datetime.now(), "hostname=node1", {})
    log.record_sent(alert1)
    # Same key but higher severity — should bypass cooldown
    alert2 = Alert("CRITICAL", "disk", "msg", datetime.now(), "hostname=node1", {})
    assert log.should_send(alert2, cooldown_minutes=30) is True


def test_evaluate_dataframe():
    from src.alerts.evaluator import evaluate_dataframe
    df = pd.DataFrame({
        "hostname": ["node1", "node2", "node3"],
        "used_pct": [95.0, 60.0, 85.0],
        "replica_status": [
            "CRITICAL - Disk full",
            "OK       - Healthy",
            "WARNING  - Getting full",
        ],
    })
    alerts = evaluate_dataframe(df, domain="disk", status_column="replica_status", key_column="hostname")
    assert len(alerts) == 2
    severities = {a.severity for a in alerts}
    assert severities == {"CRITICAL", "WARNING"}
    assert alerts[0].key == "hostname=node1"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_evaluator.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'src.alerts.evaluator'`

- [ ] **Step 3: Implement alert evaluator**

```python
# src/alerts/evaluator.py
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock
from typing import Any
import pandas as pd


_SEVERITY_PATTERN = re.compile(r"^(CRITICAL|WARNING|CAUTION|OK)\s")
_SEVERITY_RANK = {"CRITICAL": 3, "WARNING": 2, "CAUTION": 1, "OK": 0}


def extract_severity(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    match = _SEVERITY_PATTERN.match(text)
    return match.group(1) if match else None


@dataclass
class Alert:
    severity: str
    domain: str
    message: str
    timestamp: datetime
    key: str
    details: dict


class AlertLog:
    def __init__(self, max_size: int = 100):
        self._alerts: list[Alert] = []
        self._max_size = max_size
        self._sent_times: dict[tuple[str, str, str], datetime] = {}
        self._lock = Lock()

    def add(self, alert: Alert) -> None:
        with self._lock:
            self._alerts.append(alert)
            if len(self._alerts) > self._max_size:
                self._alerts = self._alerts[-self._max_size:]

    def get_all(self) -> list[Alert]:
        with self._lock:
            return list(self._alerts)

    def get_counts(self) -> dict[str, int]:
        with self._lock:
            counts: dict[str, int] = {}
            for alert in self._alerts:
                counts[alert.severity] = counts.get(alert.severity, 0) + 1
            return counts

    def should_send(self, alert: Alert, cooldown_minutes: int) -> bool:
        with self._lock:
            cooldown_key = (alert.domain, alert.severity, alert.key)
            last_sent = self._sent_times.get(cooldown_key)
            if last_sent is not None:
                if datetime.now() - last_sent < timedelta(minutes=cooldown_minutes):
                    # Check if this is an escalation (higher severity for same domain+key)
                    for prev_sev in _SEVERITY_RANK:
                        if _SEVERITY_RANK.get(prev_sev, 0) < _SEVERITY_RANK.get(alert.severity, 0):
                            prev_key = (alert.domain, prev_sev, alert.key)
                            if prev_key in self._sent_times:
                                return True
                    return False
            return True

    def record_sent(self, alert: Alert) -> None:
        with self._lock:
            cooldown_key = (alert.domain, alert.severity, alert.key)
            self._sent_times[cooldown_key] = datetime.now()


def evaluate_dataframe(
    df: pd.DataFrame,
    domain: str,
    status_column: str,
    key_column: str,
) -> list[Alert]:
    alerts = []
    if df.empty or status_column not in df.columns:
        return alerts
    for _, row in df.iterrows():
        severity = extract_severity(row.get(status_column))
        if severity and severity in ("CRITICAL", "WARNING"):
            key_value = f"{key_column}={row.get(key_column, 'unknown')}"
            alerts.append(Alert(
                severity=severity,
                domain=domain,
                message=str(row[status_column]),
                timestamp=datetime.now(),
                key=key_value,
                details=row.to_dict(),
            ))
    return alerts
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_evaluator.py -v
```

Expected: 12 passed

- [ ] **Step 5: Commit**

```bash
git add src/alerts/evaluator.py tests/test_evaluator.py
git commit -m "feat: add alert evaluator with severity parsing and cooldown logic"
```

---

## Task 6: Email Sender

**Files:**
- Create: `tests/test_email_sender.py`
- Create: `src/alerts/email_sender.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_email_sender.py
import pytest
from datetime import datetime
from src.alerts.email_sender import format_alert_email
from src.alerts.evaluator import Alert


def test_format_single_alert():
    alerts = [
        Alert(
            severity="CRITICAL",
            domain="disk",
            message="CRITICAL - Disk usage >90%",
            timestamp=datetime(2026, 4, 20, 10, 30),
            key="hostname=node1",
            details={"hostname": "node1", "used_pct": 95.0},
        ),
    ]
    subject, body = format_alert_email(alerts, cluster_name="ecredal_cluster")
    assert "CRITICAL" in subject
    assert "ecredal_cluster" in subject
    assert "disk" in body.lower()
    assert "node1" in body
    assert "95.0" in body


def test_format_multiple_alerts():
    alerts = [
        Alert("CRITICAL", "disk", "CRITICAL - Disk full", datetime.now(), "hostname=node1", {"hostname": "node1"}),
        Alert("WARNING", "cluster", "WARNING - Replica behind", datetime.now(), "hostname=node2", {"hostname": "node2"}),
    ]
    subject, body = format_alert_email(alerts, cluster_name="ecredal_cluster")
    assert "2 alert" in subject.lower()
    assert "disk" in body.lower()
    assert "cluster" in body.lower()


def test_format_empty_alerts():
    subject, body = format_alert_email([], cluster_name="test")
    assert subject == ""
    assert body == ""
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_email_sender.py -v
```

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement email sender**

```python
# src/alerts/email_sender.py
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from src.alerts.evaluator import Alert


def format_alert_email(alerts: list[Alert], cluster_name: str) -> tuple[str, str]:
    if not alerts:
        return "", ""

    if len(alerts) == 1:
        a = alerts[0]
        subject = f"[{a.severity}] ClickHouse {cluster_name} — {a.domain}: {a.message}"
    else:
        worst = max(alerts, key=lambda a: {"CRITICAL": 2, "WARNING": 1}.get(a.severity, 0))
        subject = f"[{worst.severity}] ClickHouse {cluster_name} — {len(alerts)} alerts"

    lines = [
        f"ClickHouse Monitor Alert — Cluster: {cluster_name}",
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total alerts: {len(alerts)}",
        "",
        "=" * 60,
    ]

    for alert in alerts:
        lines.append("")
        lines.append(f"[{alert.severity}] Domain: {alert.domain}")
        lines.append(f"Message: {alert.message}")
        lines.append(f"Key: {alert.key}")
        lines.append("")
        lines.append("Details:")
        for k, v in alert.details.items():
            lines.append(f"  {k}: {v}")
        lines.append("-" * 40)

    body = "\n".join(lines)
    return subject, body


def send_alert_email(
    alerts: list[Alert],
    cluster_name: str,
    smtp_config: dict,
) -> None:
    subject, body = format_alert_email(alerts, cluster_name)
    if not subject:
        return

    msg = MIMEMultipart()
    msg["From"] = smtp_config["from_address"]
    msg["To"] = ", ".join(smtp_config["recipients"])
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP(smtp_config["host"], smtp_config["port"]) as server:
        if smtp_config.get("use_tls"):
            server.starttls()
        server.login(smtp_config["user"], smtp_config["password"])
        server.sendmail(
            smtp_config["from_address"],
            smtp_config["recipients"],
            msg.as_string(),
        )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_email_sender.py -v
```

Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/alerts/email_sender.py tests/test_email_sender.py
git commit -m "feat: add SMTP email sender with alert formatting"
```

---

## Task 7: UI Formatters

**Files:**
- Create: `tests/test_formatters.py`
- Create: `src/ui/formatters.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_formatters.py
import pytest
import pandas as pd
from src.ui.formatters import severity_color, colorize_row


def test_severity_color_critical():
    assert severity_color("CRITICAL") == "background-color: #ffcccc"


def test_severity_color_warning():
    assert severity_color("WARNING") == "background-color: #ffe0b2"


def test_severity_color_caution():
    assert severity_color("CAUTION") == "background-color: #fff9c4"


def test_severity_color_ok():
    assert severity_color("OK") == ""


def test_severity_color_unknown():
    assert severity_color("UNKNOWN") == ""


def test_colorize_row_finds_severity():
    row = pd.Series({
        "hostname": "node1",
        "replica_status": "CRITICAL - Replica is read-only",
    })
    result = colorize_row(row, status_column="replica_status")
    assert all(v == "background-color: #ffcccc" for v in result)


def test_colorize_row_ok_no_color():
    row = pd.Series({
        "hostname": "node1",
        "replica_status": "OK       - Replica is healthy",
    })
    result = colorize_row(row, status_column="replica_status")
    assert all(v == "" for v in result)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_formatters.py -v
```

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement formatters**

```python
# src/ui/formatters.py
import pandas as pd
from src.alerts.evaluator import extract_severity


_SEVERITY_COLORS = {
    "CRITICAL": "background-color: #ffcccc",
    "WARNING": "background-color: #ffe0b2",
    "CAUTION": "background-color: #fff9c4",
}


def severity_color(severity: str) -> str:
    return _SEVERITY_COLORS.get(severity, "")


def colorize_row(row: pd.Series, status_column: str) -> list[str]:
    value = row.get(status_column, "")
    severity = extract_severity(value)
    color = severity_color(severity) if severity else ""
    return [color] * len(row)


def style_dataframe(df: pd.DataFrame, status_column: str) -> pd.io.formats.style.Styler:
    if status_column not in df.columns:
        return df.style
    return df.style.apply(colorize_row, axis=1, status_column=status_column)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_formatters.py -v
```

Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/ui/formatters.py tests/test_formatters.py
git commit -m "feat: add DataFrame row coloring by severity"
```

---

## Task 8: UI Components

**Files:**
- Create: `src/ui/components.py`

- [ ] **Step 1: Implement shared UI components**

No TDD — these are Streamlit widget wrappers that require a running Streamlit app.

```python
# src/ui/components.py
import streamlit as st
import pandas as pd
from math import ceil
from src.alerts.evaluator import AlertLog, extract_severity


_SEVERITY_EMOJI = {
    "CRITICAL": "\U0001f534",  # red circle
    "WARNING": "\U0001f7e0",   # orange circle
    "CAUTION": "\U0001f7e1",   # yellow circle
    "OK": "\U0001f7e2",        # green circle
}


def render_sidebar(config: dict) -> dict:
    """Render sidebar controls. Returns current settings dict."""
    st.sidebar.markdown(f"**Cluster:** `{config['clickhouse']['cluster']}`")

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Time Window**")
    preset = st.sidebar.radio(
        "Lookback",
        options=["1h", "6h", "24h", "7d", "Custom"],
        index=1,
        horizontal=True,
        label_visibility="collapsed",
    )
    hours_map = {"1h": 1, "6h": 6, "24h": 24, "7d": 168}
    if preset == "Custom":
        lookback_hours = st.sidebar.number_input("Hours", min_value=1, max_value=720, value=6)
    else:
        lookback_hours = hours_map[preset]

    lookback_days = max(1, ceil(lookback_hours / 24))

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Refresh**")
    auto_refresh = st.sidebar.toggle("Auto-refresh", value=config["refresh"]["auto_enabled"])
    refresh_interval = config["refresh"]["interval_seconds"]
    if auto_refresh:
        refresh_interval = st.sidebar.select_slider(
            "Interval (s)",
            options=[30, 60, 300],
            value=config["refresh"]["interval_seconds"],
        )

    if st.sidebar.button("Refresh Now", use_container_width=True):
        st.cache_data.clear()

    return {
        "lookback_hours": lookback_hours,
        "lookback_days": lookback_days,
        "auto_refresh": auto_refresh,
        "refresh_interval": refresh_interval,
    }


def render_alert_banner(alert_log: AlertLog) -> None:
    """Render the alert banner at the top of every page."""
    counts = alert_log.get_counts()
    critical = counts.get("CRITICAL", 0)
    warning = counts.get("WARNING", 0)
    if critical == 0 and warning == 0:
        return

    parts = []
    if critical > 0:
        parts.append(f"\U0001f534 **{critical} CRITICAL**")
    if warning > 0:
        parts.append(f"\U0001f7e0 **{warning} WARNING**")

    banner_text = " \u00b7 ".join(parts)

    if "alert_banner_dismissed" not in st.session_state:
        st.session_state.alert_banner_dismissed = False

    if not st.session_state.alert_banner_dismissed:
        col1, col2 = st.columns([9, 1])
        with col1:
            st.error(banner_text)
        with col2:
            if st.button("\u2715", key="dismiss_alert_banner"):
                st.session_state.alert_banner_dismissed = True
                st.rerun()


def metric_card(label: str, value: str, severity: str = "OK") -> None:
    """Render a single metric card with severity coloring."""
    emoji = _SEVERITY_EMOJI.get(severity, "")
    st.metric(label=f"{emoji} {label}", value=value)


def status_badge(severity: str) -> str:
    """Return a severity badge as markdown string."""
    emoji = _SEVERITY_EMOJI.get(severity, "\u26aa")
    return f"{emoji} {severity}"


def health_card(domain: str, worst_severity: str, summary: str) -> None:
    """Render a health summary card for the overview grid."""
    emoji = _SEVERITY_EMOJI.get(worst_severity, "\u26aa")
    colors = {
        "CRITICAL": "#ffcccc",
        "WARNING": "#ffe0b2",
        "CAUTION": "#fff9c4",
        "OK": "#c8e6c9",
    }
    bg = colors.get(worst_severity, "#f5f5f5")
    st.markdown(
        f"""
        <div style="background-color:{bg}; padding:12px; border-radius:8px; margin-bottom:8px;">
            <strong>{emoji} {domain}</strong><br/>
            <span style="font-size:0.85em;">{summary}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_status_summary(df: pd.DataFrame, status_column: str) -> None:
    """Render a count bar of CRITICAL/WARNING/CAUTION/OK."""
    if df.empty or status_column not in df.columns:
        return
    counts = {"CRITICAL": 0, "WARNING": 0, "CAUTION": 0, "OK": 0}
    for val in df[status_column]:
        severity = extract_severity(val)
        if severity and severity in counts:
            counts[severity] += 1

    cols = st.columns(4)
    for col, (sev, count) in zip(cols, counts.items()):
        emoji = _SEVERITY_EMOJI.get(sev, "")
        col.markdown(f"{emoji} **{sev}:** {count}")


def render_domain_page(
    title: str,
    queries: list[tuple[str, pd.DataFrame, str | None]],
) -> None:
    """
    Render a standard domain page.

    queries: list of (section_title, dataframe, status_column_or_None)
    """
    from src.ui.formatters import style_dataframe
    st.markdown(f"## {title}")

    for section_title, df, status_col in queries:
        st.markdown(f"### {section_title}")
        if df.empty:
            st.info("No data")
            continue
        if "error" in df.columns and len(df.columns) == 1:
            st.error(f"Query error: {df['error'].iloc[0]}")
            continue

        if status_col and status_col in df.columns:
            render_status_summary(df, status_col)
            st.dataframe(style_dataframe(df, status_col), use_container_width=True, hide_index=True)
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
```

- [ ] **Step 2: Commit**

```bash
git add src/ui/components.py
git commit -m "feat: add shared UI components (sidebar, alert banner, metric cards, health cards)"
```

---

## Task 9: Streamlit Entry Point (app.py)

**Files:**
- Create: `app.py`

- [ ] **Step 1: Implement app.py**

```python
# app.py
import streamlit as st
import threading
import time
from pathlib import Path
from math import ceil

from src.config import load_config, ConfigError
from src.connection import create_client, check_connection
from src.query_executor import execute_query
from src.alerts.evaluator import AlertLog, evaluate_dataframe
from src.alerts.email_sender import send_alert_email

st.set_page_config(
    page_title="ClickHouse Monitor",
    page_icon="\U0001f4ca",
    layout="wide",
)

# --- Load config ---
CONFIG_PATH = Path("config.yaml")
try:
    config = load_config(CONFIG_PATH)
except ConfigError as e:
    st.error(f"Configuration error: {e}")
    st.info("Copy `config.example.yaml` to `config.yaml` and fill in your settings.")
    st.stop()

# --- Store in session state ---
if "config" not in st.session_state:
    st.session_state.config = config

if "alert_log" not in st.session_state:
    st.session_state.alert_log = AlertLog(max_size=100)

# --- ClickHouse connection ---
if "ch_client" not in st.session_state:
    try:
        st.session_state.ch_client = create_client(config)
        if not check_connection(st.session_state.ch_client):
            st.error("Failed to connect to ClickHouse. Check config.yaml.")
            st.stop()
    except Exception as e:
        st.error(f"Connection error: {e}")
        st.stop()

# --- Alert background thread ---
_ALERT_QUERIES = [
    ("disk", "free_space", "used_pct", None),
    ("cluster", "replica_consistency", "replica_status", "hostname"),
    ("cluster", "zookeeper_health", "zk_status", "hostname"),
    ("disk", "broken_parts", None, None),
    ("merges", "mutations", None, None),
    ("dictionaries", "status", "dict_status", "hostname"),
]


def _alert_loop(config: dict, alert_log: AlertLog) -> None:
    """Background alert evaluation loop."""
    sql_dir = Path("sql")
    interval = config["alerts"]["check_interval_seconds"]
    cooldown = config["alerts"]["cooldown_minutes"]
    cluster = config["clickhouse"]["cluster"]
    severity_levels = set(config["alerts"]["severity_levels"])

    client = create_client(config)

    while True:
        try:
            pending_alerts = []
            for domain, query_name, status_col, key_col in _ALERT_QUERIES:
                df = execute_query(
                    client, sql_dir, domain, query_name,
                    cluster=cluster,
                    lookback_hours=config["lookback"]["default_hours"],
                    lookback_days=config["lookback"]["default_days"],
                )
                if status_col and key_col and status_col in df.columns:
                    alerts = evaluate_dataframe(df, domain, status_col, key_col)
                    for alert in alerts:
                        if alert.severity in severity_levels:
                            if alert_log.should_send(alert, cooldown):
                                pending_alerts.append(alert)
                            alert_log.add(alert)

            if pending_alerts and config["alerts"].get("smtp"):
                try:
                    send_alert_email(
                        pending_alerts,
                        cluster_name=cluster,
                        smtp_config=config["alerts"]["smtp"],
                    )
                    for alert in pending_alerts:
                        alert_log.record_sent(alert)
                except Exception:
                    pass  # Email failures should not crash the alert loop

        except Exception:
            pass  # Query failures should not crash the alert loop

        time.sleep(interval)


if config["alerts"]["enabled"] and "alert_thread_started" not in st.session_state:
    st.session_state.alert_thread_started = True
    thread = threading.Thread(
        target=_alert_loop,
        args=(config, st.session_state.alert_log),
        daemon=True,
    )
    thread.start()

# --- Sidebar ---
from src.ui.components import render_sidebar, render_alert_banner

settings = render_sidebar(config)
st.session_state.lookback_hours = settings["lookback_hours"]
st.session_state.lookback_days = settings["lookback_days"]

# --- Auto-refresh ---
if settings["auto_refresh"]:
    time.sleep(0.1)  # Small delay to let page render
    st.cache_data.clear()
    time.sleep(settings["refresh_interval"])
    st.rerun()

# --- Alert banner ---
render_alert_banner(st.session_state.alert_log)

# --- Home page content ---
st.title("ClickHouse Monitor")
st.markdown("Select a page from the sidebar to explore cluster health.")
```

- [ ] **Step 2: Commit**

```bash
git add app.py
git commit -m "feat: add Streamlit entry point with sidebar, alert thread, and connection setup"
```

---

## Task 10: Overview Page

**Files:**
- Create: `pages/1_Overview.py`

- [ ] **Step 1: Implement overview page**

```python
# pages/1_Overview.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.alerts.evaluator import extract_severity
from src.ui.components import render_alert_banner, health_card
from src.ui.formatters import style_dataframe

st.set_page_config(page_title="Overview — ClickHouse Monitor", layout="wide")


def _get_client():
    return st.session_state.get("ch_client")


def _get_config():
    return st.session_state.get("config", {})


def _get_lookback():
    return (
        st.session_state.get("lookback_hours", 6),
        st.session_state.get("lookback_days", 1),
    )


def _run_query(domain: str, name: str) -> pd.DataFrame:
    client = _get_client()
    config = _get_config()
    hours, days = _get_lookback()
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


def _worst_severity(df: pd.DataFrame, status_col: str) -> str:
    if df.empty or status_col not in df.columns:
        return "OK"
    rank = {"CRITICAL": 3, "WARNING": 2, "CAUTION": 1, "OK": 0}
    worst = "OK"
    for val in df[status_col]:
        sev = extract_severity(val)
        if sev and rank.get(sev, 0) > rank.get(worst, 0):
            worst = sev
    return worst


# --- Alert banner ---
alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)

st.title("Overview")

# --- Priority Panel 1: Memory ---
st.markdown("## Active Queries by Memory")
df_running = _run_query("queries", "running_now")
config = _get_config()
thresholds = config.get("thresholds", {})
mem_warn_gb = thresholds.get("memory_usage_warning_gb", 10)
mem_crit_gb = thresholds.get("memory_usage_critical_gb", 20)

if df_running.empty or ("error" in df_running.columns and len(df_running.columns) == 1):
    st.success("No running queries")
else:
    # Add severity column based on memory_usage parsing
    def _classify_memory(row):
        mem_str = str(row.get("memory", "0"))
        # Parse memory string — clickhouse returns human-readable format
        # We'll use raw memory_usage if available, otherwise estimate from readable string
        try:
            val = float(mem_str.replace(",", ""))
            gb = val / (1024**3)
        except (ValueError, TypeError):
            # Try parsing readable format like "1.23 GiB"
            gb = 0
            if "GiB" in mem_str:
                gb = float(mem_str.split()[0])
            elif "MiB" in mem_str:
                gb = float(mem_str.split()[0]) / 1024
        if gb >= mem_crit_gb:
            return "CRITICAL - Query using excessive memory"
        elif gb >= mem_warn_gb:
            return "WARNING  - Query using high memory"
        return "OK       - Normal memory usage"

    df_running["memory_status"] = df_running.apply(_classify_memory, axis=1)
    has_issues = df_running["memory_status"].apply(
        lambda x: extract_severity(x) in ("CRITICAL", "WARNING")
    ).any()

    if has_issues:
        st.dataframe(
            style_dataframe(df_running, "memory_status"),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("All queries within normal memory limits")
        with st.expander("Show running queries"):
            st.dataframe(df_running.drop(columns=["memory_status"]), use_container_width=True, hide_index=True)

# --- Priority Panel 2: Disk ---
st.markdown("## Disk Usage")
df_disk = _run_query("disk", "free_space")

if df_disk.empty or ("error" in df_disk.columns and len(df_disk.columns) == 1):
    st.info("No disk data available")
else:
    disk_warn = thresholds.get("disk_used_pct_warning", 75)
    disk_crit = thresholds.get("disk_used_pct_critical", 90)

    for _, row in df_disk.iterrows():
        hostname = row.get("hostname", "unknown")
        disk_name = row.get("disk_name", "default")
        used_pct = row.get("used_pct", 0)
        free = row.get("free", "?")
        total = row.get("total", "?")

        if used_pct >= disk_crit:
            color = "red"
        elif used_pct >= disk_warn:
            color = "orange"
        else:
            color = "green"

        label = f"{hostname} / {disk_name} — {free} free of {total}"
        st.progress(min(used_pct / 100, 1.0), text=f":{color}[{label} ({used_pct}%)]")

# --- Health Grid ---
st.markdown("## Cluster Health")

# Define domains with their key query and status column
_HEALTH_CHECKS = [
    ("Cluster", "cluster", "replica_consistency", "replica_status"),
    ("Queries", "queries", "slow_queries", None),
    ("Disk", "disk", "parts_health", "parts_assessment"),
    ("Merges", "merges", "mutations", None),
    ("Connections", "connections", "session_stats", "connection_status"),
    ("Threads", "threads", "thread_pool_usage", "pool_status"),
    ("System Metrics", "system_metrics", "current_metrics", None),
    ("Inserts", "inserts", "async_inserts", None),
    ("Dictionaries", "dictionaries", "status", "dict_status"),
]

cols = st.columns(3)
for i, (label, domain, query_name, status_col) in enumerate(_HEALTH_CHECKS):
    df = _run_query(domain, query_name)
    if status_col:
        worst = _worst_severity(df, status_col)
    else:
        worst = "OK" if (df.empty or "error" not in df.columns) else "WARNING"
    row_count = len(df) if not df.empty and "error" not in df.columns else 0
    summary = f"{row_count} items" if row_count > 0 else "No data"
    with cols[i % 3]:
        health_card(label, worst, summary)
```

- [ ] **Step 2: Commit**

```bash
git add pages/1_Overview.py
git commit -m "feat: add overview page with memory panel, disk bars, and health grid"
```

---

## Task 11: Cluster Page

**Files:**
- Create: `pages/2_Cluster.py`

- [ ] **Step 1: Implement cluster page**

```python
# pages/2_Cluster.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner, render_domain_page

st.set_page_config(page_title="Cluster — ClickHouse Monitor", layout="wide")

alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = st.session_state.get("config", {})
    hours = st.session_state.get("lookback_hours", 6)
    days = st.session_state.get("lookback_days", 1)
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


render_domain_page("Cluster", [
    ("Node Status", _run("cluster", "node_status"), None),
    ("Replica Consistency", _run("cluster", "replica_consistency"), "replica_status"),
    ("Replication Lag", _run("cluster", "replication_lag"), None),
    ("Fetch Queue", _run("cluster", "fetch_queue"), "fetch_status"),
    ("ZooKeeper Health", _run("cluster", "zookeeper_health"), "zk_status"),
])
```

- [ ] **Step 2: Commit**

```bash
git add pages/2_Cluster.py
git commit -m "feat: add cluster monitoring page"
```

---

## Task 12: Queries Page

**Files:**
- Create: `pages/3_Queries.py`

- [ ] **Step 1: Implement queries page**

```python
# pages/3_Queries.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner, render_status_summary
from src.ui.formatters import style_dataframe

st.set_page_config(page_title="Queries — ClickHouse Monitor", layout="wide")

alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = st.session_state.get("config", {})
    hours = st.session_state.get("lookback_hours", 6)
    days = st.session_state.get("lookback_days", 1)
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


st.markdown("## Queries")

# Running Now — always shown first, no time filter
st.markdown("### Running Now")
df_running = _run("queries", "running_now")
if df_running.empty:
    st.info("No queries currently running")
else:
    st.dataframe(df_running, use_container_width=True, hide_index=True)

# Tabbed sections for historical queries
tab_slow, tab_memory, tab_scans, tab_patterns = st.tabs([
    "Slow Queries", "Memory Heavy", "Full Table Scans", "Top Patterns"
])

with tab_slow:
    df = _run("queries", "slow_queries")
    if df.empty:
        st.info("No slow queries in this window")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

with tab_memory:
    df = _run("queries", "memory_heavy")
    if df.empty:
        st.info("No memory-heavy queries in this window")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

with tab_scans:
    df = _run("queries", "full_table_scans")
    if df.empty:
        st.info("No full table scans detected")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

with tab_patterns:
    df = _run("queries", "top_query_patterns")
    if df.empty:
        st.info("No query patterns in this window")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)
```

- [ ] **Step 2: Commit**

```bash
git add pages/3_Queries.py
git commit -m "feat: add queries monitoring page with tabbed sections"
```

---

## Task 13: Disk Page

**Files:**
- Create: `pages/4_Disk.py`

- [ ] **Step 1: Implement disk page**

```python
# pages/4_Disk.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner, render_domain_page

st.set_page_config(page_title="Disk — ClickHouse Monitor", layout="wide")

alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = st.session_state.get("config", {})
    hours = st.session_state.get("lookback_hours", 6)
    days = st.session_state.get("lookback_days", 1)
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


render_domain_page("Disk", [
    ("Free Space", _run("disk", "free_space"), None),
    ("Table Sizes", _run("disk", "table_sizes"), None),
    ("Parts Health", _run("disk", "parts_health"), "parts_assessment"),
    ("Broken Parts", _run("disk", "broken_parts"), None),
    ("Detached Parts", _run("disk", "detached_parts"), "status"),
    ("TTL Progress", _run("disk", "ttl_progress"), "ttl_status"),
])
```

- [ ] **Step 2: Commit**

```bash
git add pages/4_Disk.py
git commit -m "feat: add disk monitoring page"
```

---

## Task 14: Merges Page

**Files:**
- Create: `pages/5_Merges.py`

- [ ] **Step 1: Implement merges page**

```python
# pages/5_Merges.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner, render_domain_page

st.set_page_config(page_title="Merges — ClickHouse Monitor", layout="wide")

alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = st.session_state.get("config", {})
    hours = st.session_state.get("lookback_hours", 6)
    days = st.session_state.get("lookback_days", 1)
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


render_domain_page("Merges", [
    ("Active Merges", _run("merges", "active_merges"), None),
    ("Mutations", _run("merges", "mutations"), None),
    ("Queue Depth", _run("merges", "queue_depth"), None),
])
```

- [ ] **Step 2: Commit**

```bash
git add pages/5_Merges.py
git commit -m "feat: add merges monitoring page"
```

---

## Task 15: Connections Page

**Files:**
- Create: `pages/6_Connections.py`

- [ ] **Step 1: Implement connections page**

```python
# pages/6_Connections.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner, render_domain_page

st.set_page_config(page_title="Connections — ClickHouse Monitor", layout="wide")

alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = st.session_state.get("config", {})
    hours = st.session_state.get("lookback_hours", 6)
    days = st.session_state.get("lookback_days", 1)
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


render_domain_page("Connections", [
    ("Session Stats", _run("connections", "session_stats"), "connection_status"),
])
```

- [ ] **Step 2: Commit**

```bash
git add pages/6_Connections.py
git commit -m "feat: add connections monitoring page"
```

---

## Task 16: Threads Page

**Files:**
- Create: `pages/7_Threads.py`

- [ ] **Step 1: Implement threads page**

```python
# pages/7_Threads.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner, render_domain_page

st.set_page_config(page_title="Threads — ClickHouse Monitor", layout="wide")

alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = st.session_state.get("config", {})
    hours = st.session_state.get("lookback_hours", 6)
    days = st.session_state.get("lookback_days", 1)
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


render_domain_page("Threads", [
    ("Background Tasks", _run("threads", "background_tasks"), None),
    ("Thread Pool Usage", _run("threads", "thread_pool_usage"), "pool_status"),
    ("Distributed Sends", _run("threads", "distributed_sends"), "send_status"),
])
```

- [ ] **Step 2: Commit**

```bash
git add pages/7_Threads.py
git commit -m "feat: add threads monitoring page"
```

---

## Task 17: System Metrics Page

**Files:**
- Create: `pages/8_System_Metrics.py`

- [ ] **Step 1: Implement system metrics page**

```python
# pages/8_System_Metrics.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner, metric_card

st.set_page_config(page_title="System Metrics — ClickHouse Monitor", layout="wide")

alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = st.session_state.get("config", {})
    hours = st.session_state.get("lookback_hours", 6)
    days = st.session_state.get("lookback_days", 1)
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


st.markdown("## System Metrics")

# Current metrics as cards
st.markdown("### Current Metrics")
df_current = _run("system_metrics", "current_metrics")
if not df_current.empty and "error" not in df_current.columns:
    # Key metrics as metric cards in a grid
    key_metrics = ["MemoryTracking", "Query", "BackgroundMergesAndMutationsPoolTask"]
    card_cols = st.columns(len(key_metrics))
    for col, metric_name in zip(card_cols, key_metrics):
        row = df_current[df_current["metric"] == metric_name]
        if not row.empty:
            with col:
                val = row.iloc[0]["value"]
                if metric_name == "MemoryTracking":
                    # Convert bytes to human-readable
                    gb = val / (1024**3)
                    display_val = f"{gb:.2f} GiB"
                else:
                    display_val = str(int(val))
                metric_card(metric_name, display_val)

    # Full table
    with st.expander("All current metrics"):
        st.dataframe(df_current, use_container_width=True, hide_index=True)
else:
    st.info("No metrics data available")

# Events summary
st.markdown("### Events Summary (since restart)")
df_events = _run("system_metrics", "events_summary")
if not df_events.empty and "error" not in df_events.columns:
    st.dataframe(df_events, use_container_width=True, hide_index=True)
else:
    st.info("No events data available")
```

- [ ] **Step 2: Commit**

```bash
git add pages/8_System_Metrics.py
git commit -m "feat: add system metrics page with metric cards"
```

---

## Task 18: Inserts Page

**Files:**
- Create: `pages/9_Inserts.py`

- [ ] **Step 1: Implement inserts page**

```python
# pages/9_Inserts.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner, render_domain_page

st.set_page_config(page_title="Inserts — ClickHouse Monitor", layout="wide")

alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = st.session_state.get("config", {})
    hours = st.session_state.get("lookback_hours", 6)
    days = st.session_state.get("lookback_days", 1)
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


st.markdown("## Inserts")

# Insert rates as bar chart
st.markdown("### Insert Rates")
df_rates = _run("inserts", "insert_rates")
if not df_rates.empty and "error" not in df_rates.columns:
    st.bar_chart(df_rates.set_index("table_name")["insert_count"])
    with st.expander("Details"):
        st.dataframe(df_rates, use_container_width=True, hide_index=True)
else:
    st.info("No insert data in this window")

# Async inserts
st.markdown("### Async Inserts Queue")
df_async = _run("inserts", "async_inserts")
if not df_async.empty and "error" not in df_async.columns:
    st.dataframe(df_async, use_container_width=True, hide_index=True)
else:
    st.info("No pending async inserts")
```

- [ ] **Step 2: Commit**

```bash
git add pages/9_Inserts.py
git commit -m "feat: add inserts monitoring page with bar chart"
```

---

## Task 19: Dictionaries Page

**Files:**
- Create: `pages/10_Dictionaries.py`

- [ ] **Step 1: Implement dictionaries page**

```python
# pages/10_Dictionaries.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner, render_domain_page

st.set_page_config(page_title="Dictionaries — ClickHouse Monitor", layout="wide")

alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = st.session_state.get("config", {})
    hours = st.session_state.get("lookback_hours", 6)
    days = st.session_state.get("lookback_days", 1)
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


render_domain_page("Dictionaries", [
    ("Dictionary Status", _run("dictionaries", "status"), "dict_status"),
    ("Memory Usage", _run("dictionaries", "memory_usage"), "memory_status"),
])
```

- [ ] **Step 2: Commit**

```bash
git add pages/10_Dictionaries.py
git commit -m "feat: add dictionaries monitoring page"
```

---

## Task 20: User Dashboard

**Files:**
- Create: `pages/11_User_Dashboard.py`

- [ ] **Step 1: Implement user dashboard**

```python
# pages/11_User_Dashboard.py
import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner

st.set_page_config(page_title="User Dashboard — ClickHouse Monitor", layout="wide")

alert_log = st.session_state.get("alert_log")
if alert_log:
    render_alert_banner(alert_log)


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = st.session_state.get("config", {})
    hours = st.session_state.get("lookback_hours", 6)
    days = st.session_state.get("lookback_days", 1)
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=hours, lookback_days=days,
    )


st.markdown("## User Dashboard")

# Load all user activity to populate the dropdown
df_activity_all = _run("users", "activity")
if df_activity_all.empty or "error" in df_activity_all.columns:
    st.warning("Could not load user data. Check ClickHouse connection.")
    st.stop()

all_users = sorted(df_activity_all["user"].unique().tolist())
selected_user = st.selectbox("Select user", options=all_users)

# --- Tabs ---
tab_activity, tab_errors, tab_tables = st.tabs(["Activity", "Errors", "Table Usage"])

# --- Tab 1: Activity ---
with tab_activity:
    user_row = df_activity_all[df_activity_all["user"] == selected_user]
    if user_row.empty:
        st.info(f"No activity for {selected_user} in this window")
    else:
        row = user_row.iloc[0]

        # Metric cards
        col1, col2, col3 = st.columns(3)
        col1.metric("Query Count", row.get("query_count", 0))
        col2.metric("Total Duration", row.get("total_duration", "0s"))
        col3.metric("Avg Duration", row.get("avg_duration", "0s"))

        # Comparison with cluster average
        total_queries = df_activity_all["query_count"].sum()
        user_queries = row.get("query_count", 0)
        if total_queries > 0:
            pct = round(user_queries / total_queries * 100, 1)
            st.caption(f"{selected_user} accounts for **{pct}%** of all queries in this window")

        # Full activity row
        st.markdown("**Details**")
        st.dataframe(user_row, use_container_width=True, hide_index=True)

# --- Tab 2: Errors ---
with tab_errors:
    df_errors_all = _run("users", "errors")
    if df_errors_all.empty or "error" in df_errors_all.columns:
        st.info(f"No errors for any user in this window")
    else:
        df_user_errors = df_errors_all[df_errors_all["user"] == selected_user]
        if df_user_errors.empty:
            st.success(f"No errors for {selected_user} in this window")
        else:
            row = df_user_errors.iloc[0]
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Errors", row.get("error_count", 0))
            col2.metric("Before Start", row.get("errors_before_start", 0))
            col3.metric("While Processing", row.get("errors_while_processing", 0))

            st.markdown("**Last Error**")
            st.code(str(row.get("last_exception_message", "N/A")))

# --- Tab 3: Table Usage ---
with tab_tables:
    df_tables_all = _run("users", "top_tables")
    if df_tables_all.empty or "error" in df_tables_all.columns:
        st.info("No table usage data in this window")
    else:
        df_user_tables = df_tables_all[df_tables_all["user"] == selected_user]
        if df_user_tables.empty:
            st.info(f"No table usage for {selected_user} in this window")
        else:
            st.bar_chart(
                df_user_tables.set_index("table_name")["query_count"],
            )
            st.dataframe(
                df_user_tables[["table_name", "query_count"]],
                use_container_width=True,
                hide_index=True,
            )
            st.caption(
                "Queries reading >1M rows with few results may benefit from better filtering. "
                "Check the Queries page for details."
            )
```

- [ ] **Step 2: Commit**

```bash
git add pages/11_User_Dashboard.py
git commit -m "feat: add user dashboard with activity, errors, and table usage tabs"
```

---

## Task 21: Run All Tests

- [ ] **Step 1: Run the full test suite**

```bash
pytest tests/ -v
```

Expected: All 33 tests pass (4 config + 7 query_executor + 12 evaluator + 3 email_sender + 7 formatters)

- [ ] **Step 2: Fix any failures**

If any tests fail, fix the root cause before proceeding.

- [ ] **Step 3: Final commit**

```bash
git add -A
git commit -m "chore: verify all tests pass"
```

---

## Task 22: Manual Smoke Test

- [ ] **Step 1: Copy config template**

```bash
cp config.example.yaml config.yaml
```

Edit `config.yaml` with real ClickHouse connection details.

- [ ] **Step 2: Launch the dashboard**

```bash
streamlit run app.py
```

- [ ] **Step 3: Verify each page loads**

Walk through:
1. Overview — memory panel, disk bars, health grid
2. Cluster — 5 query sections
3. Queries — running now + 4 tabs
4. Disk — 6 query sections
5. Merges — 3 sections
6. Connections — session stats
7. Threads — 3 sections
8. System Metrics — metric cards + events
9. Inserts — bar chart + async queue
10. Dictionaries — status + memory
11. User Dashboard — select user, check 3 tabs

- [ ] **Step 4: Verify sidebar controls**

- Change time window, confirm queries re-execute
- Toggle auto-refresh
- Click "Refresh Now"

- [ ] **Step 5: Fix any issues found**

- [ ] **Step 6: Final commit with any fixes**

```bash
git add -A
git commit -m "fix: smoke test fixes"
```
