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
