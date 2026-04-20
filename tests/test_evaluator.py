import pytest
import pandas as pd
from datetime import datetime, timedelta
from src.alerts.evaluator import extract_severity, Alert, AlertLog, evaluate_dataframe


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
    log._sent_times[("disk", "CRITICAL", "hostname=node1")] = datetime.now() - timedelta(minutes=31)
    alert2 = Alert("CRITICAL", "disk", "msg", datetime.now(), "hostname=node1", {})
    assert log.should_send(alert2, cooldown_minutes=30) is True


def test_cooldown_escalation_bypasses():
    log = AlertLog(max_size=100)
    alert1 = Alert("WARNING", "disk", "msg", datetime.now(), "hostname=node1", {})
    log.record_sent(alert1)
    alert2 = Alert("CRITICAL", "disk", "msg", datetime.now(), "hostname=node1", {})
    assert log.should_send(alert2, cooldown_minutes=30) is True


def test_evaluate_dataframe():
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
