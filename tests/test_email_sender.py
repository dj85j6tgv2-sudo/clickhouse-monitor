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
