import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock
from typing import Any, Optional
import pandas as pd


_SEVERITY_PATTERN = re.compile(r"^(CRITICAL|WARNING|CAUTION|OK)\s")
_SEVERITY_RANK = {"CRITICAL": 3, "WARNING": 2, "CAUTION": 1, "OK": 0}


def extract_severity(value: Any) -> Optional[str]:
    if value is None:
        return None
    match = _SEVERITY_PATTERN.match(str(value).strip())
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
        self._sent_times: dict[tuple, datetime] = {}
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
                    # Allow escalation: if a lower severity was sent, bypass cooldown for higher
                    for prev_sev, prev_rank in _SEVERITY_RANK.items():
                        if prev_rank < _SEVERITY_RANK.get(alert.severity, 0):
                            prev_key = (alert.domain, prev_sev, alert.key)
                            if prev_key in self._sent_times:
                                return True
                    return False
            return True

    def record_sent(self, alert: Alert) -> None:
        with self._lock:
            self._sent_times[(alert.domain, alert.severity, alert.key)] = datetime.now()


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
        if severity in ("CRITICAL", "WARNING"):
            alerts.append(Alert(
                severity=severity,
                domain=domain,
                message=str(row[status_column]),
                timestamp=datetime.now(),
                key=f"{key_column}={row.get(key_column, 'unknown')}",
                details=row.to_dict(),
            ))
    return alerts
