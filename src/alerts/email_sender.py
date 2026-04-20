import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from src.alerts.evaluator import Alert


def format_alert_email(alerts: list, cluster_name: str) -> tuple:
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
        lines += [
            "",
            f"[{alert.severity}] Domain: {alert.domain}",
            f"Message: {alert.message}",
            f"Key: {alert.key}",
            "",
            "Details:",
        ]
        for k, v in alert.details.items():
            lines.append(f"  {k}: {v}")
        lines.append("-" * 40)

    return subject, "\n".join(lines)


def send_alert_email(alerts: list, cluster_name: str, smtp_config: dict) -> None:
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
        server.sendmail(smtp_config["from_address"], smtp_config["recipients"], msg.as_string())
