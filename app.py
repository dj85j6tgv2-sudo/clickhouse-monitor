import streamlit as st
import threading
import time
from pathlib import Path

from src.config import load_config, ConfigError
from src.connection import create_client, check_connection
from src.query_executor import execute_query
from src.alerts.evaluator import AlertLog, evaluate_dataframe
from src.alerts.email_sender import send_alert_email
from src.ui.components import render_sidebar, render_alert_banner

st.set_page_config(
    page_title="ClickHouse Monitor",
    page_icon="\U0001f4ca",
    layout="wide",
)

CONFIG_PATH = Path("config.yaml")
try:
    config = load_config(CONFIG_PATH)
except ConfigError as e:
    st.error(f"Configuration error: {e}")
    st.info("Copy `config.example.yaml` to `config.yaml` and fill in your settings.")
    st.stop()

if "config" not in st.session_state:
    st.session_state.config = config

if "alert_log" not in st.session_state:
    st.session_state.alert_log = AlertLog(max_size=100)

if "ch_client" not in st.session_state:
    try:
        client = create_client(config)
        if not check_connection(client):
            st.error("Failed to connect to ClickHouse. Check config.yaml.")
            st.stop()
        st.session_state.ch_client = client
    except Exception as e:
        st.error(f"Connection error: {e}")
        st.stop()

_ALERT_QUERIES = [
    ("disk", "free_space", "used_pct", None),
    ("cluster", "replica_consistency", "replica_status", "hostname"),
    ("cluster", "zookeeper_health", "zk_status", "hostname"),
    ("disk", "broken_parts", None, None),
    ("merges", "mutations", None, None),
    ("dictionaries", "status", "dict_status", "hostname"),
]


def _alert_loop(config: dict, alert_log: AlertLog) -> None:
    sql_dir = Path("sql")
    interval = config["alerts"]["check_interval_seconds"]
    cooldown = config["alerts"]["cooldown_minutes"]
    cluster = config["clickhouse"]["cluster"]
    severity_levels = set(config["alerts"]["severity_levels"])

    try:
        client = create_client(config)
    except Exception:
        return

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
                    pass
        except Exception:
            pass

        time.sleep(interval)


if config["alerts"]["enabled"] and "alert_thread_started" not in st.session_state:
    st.session_state.alert_thread_started = True
    t = threading.Thread(
        target=_alert_loop,
        args=(config, st.session_state.alert_log),
        daemon=True,
    )
    t.start()

settings = render_sidebar(config)
st.session_state.lookback_hours = settings["lookback_hours"]
st.session_state.lookback_days = settings["lookback_days"]

if settings["auto_refresh"]:
    time.sleep(settings["refresh_interval"])
    st.cache_data.clear()
    st.rerun()

render_alert_banner(st.session_state.alert_log)

st.title("\U0001f4ca ClickHouse Monitor")
st.markdown(f"**Cluster:** `{config['clickhouse']['cluster']}` — select a page from the sidebar.")
