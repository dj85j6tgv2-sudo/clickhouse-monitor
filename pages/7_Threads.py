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
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=st.session_state.get("lookback_hours", 6),
        lookback_days=st.session_state.get("lookback_days", 1),
    )


render_domain_page("Threads", [
    ("Background Tasks",   _run("threads", "background_tasks"),   None),
    ("Thread Pool Usage",  _run("threads", "thread_pool_usage"),  "pool_status"),
    ("Distributed Sends",  _run("threads", "distributed_sends"),  "send_status"),
])
