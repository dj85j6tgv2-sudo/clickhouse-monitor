import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import init_page, render_domain_page

st.set_page_config(page_title="Disk — ClickHouse Monitor", layout="wide")

init_page()



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


render_domain_page("Disk", [
    ("Free Space",     _run("disk", "free_space"),     None),
    ("Table Sizes",    _run("disk", "table_sizes"),    None),
    ("Parts Health",   _run("disk", "parts_health"),   "parts_assessment"),
    ("Broken Parts",   _run("disk", "broken_parts"),   None),
    ("Detached Parts", _run("disk", "detached_parts"), "status"),
    ("TTL Progress",   _run("disk", "ttl_progress"),   "ttl_status"),
])
