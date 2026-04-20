import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import init_page, render_domain_page

st.set_page_config(page_title="Merges — ClickHouse Monitor", layout="wide")

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


render_domain_page("Merges", [
    ("Active Merges", _run("merges", "active_merges"), None),
    ("Mutations",     _run("merges", "mutations"),     None),
    ("Queue Depth",   _run("merges", "queue_depth"),   None),
])
