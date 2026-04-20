import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import init_page

st.set_page_config(page_title="Queries — ClickHouse Monitor", layout="wide")

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


st.markdown("## Queries")

st.markdown("### Running Now")
df_running = _run("queries", "running_now")
if df_running.empty:
    st.info("No queries currently running")
elif "error" in df_running.columns and len(df_running.columns) == 1:
    st.error(f"Query error: {df_running['error'].iloc[0]}")
else:
    st.dataframe(df_running, use_container_width=True, hide_index=True)

tab_slow, tab_memory, tab_scans, tab_patterns = st.tabs([
    "Slow Queries", "Memory Heavy", "Full Table Scans", "Top Patterns"
])

with tab_slow:
    df = _run("queries", "slow_queries")
    if df.empty:
        st.info("No slow queries in this window")
    elif "error" in df.columns and len(df.columns) == 1:
        st.error(f"Query error: {df['error'].iloc[0]}")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

with tab_memory:
    df = _run("queries", "memory_heavy")
    if df.empty:
        st.info("No memory-heavy queries in this window")
    elif "error" in df.columns and len(df.columns) == 1:
        st.error(f"Query error: {df['error'].iloc[0]}")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

with tab_scans:
    df = _run("queries", "full_table_scans")
    if df.empty:
        st.info("No full table scans detected")
    elif "error" in df.columns and len(df.columns) == 1:
        st.error(f"Query error: {df['error'].iloc[0]}")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

with tab_patterns:
    df = _run("queries", "top_query_patterns")
    if df.empty:
        st.info("No query patterns in this window")
    elif "error" in df.columns and len(df.columns) == 1:
        st.error(f"Query error: {df['error'].iloc[0]}")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)
