import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner, metric_card

st.set_page_config(page_title="System Metrics — ClickHouse Monitor", layout="wide")

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


st.markdown("## System Metrics")

st.markdown("### Current Metrics")
df_current = _run("system_metrics", "current_metrics")
if not df_current.empty and "error" not in df_current.columns:
    key_metrics = ["MemoryTracking", "Query", "BackgroundMergesAndMutationsPoolTask"]
    cols = st.columns(len(key_metrics))
    for col, metric_name in zip(cols, key_metrics):
        row = df_current[df_current["metric"] == metric_name]
        if not row.empty:
            val = row.iloc[0]["value"]
            if metric_name == "MemoryTracking":
                display_val = f"{val / (1024**3):.2f} GiB"
            else:
                display_val = str(int(val))
            with col:
                metric_card(metric_name, display_val)

    with st.expander("All current metrics"):
        st.dataframe(df_current, use_container_width=True, hide_index=True)
else:
    st.info("No metrics data available")

st.markdown("### Events Summary (since restart)")
df_events = _run("system_metrics", "events_summary")
if not df_events.empty and "error" not in df_events.columns:
    st.dataframe(df_events, use_container_width=True, hide_index=True)
else:
    st.info("No events data available")
