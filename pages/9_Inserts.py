import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner

st.set_page_config(page_title="Inserts — ClickHouse Monitor", layout="wide")

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


st.markdown("## Inserts")

st.markdown("### Insert Rates")
df_rates = _run("inserts", "insert_rates")
if not df_rates.empty and "error" not in df_rates.columns:
    if "table_name" in df_rates.columns and "insert_count" in df_rates.columns:
        st.bar_chart(df_rates.set_index("table_name")["insert_count"])
    with st.expander("Details"):
        st.dataframe(df_rates, use_container_width=True, hide_index=True)
else:
    st.info("No insert data in this window")

st.markdown("### Async Inserts Queue")
df_async = _run("inserts", "async_inserts")
if not df_async.empty and "error" not in df_async.columns:
    st.dataframe(df_async, use_container_width=True, hide_index=True)
else:
    st.info("No pending async inserts")
