import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import render_alert_banner

st.set_page_config(page_title="User Dashboard — ClickHouse Monitor", layout="wide")

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


st.markdown("## User Dashboard")

df_activity_all = _run("users", "activity")
if df_activity_all.empty or ("error" in df_activity_all.columns and len(df_activity_all.columns) == 1):
    st.warning("Could not load user data. Check ClickHouse connection.")
    st.stop()

all_users = sorted(df_activity_all["user"].unique().tolist())
selected_user = st.selectbox("Select user", options=all_users)

tab_activity, tab_errors, tab_tables = st.tabs(["Activity", "Errors", "Table Usage"])

with tab_activity:
    user_row = df_activity_all[df_activity_all["user"] == selected_user]
    if user_row.empty:
        st.info(f"No activity for {selected_user} in this window")
    else:
        row = user_row.iloc[0]
        col1, col2, col3 = st.columns(3)
        col1.metric("Query Count", row.get("query_count", 0))
        col2.metric("Total Duration", row.get("total_duration", "0s"))
        col3.metric("Avg Duration", row.get("avg_duration", "0s"))

        total_queries = df_activity_all["query_count"].sum()
        user_queries = int(row.get("query_count", 0))
        if total_queries > 0:
            pct = round(user_queries / total_queries * 100, 1)
            st.caption(f"{selected_user} accounts for **{pct}%** of all queries in this window")

        st.markdown("**Details**")
        st.dataframe(user_row, use_container_width=True, hide_index=True)

with tab_errors:
    df_errors_all = _run("users", "errors")
    if df_errors_all.empty or ("error" in df_errors_all.columns and len(df_errors_all.columns) == 1):
        st.info("No errors for any user in this window")
    else:
        df_user_errors = df_errors_all[df_errors_all["user"] == selected_user]
        if df_user_errors.empty:
            st.success(f"No errors for {selected_user} in this window")
        else:
            row = df_user_errors.iloc[0]
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Errors", row.get("error_count", 0))
            col2.metric("Before Start", row.get("errors_before_start", 0))
            col3.metric("While Processing", row.get("errors_while_processing", 0))
            st.markdown("**Last Error**")
            st.code(str(row.get("last_exception_message", "N/A")))

with tab_tables:
    df_tables_all = _run("users", "top_tables")
    if df_tables_all.empty or ("error" in df_tables_all.columns and len(df_tables_all.columns) == 1):
        st.info("No table usage data in this window")
    else:
        df_user_tables = df_tables_all[df_tables_all["user"] == selected_user]
        if df_user_tables.empty:
            st.info(f"No table usage for {selected_user} in this window")
        else:
            st.bar_chart(df_user_tables.set_index("table_name")["query_count"])
            st.dataframe(
                df_user_tables[["table_name", "query_count"]],
                use_container_width=True,
                hide_index=True,
            )
            st.caption(
                "Queries reading >1M rows with few results may benefit from better filtering. "
                "Check the Queries page for details."
            )
