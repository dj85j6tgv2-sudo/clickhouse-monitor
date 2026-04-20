import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.alerts.evaluator import extract_severity
from src.ui.components import init_page, health_card
from src.ui.formatters import style_dataframe

st.set_page_config(
    page_title="ClickHouse Monitor — Overview",
    page_icon="📊",
    layout="wide",
)

init_page()


def _cfg():
    return st.session_state.get("config", {})


def _run(domain, name):
    client = st.session_state.get("ch_client")
    config = _cfg()
    if client is None:
        return pd.DataFrame({"error": ["Not connected"]})
    return execute_query(
        client, Path("sql"), domain, name,
        cluster=config.get("clickhouse", {}).get("cluster", ""),
        lookback_hours=st.session_state.get("lookback_hours", 6),
        lookback_days=st.session_state.get("lookback_days", 1),
    )


def _worst(df, status_col):
    if df.empty or status_col not in df.columns:
        return "OK"
    rank = {"CRITICAL": 3, "WARNING": 2, "CAUTION": 1, "OK": 0}
    worst = "OK"
    for val in df[status_col]:
        sev = extract_severity(val)
        if sev and rank.get(sev, 0) > rank.get(worst, 0):
            worst = sev
    return worst


st.title("📊 Overview")

# ── Priority Panel 1: Memory ──────────────────────────────────────────────
st.markdown("## Active Queries by Memory")
df_running = _run("queries", "running_now")
thresholds = _cfg().get("thresholds", {})
mem_warn_gb = thresholds.get("memory_usage_warning_gb", 10)
mem_crit_gb = thresholds.get("memory_usage_critical_gb", 20)

if df_running.empty or ("error" in df_running.columns and len(df_running.columns) == 1):
    st.success("No running queries")
else:
    def _classify_memory(row):
        mem_str = str(row.get("memory", "0"))
        gb = 0.0
        if "GiB" in mem_str:
            try:
                gb = float(mem_str.split()[0])
            except ValueError:
                pass
        elif "MiB" in mem_str:
            try:
                gb = float(mem_str.split()[0]) / 1024
            except ValueError:
                pass
        if gb >= mem_crit_gb:
            return "CRITICAL - Query using excessive memory"
        elif gb >= mem_warn_gb:
            return "WARNING  - Query using high memory"
        return "OK       - Normal memory usage"

    df_running["memory_status"] = df_running.apply(_classify_memory, axis=1)
    has_issues = df_running["memory_status"].apply(
        lambda x: extract_severity(x) in ("CRITICAL", "WARNING")
    ).any()

    if has_issues:
        st.dataframe(style_dataframe(df_running, "memory_status"), use_container_width=True, hide_index=True)
    else:
        st.success("All queries within normal memory limits")
        with st.expander("Show running queries"):
            st.dataframe(df_running.drop(columns=["memory_status"]), use_container_width=True, hide_index=True)

# ── Priority Panel 2: Disk ────────────────────────────────────────────────
st.markdown("## Disk Usage")
df_disk = _run("disk", "free_space")
disk_warn = thresholds.get("disk_used_pct_warning", 75)
disk_crit = thresholds.get("disk_used_pct_critical", 90)

if df_disk.empty or ("error" in df_disk.columns and len(df_disk.columns) == 1):
    st.info("No disk data available")
else:
    for _, row in df_disk.iterrows():
        hostname = row.get("hostname", "unknown")
        disk_name = row.get("disk_name", "default")
        used_pct = float(row.get("used_pct", 0))
        free = row.get("free", "?")
        total = row.get("total", "?")
        color = "red" if used_pct >= disk_crit else ("orange" if used_pct >= disk_warn else "green")
        label = f":{color}[{hostname} / {disk_name} — {free} free of {total} ({used_pct}%)]"
        st.progress(min(used_pct / 100.0, 1.0), text=label)

# ── Health Grid ───────────────────────────────────────────────────────────
st.markdown("## Cluster Health")

_HEALTH_CHECKS = [
    ("Cluster",        "cluster",        "replica_consistency", "replica_status"),
    ("Queries",        "queries",        "slow_queries",        None),
    ("Disk",           "disk",           "parts_health",        "parts_assessment"),
    ("Merges",         "merges",         "mutations",           None),
    ("Connections",    "connections",    "session_stats",       "connection_status"),
    ("Threads",        "threads",        "thread_pool_usage",   "pool_status"),
    ("System Metrics", "system_metrics", "current_metrics",     None),
    ("Inserts",        "inserts",        "async_inserts",       None),
    ("Dictionaries",   "dictionaries",   "status",              "dict_status"),
]

cols = st.columns(3)
for i, (label, domain, query_name, status_col) in enumerate(_HEALTH_CHECKS):
    df = _run(domain, query_name)
    worst = _worst(df, status_col) if status_col else (
        "WARNING" if (not df.empty and "error" in df.columns) else "OK"
    )
    row_count = len(df) if not df.empty and "error" not in df.columns else 0
    with cols[i % 3]:
        health_card(label, worst, f"{row_count} items" if row_count else "No data")
