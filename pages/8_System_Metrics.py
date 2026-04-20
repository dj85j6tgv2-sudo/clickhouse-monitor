import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import init_page, metric_card
from src.ui.formatters import style_dataframe
from src.alerts.evaluator import extract_severity

st.set_page_config(page_title="System Metrics — ClickHouse Monitor", layout="wide")

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


st.markdown("## System Metrics")

# ── Memory History ────────────────────────────────────────────────────────
st.markdown("### Memory Usage History")

df_mem = _run("system_metrics", "memory_history")

if df_mem.empty or "error" in df_mem.columns:
    st.info("No memory history data available")
else:
    thresholds = _cfg().get("thresholds", {})
    warn_pct = thresholds.get("disk_used_pct_warning", 75)
    crit_pct = thresholds.get("disk_used_pct_critical", 90)

    # ── Top-line current memory stats ──
    latest = df_mem.sort_values("minute").groupby("hostname").last().reset_index()
    for _, row in latest.iterrows():
        hostname = row["hostname"]
        os_pct = row["os_used_pct"]
        ch_pct = row["ch_pct"]
        total_gb = row["total_gb"]
        avail_gb = row["available_gb"]
        resident_gb = row["resident_gb"]
        tracked_gb = row["tracked_gb"]

        if len(latest) > 1:
            st.markdown(f"**Node: `{hostname}`**")

        sev = "CRITICAL" if os_pct >= crit_pct else ("WARNING" if os_pct >= warn_pct else "OK")
        sev_color = {"CRITICAL": "🔴", "WARNING": "🟠", "OK": "🟢"}.get(sev, "⚪")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("OS Memory Used", f"{os_pct:.1f}%",
                  delta=f"{sev_color} {sev}", delta_color="off")
        c2.metric("ClickHouse RSS", f"{resident_gb:.2f} GiB",
                  delta=f"{ch_pct:.1f}% of total", delta_color="off")
        c3.metric("Available RAM", f"{avail_gb:.2f} GiB",
                  delta=f"of {total_gb:.2f} GiB total", delta_color="off")
        c4.metric("CH Tracked Allocs", f"{tracked_gb:.2f} GiB")

    st.markdown("---")

    # ── Area chart: OS used % and CH RSS % per node ──
    st.markdown("**Memory % over time** *(OS-wide used % and ClickHouse process %)*")
    st.caption(
        f"Warning threshold: {warn_pct}% · Critical threshold: {crit_pct}% · "
        "Gaps between 🟠/🔴 lines indicate non-ClickHouse memory consumers"
    )

    hostnames = df_mem["hostname"].unique().tolist()
    if len(hostnames) == 1:
        chart_df = df_mem.set_index("minute")[["os_used_pct", "ch_pct"]].rename(
            columns={"os_used_pct": "OS used %", "ch_pct": "ClickHouse RSS %"}
        )
        st.area_chart(chart_df, height=280, use_container_width=True)
    else:
        # Multi-node: one chart per node
        for host in hostnames:
            st.markdown(f"*{host}*")
            node_df = df_mem[df_mem["hostname"] == host].set_index("minute")[
                ["os_used_pct", "ch_pct"]
            ].rename(columns={"os_used_pct": "OS used %", "ch_pct": "ClickHouse RSS %"})
            st.area_chart(node_df, height=220, use_container_width=True)

    # ── Absolute GiB chart ──
    with st.expander("Raw GiB breakdown"):
        if len(hostnames) == 1:
            gib_df = df_mem.set_index("minute")[
                ["resident_gb", "tracked_gb", "queries_peak_gb"]
            ].rename(columns={
                "resident_gb": "Process RSS (GiB)",
                "tracked_gb": "CH Tracked (GiB)",
                "queries_peak_gb": "Query Peak (GiB)",
            })
            st.line_chart(gib_df, height=220, use_container_width=True)
        else:
            for host in hostnames:
                st.markdown(f"*{host}*")
                gib_df = df_mem[df_mem["hostname"] == host].set_index("minute")[
                    ["resident_gb", "tracked_gb", "queries_peak_gb"]
                ].rename(columns={
                    "resident_gb": "Process RSS (GiB)",
                    "tracked_gb": "CH Tracked (GiB)",
                    "queries_peak_gb": "Query Peak (GiB)",
                })
                st.line_chart(gib_df, height=180, use_container_width=True)

    # ── Memory pressure periods ──
    pressure = df_mem[df_mem["os_used_pct"] >= warn_pct].copy()
    if pressure.empty:
        st.success(f"No memory pressure (>{warn_pct}%) detected in this window.")
    else:
        n_crit = (pressure["os_used_pct"] >= crit_pct).sum()
        n_warn = (pressure["os_used_pct"] < crit_pct).sum()
        st.warning(
            f"⚠️ **{len(pressure)} minute(s)** above {warn_pct}% memory usage "
            f"({n_crit} critical ≥{crit_pct}%, {n_warn} warning)"
        )
        pressure["severity"] = pressure["os_used_pct"].apply(
            lambda p: f"CRITICAL - ≥{crit_pct}% OS memory used" if p >= crit_pct
                      else f"WARNING  - ≥{warn_pct}% OS memory used"
        )
        display_cols = ["minute", "hostname", "os_used_pct", "ch_pct",
                        "resident_gb", "available_gb", "severity"]
        st.dataframe(
            style_dataframe(pressure[display_cols], "severity"),
            use_container_width=True, hide_index=True
        )

# ── Memory Events (OOM kills + high-memory queries) ───────────────────────
st.markdown("### Memory Events")

df_events = _run("system_metrics", "memory_events")
if df_events.empty or "error" in df_events.columns:
    if "error" in (df_events.columns if not df_events.empty else []):
        st.error(f"Query error: {df_events['error'].iloc[0]}")
    else:
        st.success("✅ No OOM kills or high-memory queries in this window.")
else:
    n_oom = (df_events["finish_type"].isin(["ExceptionWhileProcessing", "ExceptionBeforeStart"])).sum()
    if n_oom > 0:
        st.error(f"🔴 **{n_oom} query/queries were killed** (MEMORY_LIMIT_EXCEEDED) in this window")
    st.dataframe(
        style_dataframe(df_events, "memory_status"),
        use_container_width=True, hide_index=True
    )

# ── Current Metrics (existing) ────────────────────────────────────────────
st.markdown("### Current Metrics")
df_current = _run("system_metrics", "current_metrics")
if not df_current.empty and "error" not in df_current.columns:
    key_metrics = ["MemoryTracking", "Query", "BackgroundMergesAndMutationsPoolTask"]
    cols = st.columns(len(key_metrics))
    for col, metric_name in zip(cols, key_metrics):
        row = df_current[df_current["metric"] == metric_name]
        if not row.empty:
            val = row.iloc[0]["value"]
            display_val = f"{val / (1024**3):.2f} GiB" if metric_name == "MemoryTracking" else str(int(val))
            with col:
                metric_card(metric_name, display_val)
    with st.expander("All current metrics"):
        st.dataframe(df_current, use_container_width=True, hide_index=True)
else:
    st.info("No metrics data available")

# ── Events Summary (existing) ─────────────────────────────────────────────
st.markdown("### Events Summary (since restart)")
df_ev = _run("system_metrics", "events_summary")
if not df_ev.empty and "error" not in df_ev.columns:
    st.dataframe(df_ev, use_container_width=True, hide_index=True)
else:
    st.info("No events data available")
