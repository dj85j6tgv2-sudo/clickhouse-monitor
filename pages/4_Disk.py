import streamlit as st
import pandas as pd
from pathlib import Path
from src.query_executor import execute_query
from src.ui.components import init_page, render_domain_page, render_status_summary
from src.ui.formatters import style_dataframe

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


def _cfg():
    return st.session_state.get("config", {})


st.markdown("## Disk")

# ── Disk Usage History ────────────────────────────────────────────────────
st.markdown("### Disk Usage History")

df_hist = _run("disk", "disk_history")

if df_hist.empty or "error" in df_hist.columns:
    st.info("No disk history data available")
else:
    thresholds = _cfg().get("thresholds", {})
    warn_pct = thresholds.get("disk_used_pct_warning", 75)
    crit_pct = thresholds.get("disk_used_pct_critical", 90)

    # ── Top-line current disk stats per node ──
    latest = df_hist.sort_values("minute").groupby("hostname").last().reset_index()
    for _, row in latest.iterrows():
        hostname = row["hostname"]
        used_pct = row["used_pct"]
        total_gb = row["total_gb"]
        available_gb = row["available_gb"]
        used_gb = round(total_gb - available_gb, 2)

        if len(latest) > 1:
            st.markdown(f"**Node: `{hostname}`**")

        sev = "CRITICAL" if used_pct >= crit_pct else ("WARNING" if used_pct >= warn_pct else "OK")
        sev_color = {"CRITICAL": "🔴", "WARNING": "🟠", "OK": "🟢"}.get(sev, "⚪")

        c1, c2, c3 = st.columns(3)
        c1.metric("Disk Used", f"{used_pct:.1f}%",
                  delta=f"{sev_color} {sev}", delta_color="off")
        c2.metric("Used Space", f"{used_gb:.2f} GiB",
                  delta=f"of {total_gb:.2f} GiB total", delta_color="off")
        c3.metric("Available Space", f"{available_gb:.2f} GiB")

    st.markdown("---")

    # ── Area chart: used % per node ──
    st.markdown(f"**Disk used % over time**")
    st.caption(
        f"Warning threshold: {warn_pct}% · Critical threshold: {crit_pct}% · "
        "Main filesystem path (all processes combined)"
    )

    hostnames = df_hist["hostname"].unique().tolist()
    if len(hostnames) == 1:
        chart_df = df_hist.set_index("minute")[["used_pct"]].rename(
            columns={"used_pct": "Disk Used %"}
        )
        st.area_chart(chart_df, height=280, use_container_width=True)
    else:
        for host in hostnames:
            st.markdown(f"*{host}*")
            node_df = df_hist[df_hist["hostname"] == host].set_index("minute")[["used_pct"]].rename(
                columns={"used_pct": "Disk Used %"}
            )
            st.area_chart(node_df, height=220, use_container_width=True)

    # ── Absolute GiB chart ──
    with st.expander("Raw GiB breakdown"):
        if len(hostnames) == 1:
            gib_df = df_hist.set_index("minute")[["available_gb"]].rename(
                columns={"available_gb": "Available (GiB)"}
            )
            st.line_chart(gib_df, height=220, use_container_width=True)
        else:
            for host in hostnames:
                st.markdown(f"*{host}*")
                gib_df = df_hist[df_hist["hostname"] == host].set_index("minute")[["available_gb"]].rename(
                    columns={"available_gb": "Available (GiB)"}
                )
                st.line_chart(gib_df, height=180, use_container_width=True)

    # ── Disk pressure periods ──
    pressure = df_hist[df_hist["used_pct"] >= warn_pct].copy()
    if pressure.empty:
        st.success(f"No disk pressure (>{warn_pct}%) detected in this window.")
    else:
        n_crit = (pressure["used_pct"] >= crit_pct).sum()
        n_warn = (pressure["used_pct"] < crit_pct).sum()
        st.warning(
            f"⚠️ **{len(pressure)} minute(s)** above {warn_pct}% disk usage "
            f"({n_crit} critical ≥{crit_pct}%, {n_warn} warning)"
        )
        pressure["severity"] = pressure["used_pct"].apply(
            lambda p: f"CRITICAL - ≥{crit_pct}% disk used" if p >= crit_pct
                      else f"WARNING  - ≥{warn_pct}% disk used"
        )
        display_cols = ["minute", "hostname", "used_pct", "available_gb", "total_gb", "severity"]
        st.dataframe(
            style_dataframe(pressure[display_cols], "severity"),
            use_container_width=True, hide_index=True
        )

# ── Existing sections ─────────────────────────────────────────────────────
df_free = _run("disk", "free_space")
df_tables = _run("disk", "table_sizes")
df_parts = _run("disk", "parts_health")
df_broken = _run("disk", "broken_parts")
df_detached = _run("disk", "detached_parts")
df_ttl = _run("disk", "ttl_progress")

render_domain_page("Disk", [
    ("Free Space",     df_free,     None),
    ("Table Sizes",    df_tables,   None),
    ("Parts Health",   df_parts,    "parts_assessment"),
    ("Broken Parts",   df_broken,   None),
    ("Detached Parts", df_detached, "status"),
    ("TTL Progress",   df_ttl,      "ttl_status"),
], skip_title=True)
