from math import ceil
import streamlit as st
import pandas as pd
from src.alerts.evaluator import AlertLog, extract_severity
from src.ui.formatters import style_dataframe


_SEVERITY_EMOJI = {
    "CRITICAL": "\U0001f534",
    "WARNING": "\U0001f7e0",
    "CAUTION": "\U0001f7e1",
    "OK": "\U0001f7e2",
}


def render_sidebar(config: dict) -> dict:
    st.sidebar.markdown(f"**Cluster:** `{config['clickhouse']['cluster']}`")
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Time Window**")

    preset = st.sidebar.radio(
        "Lookback",
        options=["1h", "6h", "24h", "7d", "Custom"],
        index=1,
        horizontal=True,
        label_visibility="collapsed",
    )
    hours_map = {"1h": 1, "6h": 6, "24h": 24, "7d": 168}
    if preset == "Custom":
        lookback_hours = st.sidebar.number_input("Hours", min_value=1, max_value=720, value=6)
    else:
        lookback_hours = hours_map[preset]

    lookback_days = max(1, ceil(lookback_hours / 24))

    st.sidebar.markdown("---")
    st.sidebar.markdown("**Refresh**")
    auto_refresh = st.sidebar.toggle("Auto-refresh", value=config["refresh"]["auto_enabled"])
    refresh_interval = config["refresh"]["interval_seconds"]
    if auto_refresh:
        refresh_interval = st.sidebar.select_slider(
            "Interval (s)",
            options=[30, 60, 300],
            value=config["refresh"]["interval_seconds"],
        )

    if st.sidebar.button("Refresh Now", use_container_width=True):
        st.cache_data.clear()

    return {
        "lookback_hours": lookback_hours,
        "lookback_days": lookback_days,
        "auto_refresh": auto_refresh,
        "refresh_interval": refresh_interval,
    }


def render_alert_banner(alert_log: AlertLog) -> None:
    counts = alert_log.get_counts()
    critical = counts.get("CRITICAL", 0)
    warning = counts.get("WARNING", 0)
    if critical == 0 and warning == 0:
        return

    parts = []
    if critical > 0:
        parts.append(f"\U0001f534 **{critical} CRITICAL**")
    if warning > 0:
        parts.append(f"\U0001f7e0 **{warning} WARNING**")

    if "alert_banner_dismissed" not in st.session_state:
        st.session_state.alert_banner_dismissed = False

    if not st.session_state.alert_banner_dismissed:
        col1, col2 = st.columns([9, 1])
        with col1:
            st.error(" \u00b7 ".join(parts))
        with col2:
            if st.button("\u2715", key="dismiss_alert_banner"):
                st.session_state.alert_banner_dismissed = True
                st.rerun()


def metric_card(label: str, value: str, severity: str = "OK") -> None:
    emoji = _SEVERITY_EMOJI.get(severity, "")
    st.metric(label=f"{emoji} {label}", value=value)


def status_badge(severity: str) -> str:
    emoji = _SEVERITY_EMOJI.get(severity, "\u26aa")
    return f"{emoji} {severity}"


def health_card(domain: str, worst_severity: str, summary: str) -> None:
    emoji = _SEVERITY_EMOJI.get(worst_severity, "\u26aa")
    colors = {
        "CRITICAL": "#ffcccc",
        "WARNING": "#ffe0b2",
        "CAUTION": "#fff9c4",
        "OK": "#c8e6c9",
    }
    bg = colors.get(worst_severity, "#f5f5f5")
    st.markdown(
        f"""<div style="background-color:{bg}; padding:12px; border-radius:8px; margin-bottom:8px;">
<strong>{emoji} {domain}</strong><br/>
<span style="font-size:0.85em;">{summary}</span>
</div>""",
        unsafe_allow_html=True,
    )


def render_status_summary(df: pd.DataFrame, status_column: str) -> None:
    if df.empty or status_column not in df.columns:
        return
    counts = {"CRITICAL": 0, "WARNING": 0, "CAUTION": 0, "OK": 0}
    for val in df[status_column]:
        sev = extract_severity(val)
        if sev and sev in counts:
            counts[sev] += 1
    cols = st.columns(4)
    for col, (sev, count) in zip(cols, counts.items()):
        col.markdown(f"{_SEVERITY_EMOJI.get(sev, '')} **{sev}:** {count}")


def render_domain_page(title: str, queries: list) -> None:
    st.markdown(f"## {title}")
    for section_title, df, status_col in queries:
        st.markdown(f"### {section_title}")
        if df is None or df.empty:
            st.info("No data")
            continue
        if "error" in df.columns and len(df.columns) == 1:
            st.error(f"Query error: {df['error'].iloc[0]}")
            continue
        if status_col and status_col in df.columns:
            render_status_summary(df, status_col)
            st.dataframe(style_dataframe(df, status_col), use_container_width=True, hide_index=True)
        else:
            st.dataframe(df, use_container_width=True, hide_index=True)
