"""
dashboard.py – Streamlit real-time monitoring dashboard.

Layout
------
  Row 1 : Five KPI cards (total events, avg latency, peak latency,
           error rate %, anomaly count)
  Row 2 : Latency time-series (all endpoints) + Anomaly scatter overlay
  Row 3 : Per-endpoint rolling avg bar chart + Error rate line chart
  Row 4 : Recent anomalies alert table
  Row 5 : Raw events log (last 200)

Auto-refreshes every REFRESH_INTERVAL seconds via st.rerun().
"""
import time
import logging
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import db

logger = logging.getLogger(__name__)
REFRESH_INTERVAL = 5   # seconds

st.set_page_config(
    page_title="API Performance Monitor",
    page_icon="📡",
    layout="wide",
)

# ── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
body { background-color: #0e1117; }
.kpi-card {
    background: linear-gradient(135deg, #1e2130, #252c3f);
    border-radius: 12px;
    padding: 18px 22px;
    text-align: center;
    border: 1px solid #2e3650;
    box-shadow: 0 4px 15px rgba(0,0,0,0.4);
}
.kpi-value { font-size: 2.2rem; font-weight: 700; color: #58a6ff; }
.kpi-label { font-size: 0.82rem; color: #8b949e; margin-top: 4px; }
.kpi-anomaly .kpi-value { color: #f85149; }
.alert-row { background: #2d1b1b; border-left: 4px solid #f85149;
             border-radius: 6px; padding: 8px 12px; margin-bottom: 6px; }
</style>
""", unsafe_allow_html=True)


# ── KPI Card helper ──────────────────────────────────────────────────────────
def kpi_card(label: str, value: str, extra_class: str = "") -> str:
    return f"""
    <div class="kpi-card {extra_class}">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
    </div>"""


# ── Data fetchers (with empty-DB guards) ─────────────────────────────────────
@st.cache_data(ttl=REFRESH_INTERVAL)
def get_kpi():
    try:
        return db.fetch_kpi_summary()
    except Exception as exc:
        logger.warning("KPI fetch failed: %s", exc)
        return {"total_events": 0, "avg_latency": 0, "peak_latency": 0,
                "error_rate_pct": 0, "anomaly_count": 0}


@st.cache_data(ttl=REFRESH_INTERVAL)
def get_events_df():
    try:
        rows = db.fetch_recent_events(200)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df.sort_values("timestamp")
    except Exception as exc:
        logger.warning("Events fetch failed: %s", exc)
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_INTERVAL)
def get_metrics_df():
    try:
        rows = db.fetch_recent_metrics(500)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
        return df.sort_values("window_end")
    except Exception as exc:
        logger.warning("Metrics fetch failed: %s", exc)
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_INTERVAL)
def get_anomalies_df():
    try:
        rows = db.fetch_recent_anomalies(50)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        df["detected_at"] = pd.to_datetime(df["detected_at"], utc=True)
        return df.sort_values("detected_at", ascending=False)
    except Exception as exc:
        logger.warning("Anomalies fetch failed: %s", exc)
        return pd.DataFrame()


# ── Page render ───────────────────────────────────────────────────────────────
def render():
    st.title("📡 Real-Time API Performance Monitor")
    st.caption(f"Auto-refreshes every {REFRESH_INTERVAL} s · Data source: SQLite + Kafka")

    kpi   = get_kpi()
    ev_df = get_events_df()
    me_df = get_metrics_df()
    an_df = get_anomalies_df()

    # ── Row 1: KPI Cards ─────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.markdown(kpi_card("Total Events",    f"{kpi['total_events']:,}"),  unsafe_allow_html=True)
    c2.markdown(kpi_card("Avg Latency",     f"{kpi['avg_latency']} ms"),  unsafe_allow_html=True)
    c3.markdown(kpi_card("Peak Latency",    f"{kpi['peak_latency']} ms"), unsafe_allow_html=True)
    c4.markdown(kpi_card("Error Rate",      f"{kpi['error_rate_pct']} %"),unsafe_allow_html=True)
    c5.markdown(kpi_card("Anomalies",       f"{kpi['anomaly_count']}",
                          "kpi-anomaly"),                                  unsafe_allow_html=True)

    st.markdown("---")

    # ── Row 2: Latency time-series + anomaly overlay ─────────────────────────
    st.subheader("⏱ Latency Over Time")
    if not ev_df.empty:
        fig_lat = px.line(
            ev_df, x="timestamp", y="latency_ms", color="endpoint",
            labels={"latency_ms": "Latency (ms)", "timestamp": ""},
            template="plotly_dark",
        )
        if not an_df.empty:
            # Merge to get timestamps for anomalous events
            an_ev = ev_df[ev_df["id"].isin([])]   # placeholder
            # Use anomaly table's own latency & detected_at
            fig_lat.add_trace(go.Scatter(
                x=an_df["detected_at"], y=an_df["latency_ms"],
                mode="markers",
                marker=dict(color="red", size=10, symbol="x"),
                name="Anomaly",
            ))
        st.plotly_chart(fig_lat, use_container_width=True)
    else:
        st.info("⏳ Waiting for event data …")

    # ── Row 3: Rolling avg + error rate ──────────────────────────────────────
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("📊 Rolling Avg Latency by Endpoint")
        if not me_df.empty:
            latest = me_df.groupby("endpoint").last().reset_index()
            fig_bar = px.bar(
                latest, x="endpoint", y="rolling_avg_ms",
                labels={"rolling_avg_ms": "Avg Latency (ms)", "endpoint": ""},
                color="rolling_avg_ms",
                color_continuous_scale="Blues",
                template="plotly_dark",
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("⏳ Waiting for metrics …")

    with col_b:
        st.subheader("📉 Error Rate Over Time")
        if not me_df.empty:
            fig_err = px.line(
                me_df, x="window_end", y="error_rate", color="endpoint",
                labels={"error_rate": "Error Rate (0–1)", "window_end": ""},
                template="plotly_dark",
            )
            st.plotly_chart(fig_err, use_container_width=True)
        else:
            st.info("⏳ Waiting for metrics …")

    st.markdown("---")

    # ── Row 4: Anomaly alerts ─────────────────────────────────────────────────
    st.subheader("🚨 Recent Anomalies")
    if not an_df.empty:
        for _, row in an_df.head(10).iterrows():
            st.markdown(
                f'<div class="alert-row">'
                f'<b>{row["detected_at"].strftime("%H:%M:%S")}</b> &nbsp;│&nbsp; '
                f'<b>{row["endpoint"]}</b> &nbsp;│&nbsp; '
                f'Latency: <b>{row["latency_ms"]} ms</b> &nbsp;│&nbsp; '
                f'Score: {row["score"]:.3f}'
                f'</div>',
                unsafe_allow_html=True,
            )
    else:
        st.success("✅ No anomalies detected yet.")

    st.markdown("---")

    # ── Row 5: Raw event log ──────────────────────────────────────────────────
    with st.expander("📋 Raw Event Log (last 200)", expanded=False):
        if not ev_df.empty:
            st.dataframe(
                ev_df[["timestamp","endpoint","status_code","latency_ms"]]
                     .sort_values("timestamp", ascending=False),
                use_container_width=True,
                height=300,
            )
        else:
            st.info("No events yet.")

    # ── Auto-refresh ──────────────────────────────────────────────────────────
    time.sleep(REFRESH_INTERVAL)
    st.rerun()


render()
