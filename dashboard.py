"""
dashboard.py – Real-Time API Performance Monitoring & Anomaly Detection Dashboard
Built with Streamlit and Plotly for high-throughput stream visualization.

Features:
- Live KPI Metrics: Total requests, Average latency, Peak latency, HTTP Error rate, Anomaly count
- Dynamic Latency Time-Series with real-time IsolationForest anomaly detection overlay
- Separate HTTP status code distribution & error rate tracking views
- Per-endpoint rolling window latency performance bars
- Filterable events and dedicated anomaly alert logs
- Native automatic background refresh via st.fragment (supported in Streamlit 1.33+)
- Read-only SQLite queries; robust empty-database & error handling
"""

import datetime
import logging
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import config
import db

logger = logging.getLogger(__name__)

# ── Page Configuration ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="API Stream Monitor & Anomaly Detector",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme & Styling ───────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Metric cards styling */
    .metric-card {
        background: linear-gradient(135deg, #161b22, #21262d);
        border: 1px solid #30363d;
        border-radius: 10px;
        padding: 16px 20px;
        text-align: center;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
        transition: transform 0.15s ease-in-out;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        border-color: #58a6ff;
    }
    .metric-value {
        font-size: 2.1rem;
        font-weight: 700;
        color: #58a6ff;
        margin-bottom: 2px;
    }
    .metric-label {
        font-size: 0.85rem;
        color: #8b949e;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .anomaly-card {
        border-color: #f85149;
        background: linear-gradient(135deg, #261515, #351a1a);
    }
    .anomaly-card .metric-value {
        color: #ff7b72;
    }
    .status-badge-ok {
        background-color: #238636;
        color: white;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.8rem;
    }
    .status-badge-err {
        background-color: #da3633;
        color: white;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.8rem;
    }
</style>
""", unsafe_allow_html=True)


# ── Database Fetching (Always Fresh & Read-Only) ──────────────────────────────
def fetch_fresh_data():
    """Reads fresh data from SQLite without any cache to guarantee real-time updates."""
    try:
        kpi = db.fetch_kpi_summary()
    except Exception as exc:
        logger.error("KPI fetch error: %s", exc)
        kpi = {
            "total_events": 0, "avg_latency": 0.0,
            "peak_latency": 0.0, "error_rate_pct": 0.0,
            "anomaly_count": 0,
        }

    try:
        events = db.fetch_recent_events_with_anomalies(limit=250)
        events_df = pd.DataFrame(events) if events else pd.DataFrame()
        if not events_df.empty:
            events_df["timestamp"] = pd.to_datetime(events_df["timestamp"], format="mixed", utc=True, errors="coerce")
            events_df["timestamp"] = events_df["timestamp"].fillna(pd.Timestamp.now(datetime.timezone.utc))
            events_df = events_df.sort_values("timestamp")
    except Exception as exc:
        logger.error("Events fetch error: %s", exc)
        events_df = pd.DataFrame()

    try:
        metrics = db.fetch_recent_metrics(limit=500)
        metrics_df = pd.DataFrame(metrics) if metrics else pd.DataFrame()
        if not metrics_df.empty:
            metrics_df["window_end"] = pd.to_datetime(metrics_df["window_end"], format="mixed", utc=True, errors="coerce")
            metrics_df["window_end"] = metrics_df["window_end"].fillna(pd.Timestamp.now(datetime.timezone.utc))
            metrics_df = metrics_df.sort_values("window_end")
    except Exception as exc:
        logger.error("Metrics fetch error: %s", exc)
        metrics_df = pd.DataFrame()

    try:
        anomalies = db.fetch_recent_anomalies(limit=100)
        anomalies_df = pd.DataFrame(anomalies) if anomalies else pd.DataFrame()
        if not anomalies_df.empty:
            anomalies_df["detected_at"] = pd.to_datetime(anomalies_df["detected_at"], format="mixed", utc=True, errors="coerce")
            anomalies_df["detected_at"] = anomalies_df["detected_at"].fillna(pd.Timestamp.now(datetime.timezone.utc))
            anomalies_df = anomalies_df.sort_values("detected_at", ascending=False)
    except Exception as exc:
        logger.error("Anomalies fetch error: %s", exc)
        anomalies_df = pd.DataFrame()

    return kpi, events_df, metrics_df, anomalies_df


# ── Sidebar Controls ──────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Monitor Settings")
    st.markdown("---")

    auto_refresh = st.toggle("🔄 Auto-Refresh Stream", value=True)
    refresh_seconds = st.slider("Refresh Interval (seconds)", min_value=1, max_value=10, value=3)

    if st.button("⚡ Force Refresh Now", use_container_width=True):
        st.rerun()

    st.markdown("---")
    st.subheader("🎯 Pipeline Architecture")
    st.markdown("""
    - **Message Broker:** Apache Kafka 3.7.0 (KRaft)
    - **Topic:** `api_events`
    - **ML Engine:** IsolationForest (`models/isolation_forest.pkl`)
    - **Storage:** SQLite WAL (`data/api_monitor.db`)
    """)
    st.markdown("---")
    st.caption("College Lab Demo · Real-Time DSA Project")


# ── Main Dashboard Fragment ───────────────────────────────────────────────────
# st.fragment with run_every enables automated client-side refresh without page reloading
fragment_interval = f"{refresh_seconds}s" if auto_refresh else None


@st.fragment(run_every=fragment_interval)
def render_live_dashboard():
    kpi, events_df, metrics_df, anomalies_df = fetch_fresh_data()
    now_str = datetime.datetime.now().strftime("%H:%M:%S")

    # Header
    head_col1, head_col2 = st.columns([3, 1])
    with head_col1:
        st.title("⚡ Real-Time API Performance Monitor")
        st.caption(f"Continuous streaming ingestion & unsupervised anomaly detection · Topic: `{config.KAFKA_TOPIC}`")
    with head_col2:
        st.markdown(f"<div style='text-align:right; color:#8b949e; padding-top:20px;'>Last updated: <b style='color:#58a6ff;'>{now_str}</b><br><small>{'🟢 Live Auto-Refresh Active' if auto_refresh else '⏸ Auto-Refresh Paused'}</small></div>", unsafe_allow_html=True)

    st.markdown("---")

    # ── Section 1: KPI Summary Cards ─────────────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{kpi['total_events']:,}</div>
            <div class="metric-label">📦 Total Requests</div>
        </div>
        """, unsafe_allow_html=True)

    with k2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{kpi['avg_latency']:.1f}<span style="font-size:1.1rem"> ms</span></div>
            <div class="metric-label">⏱ Avg Latency</div>
        </div>
        """, unsafe_allow_html=True)

    with k3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{kpi['peak_latency']:.1f}<span style="font-size:1.1rem"> ms</span></div>
            <div class="metric-label">🔺 Peak Latency</div>
        </div>
        """, unsafe_allow_html=True)

    with k4:
        err_color = "#f85149" if kpi["error_rate_pct"] > 5 else "#58a6ff"
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value" style="color:{err_color}">{kpi['error_rate_pct']:.1f}%</div>
            <div class="metric-label">⚠️ HTTP Error Rate</div>
        </div>
        """, unsafe_allow_html=True)

    with k5:
        anom_class = "anomaly-card" if kpi["anomaly_count"] > 0 else "metric-card"
        st.markdown(f"""
        <div class="metric-card {anom_class}">
            <div class="metric-value">{kpi['anomaly_count']}</div>
            <div class="metric-label">🚨 Flagged Anomalies</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Empty State Check
    if events_df.empty:
        st.warning("⏳ **Database is currently empty.** Waiting for producer events on Kafka topic `api_events`...")
        return

    # ── Endpoint Filter ───────────────────────────────────────────────────────
    available_endpoints = sorted(events_df["endpoint"].unique())
    selected_endpoints = st.multiselect(
        "Filter by Endpoint",
        options=available_endpoints,
        default=available_endpoints,
    )
    if selected_endpoints:
        filtered_events = events_df[events_df["endpoint"].isin(selected_endpoints)]
        filtered_metrics = metrics_df[metrics_df["endpoint"].isin(selected_endpoints)] if not metrics_df.empty else metrics_df
    else:
        filtered_events = events_df
        filtered_metrics = metrics_df

    # ── Section 2: Visualizations (Tabs) ──────────────────────────────────────
    tab_latency, tab_errors, tab_rolling = st.tabs([
        "📈 Latency & Anomaly Overlay",
        "🚨 HTTP Errors & Status Codes",
        "📊 Rolling Metrics by Endpoint",
    ])

    with tab_latency:
        st.subheader("Response Latency Over Time with IsolationForest Anomaly Markers")
        if not filtered_events.empty:
            fig_lat = px.line(
                filtered_events,
                x="timestamp",
                y="latency_ms",
                color="endpoint",
                labels={"latency_ms": "Response Time (ms)", "timestamp": "Event Timestamp", "endpoint": "Route"},
                template="plotly_dark",
            )
            fig_lat.update_traces(line=dict(width=1.8))

            # Overlay anomalies as bold red scatter markers
            anom_points = filtered_events[filtered_events["is_anomaly"] == 1]
            if not anom_points.empty:
                fig_lat.add_trace(go.Scatter(
                    x=anom_points["timestamp"],
                    y=anom_points["latency_ms"],
                    mode="markers+text",
                    name="Anomaly Spikes (IsolationForest)",
                    marker=dict(color="#ff3333", size=11, symbol="diamond-open-dot", line=dict(width=2, color="#ff0000")),
                    customdata=anom_points[["endpoint", "status_code", "score"]],
                    hovertemplate="<b>🚨 Anomaly Detected!</b><br>Endpoint: %{customdata[0]}<br>Latency: %{y:.1f} ms<br>Status: %{customdata[1]}<br>Score: %{customdata[2]:.3f}<extra></extra>",
                ))

            # Add reference threshold line (1000 ms SLA)
            fig_lat.add_hline(
                y=1000,
                line_dash="dot",
                line_color="#e3b341",
                annotation_text="High Latency SLA Warning (1000ms)",
                annotation_position="bottom right",
            )

            fig_lat.update_layout(
                height=420,
                margin=dict(l=20, r=20, t=30, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig_lat, use_container_width=True)
        else:
            st.info("No event data matches selected filter.")

    with tab_errors:
        st.subheader("HTTP Error Tracking & Status Code Distribution")
        err_col1, err_col2 = st.columns(2)

        with err_col1:
            st.markdown("##### Status Code Distribution")
            if not filtered_events.empty:
                status_counts = filtered_events["status_code"].astype(str).value_counts().reset_index()
                status_counts.columns = ["Status Code", "Count"]

                # Color-code status: 2xx green, 3xx blue, 4xx orange, 5xx red
                color_map = {
                    "200": "#2ea043", "201": "#3fb950", "304": "#58a6ff",
                    "400": "#d29922", "404": "#db6d28", "500": "#f85149",
                    "502": "#da3633", "503": "#b62324", "504": "#8e1519",
                }
                fig_pie = px.pie(
                    status_counts,
                    names="Status Code",
                    values="Count",
                    color="Status Code",
                    color_discrete_map=color_map,
                    hole=0.45,
                    template="plotly_dark",
                )
                fig_pie.update_layout(height=340, margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig_pie, use_container_width=True)

        with err_col2:
            st.markdown("##### Error Rate Progression (Rolling Window)")
            if not filtered_metrics.empty:
                fig_err_line = px.line(
                    filtered_metrics,
                    x="window_end",
                    y="error_rate",
                    color="endpoint",
                    labels={"error_rate": "Rolling Error Fraction (0–1)", "window_end": "Window End"},
                    template="plotly_dark",
                )
                fig_err_line.update_layout(height=340, margin=dict(l=10, r=10, t=10, b=10))
                st.plotly_chart(fig_err_line, use_container_width=True)
            else:
                st.info("Waiting for rolling error metrics...")

    with tab_rolling:
        st.subheader("Endpoint Rolling Latency Comparison")
        if not filtered_metrics.empty:
            latest_metrics = filtered_metrics.groupby("endpoint").last().reset_index()
            fig_bar = px.bar(
                latest_metrics,
                x="endpoint",
                y=["rolling_avg_ms", "peak_latency_ms"],
                barmode="group",
                labels={"value": "Latency (ms)", "endpoint": "API Route", "variable": "Metric"},
                color_discrete_map={"rolling_avg_ms": "#58a6ff", "peak_latency_ms": "#f85149"},
                template="plotly_dark",
            )
            fig_bar.update_layout(height=360, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No rolling metrics available yet.")

    st.markdown("---")

    # ── Section 3: Tables (Anomalies & Recent Events) ─────────────────────────
    tab_c1, tab_c2 = st.columns([1, 1])

    with tab_c1:
        st.subheader("🚨 Flagged Anomalies Log")
        if not anomalies_df.empty:
            anom_display = anomalies_df[[
                "detected_at", "endpoint", "latency_ms", "score"
            ]].copy()
            anom_display["latency_ms"] = anom_display["latency_ms"].apply(lambda v: f"{v:,.1f} ms")
            anom_display["score"] = anom_display["score"].apply(lambda s: f"{s:.3f}")
            anom_display["detected_at"] = anom_display["detected_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
            anom_display.columns = ["Detected At", "Endpoint", "Latency", "Model Score"]
            st.dataframe(anom_display, use_container_width=True, height=280)
        else:
            st.success("✅ No anomalies detected in current stream window.")

    with tab_c2:
        st.subheader("📋 Recent Event Activity Stream")
        if not events_df.empty:
            recent_display = events_df.tail(50)[[
                "timestamp", "endpoint", "status_code", "latency_ms", "is_anomaly"
            ]].copy().sort_values("timestamp", ascending=False)
            recent_display["latency_ms"] = recent_display["latency_ms"].apply(lambda v: f"{v:,.1f} ms")
            recent_display["is_anomaly"] = recent_display["is_anomaly"].apply(lambda a: "🚨 ANOMALY" if a == 1 else "OK")
            recent_display["timestamp"] = recent_display["timestamp"].dt.strftime("%H:%M:%S")
            recent_display.columns = ["Time", "Endpoint", "Status", "Latency", "Classification"]
            st.dataframe(recent_display, use_container_width=True, height=280)
        else:
            st.info("No recent events.")


# Execute the fragment
render_live_dashboard()
