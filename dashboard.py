"""
dashboard.py – PulseWatch: Real-Time API Performance Monitoring & Anomaly Detection.

Observability presentation layer built with Streamlit and Plotly.
Features:
- Live KPI telemetry cards (Volume, Latency Avg/Peak, Error Rate, Anomaly Count)
- High-density response latency time-series with 1,000 ms SLA threshold and IsolationForest markers
- Endpoint rolling latency comparisons and HTTP status code distribution
- Live anomaly alert registry and raw telemetry activity feed
- Client-side auto-refresh via Streamlit fragment polling (@st.fragment)
- Non-blocking read-only queries against SQLite Write-Ahead Logging (WAL)
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
    page_title="PulseWatch · Real-Time API Observability",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Design System & Custom CSS ────────────────────────────────────────────────
st.markdown("""
<style>
    /* Global layout & typography */
    .block-container {
        padding-top: 2.2rem;
        padding-bottom: 2rem;
        padding-left: 2rem;
        padding-right: 2rem;
        max-width: 100% !important;
    }

    /* PulseWatch Header Bar */
    .pw-header-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding-top: 4px;
        padding-bottom: 0.9rem;
        border-bottom: 1px solid #1E293B;
        margin-bottom: 1.2rem;
    }
    .pw-brand-title {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Inter, sans-serif;
        font-size: 1.85rem;
        font-weight: 800;
        letter-spacing: -0.02em;
        color: #FFFFFF;
        margin: 0;
        line-height: 1.2;
    }
    .pw-brand-subtitle {
        font-size: 0.84rem;
        color: #94A3B8;
        margin-top: 3px;
        letter-spacing: 0.01em;
    }
    .pw-status-panel {
        display: flex;
        align-items: center;
        gap: 14px;
        font-size: 0.8rem;
        color: #94A3B8;
    }
    .pw-badge-live {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: #064E3B;
        border: 1px solid #059669;
        color: #34D399;
        padding: 3px 10px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.72rem;
        letter-spacing: 0.04em;
    }
    .pw-badge-paused {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: #3B2A06;
        border: 1px solid #B45309;
        color: #FBBF24;
        padding: 3px 10px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.72rem;
    }
    .pw-pulse-dot {
        width: 7px;
        height: 7px;
        background-color: #10B981;
        border-radius: 50%;
        box-shadow: 0 0 6px #10B981;
        animation: pulseAnimation 2s infinite ease-in-out;
    }
    @keyframes pulseAnimation {
        0% { transform: scale(0.95); opacity: 0.8; }
        50% { transform: scale(1.3); opacity: 1; }
        100% { transform: scale(0.95); opacity: 0.8; }
    }

    /* Metric Cards */
    .pw-card {
        background: #111726;
        border: 1px solid #1E293B;
        border-radius: 6px;
        padding: 10px 14px;
        box-shadow: 0 2px 6px rgba(0, 0, 0, 0.3);
        transition: border-color 0.15s ease;
    }
    .pw-card:hover {
        border-color: #334155;
    }
    .pw-card-label {
        font-size: 0.68rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94A3B8;
        margin-bottom: 2px;
    }
    .pw-card-value {
        font-size: 1.65rem;
        font-weight: 800;
        color: #F8FAFC;
        font-variant-numeric: tabular-nums;
        line-height: 1.15;
    }
    .pw-card-unit {
        font-size: 0.82rem;
        font-weight: 600;
        color: #64748B;
        margin-left: 2px;
    }
    .pw-card-sub {
        font-size: 0.7rem;
        color: #64748B;
        margin-top: 2px;
    }

    /* Anomaly Alert Card Accent */
    .pw-card-anomaly {
        background: linear-gradient(180deg, #221217 0%, #151A26 100%);
        border: 1px solid #991B1B;
    }
    .pw-card-anomaly .pw-card-value {
        color: #F87171;
    }
    .pw-card-anomaly .pw-card-label {
        color: #FCA5A5;
    }
    .pw-card-anomaly .pw-card-sub {
        color: #EF4444;
    }

    /* Section Headers */
    .pw-section-header {
        font-size: 0.76rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94A3B8;
        margin-top: 1.1rem;
        margin-bottom: 0.4rem;
        display: flex;
        align-items: center;
        gap: 6px;
    }

    /* Sidebar Telemetry Specification Card */
    .pw-sidebar-spec {
        background: #0B0F19;
        border: 1px solid #1E293B;
        border-radius: 6px;
        padding: 10px 12px;
        font-size: 0.78rem;
        color: #94A3B8;
        line-height: 1.5;
    }
    .pw-sidebar-spec b {
        color: #E2E8F0;
    }
</style>
""", unsafe_allow_html=True)

# ── Color Palettes for Observability Charts ────────────────────────────────────
ROUTE_COLORS = {
    "/api/v1/users": "#38BDF8",      # Sky Blue
    "/api/v1/orders": "#818CF8",     # Indigo
    "/api/v1/products": "#34D399",   # Emerald
    "/api/v1/payments": "#FB923C",   # Amber / Orange
    "/api/v1/auth/login": "#C084FC", # Violet
}

STATUS_COLORS = {
    "200": "#10B981", "201": "#34D399", "304": "#38BDF8",
    "400": "#F59E0B", "404": "#FB923C",
    "500": "#EF4444", "502": "#DC2626", "503": "#B91C1C", "504": "#991B1B",
}


def apply_pulsewatch_theme(fig, height=380, margin=None, show_legend=True, legend_y=1.02):
    """Applies a restrained, professional dark observability theme to Plotly figures."""
    m = margin if margin is not None else dict(l=35, r=20, t=35, b=30)
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#111726",
        plot_bgcolor="#0A0E17",
        font=dict(family="Inter, -apple-system, Segoe UI, Roboto, sans-serif", color="#94A3B8", size=11),
        margin=m,
        height=height,
        xaxis=dict(gridcolor="#1E293B", zerolinecolor="#1E293B", showgrid=True),
        yaxis=dict(gridcolor="#1E293B", zerolinecolor="#1E293B", showgrid=True),
    )
    if show_legend:
        fig.update_layout(
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=legend_y,
                xanchor="right",
                x=1.0,
                title="",
                font=dict(size=11, color="#CBD5E1"),
            )
        )
    else:
        fig.update_layout(showlegend=False)
    return fig


# ── Database Data Fetching (Strictly Read-Only) ──────────────────────────────
def fetch_fresh_data():
    """Fetch un-cached, live telemetry rows directly from SQLite."""
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
        events = db.fetch_recent_events_with_anomalies(limit=300)
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


# ── Sidebar Controls & Telemetry Specification ──────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding-bottom: 6px;">
        <div style="font-size: 1.25rem; font-weight: 800; color: #F8FAFC; letter-spacing: -0.02em;">PULSEWATCH</div>
        <div style="font-size: 0.76rem; color: #64748B;">API Observability & Anomaly Telemetry</div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("<hr style='border-color: #1E293B; margin: 8px 0;'>", unsafe_allow_html=True)

    st.markdown("<div style='font-size:0.72rem; font-weight:700; color:#94A3B8; text-transform:uppercase; margin-bottom:4px;'>Stream Controls</div>", unsafe_allow_html=True)
    auto_refresh = st.toggle("Live Polling Auto-Refresh", value=True)
    refresh_seconds = st.slider("Polling Frequency (seconds)", min_value=1, max_value=10, value=3)

    if st.button("Trigger Instant Sync", use_container_width=True):
        st.rerun()

    st.markdown("<hr style='border-color: #1E293B; margin: 12px 0;'>", unsafe_allow_html=True)

    st.markdown("<div style='font-size:0.72rem; font-weight:700; color:#94A3B8; text-transform:uppercase; margin-bottom:4px;'>Route Scope Filter</div>", unsafe_allow_html=True)
    ALL_ENDPOINTS = [
        "/api/v1/users",
        "/api/v1/orders",
        "/api/v1/products",
        "/api/v1/payments",
        "/api/v1/auth/login",
    ]
    selected_endpoints = st.multiselect(
        "Active Routes",
        options=ALL_ENDPOINTS,
        default=ALL_ENDPOINTS,
        label_visibility="collapsed",
    )

    st.markdown("<hr style='border-color: #1E293B; margin: 12px 0;'>", unsafe_allow_html=True)

    st.markdown("<div style='font-size:0.72rem; font-weight:700; color:#94A3B8; text-transform:uppercase; margin-bottom:6px;'>Pipeline Architecture</div>", unsafe_allow_html=True)
    st.markdown(f"""
    <div class="pw-sidebar-spec">
        <div>Broker: <b>Kafka 3.7.0 (KRaft)</b></div>
        <div>Topic: <b>{config.KAFKA_TOPIC}</b></div>
        <div>Engine: <b>IsolationForest (200 trees)</b></div>
        <div>Storage: <b>SQLite 3 (WAL Mode)</b></div>
        <div>SLA Target: <b>&lt; 1,000 ms Response</b></div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<hr style='border-color: #1E293B; margin: 12px 0;'>", unsafe_allow_html=True)
    st.caption("College Mini-Project Evaluation · Production Observability Standard")


# ── Live Dashboard Fragment ───────────────────────────────────────────────────
fragment_interval = f"{refresh_seconds}s" if auto_refresh else None


@st.fragment(run_every=fragment_interval)
def render_live_dashboard():
    kpi, events_df, metrics_df, anomalies_df = fetch_fresh_data()
    now_str = datetime.datetime.now().strftime("%H:%M:%S")

    # ── Header Bar ───────────────────────────────────────────────────────────
    status_badge = (
        f"""<div class="pw-badge-live"><div class="pw-pulse-dot"></div>LIVE TELEMETRY</div>"""
        if auto_refresh else
        """<div class="pw-badge-paused">PAUSED</div>"""
    )

    st.markdown(f"""
    <div class="pw-header-container">
        <div>
            <div class="pw-brand-title">PulseWatch</div>
            <div class="pw-brand-subtitle">Real-Time API Observability & Unsupervised Anomaly Detection</div>
        </div>
        <div class="pw-status-panel">
            {status_badge}
            <div>LAST SYNC: <b style="color: #F8FAFC;">{now_str}</b></div>
            <div style="border-left: 1px solid #1E293B; padding-left: 12px;">TOPIC: <span style="font-family: monospace; color:#38BDF8;">{config.KAFKA_TOPIC}</span></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Section 1: KPI Telemetry Cards ────────────────────────────────────────
    k1, k2, k3, k4, k5 = st.columns(5)

    with k1:
        st.markdown(f"""
        <div class="pw-card">
            <div class="pw-card-label">Total Requests</div>
            <div class="pw-card-value">{kpi['total_events']:,}</div>
            <div class="pw-card-sub">Ingested Kafka events</div>
        </div>
        """, unsafe_allow_html=True)

    with k2:
        st.markdown(f"""
        <div class="pw-card">
            <div class="pw-card-label">Average Latency</div>
            <div class="pw-card-value">{kpi['avg_latency']:.1f}<span class="pw-card-unit">ms</span></div>
            <div class="pw-card-sub">Across all routes</div>
        </div>
        """, unsafe_allow_html=True)

    with k3:
        st.markdown(f"""
        <div class="pw-card">
            <div class="pw-card-label">Peak Latency</div>
            <div class="pw-card-value">{kpi['peak_latency']:.1f}<span class="pw-card-unit">ms</span></div>
            <div class="pw-card-sub">Maximum observed spike</div>
        </div>
        """, unsafe_allow_html=True)

    with k4:
        err_style = "color: #F87171;" if kpi["error_rate_pct"] > 5 else "color: #38BDF8;"
        st.markdown(f"""
        <div class="pw-card">
            <div class="pw-card-label">HTTP Error Rate</div>
            <div class="pw-card-value" style="{err_style}">{kpi['error_rate_pct']:.1f}<span class="pw-card-unit">%</span></div>
            <div class="pw-card-sub">Server failures (5xx)</div>
        </div>
        """, unsafe_allow_html=True)

    with k5:
        anomaly_card_class = "pw-card-anomaly" if kpi["anomaly_count"] > 0 else ""
        st.markdown(f"""
        <div class="pw-card {anomaly_card_class}">
            <div class="pw-card-label">Anomalies Detected</div>
            <div class="pw-card-value">{kpi['anomaly_count']}</div>
            <div class="pw-card-sub">IsolationForest outliers</div>
        </div>
        """, unsafe_allow_html=True)

    # Check for empty database
    if events_df.empty:
        st.markdown("<div style='margin-top:2rem; padding: 20px; background:#1E293B; border-radius:6px; color:#94A3B8;'>Waiting for producer events on Kafka topic <code>api_events</code>...</div>", unsafe_allow_html=True)
        return

    # Filter data based on sidebar multiselect
    active_endpoints = selected_endpoints if selected_endpoints else ALL_ENDPOINTS
    filtered_events = events_df[events_df["endpoint"].isin(active_endpoints)]
    filtered_metrics = metrics_df[metrics_df["endpoint"].isin(active_endpoints)] if not metrics_df.empty else metrics_df

    # ── Section 2: Visual Centerpiece — Latency Time-Series ────────────────────
    st.markdown('<div class="pw-section-header">Live Response Latency & Anomaly Telemetry (ms)</div>', unsafe_allow_html=True)

    if not filtered_events.empty:
        fig_lat = px.line(
            filtered_events,
            x="timestamp",
            y="latency_ms",
            color="endpoint",
            color_discrete_map=ROUTE_COLORS,
            labels={"latency_ms": "Latency (ms)", "timestamp": "Timestamp (UTC)", "endpoint": "API Route"},
        )
        fig_lat.update_traces(line=dict(width=1.75))

        # Overlay detected anomalies as prominent red diamond markers
        anom_points = filtered_events[filtered_events["is_anomaly"] == 1]
        if not anom_points.empty:
            fig_lat.add_trace(go.Scatter(
                x=anom_points["timestamp"],
                y=anom_points["latency_ms"],
                mode="markers",
                name="Flagged Anomaly",
                marker=dict(
                    color="#EF4444",
                    size=10,
                    symbol="diamond",
                    line=dict(width=1.5, color="#FFFFFF"),
                ),
                customdata=anom_points[["endpoint", "status_code", "score"]],
                hovertemplate=(
                    "<b>ANOMALY DETECTED</b><br>"
                    "Route: %{customdata[0]}<br>"
                    "Latency: %{y:.1f} ms<br>"
                    "HTTP Status: %{customdata[1]}<br>"
                    "iForest Score: %{customdata[2]:.3f}<extra></extra>"
                ),
            ))

        # Explicit 1,000 ms SLA Threshold line
        fig_lat.add_hline(
            y=1000,
            line_dash="dash",
            line_color="#F59E0B",
            line_width=1.5,
            annotation_text="1,000 ms SLA Threshold",
            annotation_position="top left",
            annotation_font=dict(color="#F59E0B", size=11, family="Inter, sans-serif"),
        )

        apply_pulsewatch_theme(fig_lat, height=350, show_legend=True, legend_y=1.02)
        st.plotly_chart(fig_lat, use_container_width=True)
    else:
        st.info("No events match the selected route filter.")

    # ── Section 3: Secondary Analytics Breakdown (Two Columns) ─────────────────
    col_breakdown, col_status = st.columns([1.1, 0.9])

    with col_breakdown:
        st.markdown('<div class="pw-section-header">Endpoint Performance Breakdown (Rolling Deque)</div>', unsafe_allow_html=True)
        if not filtered_metrics.empty:
            latest_metrics = filtered_metrics.groupby("endpoint").last().reset_index()
            fig_bar = px.bar(
                latest_metrics,
                x="endpoint",
                y=["rolling_avg_ms", "peak_latency_ms"],
                barmode="group",
                labels={"value": "Latency (ms)", "endpoint": "Route", "variable": "Metric"},
                color_discrete_map={"rolling_avg_ms": "#38BDF8", "peak_latency_ms": "#EF4444"},
            )
            fig_bar.for_each_trace(lambda t: t.update(name="Rolling Avg" if t.name == "rolling_avg_ms" else "Peak Observed"))
            apply_pulsewatch_theme(fig_bar, height=250, show_legend=True, legend_y=1.02)
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("Awaiting rolling window telemetry...")

    with col_status:
        st.markdown('<div class="pw-section-header">HTTP Status & Error Trajectory</div>', unsafe_allow_html=True)
        subtab_dist, subtab_err = st.tabs(["Status Distribution", "Rolling Error Rate"])

        with subtab_dist:
            if not filtered_events.empty:
                status_counts = filtered_events["status_code"].astype(str).value_counts().reset_index()
                status_counts.columns = ["Status Code", "Count"]

                fig_pie = px.pie(
                    status_counts,
                    names="Status Code",
                    values="Count",
                    color="Status Code",
                    color_discrete_map=STATUS_COLORS,
                    hole=0.55,
                )
                fig_pie.update_traces(textposition="outside", textinfo="label+percent")
                apply_pulsewatch_theme(fig_pie, height=220, margin=dict(l=10, r=10, t=10, b=10), show_legend=False)
                st.plotly_chart(fig_pie, use_container_width=True)

        with subtab_err:
            if not filtered_metrics.empty:
                fig_err = px.line(
                    filtered_metrics,
                    x="window_end",
                    y="error_rate",
                    color="endpoint",
                    color_discrete_map=ROUTE_COLORS,
                    labels={"error_rate": "Error Fraction (0–1)", "window_end": "Timestamp"},
                )
                fig_err.update_traces(line=dict(width=1.5))
                apply_pulsewatch_theme(fig_err, height=220, margin=dict(l=25, r=15, t=15, b=20), show_legend=False)
                st.plotly_chart(fig_err, use_container_width=True)
            else:
                st.info("Awaiting error rate metrics...")

    # ── Section 4: Telemetry Tabular Registry (Two Columns) ────────────────────
    tbl_col1, tbl_col2 = st.columns([1, 1])

    with tbl_col1:
        st.markdown('<div class="pw-section-header">Detected Anomaly Registry</div>', unsafe_allow_html=True)
        if not anomalies_df.empty:
            anom_view = anomalies_df[[
                "detected_at", "endpoint", "latency_ms", "score"
            ]].copy()
            anom_view["detected_at"] = anom_view["detected_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
            anom_view["latency_ms"] = anom_view["latency_ms"].apply(lambda v: f"{v:,.1f} ms")
            anom_view["score"] = anom_view["score"].apply(lambda s: f"{s:.3f}")
            anom_view.columns = ["Timestamp", "Route", "Latency", "iForest Score"]
            st.dataframe(anom_view, use_container_width=True, height=230)
        else:
            st.success("No anomalies detected in current stream window.")

    with tbl_col2:
        st.markdown('<div class="pw-section-header">Live Telemetry Feed (Recent 50 Events)</div>', unsafe_allow_html=True)
        if not events_df.empty:
            feed_view = events_df.tail(50)[[
                "timestamp", "endpoint", "status_code", "latency_ms", "is_anomaly"
            ]].copy().sort_values("timestamp", ascending=False)
            feed_view["timestamp"] = feed_view["timestamp"].dt.strftime("%H:%M:%S")
            feed_view["latency_ms"] = feed_view["latency_ms"].apply(lambda v: f"{v:,.1f} ms")
            feed_view["is_anomaly"] = feed_view["is_anomaly"].apply(lambda a: "ANOMALY" if a == 1 else "NORMAL")
            feed_view.columns = ["Time", "Route", "Status", "Latency", "Classification"]
            st.dataframe(feed_view, use_container_width=True, height=230)
        else:
            st.info("No events recorded yet.")


# Execute the fragment
render_live_dashboard()
