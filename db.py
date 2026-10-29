"""
db.py – SQLite persistence layer.

Tables
------
events   : raw API events (deduplicated on event_id)
metrics  : per-window rolling metrics
anomalies: anomaly detection results

All writes are idempotent where practical.
"""
import sqlite3
import logging
import os
from contextlib import contextmanager
import config

logger = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    """Open (or create) the SQLite database and return a connection."""
    os.makedirs(os.path.dirname(config.DB_PATH), exist_ok=True)
    conn = sqlite3.connect(config.DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")   # allow concurrent reads
    return conn


@contextmanager
def managed_connection():
    """Context manager that auto-commits/rolls-back and closes."""
    conn = None
    try:
        conn = get_connection()
        yield conn
        conn.commit()
    except sqlite3.Error as exc:
        logger.error("Database error: %s", exc)
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


def init_db():
    """Create all tables if they do not already exist."""
    with managed_connection() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id    TEXT    UNIQUE,          -- dedup key
            timestamp   TEXT    NOT NULL,
            endpoint    TEXT    NOT NULL,
            status_code INTEGER NOT NULL,
            latency_ms  REAL    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS metrics (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            window_end       TEXT    NOT NULL,
            endpoint         TEXT    NOT NULL,
            rolling_avg_ms   REAL,
            peak_latency_ms  REAL,
            error_rate       REAL,
            sample_count     INTEGER
        );

        CREATE TABLE IF NOT EXISTS anomalies (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id     TEXT,
            detected_at  TEXT    NOT NULL,
            endpoint     TEXT    NOT NULL,
            latency_ms   REAL    NOT NULL,
            score        REAL,
            is_anomaly   INTEGER NOT NULL DEFAULT 0
        );
        """)
    logger.info("Database initialised at '%s'.", config.DB_PATH)


def insert_event(conn: sqlite3.Connection, event: dict, event_id: str) -> bool:
    """Insert a raw event; silently skip if event_id already exists (dedup).
    Returns True if inserted, False if skipped (duplicate).
    """
    try:
        cur = conn.execute(
            """INSERT OR IGNORE INTO events
               (event_id, timestamp, endpoint, status_code, latency_ms)
               VALUES (?, ?, ?, ?, ?)""",
            (event_id, event["timestamp"], event["endpoint"],
             event["status_code"], event["latency_ms"]),
        )
        return cur.rowcount > 0
    except sqlite3.Error as exc:
        logger.error("insert_event failed: %s", exc)
        raise


def insert_metrics(conn: sqlite3.Connection, metrics: dict):
    """Persist a rolling-metrics record."""
    try:
        conn.execute(
            """INSERT INTO metrics
               (window_end, endpoint, rolling_avg_ms, peak_latency_ms,
                error_rate, sample_count)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                metrics["window_end"],
                metrics["endpoint"],
                metrics["rolling_avg_ms"],
                metrics["peak_latency_ms"],
                metrics["error_rate"],
                metrics["sample_count"],
            ),
        )
    except sqlite3.Error as exc:
        logger.error("insert_metrics failed: %s", exc)
        raise


def insert_anomaly(conn: sqlite3.Connection, record: dict):
    """Persist an anomaly detection result."""
    try:
        conn.execute(
            """INSERT INTO anomalies
               (event_id, detected_at, endpoint, latency_ms, score, is_anomaly)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                record["event_id"],
                record["detected_at"],
                record["endpoint"],
                record["latency_ms"],
                record["score"],
                int(record["is_anomaly"]),
            ),
        )
    except sqlite3.Error as exc:
        logger.error("insert_anomaly failed: %s", exc)
        raise


# ── Read helpers used by the dashboard ──────────────────────────────────────

def fetch_recent_events(limit: int = 200) -> list[dict]:
    with managed_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY timestamp DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def fetch_recent_metrics(limit: int = 500) -> list[dict]:
    with managed_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM metrics ORDER BY window_end DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]


def fetch_recent_anomalies(limit: int = 100) -> list[dict]:
    with managed_connection() as conn:
        rows = conn.execute(
            """SELECT * FROM anomalies WHERE is_anomaly = 1
               ORDER BY detected_at DESC LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def fetch_kpi_summary() -> dict:
    """Return headline numbers for the dashboard KPI cards."""
    with managed_connection() as conn:
        row = conn.execute(
            """SELECT
                 COUNT(*)                                  AS total_events,
                 ROUND(AVG(latency_ms), 2)                AS avg_latency,
                 ROUND(MAX(latency_ms), 2)                AS peak_latency,
                 ROUND(
                   SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END)
                   * 100.0 / MAX(COUNT(*), 1), 2
                 )                                        AS error_rate_pct
               FROM events"""
        ).fetchone()
        anomaly_count = conn.execute(
            "SELECT COUNT(*) FROM anomalies WHERE is_anomaly = 1"
        ).fetchone()[0]

        if row and row["total_events"]:
            return {
                "total_events":   row["total_events"],
                "avg_latency":    row["avg_latency"],
                "peak_latency":   row["peak_latency"],
                "error_rate_pct": row["error_rate_pct"],
                "anomaly_count":  anomaly_count,
            }
        return {
            "total_events": 0, "avg_latency": 0.0,
            "peak_latency": 0.0, "error_rate_pct": 0.0,
            "anomaly_count": 0,
        }
