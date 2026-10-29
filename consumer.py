"""
consumer.py – Kafka consumer with rolling metrics and anomaly detection.

For every message consumed it:
1. Validates and parses the event JSON.
2. Updates a per-endpoint rolling window (deque) for latency.
3. Computes rolling average, peak latency, and error rate.
4. Scores the event latency with IsolationForest.
5. Persists everything to SQLite via db.py.
"""
import json
import pickle
import logging
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import numpy as np

import config
import db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

REQUIRED_KEYS = {"timestamp", "endpoint", "status_code", "latency_ms"}


# ── Model loading ────────────────────────────────────────────────────────────

def load_model():
    try:
        with open(config.ANOMALY_MODEL_PATH, "rb") as fh:
            model = pickle.load(fh)
        logger.info("Anomaly model loaded from '%s'.", config.ANOMALY_MODEL_PATH)
        return model
    except FileNotFoundError:
        logger.warning(
            "Model file not found at '%s'. "
            "Run train_model.py first. Anomaly detection will be SKIPPED.",
            config.ANOMALY_MODEL_PATH,
        )
        return None


# ── Rolling metric state ─────────────────────────────────────────────────────

class RollingMetrics:
    """Maintains a sliding window of latencies and error flags per endpoint."""

    def __init__(self, window: int = config.ROLLING_WINDOW):
        self.window   = window
        self.latencies: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))
        self.errors:    dict[str, deque] = defaultdict(lambda: deque(maxlen=window))

    def update(self, endpoint: str, latency_ms: float, is_error: bool):
        self.latencies[endpoint].append(latency_ms)
        self.errors[endpoint].append(1 if is_error else 0)

    def get_metrics(self, endpoint: str) -> dict:
        lats  = list(self.latencies[endpoint])
        errs  = list(self.errors[endpoint])
        count = len(lats)
        if count == 0:
            return {}
        return {
            "window_end":      datetime.now(timezone.utc).isoformat(),
            "endpoint":        endpoint,
            "rolling_avg_ms":  round(float(np.mean(lats)), 2),
            "peak_latency_ms": round(float(np.max(lats)), 2),
            "error_rate":      round(float(np.mean(errs)), 4),
            "sample_count":    count,
        }


# ── Event validation ─────────────────────────────────────────────────────────

def parse_event(raw: bytes) -> dict | None:
    try:
        event = json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning("Malformed message (decode error): %s", exc)
        return None

    missing = REQUIRED_KEYS - event.keys()
    if missing:
        logger.warning("Malformed event – missing keys %s: %s", missing, event)
        return None

    try:
        event["latency_ms"]  = float(event["latency_ms"])
        event["status_code"] = int(event["status_code"])
        if not isinstance(event["timestamp"], str):
            event["timestamp"] = str(event["timestamp"])
    except (TypeError, ValueError) as exc:
        logger.warning("Malformed event – bad field types: %s", exc)
        return None

    return event


# ── Main consumer loop ───────────────────────────────────────────────────────

def run_consumer(max_events: int | None = None, timeout_ms: int | None = None):
    db.init_db()
    model   = load_model()
    roller  = RollingMetrics()

    timeout = -1 if timeout_ms is None else timeout_ms
    logger.info("Connecting to Kafka broker at %s …", config.KAFKA_BROKER)
    consumer = KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BROKER,
        group_id=config.KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: m,   # raw bytes; we parse manually
        consumer_timeout_ms=timeout,      # -1 = block forever, or ms timeout
    )
    logger.info("Consumer ready. Listening on topic '%s' …", config.KAFKA_TOPIC)

    processed_count = 0
    anomaly_count = 0
    duplicate_count = 0

    try:
        for message in consumer:
            event = parse_event(message.value)
            if event is None:
                continue

            endpoint   = event["endpoint"]
            latency_ms = event["latency_ms"]
            is_error   = event["status_code"] >= 500
            event_id   = event.get("event_id") or str(uuid.uuid4())

            # ── Anomaly scoring ──────────────────────────────────────────────
            is_anomaly = False
            score      = None
            if model is not None:
                score      = float(model.score_samples([[latency_ms]])[0])
                prediction = model.predict([[latency_ms]])[0]   # +1 normal, -1 anomaly
                is_anomaly = (prediction == -1)

            # ── Rolling metrics ──────────────────────────────────────────────
            roller.update(endpoint, latency_ms, is_error)
            metrics = roller.get_metrics(endpoint)

            # ── Persist (with deduplication check) ───────────────────────────
            try:
                with db.managed_connection() as conn:
                    inserted = db.insert_event(conn, event, event_id)
                    if not inserted:
                        duplicate_count += 1
                        logger.info("Duplicate event %s skipped.", event_id)
                        continue
                    if metrics:
                        db.insert_metrics(conn, metrics)
                    db.insert_anomaly(conn, {
                        "event_id":   event_id,
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                        "endpoint":   endpoint,
                        "latency_ms": latency_ms,
                        "score":      score,
                        "is_anomaly": is_anomaly,
                    })
            except Exception as exc:
                logger.error("DB write failed (skipping event): %s", exc)
                continue

            processed_count += 1
            if is_anomaly:
                anomaly_count += 1

            flag = "⚠ ANOMALY" if is_anomaly else "OK"
            logger.info(
                "[%s] endpoint=%-22s latency=%.1f ms status=%s score=%s %s",
                str(event["timestamp"])[:19], endpoint, latency_ms,
                event["status_code"],
                f"{score:.3f}" if score is not None else "N/A",
                flag,
            )

            if max_events is not None and processed_count >= max_events:
                logger.info("Reached maximum event count (%d). Stopping consumer.", max_events)
                break

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    except KafkaError as exc:
        logger.error("Kafka error: %s", exc)
        raise
    finally:
        consumer.close()
        logger.info("Consumer closed.")

    return {
        "processed": processed_count,
        "anomalies": anomaly_count,
        "duplicates": duplicate_count,
    }


if __name__ == "__main__":
    run_consumer()
