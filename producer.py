"""
producer.py – Kafka producer: generates synthetic API events continuously.

Events contain:
  - timestamp   : ISO-8601 UTC
  - endpoint    : one of several simulated API routes
  - status_code : HTTP status code (200, 404, 500, …)
  - latency_ms  : response time in milliseconds

Every ~20th event is an intentional spike (high latency or server error)
so the anomaly detector has something to flag in demos.
"""
import json
import random
import time
import uuid
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

ENDPOINTS = [
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v1/products",
    "/api/v1/payments",
    "/api/v1/auth/login",
]

NORMAL_STATUS_WEIGHTS = [200, 200, 200, 200, 201, 304, 400, 404]
ERROR_STATUS_CODES    = [500, 502, 503, 504]


def generate_event(spike: bool = False) -> dict:
    endpoint = random.choice(ENDPOINTS)
    if spike:
        # Latency spike: 2–10× normal range with server error
        latency_ms = round(random.uniform(1500, 6000), 2)
        status_code = random.choice(ERROR_STATUS_CODES)
    else:
        latency_ms = round(random.gauss(mu=120, sigma=30), 2)
        latency_ms = max(10.0, latency_ms)          # floor at 10 ms
        status_code = random.choice(NORMAL_STATUS_WEIGHTS)

    return {
        "event_id":    str(uuid.uuid4()),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "endpoint":    endpoint,
        "status_code": status_code,
        "latency_ms":  latency_ms,
    }


def on_send_success(record_metadata):
    logger.debug(
        "Sent → topic=%s partition=%s offset=%s",
        record_metadata.topic, record_metadata.partition, record_metadata.offset,
    )


def on_send_error(exc):
    logger.error("Failed to send message: %s", exc)


def run_producer(max_events: int | None = None, interval: float | None = None):
    sleep_sec = config.PRODUCER_INTERVAL if interval is None else interval
    logger.info("Connecting to Kafka broker at %s …", config.KAFKA_BROKER)
    producer = KafkaProducer(
        bootstrap_servers=config.KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )
    logger.info("Producer ready. Publishing to topic '%s' every %.2fs …",
                config.KAFKA_TOPIC, sleep_sec)

    event_count = 0
    produced_events = []
    try:
        while True:
            event_count += 1
            spike = (event_count % 20 == 0)          # inject spike every 20 events
            event = generate_event(spike=spike)

            future = producer.send(config.KAFKA_TOPIC, value=event)
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)
            produced_events.append(event)

            label = "SPIKE" if spike else "normal"
            logger.info("[%d/%s] endpoint=%-22s latency=%.1f ms status=%s",
                        event_count, label,
                        event["endpoint"], event["latency_ms"], event["status_code"])

            if max_events is not None and event_count >= max_events:
                logger.info("Reached maximum event count (%d). Stopping producer.", max_events)
                break

            time.sleep(sleep_sec)

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user. Flushing …")
    except KafkaError as exc:
        logger.error("Kafka error: %s", exc)
        raise
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed.")
    return produced_events


if __name__ == "__main__":
    run_producer()
