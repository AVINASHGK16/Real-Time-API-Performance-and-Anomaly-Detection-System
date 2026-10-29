"""
config.py – Centralised configuration loaded from .env
"""
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER        = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC         = os.getenv("KAFKA_TOPIC", "api_events")
KAFKA_GROUP_ID      = os.getenv("KAFKA_GROUP_ID", "api_monitor_group")
PRODUCER_INTERVAL   = float(os.getenv("PRODUCER_INTERVAL_SEC", "0.5"))
ANOMALY_MODEL_PATH  = os.getenv("ANOMALY_MODEL_PATH", "models/isolation_forest.pkl")
DB_PATH             = os.getenv("DB_PATH", "data/api_monitor.db")
ROLLING_WINDOW      = int(os.getenv("ROLLING_WINDOW", "50"))
