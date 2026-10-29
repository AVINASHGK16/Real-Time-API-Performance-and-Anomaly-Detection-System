# Real-Time API Performance Monitoring & Anomaly Detection

A college mini-project demonstrating an end-to-end streaming data pipeline:

```
Python Producer → Apache Kafka → Python Consumer → SQLite → Streamlit Dashboard
                                      ↑
                              IsolationForest (scikit-learn)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Source | Python generator (synthetic API events) |
| Message Broker | Apache Kafka 3.7.0 (KRaft mode) |
| Stream Processor | Python kafka-python consumer |
| ML Model | scikit-learn IsolationForest |
| Database | SQLite (WAL mode) |
| Dashboard | Streamlit + Plotly |

---

## Project Structure

```
.
├── config.py          # Centralised config (reads .env)
├── producer.py        # Kafka producer – synthetic API events
├── consumer.py        # Kafka consumer – rolling metrics + anomaly scoring
├── train_model.py     # One-time IsolationForest training
├── dashboard.py       # Streamlit real-time dashboard
├── db.py              # SQLite persistence layer
├── start_kafka.ps1    # Helper: format + start Kafka (KRaft)
├── requirements.txt   # Pinned dependencies
├── .env.example       # Configuration template
├── .env               # Your local config (gitignored)
├── models/            # Saved ML model (created by train_model.py)
└── data/              # SQLite database (created at runtime)
```

---

## Quick Start

### Prerequisites
- Python 3.12+
- Java 17+ (Java 24 confirmed working)
- Kafka 3.7.0 at `C:\Kafka\kafka_2.13-3.7.0`

### 1. Install Python dependencies
```powershell
pip install -r requirements.txt
```

### 2. Start Kafka (KRaft mode)
Open **PowerShell as Administrator** and run:
```powershell
.\start_kafka.ps1
```
Or manually:
```powershell
# Format storage (first time only)
C:\Kafka\kafka_2.13-3.7.0\bin\windows\kafka-storage.bat format `
  -t (C:\Kafka\kafka_2.13-3.7.0\bin\windows\kafka-storage.bat random-uuid) `
  -c C:\Kafka\kafka_2.13-3.7.0\config\kraft\server.properties

# Start broker
C:\Kafka\kafka_2.13-3.7.0\bin\windows\kafka-server-start.bat `
  C:\Kafka\kafka_2.13-3.7.0\config\kraft\server.properties
```

### 3. Create the Kafka topic
```powershell
C:\Kafka\kafka_2.13-3.7.0\bin\windows\kafka-topics.bat `
  --create --topic api_events --bootstrap-server localhost:9092 `
  --partitions 3 --replication-factor 1
```

### 4. Train the anomaly detection model
```powershell
python train_model.py
```

### 5. Start the producer (Terminal 1)
```powershell
python producer.py
```

### 6. Start the consumer (Terminal 2)
```powershell
python consumer.py
```

### 7. Launch the dashboard (Terminal 3)
```powershell
streamlit run dashboard.py
```
Open http://localhost:8501 in your browser.

---

## Configuration

Copy `.env.example` to `.env` and edit as needed:

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `api_events` | Topic name |
| `KAFKA_GROUP_ID` | `api_monitor_group` | Consumer group |
| `PRODUCER_INTERVAL_SEC` | `0.5` | Seconds between events |
| `ANOMALY_MODEL_PATH` | `models/isolation_forest.pkl` | Saved model path |
| `DB_PATH` | `data/api_monitor.db` | SQLite database path |
| `ROLLING_WINDOW` | `50` | Rolling window size (events) |

---

## How It Works

1. **Producer** emits timestamped API events at `PRODUCER_INTERVAL_SEC` intervals.
   Every 20th event is an intentional spike (latency 1500–6000 ms, 5xx status).

2. **Consumer** reads from Kafka, maintains per-endpoint rolling deques, and for
   every event:
   - Computes rolling avg latency, peak latency, error rate.
   - Scores latency with IsolationForest (`score_samples`).
   - Writes event, metrics, and anomaly record to SQLite.

3. **Dashboard** queries SQLite every 5 s and auto-refreshes with `st.rerun()`.

---

## Milestones

- [x] M1: Environment verification & project skeleton
- [ ] M2: Kafka producer/consumer connectivity
- [ ] M3: Stream processing & rolling metrics
- [ ] M4: ML training & live anomaly scoring
- [ ] M5: SQLite persistence
- [ ] M6: Live Streamlit dashboard
- [ ] M7: End-to-end test & documentation
