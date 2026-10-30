# Real-Time API Performance Monitoring & Anomaly Detection

A production-grade streaming data pipeline and live monitoring dashboard demonstrating real-time unsupervised anomaly detection:

```
Python Producer → Apache Kafka (KRaft) → Python Consumer → SQLite (WAL) → Streamlit Dashboard
                                               ↑
                                  IsolationForest (scikit-learn)
```

---

## Tech Stack & Six-Layer Architecture

| Layer | Component | Technology / Role |
|---|---|---|
| **1. Data Ingestion** | Synthetic API Producer | Python event generator with normal distribution and controlled anomaly spikes |
| **2. Message Streaming** | Message Broker | Apache Kafka 3.7.0 (KRaft mode, ZooKeeperless) |
| **3. Stream Processing** | Real-Time Consumer | `kafka-python-ng 2.2.3` with sliding deques for rolling statistics |
| **4. Anomaly Detection** | Unsupervised ML | scikit-learn `IsolationForest` scoring response latencies |
| **5. Data Persistence** | Relational Storage | SQLite 3 (WAL mode) with idempotent unique event deduplication |
| **6. Visualization** | Live Dashboard | Streamlit 1.49.1 + Plotly with native `@st.fragment` auto-refresh |

---

## Project Structure

```
.
├── config.py          # Centralised config (reads environment variables from .env)
├── producer.py        # Kafka producer – generates synthetic API events with spikes
├── consumer.py        # Kafka consumer – rolling metrics calculation + anomaly scoring
├── train_model.py     # IsolationForest model training (generates models/isolation_forest.pkl)
├── dashboard.py       # Streamlit real-time monitoring dashboard
├── db.py              # SQLite persistence layer and query helpers
├── start_kafka.ps1    # Automated helper: checks JAVA_HOME, formats & starts Kafka KRaft
├── requirements.txt   # Pinned Python dependencies (Python 3.12 compatible)
├── .env.example       # Configuration template
├── .env               # Active environment configuration
├── models/            # Serialized ML model (isolation_forest.pkl)
└── data/              # SQLite database (api_monitor.db)
```

---

## Step-by-Step Windows Setup & Execution Guide

### Prerequisites
- Python 3.12+ (tested on Python 3.12.3)
- Java 17+ or Java 24 (verified with JDK 24 at `C:\Program Files\Java\jdk-24`)
- Apache Kafka 3.7.0 installed at `C:\Kafka\kafka_2.13-3.7.0`

### 1. Install Python Dependencies
```powershell
pip install -r requirements.txt
```

### 2. Start Kafka in KRaft Mode (Terminal 1)
Run the automated startup script:
```powershell
.\start_kafka.ps1
```
*Note: `start_kafka.ps1` automatically detects valid `JAVA_HOME`, sets `KAFKA_HEAP_OPTS` to avoid deprecated `wmic` calls on modern Windows, formats KRaft storage if first run, and starts the broker on `localhost:9092`.*

Or start manually:
```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-24"
$env:KAFKA_HEAP_OPTS = "-Xmx1G -Xms1G"

# Format KRaft storage (first time only)
$uuid = & "C:\Kafka\kafka_2.13-3.7.0\bin\windows\kafka-storage.bat" random-uuid
& "C:\Kafka\kafka_2.13-3.7.0\bin\windows\kafka-storage.bat" format -t $uuid -c "C:\Kafka\kafka_2.13-3.7.0\config\kraft\server.properties"

# Start Kafka broker
& "C:\Kafka\kafka_2.13-3.7.0\bin\windows\kafka-server-start.bat" "C:\Kafka\kafka_2.13-3.7.0\config\kraft\server.properties"
```

### 3. Verify / Create Kafka Topic
Verify broker reachability and create the `api_events` topic if not already present:
```powershell
& "C:\Kafka\kafka_2.13-3.7.0\bin\windows\kafka-topics.bat" --bootstrap-server localhost:9092 --create --topic api_events --partitions 1 --replication-factor 1
```

### 4. Train the IsolationForest Model
Train the unsupervised anomaly detector:
```powershell
python train_model.py
```
*Saves the trained model to `models/isolation_forest.pkl`.*

### 5. Initialize the SQLite Database
Ensure tables (`events`, `metrics`, `anomalies`) are initialized:
```powershell
python -c "import db; db.init_db()"
```

### 6. Start the Stream Consumer (Terminal 2)
```powershell
python consumer.py
```
*The consumer listens to `api_events`, updates per-endpoint rolling metrics, scores latencies with the ML model, and persists records to SQLite.*

### 7. Start the API Event Producer (Terminal 3)
```powershell
python producer.py
```
*The producer emits continuous synthetic API events. Every 20th event is an intentional spike (1500–6000 ms, 5xx error) to demonstrate anomaly detection.*

### 8. Launch the Live Dashboard (Terminal 4)
```powershell
streamlit run dashboard.py
```
*Open `http://localhost:8501` in your browser. The dashboard automatically refreshes every 3 seconds via Streamlit's native `st.fragment`.*

---

## Configuration Reference (`.env`)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap broker address |
| `KAFKA_TOPIC` | `api_events` | Kafka topic name |
| `KAFKA_GROUP_ID` | `api_monitor_group` | Consumer group identifier |
| `PRODUCER_INTERVAL_SEC` | `0.5` | Generation interval between API events |
| `ANOMALY_MODEL_PATH` | `models/isolation_forest.pkl` | Path to serialized IsolationForest |
| `DB_PATH` | `data/api_monitor.db` | SQLite database file location |
| `ROLLING_WINDOW` | `50` | Number of events in sliding deque window |

---

## Key Technical Features

1. **At-Least-Once Delivery with Idempotent Deduplication:**
   Every event carries a unique `event_id` (UUIDv4). The database enforces `event_id TEXT UNIQUE`. If Kafka delivers a duplicate record, `INSERT OR IGNORE` suppresses duplicate insertion and bypasses duplicate metric/anomaly logging.
2. **Resilience to Corrupted Streams:**
   The consumer validates JSON payloads, missing keys, and invalid datatypes without terminating or dropping the partition assignment.
3. **Unsupervised Anomaly Detection:**
   Zero ground-truth labels required. The IsolationForest models the multi-modal latency distribution and flags extreme deviations and server bottlenecks in real time.
4. **Native Auto-Refresh UI:**
   Built using Streamlit's `@st.fragment(run_every="3s")` to poll SQLite WAL without full-page reloads, preserving chart zoom and UI state.

---

## Milestone Completion

- [x] **Milestone 1:** Environment verification, project structure & configuration.
- [x] **Milestone 2:** Kafka KRaft broker startup, topic creation & Python 3.12 protocol compatibility fix (`kafka-python-ng 2.2.3`).
- [x] **Milestone 3:** Full streaming pipeline integration (Producer → Kafka → Consumer → IsolationForest → SQLite) with deduplication & fault tolerance.
- [x] **Milestone 4:** Live Streamlit monitoring dashboard with real-time KPI cards, time-series anomaly overlays, and native auto-refresh.
- [x] **Final Submission Audit:** Documentation, lab demonstration checklist, and comprehensive academic report.

---

## Submission Deliverables

- **Comprehensive Project Report:** [`FINAL_SUBMISSION_REPORT.md`](FINAL_SUBMISSION_REPORT.md)
- **Verified Dashboard Screenshot:** [`dashboard_final_submission.png`](dashboard_final_submission.png)
- **Automated Kafka Startup Script:** [`start_kafka.ps1`](start_kafka.ps1)

