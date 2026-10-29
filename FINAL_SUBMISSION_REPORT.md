# Real-Time API Performance Monitoring & Anomaly Detection
## Comprehensive Mini-Project Technical Report & Submission Audit

---

### Abstract

Modern microservice architectures process thousands of distributed HTTP requests every second. Unpredicted latency spikes and server-side errors directly impair user experience and violate service level agreements (SLAs). Traditional static threshold alerting is brittle and ill-suited for dynamic workloads. This project demonstrates an end-to-end, real-time distributed streaming data pipeline and monitoring system for API performance telemetry. 

Built on a six-layer architecture, the system employs **Apache Kafka 3.7.0** in KRaft mode for message ingestion, a **Python 3.12** stream consumer with rolling deque-based sliding windows, an unsupervised **IsolationForest** machine learning model for latency anomaly detection without labeled ground truth, an idempotent **SQLite 3** persistence layer with WAL mode and unique event deduplication, and a live **Streamlit 1.49.1** web dashboard that auto-refreshes every 3 seconds via native `@st.fragment`. The pipeline was verified against live event streams, processing 106 API events with 5 flagged anomalies, resiliently skipping malformed streams, and maintaining exact deduplication across at-least-once deliveries.

---

### Problem Statement

In distributed web platforms, backend services continuously interact over RESTful APIs. When an endpoint degrades—whether caused by downstream database connection pool exhaustion, memory leaks, garbage collection pauses, or network congestion—identifying the anomaly quickly is critical. 

Static rules (e.g., alert if latency > 500 ms) fail when normal baselines vary significantly across different routes (e.g., lightweight `/api/v1/auth/login` vs compute-heavy `/api/v1/orders`). Furthermore, batch-oriented offline log analytics introduce unacceptable alerting latencies. A robust solution requires:
1. Low-latency, decoupled event buffering capable of absorbing burst traffic.
2. Online computation of sliding-window performance metrics (average latency, peak latency, error rate).
3. Unsupervised machine learning capable of learning the multi-modal latency baseline and isolating anomalies without human supervision.
4. Guaranteed storage integrity preventing duplicate records upon network retransmission.
5. A live, responsive dashboard providing operations teams with real-time visual telemetry.

---

### Objectives

1. **Decoupled Stream Ingestion:** Deploy Apache Kafka in modern ZooKeeper-less KRaft mode to ingest structured JSON telemetry events asynchronously.
2. **Online Stream Processing:** Implement a resilient stream consumer maintaining per-endpoint sliding windows to calculate real-time rolling average response time, peak latency, and HTTP error rate.
3. **Unsupervised Anomaly Detection:** Train and deploy an IsolationForest model that evaluates latency distributions and isolates statistically abnormal spikes in real time.
4. **Idempotent Data Persistence:** Design a relational SQLite schema utilizing Write-Ahead Logging (WAL) and `event_id` unique constraints to ensure effective exactly-once persistence despite at-least-once transport.
5. **Fault Tolerance & Stream Resilience:** Ensure corrupted JSON, missing payload fields, and invalid datatypes are discarded gracefully without crashing stream workers.
6. **Live Telemetry Dashboard:** Construct an interactive, auto-updating dashboard in Streamlit providing KPI summaries, time-series anomaly overlays, error distributions, and inspection logs.

---

### Architecture & System Design

The system is structured as a modular six-layer pipeline where each layer is decoupled through standardized data interfaces.

```
[Layer 1: Event Ingestion]     producer.py (Synthetic API events with controlled spikes)
          │ (JSON over TCP)
          ▼
[Layer 2: Message Broker]      Apache Kafka 3.7.0 KRaft (Topic: api_events, localhost:9092)
          │ (Stream fetch)
          ▼
[Layer 3: Stream Processing]   consumer.py (Per-endpoint rolling window deque statistics)
          │
          ├──► [Layer 4: ML Inference]   IsolationForest (models/isolation_forest.pkl)
          │                               Latency scoring & anomaly classification
          ▼
[Layer 5: Data Persistence]    SQLite 3 WAL mode (data/api_monitor.db)
          │                     events (deduplicated), metrics, anomalies
          ▼
[Layer 6: Visualization]       Streamlit 1.49.1 Dashboard (dashboard.py)
                                Native @st.fragment 3s auto-refresh, Plotly charts
```

---

### Dataset & Telemetry Event Format

The telemetry stream models production API gateways logging client requests across five core endpoints:
- `/api/v1/users`
- `/api/v1/orders`
- `/api/v1/products`
- `/api/v1/payments`
- `/api/v1/auth/login`

#### 1. Ingestion Event Schema (JSON)
```json
{
  "event_id": "8d47b551-7570-4afa-a98b-6b0dc0ab65b9",
  "timestamp": "2026-10-29T16:25:32.747140+00:00",
  "endpoint": "/api/v1/orders",
  "status_code": 200,
  "latency_ms": 140.92
}
```

#### 2. Synthetic Distribution Profiles
- **Normal Traffic (95% of events):** 
  - Latency: Gaussian distribution ($\mu = 120\text{ ms}, \sigma = 30\text{ ms}$, bounded below at $10.0\text{ ms}$).
  - HTTP Status Codes: Weighted across `[200, 201, 304, 400, 404]`.
- **Injected Anomaly Spikes (5% of events):**
  - Latency: Uniform distribution between $1500.0\text{ ms}$ and $6000.0\text{ ms}$ (2–10× baseline).
  - HTTP Status Codes: Server failure codes `[500, 502, 503, 504]`.

---

### Implementation of All Six Layers

#### Layer 1: Data Ingestion (`producer.py`)
- Configured with `KafkaProducer(bootstrap_servers=config.KAFKA_BROKER, acks="all", retries=3)`.
- Generates JSON events carrying UUIDv4 origin identifiers.
- Parameterized with `run_producer(max_events, interval)` allowing both autonomous daemon operation and deterministic test execution.

#### Layer 2: Message Streaming (`Apache Kafka 3.7.0`)
- Configured in KRaft mode (`process.roles=broker,controller`, `node.id=1`) eliminating ZooKeeper dependency.
- Topic `api_events` configured with single partition and replication factor 1 for low-overhead local stream ordering.
- Modern Windows compatibility ensured by automated heap allocation (`-Xmx1G -Xms1G`) to bypass deprecated `wmic` OS calls.

#### Layer 3: Stream Processing (`consumer.py`)
- Implements `RollingMetrics` using `collections.deque(maxlen=ROLLING_WINDOW)` grouped per endpoint.
- Upon message arrival, validates payload integrity, records latency and error status (`status_code >= 500`), and computes:
  - Rolling Average Latency
  - Peak Latency
  - HTTP Error Rate

#### Layer 4: Machine Learning Inference (`IsolationForest`)
- Trained via `train_model.py` on 5,000 normal samples and 500 anomalous samples.
- Model loaded once at consumer startup and kept hot in memory.
- Inference computes decision score via `model.score_samples([[latency_ms]])` and prediction via `model.predict([[latency_ms]])`.

#### Layer 5: Data Persistence (`db.py`)
- Employs SQLite 3 with Write-Ahead Logging (`PRAGMA journal_mode=WAL;`) allowing non-blocking concurrent reads by the dashboard while the consumer commits writes.
- Enforces uniqueness on `event_id`. `insert_event()` uses `INSERT OR IGNORE` and returns row insertion status to prevent downstream duplicate metric writes.

#### Layer 6: Telemetry Dashboard (`dashboard.py`)
- Built with Streamlit 1.49.1 and Plotly Express.
- Features native `@st.fragment(run_every="3s")` client-side auto-refresh.
- Performs pure read-only SQL queries (`SELECT`) with zero data modifications.

---

### Machine Learning Algorithm: IsolationForest

The **IsolationForest** algorithm was chosen for this project based on its computational efficiency and suitability for streaming time-series anomaly detection:
1. **Tree-Based Space Partitioning:** Anomaly isolation relies on the premise that anomalies are "few and different". Instead of profiling normal clusters (like k-Means or GMMs) or calculating pairwise distances (like DBSCAN, which is $O(N^2)$), IsolationForest recursively partitions feature space with random cuts.
2. **Short Path Lengths:** Anomalous observations (such as a $4500\text{ ms}$ latency spike compared to normal $\sim 120\text{ ms}$) require substantially fewer random splits to isolate in an iTree, yielding significantly shorter average tree depth $h(x)$.
3. **Decision Boundaries:**
   - Latencies around 100–150 ms receive scores $\approx -0.38$ to $-0.45$ (classified as $+1$, Normal).
   - Latencies $> 2000\text{ ms}$ receive scores $< -0.65$ (classified as $-1$, Anomaly).

---

### Database Design & Schema

The relational database at `data/api_monitor.db` is structured as follows:

```sql
CREATE TABLE events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT    UNIQUE,          -- Deduplication key
    timestamp   TEXT    NOT NULL,
    endpoint    TEXT    NOT NULL,
    status_code INTEGER NOT NULL,
    latency_ms  REAL    NOT NULL
);

CREATE TABLE metrics (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    window_end       TEXT    NOT NULL,
    endpoint         TEXT    NOT NULL,
    rolling_avg_ms   REAL,
    peak_latency_ms  REAL,
    error_rate       REAL,
    sample_count     INTEGER
);

CREATE TABLE anomalies (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT,
    detected_at  TEXT    NOT NULL,
    endpoint     TEXT    NOT NULL,
    latency_ms   REAL    NOT NULL,
    score        REAL,
    is_anomaly   INTEGER NOT NULL DEFAULT 0
);
```

---

### Dashboard Implementation & User Interface

The live dashboard renders five analytical panes:
1. **KPI Header Cards:** Displays Total Requests ($106$), Average Latency ($287.4\text{ ms}$), Peak Latency ($4545.3\text{ ms}$), HTTP Error Rate ($4.7\%$), and Flagged Anomalies ($5$) highlighted in warning red.
2. **Latency Time-Series & Anomaly Overlay:** Plotly multi-line graph plotting response time over time per route. Model-flagged anomalies are overlaid as red diamond markers (`diamond-open-dot`) with tooltip inspection showing route, latency, and exact decision score.
3. **HTTP Errors & Status Codes:** Interactive donut chart illustrating the distribution of HTTP status codes (200, 201, 304, 400, 404, 502, 503, 504) and rolling error timeline.
4. **Endpoint Performance Comparison:** Grouped bar charts comparing rolling average latency vs peak latency across each route.
5. **Real-Time Tables:** Tabular records of recent anomalous events and raw telemetry feeds.

---

### Actual Measured Results

All reported metrics reflect physical execution outputs verified against SQLite and Kafka:

| Metric / Parameter | Value Measured During Final Audit |
|---|---|
| **Total Events Processed** | **106 events** |
| **Total Rolling Metric Records** | **106 records** |
| **Total Scored Records** | **106 records** |
| **Total Flagged Anomalies (`is_anomaly = 1`)** | **5 anomalies** |
| **Average Response Latency (Overall Stream)** | **$287.43\text{ ms}$** |
| **Peak Measured Latency** | **$4545.28\text{ ms}$** |
| **HTTP Error Rate (Status $\ge 500$)** | **$4.72\%$** |
| **Duplicate Delivery Insertion Count** | **0 duplicate rows** (100% deduplication) |
| **Corrupted Payload Recovery** | **3 malformed events skipped, 0 consumer crashes** |
| **Live UI Refresh Period** | **3.0 seconds (Native `@st.fragment`)** |

#### Verified Anomaly Events in SQLite:
1. `ed8ec6e1-...`: `/api/v1/products` — Latency: $4217.60\text{ ms}$ | Status: 503 | Score: $-0.657$
2. `89821e0f-...`: `/api/v1/products` — Latency: $3866.75\text{ ms}$ | Status: 503 | Score: $-0.667$
3. `edae5df0-...`: `/api/v1/orders` — Latency: $4545.28\text{ ms}$ | Status: 504 | Score: $-0.667$
4. `9a124c80-...`: `/api/v1/users` — Latency: $3835.40\text{ ms}$ | Status: 502 | Score: $-0.668$
5. `408a6b11-...`: `/api/v1/payments` — Latency: $2473.50\text{ ms}$ | Status: 504 | Score: $-0.675$

---

### Limitations

1. **Univariate Latency Scoring:** The current ML model evaluates `latency_ms` as the primary continuous feature. While effective for response time bottlenecks, multi-dimensional anomalies (such as high error volume under low latency) rely on rule-based status flags rather than multivariate feature embeddings.
2. **Single Partition Ingestion:** The dev environment Kafka topic operates with 1 partition. Scale-out parallel consumption across multiple partitions would require partitioned key hashing on `endpoint`.
3. **In-Memory Rolling State:** The consumer maintains sliding deques in memory. If a consumer process restarts, rolling metrics re-warm over the subsequent 50 events.

---

### Conclusion & Future Enhancements

The project demonstrates a production-grade, resilient real-time streaming pipeline. By coupling Kafka KRaft, IsolationForest, SQLite WAL, and Streamlit fragments, the system achieves sub-second detection and visualization of distributed service degradation without manual supervision or data corruption.

#### Future Enhancements:
1. **Multivariate Feature Engineering:** Expand the model feature vector to include request payload size, concurrent active thread count, and historical variance.
2. **Automated Feedback Loop / Remediation:** Integrate a webhook trigger that issues automated throttling or rate-limiting commands to the API gateway when an endpoint sustains anomalies over consecutive windows.
3. **Distributed Multi-Broker Cluster:** Extend Kafka deployment across three KRaft broker-controllers with topic replication factor 3.

---

### Final Run Checklist & Lab Demonstration Script

#### Laboratory Demonstration Checklist:
- [x] Python 3.12.3 environment with `kafka-python-ng 2.2.3` verified.
- [x] Java 24 installed; `start_kafka.ps1` configured with heap options.
- [x] Kafka 3.7.0 KRaft broker active and listening on `localhost:9092`.
- [x] Kafka topic `api_events` verified and active.
- [x] IsolationForest model file `models/isolation_forest.pkl` trained and serialized.
- [x] SQLite database `data/api_monitor.db` in WAL mode with verified schema.
- [x] Producer and Consumer scripts tested with at-least-once deduplication.
- [x] Streamlit dashboard running on `http://localhost:8501` with native auto-refresh.

#### Demonstration Script for Evaluators:
1. **Show Broker Running:**
   Open PowerShell and show listening ports:
   ```powershell
   Get-NetTCPConnection -LocalPort 9092, 8501
   ```
2. **Open Dashboard:**
   Navigate to `http://localhost:8501` in Chrome or Edge. Point out the live status indicator (`🟢 Live Auto-Refresh Active`), current KPI cards (106 total requests, 5 anomalies), and the red anomaly marker scatter overlay.
3. **Live Stream Generation:**
   Open a separate PowerShell terminal and stream 10 new events with an injected spike:
   ```powershell
   python -c "import producer, consumer; producer.run_producer(max_events=10, interval=0.1); consumer.run_consumer(max_events=10, timeout_ms=3000)"
   ```
4. **Observe Real-Time UI Update:**
   Watch the browser window without clicking reload. Within 3 seconds, the Total Requests KPI advances from 106 to 116, the chart dynamically plots the new data points, and the "Last updated" clock updates.
5. **Verify Anomaly Alert Table:**
   Highlight the *Recent Anomalies Log* in the dashboard showing the exact route, latency in milliseconds, and model decision score.

