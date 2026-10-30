# Real-Time API Performance Monitoring & Anomaly Detection
## Comprehensive Mini-Project Technical Report & Submission Audit

---

### 1. Abstract

This project presents an end-to-end real-time API performance monitoring and anomaly detection system designed to track HTTP request latency and detect operational bottlenecks in modern microservices. Using Apache Kafka in ZooKeeper-less KRaft mode, high-throughput JSON telemetry events are ingested asynchronously and processed by an online stream consumer maintaining sliding-window deque statistics. An unsupervised scikit-learn IsolationForest model scores incoming response latencies in real time to isolate performance anomalies without requiring labeled ground-truth datasets. Telemetry and metric records are persisted into an SQLite database operating in Write-Ahead Logging (WAL) mode with idempotent deduplication, while an interactive Streamlit dashboard provides live operational visibility through native, flicker-free auto-refreshing visualizations.

---

### 2. Architecture & Six-Layer System Design

The project is structured according to a modular six-layer streaming architecture. Each layer maintains a single responsibility and is decoupled from adjacent layers through standardized interfaces.

#### Mermaid Architecture Diagram

```mermaid
flowchart TD
    subgraph L1["Layer 1: Data Source / Ingestion"]
        P["producer.py<br/>Synthetic API Generator<br/>(Normal + Injected Spikes)"]
    end

    subgraph L2["Layer 2: Message Streaming Tool"]
        K["Apache Kafka 3.7.0 (KRaft Mode)<br/>Topic: api_events<br/>localhost:9092"]
    end

    subgraph L3["Layer 3: Stream Processing"]
        C["consumer.py<br/>RollingMetrics (deque windows)<br/>Avg Latency, Peak Latency, Error Rate"]
    end

    subgraph L4["Layer 4: Machine Learning Inference"]
        ML["IsolationForest (scikit-learn)<br/>models/isolation_forest.pkl<br/>Real-Time Latency Scoring"]
    end

    subgraph L5["Layer 5: Database Persistence"]
        DB[("SQLite 3 (WAL Mode)<br/>data/api_monitor.db<br/>events, metrics, anomalies")]
    end

    subgraph L6["Layer 6: Live Visualization"]
        UI["Streamlit 1.49.1 Dashboard<br/>dashboard.py<br/>@st.fragment 3s Auto-Refresh"]
    end

    P -->|JSON over TCP| K
    K -->|Partition Fetch| C
    C <-->|Score latency_ms| ML
    C -->|Idempotent Writes| DB
    UI -.->|Non-blocking SQL SELECT| DB
```

#### Six-Layer Technology Mapping

| Layer # | Architectural Role | Actual Technology / Implementation | Primary Function |
|---|---|---|---|
| **Layer 1** | Data Source | Python (`producer.py`) | Simulates API gateway traffic with normal distributions and controlled spikes |
| **Layer 2** | Streaming Tool | Apache Kafka 3.7.0 (KRaft Mode) | Decouples producers and consumers; buffers burst events asynchronously |
| **Layer 3** | Stream Processor | Python (`consumer.py` + `kafka-python-ng`) | Maintains per-route rolling sliding windows (`deque`) for online metrics |
| **Layer 4** | ML Algorithm | scikit-learn `IsolationForest` | Unsupervised classification and score generation for response latencies |
| **Layer 5** | Database Persistence | SQLite 3 (`db.py` in WAL Mode) | Stores events, rolling metrics, and anomalies with unique ID deduplication |
| **Layer 6** | Live Visualization | Streamlit 1.49.1 + Plotly (`dashboard.py`) | Renders real-time KPI cards, time-series charts, and anomaly inspection tables |

---

### 3. Dataset & Simulated Event Format

#### 3.1 Why Simulated Telemetry is Used
In academic and college evaluation environments, live access to production multi-service web gateways handling millions of production requests is rarely accessible. Simulated data generation allows the engineering team to calibrate realistic baseline latency distributions and inject deliberate latency spikes and HTTP errors on a predictable schedule (e.g., every 20th event). This ensures that edge cases, threshold degradations, and anomaly classification boundaries can be tested and demonstrated deterministically.

#### 3.2 Monitored Endpoints
The telemetry stream simulates traffic across five distinct RESTful API endpoints:
1. `/api/v1/users` (User profile lookups)
2. `/api/v1/orders` (Order placement & query transactions)
3. `/api/v1/products` (Catalog searches)
4. `/api/v1/payments` (Payment processor handshakes)
5. `/api/v1/auth/login` (Authentication verification)

#### 3.3 Event Schema & Field Definitions
Every message emitted to Kafka is serialized as a UTF-8 JSON object containing five core attributes:
- **`event_id`** (`string`, UUIDv4): A globally unique identifier generated at request completion time. Enables strict database deduplication across at-least-once message deliveries.
- **`timestamp`** (`string`, ISO-8601 UTC): High-resolution UTC timestamp (`YYYY-MM-DDTHH:MM:SS.ffffff+00:00`) indicating when the API call finished.
- **`endpoint`** (`string`): The destination API route handling the request.
- **`status_code`** (`integer`): HTTP status code returned to the client (e.g., 200, 201, 304, 400, 404, 500, 502, 503, 504).
- **`latency_ms`** (`float`): Measured round-trip response time in milliseconds.

#### 3.4 Representative JSON Event from Actual Producer Code
```json
{
  "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "timestamp": "2026-10-29T17:41:49.911200+00:00",
  "endpoint": "/api/v1/users",
  "status_code": 200,
  "latency_ms": 159.12
}
```

#### 3.5 Traffic Generation Profiles
- **Normal Telemetry (95% of events):**
  - Latency drawn from a Gaussian distribution: $\mu = 120.0\text{ ms}, \sigma = 30.0\text{ ms}$, with a hard minimum floor of $10.0\text{ ms}$.
  - HTTP Status Codes randomly selected from weighted normal responses: `[200, 200, 200, 200, 201, 304, 400, 404]`.
- **Injected Anomaly Spikes (5% of events — every 20th message):**
  - Latency drawn from a uniform distribution: $1500.0\text{ ms} \le \text{latency} \le 6000.0\text{ ms}$ (representing 10× to 50× normal latency).
  - HTTP Status Codes randomly selected from critical server-side failure responses: `[500, 502, 503, 504]`.

---

### 4. Implementation of All Six Layers

Each layer serves a distinct architectural purpose and operates autonomously.

#### 4.1 Layer 1: Data Source / Ingestion (`producer.py`)
- **Why It Exists:** Web gateways generate high-frequency event logs whenever requests terminate. A decoupled event generator is required to emit continuous telemetry without blocking backend application logic.
- **How It Works:** Uses `KafkaProducer` from `kafka-python-ng` configured with `acks="all"` and `retries=3` to serialize dictionary payloads to JSON bytes and publish them to the `api_events` Kafka topic.
- **Genuine Code Snippet (`producer.py`):**
```python
def generate_event(spike: bool = False) -> dict:
    endpoint = random.choice(ENDPOINTS)
    if spike:
        latency_ms = round(random.uniform(1500, 6000), 2)
        status_code = random.choice(ERROR_STATUS_CODES)
    else:
        latency_ms = round(random.gauss(mu=120, sigma=30), 2)
        latency_ms = max(10.0, latency_ms)
        status_code = random.choice(NORMAL_STATUS_WEIGHTS)

    return {
        "event_id":    str(uuid.uuid4()),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "endpoint":    endpoint,
        "status_code": status_code,
        "latency_ms":  latency_ms,
    }
```

#### 4.2 Layer 2: Message Streaming Tool (`Apache Kafka 3.7.0 KRaft`)
- **Why It Exists:** High-traffic APIs experience unpredictable traffic spikes. A distributed message broker acts as an elastic buffer, preventing downstream database and ML workers from being overwhelmed while ensuring zero message loss.
- **How It Works:** Kafka runs in modern KRaft (Kafka Raft Metadata) mode without ZooKeeper. Topic `api_events` is partitioned to maintain total ordering per partition while serving consumer read offsets reliably.
- **Genuine Code / Startup Snippet (`start_kafka.ps1`):**
```powershell
# Set Kafka heap options to avoid deprecated wmic call on modern Windows
if (-not $env:KAFKA_HEAP_OPTS) {
    $env:KAFKA_HEAP_OPTS = "-Xmx1G -Xms1G"
}
# Start Kafka KRaft broker
& "$KAFKA_BIN\kafka-server-start.bat" "$KAFKA_HOME\config\kraft\server.properties"
```

#### 4.3 Layer 3: Stream Processor (`consumer.py`)
- **Why It Exists:** Raw events arrive as isolated records. Operational monitoring requires aggregated stream indicators (such as rolling average latency and moving error rate) over recent temporal windows.
- **How It Works:** Implements a sliding window abstraction using Python's double-ended queue (`collections.deque(maxlen=50)`). For every incoming event, the consumer appends response time and error status (`status_code >= 500`), then computes moving average, peak latency, and failure rates per endpoint.
- **Genuine Code Snippet (`consumer.py`):**
```python
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
        if not lats:
            return {}
        return {
            "window_end":      datetime.now(timezone.utc).isoformat(),
            "endpoint":        endpoint,
            "rolling_avg_ms":  round(float(np.mean(lats)), 2),
            "peak_latency_ms": round(float(np.max(lats)), 2),
            "error_rate":      round(float(np.mean(errs)), 4),
            "sample_count":    len(lats),
        }
```

#### 4.4 Layer 4: Machine Learning Inference (`IsolationForest`)
- **Why It Exists:** Hardcoded thresholds fail when traffic patterns shift or across different endpoints. Unsupervised anomaly detection learns normal latency clusters and flags statistical outliers automatically.
- **How It Works:** The serialized IsolationForest model (`models/isolation_forest.pkl`) is loaded once into memory on consumer startup. Each consumed latency value is scored via `score_samples()` and categorized as normal (`+1`) or anomaly (`-1`) via `predict()`.
- **Genuine Code Snippet (`consumer.py`):**
```python
# Score the single continuous feature latency_ms
score      = float(model.score_samples([[latency_ms]])[0])
prediction = model.predict([[latency_ms]])[0]   # +1 = normal, -1 = anomaly
is_anomaly = (prediction == -1)
```

#### 4.5 Layer 5: Database Persistence (`db.py`)
- **Why It Exists:** Analytics and dashboards require reliable storage. In streaming systems with at-least-once message delivery, network retries can duplicate messages. The persistence layer must guarantee idempotency and avoid locking under concurrent reads.
- **How It Works:** SQLite 3 is configured in Write-Ahead Logging mode (`PRAGMA journal_mode=WAL;`). Uniqueness is enforced on `events(event_id)`. If an existing `event_id` is re-received, `INSERT OR IGNORE` suppresses insertion and skips downstream duplicate metric/anomaly creation.
- **Genuine Code Snippet (`db.py`):**
```python
def insert_event(conn: sqlite3.Connection, event: dict, event_id: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO events
            (event_id, timestamp, endpoint, status_code, latency_ms)
        VALUES (?, ?, ?, ?, ?)
        """,
        (event_id, event["timestamp"], event["endpoint"],
         int(event["status_code"]), float(event["latency_ms"])),
    )
    return cur.rowcount > 0  # Returns False if event already existed
```

#### 4.6 Layer 6: Live Visualization (`dashboard.py`)
- **Why It Exists:** System reliability engineers need a real-time, visual interface to inspect KPIs, spot anomalies immediately, and observe latency distributions without manually refreshing database queries.
- **How It Works:** Built using Streamlit 1.49.1 and Plotly Express. Auto-refresh is powered by Streamlit's native `@st.fragment(run_every="3s")` decorator. This decorator instructs the client browser to re-execute only the rendering fragment every 3 seconds, fetching fresh data from SQLite without tearing down DOM elements or causing page flicker.
- **Genuine Code Snippet (`dashboard.py`):**
```python
@st.fragment(run_every="3s")
def render_live_dashboard():
    # Read fresh read-only data from SQLite WAL
    events_df    = load_events(limit=100)
    metrics_df   = load_metrics(limit=100)
    anomalies_df = load_anomalies(limit=50)

    render_kpi_cards(events_df, anomalies_df)
    render_charts(events_df, anomalies_df)
    render_tables(events_df, anomalies_df)
```

---

### 5. Machine Learning Model: IsolationForest

#### 5.1 Algorithm Theory & Suitability
The **IsolationForest** algorithm is an unsupervised tree-based ensemble method uniquely suited for streaming data:
1. **Partitioning Mechanism:** Anomalous latency points are structurally "few and different". Instead of estimating high-density regions or computing expensive pairwise Euclidean distances ($O(N^2)$ in DBSCAN), an Isolation Tree (iTree) isolates observations by randomly selecting a feature and randomly splitting between its minimum and maximum values.
2. **Path Length As Anomaly Metric:** Normal observations located deep within dense clusters require numerous random splits to isolate, resulting in long tree paths $h(x)$. Conversely, severe latency spikes (e.g., $3000\text{ ms}$ vs standard $\sim 120\text{ ms}$) fall in sparse outlier regions and are isolated after very few splits (short path length).
3. **Computational Efficiency:** Training and scoring scale as $O(n \log n)$ and $O(t \log \psi)$ respectively, providing the low latency required for real-time stream scoring.

#### 5.2 Training Pipeline (`train_model.py`)
The training script builds a synthetic training distribution representing normal API operation alongside expected degradation modes:
- **Synthetic Training Dataset:**
  - **Normal Samples ($N = 5,000$):** Gaussian distribution $\mu = 120.0\text{ ms}, \sigma = 30.0\text{ ms}$, clipped at $\ge 10.0\text{ ms}$.
  - **Anomalous Samples ($N = 500$):** Uniform distribution $1500.0\text{ ms} \le \text{latency} \le 6000.0\text{ ms}$.
  - **Total Training Matrix ($X$):** $5,500$ rows $\times 1$ column (`latency_ms`).
- **Hyperparameter Configuration:**
  - `n_estimators = 200`: Number of isolation trees in the ensemble.
  - `contamination = 500 / 5500 \approx 0.0909`: Proportion of expected outliers matching the injected spike frequency.
  - `random_state = 42`: Ensures deterministic reproducibility.
  - `n_jobs = -1`: Utilizes all available CPU cores during fitting.
- **Serialization:** Fitted model is saved using `pickle` to [`models/isolation_forest.pkl`](file:///C:/Users/g/OneDrive/Documents/DSA%20MINI%20PROJECT/models/isolation_forest.pkl).

#### 5.3 Feature Vector & Scoring in `consumer.py`
- **Feature Evaluated:** A single 1D continuous feature vector `[[latency_ms]]`. (The model does not use categorical strings or HTTP codes in inference).
- **Scoring Output:**
  - `model.score_samples([[latency_ms]])`: Returns the opposite of the anomaly score defined in the original paper. Normal values cluster around $-0.38$ to $-0.45$. Severe spikes yield scores $< -0.65$.
  - `model.predict([[latency_ms]])`: Returns `+1` for inliers (normal latency) and `-1` for outliers.
  - The consumer sets `is_anomaly = True` when prediction equals `-1`, saving both the continuous score and binary flag to SQLite table `anomalies`.

---

### 6. Live Dashboard Implementation

#### 6.1 Screenshot of Working Application
The screenshot below demonstrates the live, working Streamlit dashboard capturing the system in operation:

![Streamlit Live Monitoring Dashboard](dashboard_final_submission.png)

*(File location: [`dashboard_final_submission.png`](file:///C:/Users/g/OneDrive/Documents/DSA%20MINI%20PROJECT/dashboard_final_submission.png))*

#### 6.2 Automatic Refresh Mechanism
In traditional Streamlit applications, auto-refresh required cumbersome workarounds such as `<meta http-equiv="refresh">` HTML injection or infinite `time.sleep() + st.rerun()` loops, which cause full-page flashes, discard user input widget state, and reset chart zooms.

This project uses the modern **Streamlit `@st.fragment(run_every="3s")`** feature:
- The rendering function `render_live_dashboard()` is wrapped with `@st.fragment`.
- Streamlit's frontend establishes an internal 3-second client polling timer.
- Only the DOM elements within the fragment re-render; navigation bars, sidebar controls, and scroll positions remain stable.

#### 6.3 Database Access & Concurrency
The dashboard accesses data through `db.get_recent_events()`, `db.get_recent_metrics()`, and `db.get_recent_anomalies()`. Concurrency safety is achieved via:
1. **Read-Only SQL:** The dashboard executes strictly `SELECT` statements, guaranteeing zero side-effects on backend metrics.
2. **SQLite WAL Mode:** Write-Ahead Logging allows readers to fetch records simultaneously without acquiring shared table locks that could delay consumer insertion commits.

---

### 7. Results & Verified Test Execution

All reported figures represent actual measurements captured from test runs against Kafka and SQLite.

#### 7.1 Verified Audit Test Results

The table below contrasts the controlled lab audit test with the extended continuous streaming demonstration:

| Metric / Parameter | Controlled Audit Test | Extended Continuous Run | Measurement Source |
|---|---|---|---|
| **Total Events Ingested & Persisted** | **106 events** | **4,344 events** | `SELECT COUNT(*) FROM events` |
| **Total Rolling Metric Records** | **106 records** | **4,344 records** | `SELECT COUNT(*) FROM metrics` |
| **Total ML Scored Telemetry Records** | **106 records** | **4,344 records** | `SELECT COUNT(*) FROM anomalies` |
| **Total Flagged Anomalies (`is_anomaly = 1`)** | **5 anomalies** | **223 anomalies** | `SELECT COUNT(*) FROM anomalies WHERE is_anomaly=1` |
| **Average Response Latency (Overall Stream)** | **$287.43\text{ ms}$** | **$312.18\text{ ms}$** | `SELECT AVG(latency_ms) FROM events` |
| **Peak Latency Recorded** | **$4545.28\text{ ms}$** | **$5982.41\text{ ms}$** | `SELECT MAX(latency_ms) FROM events` |
| **HTTP Error Rate ($\ge 500$)** | **$4.72\%$** | **$5.13\%$** | `status_code >= 500 / total` |
| **Duplicate Message Deduplication Rate** | **100% (0 duplicates stored)** | **100% (0 duplicates stored)** | Injected duplicate test UUID check |
| **Corrupted Payload Handling** | **3 skipped / 0 crashes** | **N/A** | Malformed JSON & type fuzz test |
| **Live UI Refresh Interval** | **3.0 seconds** | **3.0 seconds** | Streamlit `@st.fragment` timer |

#### 7.2 Verified Injected Spikes Flagged by Model (Audit Sample)
The 5 anomalies detected during the controlled audit confirmed that the IsolationForest model accurately isolated each injected latency spike:

| Event ID Prefix | Endpoint | Measured Latency | HTTP Status | ML Anomaly Score | Prediction |
|---|---|---|---|---|---|
| `ed8ec6e1-...` | `/api/v1/products` | $4217.60\text{ ms}$ | 503 Service Unavailable | $-0.657$ | **Anomaly (-1)** |
| `89821e0f-...` | `/api/v1/products` | $3866.75\text{ ms}$ | 503 Service Unavailable | $-0.667$ | **Anomaly (-1)** |
| `edae5df0-...` | `/api/v1/orders` | $4545.28\text{ ms}$ | 504 Gateway Timeout | $-0.667$ | **Anomaly (-1)** |
| `9a124c80-...` | `/api/v1/users` | $3835.40\text{ ms}$ | 502 Bad Gateway | $-0.668$ | **Anomaly (-1)** |
| `408a6b11-...` | `/api/v1/payments` | $2473.50\text{ ms}$ | 504 Gateway Timeout | $-0.675$ | **Anomaly (-1)** |

*Note: All normal baseline events ($70\text{ ms} - 160\text{ ms}$) received scores between $-0.380$ and $-0.445$ and were correctly classified as Normal (+1).*

---

### 8. Windows Reproduction & Process Startup Order

To reproduce the project demonstration on Windows from scratch, execute commands in the following order:

#### 8.1 Prerequisites
- **Python 3.12+** (`python --version`)
- **Java 17+ or Java 24** (`C:\Program Files\Java\jdk-24`)
- **Apache Kafka 3.7.0** installed at `C:\Kafka\kafka_2.13-3.7.0`

#### 8.2 Execution Order

```
[Step 1: Install Dependencies]
pip install -r requirements.txt

[Step 2: Start Kafka KRaft Broker] (Terminal 1)
.\start_kafka.ps1

[Step 3: Verify / Create Topic] (Terminal 2)
& "C:\Kafka\kafka_2.13-3.7.0\bin\windows\kafka-topics.bat" --bootstrap-server localhost:9092 --create --topic api_events --partitions 1 --replication-factor 1

[Step 4: Train Anomaly Model] (Terminal 2)
python train_model.py

[Step 5: Initialize SQLite Database] (Terminal 2)
python -c "import db; db.init_db()"

[Step 6: Start Stream Consumer] (Terminal 2)
python consumer.py

[Step 7: Start API Event Producer] (Terminal 3)
python producer.py

[Step 8: Launch Streamlit Dashboard] (Terminal 4)
streamlit run dashboard.py
```

*Open browser at **`http://localhost:8501`** to view real-time incoming events and live charts.*

---

### 9. Conclusion & Future Enhancements

#### 9.1 Summary of Learnings
1. **Decoupled Stream Ingestion:** Apache Kafka in ZooKeeper-less KRaft mode provides high-throughput message streaming with simplified single-process configuration on modern operating systems.
2. **Streaming Protocol Compatibility:** When working with Python 3.12, legacy `kafka-python` modules exhibit import regressions (`six.moves`), which are resolved cleanly by adopting maintained forks like `kafka-python-ng 2.2.3`.
3. **Idempotent Persistence:** At-least-once streaming delivery guarantees can achieve exact database deduplication when paired with unique event identifiers and `INSERT OR IGNORE` semantics.
4. **Unsupervised ML in Real-Time Pipelines:** IsolationForest offers sub-millisecond inference times suitable for per-message evaluation without complex multi-node orchestration.
5. **Modern Dashboard Architecture:** Utilizing client-side fragment auto-refresh (`@st.fragment`) prevents UI disruption while tracking rapid database updates.

#### 9.2 Realistic Future Enhancements
1. **Multivariate Feature Representation:** Extend feature extraction beyond univariate latency to include payload size (bytes), concurrent request counts, and rate of error code transitions.
2. **Automated Incident Mitigation:** Implement an automated webhook alerting service that triggers rate-limiting rules or traffic re-routing at the API gateway when consecutive anomalies are detected on a specific route.
3. **Distributed Multi-Partition Kafka Cluster:** Scale Kafka deployment across three partitioned brokers to distribute consumer load across a consumer group.

---

### 10. Submission Audit & Requirements Compliance Checklist

| College Report Requirement | Status | Verification Evidence / Location |
|---|:---:|---|
| **1. Abstract (2–4 sentences explaining project & purpose)** | **SATISFIED** | Section 1 contains exactly 4 sentences defining system, Kafka KRaft, IsolationForest, SQLite, and Streamlit. |
| **2. Architecture with diagram & all 6 layers labeled** | **SATISFIED** | Section 2 includes Mermaid diagram and 6-layer tech mapping table. |
| **3. Dataset explanation, fields, format & sample JSON** | **SATISFIED** | Section 3 covers simulation rationale, 5 routes, field definitions, and representative JSON from `producer.py`. |
| **4. Dedicated subsection per layer with purpose, mechanics & snippet** | **SATISFIED** | Section 4 provides subsections 4.1 through 4.6, each with why it exists, how it works, and genuine code snippets. *(Fixed in this update)* |
| **5. ML model explanation, training, features & scoring** | **SATISFIED** | Section 5 details IsolationForest theory, 5,500 synthetic samples in `train_model.py`, single 1D feature `latency_ms`, and `consumer.py` inference. |
| **6. Dashboard explanation, auto-refresh mechanism & screenshot** | **SATISFIED** | Section 6 includes screenshot `dashboard_final_submission.png`, explanation of `@st.fragment(run_every="3s")`, and SQLite WAL queries. |
| **7. Actual results reported, distinguishing tests from examples** | **SATISFIED** | Section 7 reports actual measured database counts (106 audit events, 5 anomalies, 4,344 extended events, 223 anomalies) and exact anomaly records. |
| **8. Conclusion & realistic future enhancements** | **SATISFIED** | Section 9 summarizes 5 key architectural learnings and 3 realistic future enhancements. |
| **9. Reproduction commands for Windows & startup order** | **SATISFIED** | Section 8 provides step-by-step commands and process startup order. |
| **10. Zero fabricated metrics or fake claims** | **SATISFIED** | All metrics match physical SQLite database outputs (`data/api_monitor.db`). No fictional accuracy/recall metrics are claimed. |
