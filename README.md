# 📈 Stock MLOps & Data Engineering — End-to-End

![Python](https://img.shields.io/badge/Python-3.10%2F3.11-blue?logo=python)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft%207.6.0-black?logo=apachekafka)
![Flink](https://img.shields.io/badge/Apache%20Flink-1.18-orange?logo=apacheflink)
![ClickHouse](https://img.shields.io/badge/ClickHouse-24.3-yellow?logo=clickhouse)
![ScyllaDB](https://img.shields.io/badge/ScyllaDB-5.4-purple)
![FinBERT](https://img.shields.io/badge/AI-FinBERT%20Sentiment-green?logo=huggingface)
![Docker](https://img.shields.io/badge/Docker-Compose%20v2-blue?logo=docker)
![Status](https://img.shields.io/badge/status-active%20development-brightgreen)

Hệ thống xử lý dữ liệu chứng khoán **real-time** end-to-end, kết hợp Data Engineering và MLOps.
Pipeline hiện tại: thu thập giá từ Yahoo Finance WebSocket → Kafka → Flink → ClickHouse / ScyllaDB, phân tích sentiment bằng FinBERT, phát hiện changepoint bằng BOCPD, dự báo hậu bất thường bằng Whale ML, hiển thị realtime trên web.
ClickHouse đóng vai trò **Data Warehouse trung tâm** cho stream analytics và pipeline MLOps.

---

## 🗂️ Mục lục

- [Tổng quan kiến trúc](#-tổng-quan-kiến-trúc)
- [Luồng dữ liệu](#-luồng-dữ-liệu)
- [Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [Tech Stack](#-tech-stack)
- [Danh sách cổ phiếu theo dõi](#-danh-sách-cổ-phiếu-theo-dõi)
- [Services & Ports](#-services--ports)
- [Hướng dẫn triển khai](#-hướng-dẫn-triển-khai)
- [API Endpoints](#-api-endpoints)
- [Schema dữ liệu](#-schema-dữ-liệu)
- [📐 Công thức toán chính](#-công-thức-toán-chính)
- [🤖 MLOps Whale ML (Implemented)](#-mlops-whale-ml-implemented)

---

## 🏗️ Tổng quan kiến trúc

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│   Yahoo Finance WebSocket (giá real-time)  │  RSS / Google News     │
└────────────────────┬────────────────────────────────┬───────────────┘
                     │                                │
                     ▼                                ▼
┌─────────────────────────────┐        ┌──────────────────────────────┐
│   stream_data.py (Producer) │        │      news_worker.py          │
│   Avro → Kafka              │        │  Crawl tin tức → ScyllaDB    │
└────────────┬────────────────┘        │  Publish → Kafka             │
             │                         └──────────┬───────────────────┘
             ▼                                    ▼
┌────────────────────────────────┐   ┌────────────────────────────────┐
│   Apache Kafka  (KRaft 3-node) │   │   sentiment_worker.py          │
│   Topics:                      │   │   FinBERT AI Sentiment         │
│   • stock_price_vn (30 part.)  │   │   (VI→EN dịch + phân tích)     │
│   • stock_price_dif (30 part.) │   │   → Update ScyllaDB            │
│   • news-sentiment             │   └────────────────────────────────┘
└────────────┬───────────────────┘
             │
             ▼
┌────────────────────────────────┐
│   Apache Flink (Stream Job)    │
│   Đọc Avro từ Kafka            │
│   → Write ClickHouse           │
└────────────┬───────────────────┘
             │
             ▼
┌────────────────────────────────┐   ┌────────────────────────────────┐
│   ClickHouse  (Data Warehouse) │──▶│   agg_worker.py               │
│   • Raw ticks (TTL 2 năm)      │   │   Đọc OHLCV từ ClickHouse      │
│   • Materialized Views OHLCV   │   │   → Write ScyllaDB (60s/lần)  │
│     1m / 5m / 1h / 3h / 6h    │   └──────────────────┬─────────────┘
│   • Feature store cho Whale ML     │                      │
└──────────────┬─────────────────┘                      │
               │ (BOCPD + Whale ML)                     │
               ▼                                        ▼
                                     ┌────────────────────────────────┐
                                     │   ScyllaDB  (3-node cluster)   │
                                     │   Tables:                      │
                                     │   • stock_prices (raw ticks)   │
                                     │   • stock_prices_agg (OHLCV)   │
                                     │   • stock_daily_summary        │
                                     │   • stock_latest_prices        │
                                     │   • stock_news + sentiment     │
                                     └──────────────┬─────────────────┘
                                                    │
                                                    ▼
                                     ┌────────────────────────────────┐
                                     │   FastAPI Backend              │
                                     │   REST API + WebSocket         │
                                     └──────────────┬─────────────────┘
                                                    │
                                                    ▼
                                     ┌────────────────────────────────┐
                                     │   Frontend Dashboard (Web)     │
                                     │   Realtime price + news chart  │
                                     └────────────────────────────────┘

          ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
                        🤖 DEPLOYED MLOps — Whale ML
          │                                                     │
            ClickHouse changepoint_events + v_ohlcv_daily
          │               │                                     │
                          ▼
          │   Train service (LogReg + RF)                      │
          │   MLflow run + metrics + artifact                 │
                          │
          │               ▼                                     │
              MLflow Registry: whale_move_forecaster@production
          │               │                                     │
                          ▼
          │   Web backend enrich alert: /predict-batch         │
                          │
          │               ▼                                     │
              Dashboard: hướng tăng/giảm + số phiên + xác suất
          └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
```

---

## 🔄 Luồng dữ liệu

| Bước | Nguồn | Xử lý | Đích |
|------|-------|-------|------|
| 1 | Yahoo Finance WebSocket | `stream_data.py` serialize Avro | Kafka `stock_price_vn` / `stock_price_dif` |
| 2 | Kafka topics | Apache Flink stream job | ClickHouse `stock_ticks` |
| 3 | ClickHouse Materialized Views | `agg_worker.py` (mỗi 60s) | ScyllaDB `stock_prices_agg`, `stock_daily_summary` |
| 4 | ScyllaDB CDC | Backend polling | ScyllaDB `stock_latest_prices` |
| 5 | Yahoo Finance RSS / Google News | `news_worker.py` (mỗi 5 phút) | ScyllaDB `stock_news` + Kafka `news-sentiment` |
| 6 | Kafka `news-sentiment` | `sentiment_worker.py` (FinBERT) | ScyllaDB `stock_news.sentiment_score` |
| 7 | Scylla latest prices | `search/changepoint_worker.py` (BOCPD) | ClickHouse `stock_changepoint_events`, Scylla `stock_changepoint_latest/history` |
| 8 | ClickHouse changepoint + daily close | `whale-ml-service` train/predict | MLflow Registry + forecast `up/down/sessions` |
| 9 | ScyllaDB + ClickHouse + Whale ML | FastAPI REST + WebSocket | Frontend Dashboard |

---

## 📁 Cấu trúc thư mục

```
Stock_MLops_DE_end_to_end/
├── data/                        # Data workers (Producer & Consumers)
│   ├── stream_data.py           # Yahoo Finance WebSocket → Kafka (Avro)
│   ├── agg_worker.py            # ClickHouse → ScyllaDB OHLCV sync (60s)
│   ├── news_worker.py           # RSS/GNews crawl → ScyllaDB + Kafka
│   ├── sentiment_worker.py      # FinBERT sentiment analysis consumer
│   ├── reprocess_sentiment.py   # Reprocess existing news for sentiment
│   ├── logger.py                # Shared logging utility
│   ├── Dockerfile               # Image cho producer + workers
│   ├── Dockerfile.sentiment     # Image riêng cho FinBERT (4GB RAM)
│   ├── docker-compose.yml       # Compose: producer, agg, news, sentiment
│   └── recordings/              # JSONL replay files (test data)
│
├── kafka-service/               # Apache Kafka KRaft cluster
│   ├── docker-compose.yml       # 3 brokers + Schema Registry + setup
│   ├── reassign_rf3.json        # Partition reassignment (RF=3)
│   └── log4j.properties         # Kafka logging config
│
├── clickhouse_service/          # ClickHouse — Data Warehouse trung tâm cho MLOps
│   ├── docker-compose.yml
│   └── init/
│       └── 01_create_tables.sql # Raw ticks + Materialized Views + changepoint tables
│
├── scylla_service/              # ScyllaDB NoSQL cluster
│   ├── docker-compose.yml       # 3-node cluster + auto setup
│   ├── scylla_setup.py          # Schema init (keyspace + tables + indexes)
│   └── Dockerfile.setup
│
├── flink-service/               # Apache Flink stream processing
│   ├── docker-compose.yml       # JobManager + TaskManager + job submit
│   ├── flink/job.py             # PyFlink job (Kafka → ClickHouse)
│   ├── Dockerfile
│   └── pom.xml                  # Maven build (Java job)
│
├── web/                         # Web application
│   ├── backend/
│   │   ├── main.py              # FastAPI app entry point
│   │   ├── routes.py            # REST API endpoints
│   │   ├── ws.py                # WebSocket handler (realtime push)
│   │   ├── database.py          # ScyllaDB singleton connection
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   ├── frontend/
│   │   └── Dockerfile
│   └── docker-compose.yml
│
├── search/                      # BOCPD changepoint detection module
│   ├── bocpd.py
│   ├── changepoint_worker.py
│   ├── docker-compose.yml
│   └── README.md
│
├── mlops/infra/                 # MLOps stack (Airflow + MLflow + Whale ML)
│   ├── docker-compose.yml
│   ├── dags/whale_ml_retrain_pipeline.py
│   ├── whale_ml/
│   │   ├── modeling.py
│   │   ├── service.py
│   │   └── README.md
│   └── README.md
│
├── monitor_service/             # Observability stack
│   ├── docker-compose.yml       # Prometheus + Grafana + cAdvisor + Loki
│   └── prometheus/
│       ├── prometheus.yml       # Scrape configs
│       └── alert_rules.yml      # Alert rules
│
├── gui/                         # Management UIs
│   └── docker-compose.yml       # Portainer + Kafka UI
│
└── scylla-monitoring/           # ScyllaDB dedicated monitoring dashboards
    └── docker-compose.yml
```

---

## 🛠️ Tech Stack

| Layer | Công nghệ | Version |
|-------|-----------|---------|
| **Message Broker** | Apache Kafka (KRaft mode) | 7.6.0 (`confluentinc/cp-kafka:7.6.0`) |
| **Schema Registry** | Confluent Schema Registry | 7.6.0 (`confluentinc/cp-schema-registry:7.6.0`) |
| **Stream Processing** | Apache Flink | 1.18+ (xem `flink-service/Dockerfile`) |
| **Data Warehouse** | ClickHouse (Data Warehouse cho MLOps) | 24.3 (`clickhouse-server:24.3-alpine`) |
| **NoSQL Database** | ScyllaDB | 5.4 (`scylladb/scylla:5.4`) |
| **AI / ML** | FinBERT | `ProsusAI/finbert` (HuggingFace Transformers) |
| **MLOps Tracking/Registry** | MLflow | 2.17.x |
| **MLOps Orchestration** | Apache Airflow | 2.9.x |
| **Backend API** | FastAPI + Uvicorn | Python 3.10 (`python:3.10-slim-bookworm`) |
| **Data Workers** | Python | 3.11 (`python:3.11-slim`) |
| **Data Source** | yfinance (Yahoo Finance WebSocket) | ≥ 0.2 |
| **Serialization** | Apache Avro (confluent-kafka) | 7.6.0 |
| **Monitoring** | Prometheus + Grafana | `prom/prometheus:latest`, `grafana/grafana:latest` |
| **Log Aggregation** | Loki + Promtail | Loki 2.9.8, Promtail 2.9.0 |
| **Container** | Docker + Docker Compose | Engine ≥ 24.x, Compose v2 |
| **Management UI** | Portainer CE + Kafka UI | `portainer-ce:latest`, `kafka-ui:latest` |

---

## 📊 Danh sách cổ phiếu theo dõi

### 🇻🇳 Cổ phiếu Việt Nam (50 mã)
```
VCB, VIC, VHM, BID, TCB, CTG, VGI, ACV, FPT, HPG,
MBB, MSN, GVR, GAS, VNM, VPB, ACB, SSB, STB, HDB,
SHB, LPB, VIB, MWG, BCM, PLX, SAB, POW, VJC, REE,
GMD, TPB, VRE, VCI, SSI, HCM, DGC, PDR, KDH, NVL,
KBC, DPM, DCM, PVD, PNJ, DHG, DIG, BVH, BSR, EIB
```

### 🌍 Cổ phiếu Quốc tế (50 mã)
```
NVDA, AAPL, GOOGL, MSFT, AMZN, META, TSLA, AVGO, TSM, BRK-B,
WMT, LLY, JPM, TCEHY, XOM, V, JNJ, ASML, MA, NFLX,
ORCL, COST, PG, HD, ABBV, CVX, KO, SAP, BABA, TMUS,
PLTR, CRM, CSCO, AZN, AMD, DIS, PEP, INTC, IBM, ADBE,
TM, SHOP, UBER, BAC, MRK, LIN, MCD, ABT, UNH, PYPL
```

---

## 🌐 Services & Ports

| Service | Container | Port |
|---------|-----------|------|
| **Kafka Broker 1** | `kafka-1` | `9092` |
| **Schema Registry** | `schema-registry` | `8081` |
| **ClickHouse HTTP** | `clickhouse` | `8123` |
| **ClickHouse Native** | `clickhouse` | `9000` |
| **ScyllaDB Node 1** | `scylla-node1` | `9042` |
| **ScyllaDB Node 2** | `scylla-node2` | `9043` |
| **ScyllaDB Node 3** | `scylla-node3` | `9044` |
| **Flink Web UI** | `jobmanager` | `8084` |
| **FastAPI Backend** | `backend-stock` | `8020` |
| **Whale ML Service** | `whale-ml-service` | `8090` |
| **MLflow** | `student_mlflow` | `5000` |
| **Airflow Webserver** | `student_airflow_webserver` | `8080` |
| **Gradio** | `student_gradio` | `7860` |
| **MinIO API/Console** | `student_minio` | `9100` / `9101` |
| **Prometheus** | `prometheus` | `9090` |
| **Grafana** | `grafana` | `3000` |
| **cAdvisor** | `cadvisor` | `8090` |
| **Loki** | `loki` | `3100` |
| **Portainer** | `portainer` | `9839` |
| **Kafka UI** | `kafka-ui` | `8077` |
| **ScyllaDB Alternator** | `scylla-node1` | `8000` |
| **ClickHouse Exporter** | `clickhouse-exporter` | `9116` |

---

## 🚀 Hướng dẫn triển khai

### Yêu cầu hệ thống
- Docker Engine ≥ 24.x
- Docker Compose v2
- RAM tối thiểu: **16 GB** (FinBERT cần 4 GB riêng)
- CPU: ≥ 4 cores

### Bước 1 — Tạo Docker network

```bash
docker network create stock-network
```

### Bước 2 — Khởi động Kafka cluster

```bash
cd kafka-service
docker compose up -d
```

> Chờ `kafka-setup` hoàn thành: tạo topics `stock_price_vn`, `stock_price_dif` (30 partitions, RF=3) và `news-sentiment`.

### Bước 3 — Khởi động ScyllaDB

```bash
cd scylla_service
docker compose up -d
```

> Container `scylla-setup` sẽ tự động tạo keyspace `stock_data` và tất cả tables.

### Bước 4 — Khởi động ClickHouse

```bash
cd clickhouse_service
docker compose up -d
```

> Script `01_create_tables.sql` tự chạy khi khởi tạo: tạo bảng raw ticks và các Materialized Views (1m/5m/1h/3h/6h/daily OHLCV).

### Bước 5 — Khởi động Flink

```bash
cd flink-service
docker compose up -d
```

> Flink JobManager UI: http://localhost:8084

### Bước 6 — Khởi động Data Workers

```bash
cd data
docker compose up -d
```

Các worker được khởi động:
- `producer` — stream giá real-time từ Yahoo Finance → Kafka
- `agg-worker` — sync OHLCV ClickHouse → ScyllaDB (mỗi 60s)
- `news-worker` — crawl tin tức mỗi 5 phút → ScyllaDB + Kafka
- `sentiment-worker` — phân tích sentiment bằng FinBERT

### Bước 7 — Khởi động Web App

Tạo file `.env.be` trong thư mục `web/`:
```env
SCYLLA_CONTACT_POINTS=scylla-node1,scylla-node2,scylla-node3
SCYLLA_PORT=9042
SCYLLA_KEYSPACE=stock_data
```

```bash
cd web
docker compose up -d
```

> Dashboard: http://localhost:8020

### Bước 8 — Khởi động BOCPD Search Module

```bash
cd search
docker compose up -d
```

### Bước 9 — Khởi động MLOps Infra (MLflow + Airflow + Whale ML)

```bash
cd mlops/infra
docker compose up -d --build
```

| URL | Service |
|-----|---------|
| http://localhost:5000 | MLflow |
| http://localhost:8080 | Airflow |
| http://localhost:8090 | Whale ML API |
| http://localhost:7860 | Gradio |

### Bước 10 — Khởi động Monitoring (tùy chọn)

```bash
cd monitor_service
docker compose up -d
```

| URL | Service |
|-----|---------|
| http://localhost:3000 | Grafana |
| http://localhost:9090 | Prometheus |
| http://localhost:8090 | cAdvisor |

### Bước 11 — Khởi động Management UI (tùy chọn)

```bash
cd gui
docker compose up -d
```

| URL | Service |
|-----|---------|
| http://localhost:9839 | Portainer |
| http://localhost:8077 | Kafka UI |

---

## 📡 API Endpoints

Base URL: `http://localhost:8020/api`

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/stocks/latest` | Giá mới nhất của tất cả cổ phiếu |
| `GET` | `/stocks/{symbol}` | Thông tin chi tiết một cổ phiếu |
| `GET` | `/stocks/ohlcv/{symbol}` | Dữ liệu OHLCV theo interval |
| `GET` | `/news/{symbol}` | Tin tức và sentiment theo mã |
| `GET` | `/market/overview` | Tổng quan thị trường + alert BOCPD + ML forecast |
| `GET` | `/changepoint/latest` | Snapshot BOCPD mới nhất toàn bộ mã |
| `GET` | `/changepoint/abnormal` | Danh sách mã bất thường đã enrich ML forecast |
| `GET` | `/changepoint/{symbol}` | Trạng thái BOCPD mới nhất theo mã |
| `GET` | `/changepoint/{symbol}/history` | Lịch sử r_t và cp_prob theo mã |
| `GET` | `/system/symbols` | Danh sách mã cấu hình hệ thống |
| `POST` | `/system/symbols` | Thêm mã mới + mở rộng partition nếu cần |
| `WS` | `/ws` | WebSocket realtime price stream |

---

## 🗄️ Schema dữ liệu

### ScyllaDB — Keyspace `stock_data`

| Table | Mô tả |
|-------|-------|
| `stock_prices` | Raw ticks từ WebSocket (CDC enabled) |
| `stock_prices_agg` | OHLCV aggregated (1m/5m/1h/3h/6h) |
| `stock_daily_summary` | Tóm tắt ngày giao dịch (OHLCV, VWAP) |
| `stock_latest_prices` | Giá mới nhất (CDC enabled, upsert) |
| `stock_news` | Tin tức + sentiment score/label |

### ClickHouse — Database `stock_warehouse` (Data Warehouse & Feature Store)

> **Mục đích chính:** ClickHouse là **Data Warehouse trung tâm** cho lịch sử giá, OHLCV, changepoint events và dữ liệu train ML.

| Table / View | Mô tả |
|-------------|-------|
| `stock_ticks` | Bảng raw ticks (MergeTree, TTL 2 năm) |
| `mv_ohlcv_1m` → `stock_ohlcv_1m` | Materialized View OHLCV 1 phút |
| `mv_ohlcv_5m` → `stock_ohlcv_5m` | Materialized View OHLCV 5 phút |
| `mv_ohlcv_1h` → `stock_ohlcv_1h` | Materialized View OHLCV 1 giờ |
| `v_ohlcv_3h`, `v_ohlcv_6h` | Views OHLCV 3h / 6h |
| `stock_changepoint_events` | Event stream BOCPD (source-of-truth cho train/backtest) |
| `v_changepoint_latest` | Snapshot BOCPD mới nhất theo mã |
| `v_ohlcv_daily` | Daily close phục vụ gắn nhãn Whale ML |

### Kafka Topics

| Topic | Partitions | Replication | Mô tả |
|-------|-----------|-------------|-------|
| `stock_price_vn` | 30 | 3 | Giá cổ phiếu Việt Nam (Avro) |
| `stock_price_dif` | 30 | 3 | Giá cổ phiếu quốc tế (Avro) |
| `news-sentiment` | 1 | 1 | Tin tức chờ phân tích sentiment (JSON) |

---

## 🤖 AI Sentiment Analysis

Dự án sử dụng **[FinBERT](https://huggingface.co/ProsusAI/finbert)** — mô hình BERT được fine-tune trên dữ liệu tài chính để phân tích sentiment.

**Pipeline:**
1. `news_worker.py` crawl tin tức → publish JSON lên Kafka topic `news-sentiment`
2. `sentiment_worker.py` consume từ Kafka:
   - Phát hiện ngôn ngữ tiếng Việt (regex unicode)
   - Dịch VI → EN qua [MyMemory API](https://mymemory.translated.net/) (miễn phí)
   - Chạy FinBERT inference → `positive` / `negative` / `neutral`
   - Update `sentiment_score` và `sentiment_label` vào ScyllaDB `stock_news`

**Output:**
- `sentiment_label`: `positive` | `negative` | `neutral`
- `sentiment_score`: giá trị float [-1.0, 1.0]

---

## 📐 Công thức toán chính

### BOCPD (module `search/`)

- Return đầu vào:

$$
R_t=\frac{p_t}{p_{t-1}}-1
$$

- Hazard hằng số:

$$
H(\tau)=\frac{1}{\lambda_{\text{gap}}}
$$

- Posterior run-length (chuẩn hóa từ growth/cp joint):

$$
P(r_t \mid x_{1:t}) \propto P(r_t, x_{1:t})
$$

- Sufficient statistics update (zero-mean Gaussian variance model):

$$
\alpha'=\alpha+\frac12,\quad \beta'=\beta+\frac{x_t^2}{2}
$$

Chi tiết đầy đủ (kèm mapping công thức (1)-(14)) xem:

- `search/README.md`

### Whale ML (module `mlops/infra/whale_ml`)

- Nhãn hướng phiên kế tiếp:

$$
y_{\text{dir}}=\mathbf{1}[R_{d+1}\ge 0]
$$

- Nhãn số phiên liên tiếp cùng hướng trong horizon `H`:

$$
y_{\text{sess}}=\min(H,\text{consecutive same-sign sessions})
$$

- Direction models: `logistic_regression`, `random_forest_classifier`, `gradient_boosting_classifier`.
- Sessions models: `random_forest_regressor`, `extra_trees_regressor`, `gradient_boosting_regressor`.
- Auto-selection:
  - direction chọn score cao nhất theo `ROC-AUC` (fallback `Accuracy`)
  - sessions chọn score cao nhất theo `-MAE` (MAE thấp nhất)

Chi tiết đầy đủ xem:

- `mlops/infra/whale_ml/README.md`

---

## 🤖 MLOps Whale ML (Implemented)

### Luồng đã chạy thật trong hệ thống

1. Dữ liệu train lấy từ ClickHouse:
   - `stock_changepoint_events`
   - `v_ohlcv_daily`
2. Whale ML train 3 classifier + 3 regressor.
3. Tự chấm điểm trên test set và chọn winner cho từng task.
4. Log MLflow run: params, metrics, leaderboard, artifacts.
5. Register model vào MLflow Registry:
   - model name: `whale_move_forecaster`
   - alias serving: `production` (auto cập nhật về winner mới)
6. Airflow DAG điều phối retrain:
   - `whale_ml_retrain_pipeline`
   - chuẩn `dag_run.conf`: `lookback_days`, `max_rows`, `horizon`, `timeout_seconds`
7. Web backend gọi `predict-batch` để enrich alert BOCPD.
8. Frontend hiển thị: hướng dự báo, xác suất lên/xuống, số phiên kỳ vọng.

### API của Whale ML service

- `GET /health`
- `GET /model/info`
- `POST /train`
- `POST /predict-event`
- `POST /predict-batch`

### Tài liệu MLOps chi tiết

- `mlops/infra/README.md`
- `mlops/infra/whale_ml/README.md`
- `mlops/infra/dags/whale_ml_retrain_pipeline.py`

---

## 📦 Dependencies chính

### Data Workers (`data/Dockerfile`)
```
confluent-kafka[avro,schema-registry]
yfinance
requests, beautifulsoup4, lxml
clickhouse-connect
cassandra-driver
pytz
```

### Sentiment Worker (`data/Dockerfile.sentiment`)
```
confluent-kafka
cassandra-driver
torch (CPU)
transformers, sentencepiece
```

### Backend (`web/backend/requirements.txt`)
```
fastapi, uvicorn
cassandra-driver
aiokafka, confluent-kafka
websockets
```

### Whale ML (`mlops/infra/whale_ml/requirements.txt`)
```
fastapi, uvicorn
clickhouse-connect
pandas, numpy, scikit-learn
joblib
mlflow
boto3
```

### Airflow image (`mlops/infra/airflow/Dockerfile`)
```
apache/airflow:2.9.3-python3.10
psycopg2-binary
```

---

## 📝 Ghi chú

- **Network**: Tất cả services dùng chung Docker network `stock-network` (external).
- **FinBERT** được pre-download vào Docker image để tăng tốc startup; model cache được mount volume để tránh download lại.
- **ScyllaDB CDC** được bật trên `stock_prices` và `stock_latest_prices` để hỗ trợ change data capture.
- **ClickHouse** lưu cả dữ liệu stream và BOCPD events để phục vụ trực tiếp pipeline Whale ML.
- Dữ liệu trong ClickHouse có **TTL 2 năm** tự động xoá.

---

## 🙏 Tác giả

**Truong-itt** — [GitHub](https://github.com/Truong-itt)
