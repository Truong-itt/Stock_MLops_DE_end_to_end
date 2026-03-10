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
Pipeline hiện tại: thu thập giá cổ phiếu từ Yahoo Finance WebSocket → Kafka → Flink → ClickHouse / ScyllaDB, phân tích sentiment tin tức bằng FinBERT, hiển thị qua dashboard web thời gian thực.
**Sắp ra mắt:** dự đoán giá chứng khoán dựa trên timeseries + tin tức, với ClickHouse đóng vai trò **Data Warehouse trung tâm** cho toàn bộ pipeline MLOps.

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
- [🔮 MLOps & Dự đoán giá (Coming Soon)](#-mlops--dự-đoán-giá-coming-soon)

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
│   • Feature store cho ML (planned)  │                      │
└──────────────┬─────────────────┘                      │
               │ (Coming Soon)                          │
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
                         🔮 COMING SOON — MLOps
          │                                                     │
            ClickHouse (Feature Store)
          │        │                                            │
                   ├─ OHLCV features (1m/5m/1h/daily)
          │        ├─ Technical indicators (RSI, MACD, BB…)    │
                   └─ News sentiment scores (FinBERT)
          │               │                                     │
                          ▼
          │   ML Training Pipeline (Airflow / MLflow)          │
                   LSTM / XGBoost / Prophet
          │               │                                     │
                          ▼
          │   Model Registry (MLflow)                          │
                          │
          │               ▼                                     │
              Prediction API → Dashboard (giá T+1, T+5)
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
| 7 | ScyllaDB | FastAPI REST + WebSocket | Frontend Dashboard |

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
│       └── 01_create_tables.sql # Raw ticks + Materialized Views (1m/5m/1h/3h/6h) + Feature tables (planned)
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

### 🇻🇳 Cổ phiếu Việt Nam (30 mã)
```
VCB, BID, FPT, HPG, CTG, VHM, TCB, VPB, VNM, MBB,
GAS, ACB, MSN, GVR, LPB, SSB, STB, VIB, MWG, HDB,
PLX, POW, SAB, BCM, PDR, KDH, NVL, DGC, SHB, EIB
```

### 🌍 Cổ phiếu Quốc tế (30 mã)
```
AAPL, MSFT, NVDA, AMZN, GOOGL, META, TSLA, BRK-B, LLY, AVGO,
JPM,  V,    UNH,  WMT,  MA,    XOM,  JNJ,  PG,    HD,  COST,
NFLX, AMD,  INTC, DIS,  PYPL,  BA,   CRM,  ORCL,  CSCO, ABT
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

### Bước 8 — Khởi động Monitoring (tùy chọn)

```bash
cd monitor_service
docker compose up -d
```

| URL | Service |
|-----|---------|
| http://localhost:3000 | Grafana |
| http://localhost:9090 | Prometheus |
| http://localhost:8090 | cAdvisor |

### Bước 9 — Khởi động Management UI (tùy chọn)

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
| `GET` | `/symbols` | Danh sách tất cả mã cổ phiếu |
| `GET` | `/stocks/latest` | Giá mới nhất của tất cả cổ phiếu |
| `GET` | `/stocks/{symbol}` | Thông tin chi tiết một cổ phiếu |
| `GET` | `/stocks/{symbol}/ohlcv` | Dữ liệu OHLCV theo interval |
| `GET` | `/stocks/{symbol}/news` | Tin tức và sentiment của cổ phiếu |
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

> **Mục đích chính:** ClickHouse không chỉ là OLAP engine — đây là **Data Warehouse trung tâm** lưu trữ toàn bộ lịch sử giá, OHLCV aggregated và (sắp tới) feature vectors cho pipeline MLOps dự đoán giá.

| Table / View | Mô tả |
|-------------|-------|
| `stock_ticks` | Bảng raw ticks (MergeTree, TTL 2 năm) |
| `mv_ohlcv_1m` → `stock_ohlcv_1m` | Materialized View OHLCV 1 phút |
| `mv_ohlcv_5m` → `stock_ohlcv_5m` | Materialized View OHLCV 5 phút |
| `mv_ohlcv_1h` → `stock_ohlcv_1h` | Materialized View OHLCV 1 giờ |
| `v_ohlcv_3h`, `v_ohlcv_6h` | Views OHLCV 3h / 6h |
| `stock_features` *(planned)* | Feature store: technical indicators + sentiment scores |
| `ml_predictions` *(planned)* | Kết quả dự đoán giá từ ML models |

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

## 🔮 MLOps & Dự đoán giá (Coming Soon)

> ⚠️ **Phần này đang trong kế hoạch phát triển** — chưa được implemented nhưng sẽ có trong thời gian tới.

### Mục tiêu

Sử dụng **ClickHouse** làm Data Warehouse trung tâm để xây dựng pipeline MLOps hoàn chỉnh: **dự đoán giá chứng khoán** dựa trên:
- **Dữ liệu timeseries** — giá OHLCV lịch sử theo nhiều granularity (1m / 5m / 1h / daily)
- **Tin tức + Sentiment** — điểm sentiment từ FinBERT cho từng mã chứng khoán

### Kiến trúc MLOps dự kiến

```
ClickHouse (Data Warehouse)
        │
        ├─── stock_ticks          ──┐
        ├─── stock_ohlcv_1m/5m/1h  ├──▶  Feature Engineering
        └─── stock_news_sentiment  ──┘         │
                                               ▼
                                   ┌─────────────────────┐
                                   │  stock_features     │  ← bảng CH planned
                                   │  (Technical + NLP)  │
                                   └──────────┬──────────┘
                                              │
                                              ▼
                                   ┌─────────────────────┐
                                   │  ML Training        │
                                   │  (Airflow Pipeline) │
                                   └──────────┬──────────┘
                                              │
                              ┌───────────────┼────────────────┐
                              ▼               ▼                ▼
                         LSTM/GRU        XGBoost /        Prophet
                         (Deep TS)       LightGBM         (Trend)
                              │               │                │
                              └───────────────┼────────────────┘
                                              │  Ensemble
                                              ▼
                                   ┌─────────────────────┐
                                   │  MLflow Model       │
                                   │  Registry           │
                                   └──────────┬──────────┘
                                              │
                                              ▼
                                   ┌─────────────────────┐
                                   │  Prediction API     │  ← FastAPI endpoint
                                   │  /predict/{symbol}  │
                                   └──────────┬──────────┘
                                              │
                                              ▼
                                   Dashboard (giá T+1, T+5, T+30)
```

### Features dự kiến

#### 📐 Technical Indicators (từ OHLCV trong ClickHouse)
| Feature | Mô tả |
|---------|-------|
| RSI (14) | Relative Strength Index |
| MACD | Moving Average Convergence Divergence |
| Bollinger Bands | Upper / Middle / Lower bands |
| EMA (9, 21, 50, 200) | Exponential Moving Averages |
| ATR | Average True Range (volatility) |
| OBV | On-Balance Volume |
| VWAP | Volume-Weighted Average Price |

#### 📰 NLP Features (từ FinBERT Sentiment)
| Feature | Mô tả |
|---------|-------|
| `sentiment_score_avg_1d` | Trung bình sentiment score 1 ngày gần nhất |
| `sentiment_score_avg_7d` | Trung bình sentiment score 7 ngày |
| `news_volume_1d` | Số lượng tin tức trong 1 ngày |
| `positive_ratio_7d` | Tỷ lệ tin tích cực 7 ngày |

### Công nghệ dự kiến

| Layer | Công nghệ |
|-------|-----------|
| **Workflow Orchestration** | Apache Airflow |
| **Experiment Tracking** | MLflow |
| **Time-Series Models** | LSTM / GRU (PyTorch), Prophet |
| **Gradient Boosting** | XGBoost / LightGBM |
| **Feature Store** | ClickHouse (`stock_features` table) |
| **Model Serving** | FastAPI + MLflow model loading |

### Endpoints dự kiến

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| `GET` | `/api/predict/{symbol}` | Dự đoán giá T+1 / T+5 / T+30 |
| `GET` | `/api/predict/{symbol}/history` | Lịch sử dự đoán vs thực tế |
| `GET` | `/api/models` | Danh sách models đã train và metrics |

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

---

## 📝 Ghi chú

- **Network**: Tất cả services dùng chung Docker network `stock-network` (external).
- **FinBERT** được pre-download vào Docker image để tăng tốc startup; model cache được mount volume để tránh download lại.
- **ScyllaDB CDC** được bật trên `stock_prices` và `stock_latest_prices` để hỗ trợ change data capture.
- **ClickHouse** sử dụng `AggregatingMergeTree` + Materialized Views để tính OHLCV real-time, và sẽ được mở rộng thành **Feature Store** cho pipeline dự đoán giá ML (coming soon).
- Dữ liệu trong ClickHouse có **TTL 2 năm** tự động xoá.

---

## 🙏 Tác giả

**Truong-itt** — [GitHub](https://github.com/Truong-itt)

