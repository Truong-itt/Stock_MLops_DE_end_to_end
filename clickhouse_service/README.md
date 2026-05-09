# ClickHouse Service

## Vai tro trong he thong

`clickhouse_service` la data warehouse layer cho du an stock system, tap trung vao:

- Luu tru lich su tick gia co phieu (`stock_ticks`).
- Tong hop OHLCV theo nhieu khung thoi gian bang materialized views.
- Cung cap query layer on dinh cho app/worker/ML thong qua cac view `v_ohlcv_*`.
- Luu changepoint event stream phuc vu train/backtest Whale ML.

Noi ngan gon: ClickHouse la noi giu "nguon su that lich su" va cung cap du lieu da tong hop toc do cao cho downstream services.

## Kien truc du lieu da trien khai

### 1) Raw fact table

- `stock_warehouse.stock_ticks`
- Engine: `MergeTree`
- Partition: `toYYYYMM(event_time)`
- Order key: `(symbol, event_time)`
- TTL: 2 nam

Muc dich: nhan du lieu tick tu producer/Flink, giu du lieu thoi gian thuc o muc chi tiet.

### 2) Aggregation layer (storage state)

Da trien khai day du bo bang `AggregatingMergeTree`:

- `stock_ohlcv_1m`
- `stock_ohlcv_5m`
- `stock_ohlcv_1h`
- `stock_ohlcv_3h`
- `stock_ohlcv_6h`
- `stock_ohlcv_daily`

Moi bang luu `AggregateFunction state` (`argMinState`, `argMaxState`, `maxState`, `minState`, `countState`) thay vi gia tri final.

### 3) Materialized views (fan-out tu raw ticks)

Da trien khai cac MV:

- `mv_ohlcv_1m  -> stock_ohlcv_1m`
- `mv_ohlcv_5m  -> stock_ohlcv_5m`
- `mv_ohlcv_1h  -> stock_ohlcv_1h`
- `mv_ohlcv_3h  -> stock_ohlcv_3h`
- `mv_ohlcv_6h  -> stock_ohlcv_6h`
- `mv_ohlcv_daily -> stock_ohlcv_daily`

Luu y quan trong: tat ca MV deu doc truc tiep tu `stock_ticks` (fan-out), khong chain tu 1m len 5m/1h.

### 4) Query layer (finalized values)

Da trien khai cac view:

- `v_ohlcv_1m`
- `v_ohlcv_5m`
- `v_ohlcv_1h`
- `v_ohlcv_3h`
- `v_ohlcv_6h`
- `v_ohlcv_daily`

View su dung `argMinMerge/argMaxMerge/maxMerge/minMerge/countMerge` de final hoa aggregate state thanh OHLCV co the query truc tiep.

### 5) Changepoint feature store cho Whale ML

- Table: `stock_changepoint_events` (MergeTree, TTL 2 nam)
- View: `v_changepoint_latest` (ban ghi changepoint moi nhat theo symbol)

Muc dich: lam source-of-truth cho training/backtest va serving metadata cua Whale ML pipeline.

## Data flow tom tat

```text
Producer/Flink
    -> stock_ticks
       -> mv_ohlcv_{1m,5m,1h,3h,6h,daily}
          -> stock_ohlcv_* (aggregate states)
             -> v_ohlcv_* (final query values)
```

## Thanh phan runtime

`docker-compose.yml` da trien khai:

- Image: `clickhouse/clickhouse-server:24.3-alpine`
- HTTP port: `8123`
- Native TCP port: `9000`
- Persistent volume: `clickhouse-data:/var/lib/clickhouse`
- Init schema mount: `./init:/docker-entrypoint-initdb.d`
- Prometheus config mount: `./config/prometheus.xml`
- Healthcheck: `clickhouse-client --query "SELECT 1"`
- Network: external `stock-network`

## Monitoring

Trong `config/prometheus.xml` da bat:

- Endpoint: `/metrics`
- Port trong container: `9363`
- Bat `metrics`, `events`, `asynchronous_metrics`, `status_info`

Neu can scrape tu host, can bo sung mapping port `9363:9363` trong compose.

## Cach chay module

Tu thu muc `clickhouse_service`:

```bash
docker compose up -d
```

Kiem tra service:

```bash
docker compose ps
docker compose logs -f clickhouse
```

Kiem tra SQL nhanh:

```bash
docker exec -it clickhouse clickhouse-client --query "SHOW DATABASES"
docker exec -it clickhouse clickhouse-client --query "SHOW TABLES FROM stock_warehouse"
```

## Ghi chu van hanh

- SQL trong `init/01_create_tables.sql` chi tu dong chay khi volume ClickHouse moi duoc tao lan dau.
- Khi da co volume cu, thay doi schema can migration thu cong.
- Truy cap app nen uu tien query qua `v_ohlcv_*` thay vi doc truc tiep `stock_ohlcv_*`.
- `volume` trong OHLCV hien la so tick (`countState`) chu khong phai khoi luong khop lenh chuan tu san.
