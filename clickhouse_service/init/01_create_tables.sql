-- ================================================================
-- ClickHouse Data Warehouse - Stock Price Analytics
-- Tự động chạy khi container khởi tạo lần đầu
-- ================================================================

CREATE DATABASE IF NOT EXISTS stock_warehouse;

-- ── 1. BẢNG RAW TICKS (fact table) ────────────────────────────────
-- Lưu mọi tick từ Flink, partition theo tháng, order theo time
CREATE TABLE IF NOT EXISTS stock_warehouse.stock_ticks
(
    symbol          String,
    price           Float64,
    event_time      DateTime64(3),     -- ms precision
    exchange        Nullable(String),
    quote_type      Nullable(Int32),
    market_hours    Nullable(Int32),
    change_percent  Nullable(Float64),
    change          Nullable(Float64),
    price_hint      Nullable(String),
    received_at     DateTime64(3),     -- producer timestamp
    day_volume      Nullable(Int64),
    last_size       Nullable(Int64),
    inserted_at     DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (symbol, event_time)
TTL toDate(event_time) + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- ── 2. BẢNG OHLCV 1 PHÚT (Aggregated) ─────────────────────────────
-- Materialized View tự tính OHLCV mỗi phút
CREATE TABLE IF NOT EXISTS stock_warehouse.stock_ohlcv_1m
(
    symbol          String,
    bucket          DateTime,          -- truncated to minute
    open            AggregateFunction(argMin, Float64, DateTime64(3)),
    close           AggregateFunction(argMax, Float64, DateTime64(3)),
    high            AggregateFunction(max, Float64),
    low             AggregateFunction(min, Float64),
    volume          AggregateFunction(count, UInt64),
    last_change_pct AggregateFunction(argMax, Float64, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket)
ORDER BY (symbol, bucket);

CREATE MATERIALIZED VIEW IF NOT EXISTS stock_warehouse.mv_ohlcv_1m
TO stock_warehouse.stock_ohlcv_1m
AS
SELECT
    symbol,
    toStartOfMinute(event_time) AS bucket,
    argMinState(price, event_time)          AS open,
    argMaxState(price, event_time)          AS close,
    maxState(price)                         AS high,
    minState(price)                         AS low,
    countState(toUInt64(1))                 AS volume,
    argMaxState(coalesce(change_percent, 0), event_time) AS last_change_pct
FROM stock_warehouse.stock_ticks
GROUP BY symbol, bucket;

-- ── 3. BẢNG OHLCV 5 PHÚT ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stock_warehouse.stock_ohlcv_5m
(
    symbol          String,
    bucket          DateTime,
    open            AggregateFunction(argMin, Float64, DateTime64(3)),
    close           AggregateFunction(argMax, Float64, DateTime64(3)),
    high            AggregateFunction(max, Float64),
    low             AggregateFunction(min, Float64),
    volume          AggregateFunction(count, UInt64),
    last_change_pct AggregateFunction(argMax, Float64, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket)
ORDER BY (symbol, bucket);

CREATE MATERIALIZED VIEW IF NOT EXISTS stock_warehouse.mv_ohlcv_5m
TO stock_warehouse.stock_ohlcv_5m
AS
SELECT
    symbol,
    toStartOfFiveMinutes(event_time) AS bucket,
    argMinState(price, event_time)          AS open,
    argMaxState(price, event_time)          AS close,
    maxState(price)                         AS high,
    minState(price)                         AS low,
    countState(toUInt64(1))                 AS volume,
    argMaxState(coalesce(change_percent, 0), event_time) AS last_change_pct
FROM stock_warehouse.stock_ticks
GROUP BY symbol, bucket;

-- ── 4. BẢNG OHLCV 1 GIỜ ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stock_warehouse.stock_ohlcv_1h
(
    symbol          String,
    bucket          DateTime,
    open            AggregateFunction(argMin, Float64, DateTime64(3)),
    close           AggregateFunction(argMax, Float64, DateTime64(3)),
    high            AggregateFunction(max, Float64),
    low             AggregateFunction(min, Float64),
    volume          AggregateFunction(count, UInt64),
    last_change_pct AggregateFunction(argMax, Float64, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(bucket)
ORDER BY (symbol, bucket);

CREATE MATERIALIZED VIEW IF NOT EXISTS stock_warehouse.mv_ohlcv_1h
TO stock_warehouse.stock_ohlcv_1h
AS
SELECT
    symbol,
    toStartOfHour(event_time) AS bucket,
    argMinState(price, event_time)          AS open,
    argMaxState(price, event_time)          AS close,
    maxState(price)                         AS high,
    minState(price)                         AS low,
    countState(toUInt64(1))                 AS volume,
    argMaxState(coalesce(change_percent, 0), event_time) AS last_change_pct
FROM stock_warehouse.stock_ticks
GROUP BY symbol, bucket;

-- ── 5. BẢNG DAILY SUMMARY ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stock_warehouse.stock_ohlcv_daily
(
    symbol          String,
    trade_date      Date,
    open            AggregateFunction(argMin, Float64, DateTime64(3)),
    close           AggregateFunction(argMax, Float64, DateTime64(3)),
    high            AggregateFunction(max, Float64),
    low             AggregateFunction(min, Float64),
    volume          AggregateFunction(count, UInt64),
    last_change_pct AggregateFunction(argMax, Float64, DateTime64(3))
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYear(trade_date)
ORDER BY (symbol, trade_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS stock_warehouse.mv_ohlcv_daily
TO stock_warehouse.stock_ohlcv_daily
AS
SELECT
    symbol,
    toDate(event_time) AS trade_date,
    argMinState(price, event_time)          AS open,
    argMaxState(price, event_time)          AS close,
    maxState(price)                         AS high,
    minState(price)                         AS low,
    countState(toUInt64(1))                 AS volume,
    argMaxState(coalesce(change_percent, 0), event_time) AS last_change_pct
FROM stock_warehouse.stock_ticks
GROUP BY symbol, trade_date;

-- ── 6. VIEW TIỆN DÙNG ĐỂ QUERY ─────────────────────────────────────
-- Query OHLCV 1 phút: SELECT * FROM stock_warehouse.v_ohlcv_1m WHERE symbol='FPT'
CREATE VIEW IF NOT EXISTS stock_warehouse.v_ohlcv_1m AS
SELECT
    symbol,
    bucket,
    argMinMerge(open)          AS open,
    argMaxMerge(close)         AS close,
    maxMerge(high)             AS high,
    minMerge(low)              AS low,
    countMerge(volume)         AS volume,
    argMaxMerge(last_change_pct) AS change_percent
FROM stock_warehouse.stock_ohlcv_1m
GROUP BY symbol, bucket
ORDER BY symbol, bucket;

CREATE VIEW IF NOT EXISTS stock_warehouse.v_ohlcv_5m AS
SELECT
    symbol,
    bucket,
    argMinMerge(open)          AS open,
    argMaxMerge(close)         AS close,
    maxMerge(high)             AS high,
    minMerge(low)              AS low,
    countMerge(volume)         AS volume,
    argMaxMerge(last_change_pct) AS change_percent
FROM stock_warehouse.stock_ohlcv_5m
GROUP BY symbol, bucket
ORDER BY symbol, bucket;

CREATE VIEW IF NOT EXISTS stock_warehouse.v_ohlcv_1h AS
SELECT
    symbol,
    bucket,
    argMinMerge(open)          AS open,
    argMaxMerge(close)         AS close,
    maxMerge(high)             AS high,
    minMerge(low)              AS low,
    countMerge(volume)         AS volume,
    argMaxMerge(last_change_pct) AS change_percent
FROM stock_warehouse.stock_ohlcv_1h
GROUP BY symbol, bucket
ORDER BY symbol, bucket;

CREATE VIEW IF NOT EXISTS stock_warehouse.v_ohlcv_daily AS
SELECT
    symbol,
    trade_date,
    argMinMerge(open)          AS open,
    argMaxMerge(close)         AS close,
    maxMerge(high)             AS high,
    minMerge(low)              AS low,
    countMerge(volume)         AS volume,
    argMaxMerge(last_change_pct) AS change_percent
FROM stock_warehouse.stock_ohlcv_daily
GROUP BY symbol, trade_date
ORDER BY symbol, trade_date;
