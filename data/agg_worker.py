"""
agg_worker.py
─────────────
Đọc OHLCV đã tính sẵn từ ClickHouse Materialized Views,
ghi vào ScyllaDB stock_prices_agg theo chu kỳ.

Intervals: 1m, 5m, 1h, daily

Chạy mỗi 60 giây, mỗi lần lấy dữ liệu 10 phút gần nhất.
"""

import time
import signal
import sys
from datetime import datetime, timedelta, timezone

import clickhouse_connect
from cassandra.cluster import Cluster

from logger import get_logger

logger = get_logger()

# ═══════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = "truongittstock"
CLICKHOUSE_DB   = "stock_warehouse"

SCYLLA_HOSTS = ["scylla-node1", "scylla-node2", "scylla-node3"]
SCYLLA_PORT  = 9042
SCYLLA_KS    = "stock_data"

POLL_INTERVAL_SEC = 60      # chạy mỗi 60s
LOOKBACK_MINUTES  = 10      # mỗi lần lấy 10 phút gần nhất (1m, 5m)
LOOKBACK_HOURS    = 12      # cho interval 1h, 3h, 6h

# Map ClickHouse view → ScyllaDB interval label
INTERVAL_VIEWS = {
    "1m":    "v_ohlcv_1m",
    "5m":    "v_ohlcv_5m",
    "1h":    "v_ohlcv_1h",
    "3h":    "v_ohlcv_3h",
    "6h":    "v_ohlcv_6h",
}

# ═══════════════════════════════════════════════════════════════════════
# CONNECTIONS
# ═══════════════════════════════════════════════════════════════════════
def connect_clickhouse():
    for attempt in range(30):
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASS,
                database=CLICKHOUSE_DB,
            )
            logger.info("Connected to ClickHouse")
            return client
        except Exception as e:
            logger.warning("ClickHouse not ready (%d/30): %s", attempt + 1, e)
            time.sleep(5)
    raise RuntimeError("Cannot connect to ClickHouse")


def connect_scylla():
    for attempt in range(30):
        try:
            cluster = Cluster(SCYLLA_HOSTS, port=SCYLLA_PORT)
            session = cluster.connect(SCYLLA_KS)
            logger.info("Connected to ScyllaDB")
            return cluster, session
        except Exception as e:
            logger.warning("ScyllaDB not ready (%d/30): %s", attempt + 1, e)
            time.sleep(5)
    raise RuntimeError("Cannot connect to ScyllaDB")


# ═══════════════════════════════════════════════════════════════════════
# SYNC LOGIC
# ═══════════════════════════════════════════════════════════════════════
def sync_ohlcv(ch_client, scylla_session, interval: str, view_name: str, since: datetime):
    """
    Đọc OHLCV từ ClickHouse view, ghi vào ScyllaDB stock_prices_agg.
    """
    since_str = since.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        SELECT
            symbol,
            bucket AS ts,
            open,
            high,
            low,
            close,
            volume,
            change_percent
        FROM {view_name}
        WHERE bucket >= '{since_str}'
        ORDER BY symbol, bucket
    """

    try:
        result = ch_client.query(query)
    except Exception as e:
        logger.error("ClickHouse query failed for %s: %s", view_name, e)
        return 0

    rows = result.result_rows
    if not rows:
        return 0

    insert_cql = """
        INSERT INTO stock_prices_agg
            (symbol, bucket_date, interval, ts, open, high, low, close, volume, vwap)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = scylla_session.prepare(insert_cql)

    count = 0
    for row in rows:
        symbol, ts, open_p, high_p, low_p, close_p, volume, change_pct = row
        try:
            if isinstance(ts, str):
                ts = datetime.fromisoformat(ts)

            bucket_date = ts.date() if hasattr(ts, 'date') else ts

            # vwap ước tính: (open + high + low + close) / 4
            vwap = (
                ((open_p or 0) + (high_p or 0) + (low_p or 0) + (close_p or 0)) / 4
                if all(v is not None for v in [open_p, high_p, low_p, close_p])
                else None
            )

            scylla_session.execute(prepared, (
                symbol,
                bucket_date,
                interval,
                ts,
                float(open_p)  if open_p  is not None else None,
                float(high_p)  if high_p  is not None else None,
                float(low_p)   if low_p   is not None else None,
                float(close_p) if close_p is not None else None,
                int(volume)    if volume  is not None else None,
                float(vwap)    if vwap    is not None else None,
            ))
            count += 1
        except Exception as e:
            logger.error("ScyllaDB insert error (%s/%s): %s", symbol, interval, e)

    return count


def sync_daily_summary(ch_client, scylla_session, since: datetime):
    """
    Đọc OHLCV daily từ ClickHouse, ghi vào ScyllaDB stock_daily_summary.
    """
    since_str = since.strftime("%Y-%m-%d")

    query = f"""
        SELECT
            symbol,
            trade_date,
            open,
            high,
            low,
            close,
            volume,
            change_percent
        FROM v_ohlcv_daily
        WHERE trade_date >= '{since_str}'
        ORDER BY symbol, trade_date
    """

    try:
        result = ch_client.query(query)
    except Exception as e:
        logger.error("ClickHouse daily query failed: %s", e)
        return 0

    rows = result.result_rows
    if not rows:
        return 0

    insert_cql = """
        INSERT INTO stock_daily_summary
            (symbol, trade_date, open, high, low, close, volume,
             change, change_percent, vwap, exchange, quote_type, market_hours)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = scylla_session.prepare(insert_cql)

    count = 0
    for row in rows:
        symbol, trade_date, open_p, high_p, low_p, close_p, volume, change_pct = row
        try:
            if isinstance(trade_date, str):
                trade_date = datetime.fromisoformat(trade_date).date()
            elif hasattr(trade_date, 'date'):
                trade_date = trade_date.date()

            change_val = (close_p - open_p) if (close_p and open_p) else None
            vwap = (
                ((open_p or 0) + (high_p or 0) + (low_p or 0) + (close_p or 0)) / 4
                if all(v is not None for v in [open_p, high_p, low_p, close_p])
                else None
            )

            scylla_session.execute(prepared, (
                symbol,
                trade_date,
                float(open_p)      if open_p      is not None else None,
                float(high_p)      if high_p      is not None else None,
                float(low_p)       if low_p       is not None else None,
                float(close_p)     if close_p     is not None else None,
                int(volume)        if volume       is not None else None,
                float(change_val)  if change_val   is not None else None,
                float(change_pct)  if change_pct   is not None else None,
                float(vwap)        if vwap         is not None else None,
                None,  # exchange — không có trong ClickHouse MV
                None,  # quote_type
                None,  # market_hours
            ))
            count += 1
        except Exception as e:
            logger.error("ScyllaDB daily insert error (%s): %s", symbol, e)

    return count


# ═══════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════
def main():
    logger.info("Starting agg_worker ...")

    ch_client = connect_clickhouse()
    cluster, scylla_session = connect_scylla()

    running = True

    def _stop(sig, frame):
        nonlocal running
        logger.info("Received signal %s, stopping ...", sig)
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    while running:
        total = 0
        for interval, view in INTERVAL_VIEWS.items():
            # 1h, 3h, 6h dùng lookback lớn hơn để bắt được bucket đầu giờ
            if interval in ("1h", "3h", "6h"):
                since = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
            else:
                since = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MINUTES)
            n = sync_ohlcv(ch_client, scylla_session, interval, view, since)
            total += n
            if n > 0:
                logger.info("[AGG] %s: synced %d rows", interval, n)

        n_daily = sync_daily_summary(ch_client, scylla_session, since)
        total += n_daily
        if n_daily > 0:
            logger.info("[DAILY] synced %d rows", n_daily)

        if total == 0:
            logger.debug("No new aggregation data")

        time.sleep(POLL_INTERVAL_SEC)

    # Cleanup
    try:
        scylla_session.shutdown()
        cluster.shutdown()
    except Exception:
        pass
    logger.info("agg_worker stopped.")


if __name__ == "__main__":
    main()
