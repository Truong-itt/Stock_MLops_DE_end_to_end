#!/usr/bin/env python3
"""Clean mixed-scale contamination for one symbol and rebuild derived layers.

Default mode is dry-run. Use --apply to execute mutations.

What this script does for one symbol:
1) Identify and optionally delete out-of-scale rows in:
   - Scylla stock_prices
   - ClickHouse stock_ticks
2) Rebuild ClickHouse aggregate-state tables for that symbol.
3) Rebuild Scylla derived tables (stock_prices_agg, stock_daily_summary)
   from ClickHouse clean views.
4) Refresh Scylla stock_latest_prices from remaining Scylla raw tick.

Scale compatibility uses ratio bounds vs anchor price:
  low_ratio <= price / anchor_price <= high_ratio
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import json
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import clickhouse_connect
from cassandra.cluster import Cluster
from cassandra.query import dict_factory


SCYLLA_PORT = int(os.getenv("SCYLLA_PORT", "9042"))
SCYLLA_KEYSPACE = os.getenv("SCYLLA_KEYSPACE", "stock_data")
SCYLLA_CONTACT_POINTS = [
    x.strip()
    for x in os.getenv("SCYLLA_CONTACT_POINTS", "scylla-node1,scylla-node2,scylla-node3").split(",")
    if x.strip()
]

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "truongittstock")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "stock_warehouse")
REGISTRY_PATH = os.getenv("SYMBOL_REGISTRY_PATH", "/app/config/symbol_registry.json")

CLICKHOUSE_STATE_TABLES = [
    "stock_ohlcv_1m",
    "stock_ohlcv_5m",
    "stock_ohlcv_1h",
    "stock_ohlcv_3h",
    "stock_ohlcv_6h",
    "stock_ohlcv_daily",
]

CLICKHOUSE_INTERVAL_VIEWS = {
    "1m": "v_ohlcv_1m",
    "5m": "v_ohlcv_5m",
    "1h": "v_ohlcv_1h",
    "3h": "v_ohlcv_3h",
    "6h": "v_ohlcv_6h",
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_scylla_ts(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    raw = str(value).strip()
    if not raw:
        return None

    if raw.isdigit():
        iv = int(raw)
        if iv > 10**12:
            return datetime.fromtimestamp(iv / 1000.0, tz=timezone.utc)
        if iv > 10**9:
            return datetime.fromtimestamp(iv, tz=timezone.utc)

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_float(value: object) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def to_int(value: object) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def is_out_of_scale(price: object, anchor: float, low_ratio: float, high_ratio: float) -> bool:
    p = to_float(price)
    if p is None or p <= 0:
        return True
    ratio = p / anchor
    return ratio < low_ratio or ratio > high_ratio


def connect_scylla():
    cluster = Cluster(
        contact_points=SCYLLA_CONTACT_POINTS,
        port=SCYLLA_PORT,
        protocol_version=4,
    )
    session = cluster.connect(SCYLLA_KEYSPACE)
    session.row_factory = dict_factory
    return cluster, session


def connect_clickhouse():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def load_market_sets(registry_path: str) -> Dict[str, set]:
    markets = {"vn": set(), "world": set()}
    try:
        with open(registry_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        m = (data.get("markets") or {})
        markets["vn"] = set((m.get("vn") or {}).get("symbols") or [])
        markets["world"] = set((m.get("world") or {}).get("symbols") or [])
    except Exception:
        pass
    return markets


def infer_symbol_market(symbol: str, market_sets: Dict[str, set]) -> Optional[str]:
    if symbol in market_sets.get("vn", set()):
        return "vn"
    if symbol in market_sets.get("world", set()):
        return "world"
    return None


def expected_exchange_for_market(market: Optional[str]) -> Optional[str]:
    if market == "vn":
        return "VSE"
    return None


def get_anchor_price(
    session,
    symbol: str,
    explicit_anchor: Optional[float],
    expected_exchange: Optional[str],
) -> Tuple[float, str]:
    if explicit_anchor is not None:
        if explicit_anchor <= 0:
            raise ValueError("--anchor-price must be > 0")
        return explicit_anchor, "--anchor-price"

    rows = list(
        session.execute(
            "SELECT price, exchange FROM stock_latest_prices WHERE symbol = %s",
            [symbol],
        )
    )
    if rows:
        latest_price = to_float(rows[0].get("price"))
        latest_exchange = str(rows[0].get("exchange") or "").upper()
        if latest_price is not None and latest_price > 0:
            if expected_exchange is None or latest_exchange == expected_exchange:
                return latest_price, "stock_latest_prices"

    # Fallback to raw ticks within symbol partition (newest first).
    rows = list(
        session.execute(
            "SELECT timestamp, price, exchange FROM stock_prices WHERE symbol = %s LIMIT 2000",
            [symbol],
        )
    )

    if expected_exchange:
        exchange_prices = []
        for row in rows:
            exch = str(row.get("exchange") or "").upper()
            if exch != expected_exchange:
                continue
            price = to_float(row.get("price"))
            if price is not None and price > 0:
                exchange_prices.append(price)
                if len(exchange_prices) >= 200:
                    break

        if exchange_prices:
            exchange_prices.sort()
            mid = len(exchange_prices) // 2
            return exchange_prices[mid], f"stock_prices(exchange={expected_exchange},median)"

    # Fallback to latest positive raw tick (any exchange).
    if rows:
        for row in rows:
            price = to_float(row.get("price"))
            if price is not None and price > 0:
                return price, "stock_prices(latest_positive_any_exchange)"

    if rows and len(rows) > 0:
        price = to_float(rows[0].get("price"))
        if price is not None and price > 0:
            return price, "stock_prices(first_row)"

    # Last resort: latest table even if exchange mismatched.
    if rows:
        pass
    latest_rows = list(
        session.execute(
            "SELECT price FROM stock_latest_prices WHERE symbol = %s",
            [symbol],
        )
    )
    if latest_rows:
        fallback_price = to_float(latest_rows[0].get("price"))
        if fallback_price is not None and fallback_price > 0:
            return fallback_price, "stock_latest_prices(fallback_any_exchange)"

    raise RuntimeError(
        f"Cannot determine anchor price for {symbol}. Provide --anchor-price explicitly."
    )


def collect_scylla_outlier_keys(
    session,
    symbol: str,
    anchor: float,
    low_ratio: float,
    high_ratio: float,
) -> Dict[str, List[Tuple]]:
    out: Dict[str, List[Tuple]] = {
        "stock_prices": [],
        "stock_daily_summary": [],
        "stock_prices_agg": [],
    }

    raw_rows = list(
        session.execute(
            "SELECT timestamp, price FROM stock_prices WHERE symbol = %s",
            [symbol],
        )
    )
    for row in raw_rows:
        if is_out_of_scale(row.get("price"), anchor, low_ratio, high_ratio):
            out["stock_prices"].append((row.get("timestamp"), row.get("price")))

    daily_rows = list(
        session.execute(
            "SELECT trade_date, close FROM stock_daily_summary WHERE symbol = %s",
            [symbol],
        )
    )
    for row in daily_rows:
        if is_out_of_scale(row.get("close"), anchor, low_ratio, high_ratio):
            out["stock_daily_summary"].append((row.get("trade_date"), row.get("close")))

    agg_rows = list(
        session.execute(
            "SELECT bucket_date, interval, ts, close FROM stock_prices_agg WHERE symbol = %s ALLOW FILTERING",
            [symbol],
        )
    )
    for row in agg_rows:
        if is_out_of_scale(row.get("close"), anchor, low_ratio, high_ratio):
            out["stock_prices_agg"].append(
                (
                    row.get("bucket_date"),
                    row.get("interval"),
                    row.get("ts"),
                    row.get("close"),
                )
            )

    return out


def collect_scylla_agg_partitions(session, symbol: str) -> List[Tuple[object, str]]:
    rows = list(
        session.execute(
            "SELECT bucket_date, interval FROM stock_prices_agg WHERE symbol = %s ALLOW FILTERING",
            [symbol],
        )
    )
    partitions = sorted({(row.get("bucket_date"), row.get("interval")) for row in rows})
    return partitions


def count_clickhouse_outliers(
    ch_client,
    symbol: str,
    anchor: float,
    low_ratio: float,
    high_ratio: float,
) -> int:
    low_price = anchor * low_ratio
    high_price = anchor * high_ratio
    sql = (
        "SELECT count() FROM stock_ticks "
        "WHERE symbol = %(symbol)s "
        "AND (price <= 0 OR price < %(low)s OR price > %(high)s)"
    )
    rows = ch_client.query(sql, parameters={"symbol": symbol, "low": low_price, "high": high_price}).result_rows
    return int(rows[0][0]) if rows else 0


def wait_clickhouse_mutations(
    ch_client,
    table_names: Sequence[str],
    since: datetime,
    timeout_sec: int = 1800,
) -> None:
    table_sql = ",".join(f"'{t}'" for t in table_names)
    since_str = since.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    deadline = time.time() + timeout_sec

    while True:
        rows = ch_client.query(
            f"""
            SELECT table, mutation_id, is_done, parts_to_do
            FROM system.mutations
            WHERE database = '{CLICKHOUSE_DB}'
              AND table IN ({table_sql})
              AND create_time >= toDateTime('{since_str}')
            ORDER BY create_time DESC
            """
        ).result_rows

        pending = [r for r in rows if int(r[2]) == 0]
        if not pending:
            return

        if time.time() > deadline:
            raise TimeoutError("Timed out waiting ClickHouse mutations")
        time.sleep(2)


def apply_clickhouse_cleanup(
    ch_client,
    symbol: str,
    anchor: float,
    low_ratio: float,
    high_ratio: float,
) -> None:
    low_price = anchor * low_ratio
    high_price = anchor * high_ratio

    mutation_since = now_utc()
    ch_client.command(
        "ALTER TABLE stock_ticks DELETE WHERE "
        f"symbol = '{symbol}' AND (price <= 0 OR price < {low_price} OR price > {high_price})"
    )
    wait_clickhouse_mutations(ch_client, ["stock_ticks"], mutation_since)


def rebuild_clickhouse_states(ch_client, symbol: str) -> None:
    # 1) Clear existing aggregate-state rows for this symbol.
    mutation_since = now_utc()
    for table in CLICKHOUSE_STATE_TABLES:
        ch_client.command(
            f"ALTER TABLE {table} DELETE WHERE symbol = '{symbol}'"
        )
    wait_clickhouse_mutations(ch_client, CLICKHOUSE_STATE_TABLES, mutation_since)

    # 2) Reinsert state rows from cleaned stock_ticks.
    statements = [
        f"""
        INSERT INTO stock_ohlcv_1m
        SELECT
            symbol,
            toStartOfMinute(event_time) AS bucket,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM stock_ticks
        WHERE symbol = '{symbol}'
        GROUP BY symbol, bucket
        """,
        f"""
        INSERT INTO stock_ohlcv_5m
        SELECT
            symbol,
            toStartOfFiveMinutes(event_time) AS bucket,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM stock_ticks
        WHERE symbol = '{symbol}'
        GROUP BY symbol, bucket
        """,
        f"""
        INSERT INTO stock_ohlcv_1h
        SELECT
            symbol,
            toStartOfHour(event_time) AS bucket,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM stock_ticks
        WHERE symbol = '{symbol}'
        GROUP BY symbol, bucket
        """,
        f"""
        INSERT INTO stock_ohlcv_3h
        SELECT
            symbol,
            toStartOfInterval(event_time, INTERVAL 3 HOUR) AS bucket,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM stock_ticks
        WHERE symbol = '{symbol}'
        GROUP BY symbol, bucket
        """,
        f"""
        INSERT INTO stock_ohlcv_6h
        SELECT
            symbol,
            toStartOfInterval(event_time, INTERVAL 6 HOUR) AS bucket,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM stock_ticks
        WHERE symbol = '{symbol}'
        GROUP BY symbol, bucket
        """,
        f"""
        INSERT INTO stock_ohlcv_daily
        SELECT
            symbol,
            toDate(event_time) AS trade_date,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM stock_ticks
        WHERE symbol = '{symbol}'
        GROUP BY symbol, trade_date
        """,
    ]

    for sql in statements:
        ch_client.command(sql)


def clear_scylla_derived(session, symbol: str) -> Dict[str, int]:
    stats = {
        "agg_partitions_deleted": 0,
        "daily_partition_deleted": 0,
    }

    for bucket_date, interval in collect_scylla_agg_partitions(session, symbol):
        session.execute(
            "DELETE FROM stock_prices_agg WHERE symbol = %s AND bucket_date = %s AND interval = %s",
            [symbol, bucket_date, interval],
        )
        stats["agg_partitions_deleted"] += 1

    session.execute("DELETE FROM stock_daily_summary WHERE symbol = %s", [symbol])
    stats["daily_partition_deleted"] = 1

    return stats


def sync_scylla_derived_from_clickhouse(session, ch_client, symbol: str, exchange: Optional[str]) -> Dict[str, int]:
    insert_agg = session.prepare(
        """
        INSERT INTO stock_prices_agg
            (symbol, bucket_date, interval, ts, open, high, low, close, volume, vwap)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )
    insert_daily = session.prepare(
        """
        INSERT INTO stock_daily_summary
            (symbol, trade_date, open, high, low, close, volume,
             change, change_percent, vwap, exchange, quote_type, market_hours)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    stats = {
        "agg_rows_inserted": 0,
        "daily_rows_inserted": 0,
    }

    for interval, view_name in CLICKHOUSE_INTERVAL_VIEWS.items():
        rows = ch_client.query(
            (
                f"SELECT symbol, bucket, open, high, low, close, volume "
                f"FROM {view_name} WHERE symbol = %(symbol)s ORDER BY bucket"
            ),
            parameters={"symbol": symbol},
        ).result_rows

        for _sym, ts, open_p, high_p, low_p, close_p, volume in rows:
            if isinstance(ts, str):
                ts = parse_scylla_ts(ts)
            if ts is None:
                continue

            open_v = to_float(open_p)
            high_v = to_float(high_p)
            low_v = to_float(low_p)
            close_v = to_float(close_p)
            volume_v = to_int(volume)
            if None not in (open_v, high_v, low_v, close_v):
                vwap = (open_v + high_v + low_v + close_v) / 4.0
            else:
                vwap = None

            session.execute(
                insert_agg,
                [symbol, ts.date(), interval, ts, open_v, high_v, low_v, close_v, volume_v, vwap],
            )
            stats["agg_rows_inserted"] += 1

    rows = ch_client.query(
        (
            "SELECT symbol, trade_date, open, high, low, close, volume, change_percent "
            "FROM v_ohlcv_daily WHERE symbol = %(symbol)s ORDER BY trade_date"
        ),
        parameters={"symbol": symbol},
    ).result_rows

    for _sym, trade_date, open_p, high_p, low_p, close_p, volume, change_pct in rows:
        if isinstance(trade_date, str):
            dt = parse_scylla_ts(trade_date)
            if dt is None:
                continue
            trade_date = dt.date()
        elif hasattr(trade_date, "date"):
            trade_date = trade_date.date()

        open_v = to_float(open_p)
        high_v = to_float(high_p)
        low_v = to_float(low_p)
        close_v = to_float(close_p)
        volume_v = to_int(volume)
        change_pct_v = to_float(change_pct)

        change_v = None
        if open_v is not None and close_v is not None:
            change_v = close_v - open_v

        vwap = None
        if None not in (open_v, high_v, low_v, close_v):
            vwap = (open_v + high_v + low_v + close_v) / 4.0

        session.execute(
            insert_daily,
            [
                symbol,
                trade_date,
                open_v,
                high_v,
                low_v,
                close_v,
                volume_v,
                change_v,
                change_pct_v,
                vwap,
                exchange,
                None,
                None,
            ],
        )
        stats["daily_rows_inserted"] += 1

    return stats


def refresh_scylla_latest(session, symbol: str) -> bool:
    rows = list(
        session.execute(
            """
            SELECT timestamp, price, exchange, quote_type, market_hours,
                   change_percent, day_volume, change, last_size, price_hint, producer_timestamp
            FROM stock_prices
            WHERE symbol = %s
            LIMIT 1
            """,
            [symbol],
        )
    )

    if not rows:
        session.execute("DELETE FROM stock_latest_prices WHERE symbol = %s", [symbol])
        return False

    row = rows[0]
    ts = parse_scylla_ts(row.get("timestamp"))
    price = to_float(row.get("price"))
    if ts is None or price is None:
        session.execute("DELETE FROM stock_latest_prices WHERE symbol = %s", [symbol])
        return False

    session.execute(
        """
        INSERT INTO stock_latest_prices
            (symbol, price, timestamp, exchange, quote_type, market_hours,
             change_percent, day_volume, change, last_size, price_hint, producer_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [
            symbol,
            price,
            ts,
            row.get("exchange"),
            to_int(row.get("quote_type")),
            to_int(row.get("market_hours")),
            to_float(row.get("change_percent")),
            to_int(row.get("day_volume")),
            to_float(row.get("change")),
            to_int(row.get("last_size")),
            row.get("price_hint"),
            to_int(row.get("producer_timestamp")),
        ],
    )
    return True


def apply_scylla_raw_cleanup(
    session,
    symbol: str,
    outlier_keys: Dict[str, List[Tuple]],
) -> Dict[str, int]:
    deleted = {
        "stock_prices": 0,
        "stock_daily_summary_outliers": 0,
        "stock_prices_agg_outliers": 0,
    }

    for timestamp, _price in outlier_keys["stock_prices"]:
        session.execute(
            "DELETE FROM stock_prices WHERE symbol = %s AND timestamp = %s",
            [symbol, timestamp],
        )
        deleted["stock_prices"] += 1

    for trade_date, _close in outlier_keys["stock_daily_summary"]:
        session.execute(
            "DELETE FROM stock_daily_summary WHERE symbol = %s AND trade_date = %s",
            [symbol, trade_date],
        )
        deleted["stock_daily_summary_outliers"] += 1

    for bucket_date, interval, ts, _close in outlier_keys["stock_prices_agg"]:
        session.execute(
            "DELETE FROM stock_prices_agg WHERE symbol = %s AND bucket_date = %s AND interval = %s AND ts = %s",
            [symbol, bucket_date, interval, ts],
        )
        deleted["stock_prices_agg_outliers"] += 1

    return deleted


def print_plan(
    symbol: str,
    market: Optional[str],
    expected_exchange: Optional[str],
    anchor: float,
    anchor_source: str,
    low_ratio: float,
    high_ratio: float,
    outlier_keys: Dict[str, List[Tuple]],
    ch_outliers: int,
    agg_partitions: List[Tuple[object, str]],
) -> None:
    print("=" * 72)
    print(f"Symbol: {symbol}")
    print(f"Market: {market or '--'} | Expected exchange: {expected_exchange or '--'}")
    print(f"Anchor price: {anchor:.6f}")
    print(f"Anchor source: {anchor_source}")
    print(f"Scale window: [{low_ratio:.4f}, {high_ratio:.4f}] -> [{anchor*low_ratio:.6f}, {anchor*high_ratio:.6f}]")
    print("-" * 72)
    print(f"Scylla outliers stock_prices: {len(outlier_keys['stock_prices'])}")
    print(f"Scylla outliers stock_daily_summary: {len(outlier_keys['stock_daily_summary'])}")
    print(f"Scylla outliers stock_prices_agg: {len(outlier_keys['stock_prices_agg'])}")
    print(f"Scylla agg partitions to clear for rebuild: {len(agg_partitions)}")
    print(f"ClickHouse outliers stock_ticks: {ch_outliers}")

    def sample(items: Sequence[Tuple], n: int = 3) -> str:
        return " | ".join(str(x) for x in items[:n]) if items else "-"

    print("-" * 72)
    print(f"Sample stock_prices outliers: {sample(outlier_keys['stock_prices'])}")
    print(f"Sample daily outliers: {sample(outlier_keys['stock_daily_summary'])}")
    print(f"Sample agg outliers: {sample(outlier_keys['stock_prices_agg'])}")
    print("=" * 72)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clean mixed-scale contamination for one symbol")
    p.add_argument("--symbol", required=True, help="Ticker symbol, e.g. ACB")
    p.add_argument(
        "--anchor-price",
        type=float,
        default=None,
        help="Optional anchor price. If omitted, use stock_latest_prices then latest raw tick.",
    )
    p.add_argument("--low-ratio", type=float, default=0.2, help="Lower compatibility ratio")
    p.add_argument("--high-ratio", type=float, default=5.0, help="Upper compatibility ratio")
    p.add_argument(
        "--skip-clickhouse",
        action="store_true",
        help="Only clean/rebuild Scylla; do not mutate ClickHouse",
    )
    p.add_argument(
        "--skip-scylla-rebuild",
        action="store_true",
        help="Do not rebuild Scylla derived tables from ClickHouse views",
    )
    p.add_argument(
        "--apply",
        action="store_true",
        help="Execute deletions/rebuild. Default is dry-run.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    symbol = str(args.symbol or "").strip().upper()
    if not symbol:
        print("[ERROR] --symbol is required", file=sys.stderr)
        return 1

    if args.low_ratio <= 0 or args.high_ratio <= 0 or args.low_ratio >= args.high_ratio:
        print("[ERROR] invalid ratio bounds", file=sys.stderr)
        return 1

    cluster = None
    session = None
    ch_client = None

    try:
        cluster, session = connect_scylla()
        ch_client = connect_clickhouse()

        market_sets = load_market_sets(REGISTRY_PATH)
        market = infer_symbol_market(symbol, market_sets)
        expected_exchange = expected_exchange_for_market(market)

        anchor, anchor_source = get_anchor_price(
            session,
            symbol,
            args.anchor_price,
            expected_exchange,
        )
        outlier_keys = collect_scylla_outlier_keys(
            session,
            symbol,
            anchor,
            args.low_ratio,
            args.high_ratio,
        )
        agg_partitions = collect_scylla_agg_partitions(session, symbol)

        ch_outliers = 0
        if not args.skip_clickhouse:
            ch_outliers = count_clickhouse_outliers(
                ch_client,
                symbol,
                anchor,
                args.low_ratio,
                args.high_ratio,
            )

        print_plan(
            symbol,
            market,
            expected_exchange,
            anchor,
            anchor_source,
            args.low_ratio,
            args.high_ratio,
            outlier_keys,
            ch_outliers,
            agg_partitions,
        )

        if not args.apply:
            print("[DRY-RUN] No mutation executed. Re-run with --apply to perform cleanup.")
            return 0

        print("[APPLY] Cleaning Scylla raw outliers...")
        deleted = apply_scylla_raw_cleanup(session, symbol, outlier_keys)
        print(f"[APPLY] Deleted from Scylla raw/outlier tables: {deleted}")

        if not args.skip_clickhouse:
            print("[APPLY] Cleaning ClickHouse stock_ticks outliers...")
            apply_clickhouse_cleanup(
                ch_client,
                symbol,
                anchor,
                args.low_ratio,
                args.high_ratio,
            )
            print("[APPLY] Rebuilding ClickHouse aggregate-state tables...")
            rebuild_clickhouse_states(ch_client, symbol)

        if not args.skip_scylla_rebuild:
            print("[APPLY] Clearing Scylla derived partitions...")
            clear_stats = clear_scylla_derived(session, symbol)
            print(f"[APPLY] Cleared Scylla derived: {clear_stats}")

            # Try to keep exchange from latest row if available.
            latest_rows = list(
                session.execute(
                    "SELECT exchange FROM stock_latest_prices WHERE symbol = %s",
                    [symbol],
                )
            )
            exchange = expected_exchange
            if exchange is None:
                exchange = latest_rows[0].get("exchange") if latest_rows else None

            print("[APPLY] Rebuilding Scylla derived from ClickHouse views...")
            sync_stats = sync_scylla_derived_from_clickhouse(session, ch_client, symbol, exchange)
            print(f"[APPLY] Rebuilt Scylla derived: {sync_stats}")

        has_latest = refresh_scylla_latest(session, symbol)
        print(f"[APPLY] Refreshed stock_latest_prices: {'ok' if has_latest else 'removed (no valid raw tick)'}")

        print("[DONE] Cleanup completed.")
        return 0

    except Exception as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    finally:
        if cluster is not None:
            cluster.shutdown()
        if ch_client is not None:
            try:
                ch_client.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
