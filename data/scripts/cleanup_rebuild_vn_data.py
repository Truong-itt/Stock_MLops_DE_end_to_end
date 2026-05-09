#!/usr/bin/env python3
"""One-off maintenance for VN symbols.

This script does both tasks:
1) Rebuild ClickHouse OHLCV aggregate-state tables for VN symbols
   (and remove obsolete ACV/VGI states).
2) Clean Scylla historical data for VN policy and rebuild Scylla
   derived tables from ClickHouse clean views.

Safe defaults:
- VN symbols are loaded from symbol_registry.json.
- ACV/VGI are treated as obsolete symbols and are purged.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import clickhouse_connect
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

SCYLLA_HOSTS = ["scylla-node1", "scylla-node2", "scylla-node3"]
SCYLLA_PORT = 9042
SCYLLA_KEYSPACE = "stock_data"

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "truongittstock"
CLICKHOUSE_DB = "stock_warehouse"

REGISTRY_PATH = os.getenv("SYMBOL_REGISTRY_PATH", "/app/config/symbol_registry.json")
OBSOLETE_SYMBOLS = ["ACV", "VGI"]

AGG_STATE_TABLES = [
    "stock_ohlcv_1m",
    "stock_ohlcv_5m",
    "stock_ohlcv_1h",
    "stock_ohlcv_3h",
    "stock_ohlcv_6h",
    "stock_ohlcv_daily",
]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_sql_str_list(values: Sequence[str]) -> str:
    escaped = ["'" + v.replace("'", "''") + "'" for v in values]
    return ",".join(escaped)


def parse_scylla_timestamp(value: object) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    raw = str(value).strip()
    if not raw:
        return None

    try:
        # Epoch millis or seconds fallback
        if raw.isdigit():
            iv = int(raw)
            if iv > 10**12:
                return datetime.fromtimestamp(iv / 1000.0, tz=timezone.utc)
            if iv > 10**9:
                return datetime.fromtimestamp(iv, tz=timezone.utc)
    except Exception:
        pass

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_float(v: object) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def to_int(v: object) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def load_vn_symbols(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    symbols = data.get("markets", {}).get("vn", {}).get("symbols", [])
    out = []
    for sym in symbols:
        ss = str(sym).strip().upper()
        if ss:
            out.append(ss)
    return out


def connect_clickhouse():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def connect_scylla():
    cluster = Cluster(SCYLLA_HOSTS, port=SCYLLA_PORT, protocol_version=4)
    session = cluster.connect(SCYLLA_KEYSPACE)
    session.row_factory = dict_factory
    return cluster, session


def wait_clickhouse_mutations(
    ch_client,
    table_names: Sequence[str],
    since: datetime,
    timeout_sec: int = 1800,
) -> None:
    table_sql = to_sql_str_list(table_names)
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
            print("[ClickHouse] mutations done")
            return

        sample = ", ".join(f"{r[0]}:{r[1]} parts={r[3]}" for r in pending[:4])
        print(f"[ClickHouse] waiting mutations ({len(pending)} pending): {sample}")

        if time.time() > deadline:
            raise TimeoutError("Timed out waiting ClickHouse mutations")
        time.sleep(2)


def rebuild_clickhouse_vn_aggregates(ch_client, vn_symbols: Sequence[str]) -> None:
    target_cleanup = list(dict.fromkeys(list(vn_symbols) + OBSOLETE_SYMBOLS))
    cleanup_sql = to_sql_str_list(target_cleanup)
    vn_sql = to_sql_str_list(vn_symbols)

    print(f"[ClickHouse] rebuilding aggregates for {len(vn_symbols)} VN symbols")

    mutation_since = now_utc()
    for table in AGG_STATE_TABLES:
        ch_client.command(
            f"ALTER TABLE {CLICKHOUSE_DB}.{table} DELETE WHERE symbol IN ({cleanup_sql})"
        )
        print(f"  - queued delete: {table}")

    wait_clickhouse_mutations(ch_client, AGG_STATE_TABLES, mutation_since)

    insert_statements = [
        f"""
        INSERT INTO {CLICKHOUSE_DB}.stock_ohlcv_1m
        SELECT
            symbol,
            toStartOfMinute(event_time) AS bucket,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM {CLICKHOUSE_DB}.stock_ticks
        WHERE symbol IN ({vn_sql})
        GROUP BY symbol, bucket
        """,
        f"""
        INSERT INTO {CLICKHOUSE_DB}.stock_ohlcv_5m
        SELECT
            symbol,
            toStartOfFiveMinutes(event_time) AS bucket,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM {CLICKHOUSE_DB}.stock_ticks
        WHERE symbol IN ({vn_sql})
        GROUP BY symbol, bucket
        """,
        f"""
        INSERT INTO {CLICKHOUSE_DB}.stock_ohlcv_1h
        SELECT
            symbol,
            toStartOfHour(event_time) AS bucket,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM {CLICKHOUSE_DB}.stock_ticks
        WHERE symbol IN ({vn_sql})
        GROUP BY symbol, bucket
        """,
        f"""
        INSERT INTO {CLICKHOUSE_DB}.stock_ohlcv_3h
        SELECT
            symbol,
            toStartOfInterval(event_time, INTERVAL 3 HOUR) AS bucket,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM {CLICKHOUSE_DB}.stock_ticks
        WHERE symbol IN ({vn_sql})
        GROUP BY symbol, bucket
        """,
        f"""
        INSERT INTO {CLICKHOUSE_DB}.stock_ohlcv_6h
        SELECT
            symbol,
            toStartOfInterval(event_time, INTERVAL 6 HOUR) AS bucket,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM {CLICKHOUSE_DB}.stock_ticks
        WHERE symbol IN ({vn_sql})
        GROUP BY symbol, bucket
        """,
        f"""
        INSERT INTO {CLICKHOUSE_DB}.stock_ohlcv_daily
        SELECT
            symbol,
            toDate(event_time) AS trade_date,
            argMinState(price, event_time),
            argMaxState(price, event_time),
            maxState(price),
            minState(price),
            countState(toUInt64(1)),
            argMaxState(coalesce(change_percent, 0), event_time)
        FROM {CLICKHOUSE_DB}.stock_ticks
        WHERE symbol IN ({vn_sql})
        GROUP BY symbol, trade_date
        """,
    ]

    for idx, sql in enumerate(insert_statements, start=1):
        ch_client.command(sql)
        print(f"  - inserted aggregate states step {idx}/{len(insert_statements)}")


def fetch_agg_partitions(session, symbol: str) -> List[Tuple[object, str]]:
    rows = session.execute(
        "SELECT bucket_date, interval FROM stock_prices_agg WHERE symbol = %s ALLOW FILTERING",
        [symbol],
    )
    parts = set()
    for row in rows:
        parts.add((row["bucket_date"], row["interval"]))
    return list(parts)


def delete_agg_symbol_partitions(session, symbol: str) -> int:
    parts = fetch_agg_partitions(session, symbol)
    for bucket_date, interval in parts:
        session.execute(
            "DELETE FROM stock_prices_agg WHERE symbol = %s AND bucket_date = %s AND interval = %s",
            [symbol, bucket_date, interval],
        )
    return len(parts)


def clean_scylla_raw_and_obsolete(session, vn_symbols: Sequence[str]) -> Dict[str, int]:
    stats = {
        "vn_non_vse_deleted": 0,
        "obsolete_raw_deleted_symbols": 0,
        "obsolete_latest_deleted_symbols": 0,
        "obsolete_daily_deleted_symbols": 0,
        "obsolete_agg_deleted_partitions": 0,
    }

    # Remove non-VSE raw ticks for current VN symbols if any.
    for sym in vn_symbols:
        rows = session.execute(
            "SELECT timestamp, exchange FROM stock_prices WHERE symbol = %s",
            [sym],
        )
        for row in rows:
            exchange = (row.get("exchange") or "").upper()
            if exchange != "VSE":
                session.execute(
                    "DELETE FROM stock_prices WHERE symbol = %s AND timestamp = %s",
                    [sym, row["timestamp"]],
                )
                stats["vn_non_vse_deleted"] += 1

    # Purge obsolete symbols entirely from Scylla tables.
    for sym in OBSOLETE_SYMBOLS:
        session.execute("DELETE FROM stock_prices WHERE symbol = %s", [sym])
        stats["obsolete_raw_deleted_symbols"] += 1

        session.execute("DELETE FROM stock_latest_prices WHERE symbol = %s", [sym])
        stats["obsolete_latest_deleted_symbols"] += 1

        session.execute("DELETE FROM stock_daily_summary WHERE symbol = %s", [sym])
        stats["obsolete_daily_deleted_symbols"] += 1

        stats["obsolete_agg_deleted_partitions"] += delete_agg_symbol_partitions(session, sym)

    return stats


def clear_scylla_vn_derived(session, vn_symbols: Sequence[str]) -> Dict[str, int]:
    stats = {
        "latest_symbols_deleted": 0,
        "daily_symbols_deleted": 0,
        "agg_partitions_deleted": 0,
    }

    for sym in vn_symbols:
        session.execute("DELETE FROM stock_latest_prices WHERE symbol = %s", [sym])
        stats["latest_symbols_deleted"] += 1

        session.execute("DELETE FROM stock_daily_summary WHERE symbol = %s", [sym])
        stats["daily_symbols_deleted"] += 1

        stats["agg_partitions_deleted"] += delete_agg_symbol_partitions(session, sym)

    return stats


def sync_scylla_agg_from_clickhouse(ch_client, session, vn_symbols: Sequence[str]) -> Dict[str, int]:
    stats = {"agg_rows_inserted": 0, "daily_rows_inserted": 0}

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

    vn_sql = to_sql_str_list(vn_symbols)
    interval_views = {
        "1m": "v_ohlcv_1m",
        "5m": "v_ohlcv_5m",
        "1h": "v_ohlcv_1h",
        "3h": "v_ohlcv_3h",
        "6h": "v_ohlcv_6h",
    }

    for interval, view in interval_views.items():
        rows = ch_client.query(
            f"""
            SELECT symbol, bucket, open, high, low, close, volume, change_percent
            FROM {view}
            WHERE symbol IN ({vn_sql})
            ORDER BY symbol, bucket
            """
        ).result_rows

        inserted = 0
        for symbol, ts, open_p, high_p, low_p, close_p, volume, _change_pct in rows:
            if isinstance(ts, str):
                ts = parse_scylla_timestamp(ts)
            if ts is None:
                continue

            open_v = to_float(open_p)
            high_v = to_float(high_p)
            low_v = to_float(low_p)
            close_v = to_float(close_p)
            vol_v = to_int(volume)
            if None not in (open_v, high_v, low_v, close_v):
                vwap = (open_v + high_v + low_v + close_v) / 4.0
            else:
                vwap = None

            session.execute(
                insert_agg,
                [
                    symbol,
                    ts.date(),
                    interval,
                    ts,
                    open_v,
                    high_v,
                    low_v,
                    close_v,
                    vol_v,
                    vwap,
                ],
            )
            inserted += 1

        stats["agg_rows_inserted"] += inserted
        print(f"[Scylla] inserted {inserted} rows into stock_prices_agg for interval {interval}")

    daily_rows = ch_client.query(
        f"""
        SELECT symbol, trade_date, open, high, low, close, volume, change_percent
        FROM v_ohlcv_daily
        WHERE symbol IN ({vn_sql})
        ORDER BY symbol, trade_date
        """
    ).result_rows

    inserted_daily = 0
    for symbol, trade_date, open_p, high_p, low_p, close_p, volume, change_pct in daily_rows:
        if isinstance(trade_date, str):
            dt = parse_scylla_timestamp(trade_date)
            if dt is None:
                continue
            trade_date = dt.date()
        elif hasattr(trade_date, "date"):
            trade_date = trade_date.date()

        open_v = to_float(open_p)
        high_v = to_float(high_p)
        low_v = to_float(low_p)
        close_v = to_float(close_p)
        vol_v = to_int(volume)
        chg_pct = to_float(change_pct)

        if open_v is not None and close_v is not None:
            change_val = close_v - open_v
        else:
            change_val = None

        if None not in (open_v, high_v, low_v, close_v):
            vwap = (open_v + high_v + low_v + close_v) / 4.0
        else:
            vwap = None

        session.execute(
            insert_daily,
            [
                symbol,
                trade_date,
                open_v,
                high_v,
                low_v,
                close_v,
                vol_v,
                change_val,
                chg_pct,
                vwap,
                "VSE",
                None,
                None,
            ],
        )
        inserted_daily += 1

    stats["daily_rows_inserted"] = inserted_daily
    print(f"[Scylla] inserted {inserted_daily} rows into stock_daily_summary")
    return stats


def rebuild_scylla_latest_from_raw(session, vn_symbols: Sequence[str]) -> int:
    insert_latest = session.prepare(
        """
        INSERT INTO stock_latest_prices
            (symbol, price, timestamp, exchange, quote_type, market_hours,
             change_percent, day_volume, change, last_size, price_hint, producer_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    )

    inserted = 0
    for sym in vn_symbols:
        rows = session.execute(
            """
            SELECT timestamp, price, exchange, quote_type, market_hours,
                   change_percent, day_volume, change, last_size, price_hint, producer_timestamp
            FROM stock_prices WHERE symbol = %s LIMIT 1
            """,
            [sym],
        )
        row = next(iter(rows), None)
        if not row:
            continue

        ts = parse_scylla_timestamp(row.get("timestamp"))
        price = to_float(row.get("price"))
        if ts is None or price is None:
            continue

        session.execute(
            insert_latest,
            [
                sym,
                price,
                ts,
                "VSE",
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
        inserted += 1

    return inserted


def validate_scylla(session, vn_symbols: Sequence[str]) -> Dict[str, int]:
    stats = {
        "vn_non_vse_raw": 0,
        "vn_non_vse_latest": 0,
        "vn_non_vse_daily": 0,
        "obsolete_raw": 0,
        "obsolete_latest": 0,
        "obsolete_daily": 0,
        "obsolete_agg": 0,
    }

    vn_set = set(vn_symbols)

    for sym in vn_symbols:
        for row in session.execute("SELECT exchange FROM stock_prices WHERE symbol = %s", [sym]):
            if (row.get("exchange") or "").upper() != "VSE":
                stats["vn_non_vse_raw"] += 1

        row = next(
            iter(session.execute("SELECT exchange FROM stock_latest_prices WHERE symbol = %s", [sym])),
            None,
        )
        if row and (row.get("exchange") or "").upper() != "VSE":
            stats["vn_non_vse_latest"] += 1

        for row in session.execute("SELECT exchange FROM stock_daily_summary WHERE symbol = %s", [sym]):
            if (row.get("exchange") or "").upper() != "VSE":
                stats["vn_non_vse_daily"] += 1

    for sym in OBSOLETE_SYMBOLS:
        stats["obsolete_raw"] += sum(1 for _ in session.execute("SELECT timestamp FROM stock_prices WHERE symbol = %s", [sym]))
        stats["obsolete_latest"] += sum(1 for _ in session.execute("SELECT symbol FROM stock_latest_prices WHERE symbol = %s", [sym]))
        stats["obsolete_daily"] += sum(1 for _ in session.execute("SELECT trade_date FROM stock_daily_summary WHERE symbol = %s", [sym]))
        stats["obsolete_agg"] += sum(1 for _ in session.execute("SELECT ts FROM stock_prices_agg WHERE symbol = %s ALLOW FILTERING", [sym]))

    return stats


def validate_clickhouse(ch_client, vn_symbols: Sequence[str]) -> int:
    vn_sql = to_sql_str_list(vn_symbols)
    rows = ch_client.query(
        f"""
        SELECT count()
        FROM {CLICKHOUSE_DB}.stock_ticks
        WHERE symbol IN ({vn_sql}) AND exchange != 'VSE'
        """
    ).result_rows
    return int(rows[0][0]) if rows else 0


def main() -> int:
    start = now_utc()
    print(f"[START] cleanup + rebuild started at {start.isoformat()}")

    vn_symbols = load_vn_symbols(REGISTRY_PATH)
    if not vn_symbols:
        print("[ERROR] VN symbols are empty in registry", file=sys.stderr)
        return 1

    print(f"[INFO] VN symbols: {len(vn_symbols)} | obsolete purge: {','.join(OBSOLETE_SYMBOLS)}")

    ch = None
    cluster = None
    session = None

    try:
        ch = connect_clickhouse()
        cluster, session = connect_scylla()

        rebuild_clickhouse_vn_aggregates(ch, vn_symbols)

        clean_stats = clean_scylla_raw_and_obsolete(session, vn_symbols)
        print(f"[Scylla] raw/obsolete cleanup stats: {clean_stats}")

        clear_stats = clear_scylla_vn_derived(session, vn_symbols)
        print(f"[Scylla] derived clear stats: {clear_stats}")

        sync_stats = sync_scylla_agg_from_clickhouse(ch, session, vn_symbols)
        print(f"[Scylla] sync from ClickHouse stats: {sync_stats}")

        latest_count = rebuild_scylla_latest_from_raw(session, vn_symbols)
        print(f"[Scylla] rebuilt stock_latest_prices rows: {latest_count}")

        scylla_valid = validate_scylla(session, vn_symbols)
        clickhouse_non_vse = validate_clickhouse(ch, vn_symbols)

        print("[VERIFY] Scylla:", scylla_valid)
        print(f"[VERIFY] ClickHouse vn non-VSE raw rows: {clickhouse_non_vse}")

        end = now_utc()
        print(f"[DONE] completed at {end.isoformat()} (duration={(end-start).total_seconds():.1f}s)")

        bad = (
            scylla_valid["vn_non_vse_raw"]
            + scylla_valid["vn_non_vse_latest"]
            + scylla_valid["vn_non_vse_daily"]
            + scylla_valid["obsolete_raw"]
            + scylla_valid["obsolete_latest"]
            + scylla_valid["obsolete_daily"]
            + scylla_valid["obsolete_agg"]
            + clickhouse_non_vse
        )
        return 0 if bad == 0 else 2

    finally:
        if cluster is not None:
            cluster.shutdown()
        if ch is not None:
            try:
                ch.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
