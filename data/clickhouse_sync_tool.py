"""
Audit and sync ClickHouse stock_ticks from Scylla stock_prices.

Why this exists:
- ClickHouse is the warehouse/source for materialized OHLCV views
- Scylla may be ahead after direct backfills into stock_prices/_agg/daily
- we want a repeatable tool in `data/` to bring ClickHouse coverage back in
  line with the current registry and with Scylla raw ticks
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

import clickhouse_connect
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from logger import get_logger
from symbol_registry import SymbolRegistry


logger = get_logger(log_filename="clickhouse_sync_tool.log")

SCYLLA_HOSTS = ["scylla-node1", "scylla-node2", "scylla-node3"]
SCYLLA_PORT = 9042
SCYLLA_KS = "stock_data"

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = "truongittstock"
CLICKHOUSE_DB = "stock_warehouse"

REPORT_PATH = Path("/var/log/stockraw/clickhouse_sync_report.json")
INSERT_COLUMNS = [
    "symbol",
    "price",
    "event_time",
    "exchange",
    "quote_type",
    "market_hours",
    "change_percent",
    "change",
    "price_hint",
    "received_at",
    "day_volume",
    "last_size",
]
DERIVED_LAYER_QUERIES = {
    "agg_1m": "SELECT DISTINCT symbol FROM v_ohlcv_1m",
    "agg_5m": "SELECT DISTINCT symbol FROM v_ohlcv_5m",
    "agg_1h": "SELECT DISTINCT symbol FROM v_ohlcv_1h",
    "agg_3h": "SELECT DISTINCT symbol FROM v_ohlcv_3h",
    "agg_6h": "SELECT DISTINCT symbol FROM v_ohlcv_6h",
    "daily": "SELECT DISTINCT symbol FROM v_ohlcv_daily",
}


@dataclass
class SymbolLag:
    symbol: str
    market: str
    scylla_latest: Optional[datetime]
    clickhouse_latest: Optional[datetime]
    lag_seconds: Optional[float]
    missing_in_clickhouse: bool
    needs_sync: bool


class ClickHouseSyncTool:
    def __init__(self):
        self.registry = SymbolRegistry()
        self.cluster = None
        self.session = None
        self.ch_client = None

    def connect(self) -> None:
        logger.info("Connecting to ScyllaDB and ClickHouse")
        self.cluster = Cluster(contact_points=SCYLLA_HOSTS, port=SCYLLA_PORT, protocol_version=4)
        self.session = self.cluster.connect(SCYLLA_KS)
        self.session.row_factory = dict_factory
        self.ch_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASS,
            database=CLICKHOUSE_DB,
        )

    def close(self) -> None:
        if self.cluster:
            self.cluster.shutdown()
        if self.ch_client:
            try:
                self.ch_client.close()
            except Exception:
                pass
        logger.info("Closed ClickHouse/Scylla connections")

    def execute(self, query: str, params: Optional[Sequence] = None):
        return self.session.execute(query, params or [])

    def registry_rows(self) -> List[Tuple[str, str]]:
        data = self.registry.load()
        rows: List[Tuple[str, str]] = []
        for market, meta in data["markets"].items():
            for symbol in meta.get("symbols", []):
                rows.append((symbol, market))
        return rows

    def registry_symbols(self) -> Set[str]:
        return {symbol for symbol, _ in self.registry_rows()}

    def scylla_raw_symbols(self) -> Set[str]:
        return {row["symbol"] for row in self.execute("SELECT DISTINCT symbol FROM stock_prices")}

    def scylla_daily_symbols(self) -> Set[str]:
        return {row["symbol"] for row in self.execute("SELECT DISTINCT symbol FROM stock_daily_summary")}

    def scylla_agg_symbols(self) -> Dict[str, Set[str]]:
        rows = list(self.execute("SELECT symbol, interval FROM stock_prices_agg ALLOW FILTERING"))
        agg: Dict[str, Set[str]] = {}
        for row in rows:
            agg.setdefault(row["interval"], set()).add(row["symbol"])
        return agg

    def scylla_latest_map(self) -> Dict[str, Optional[datetime]]:
        rows = list(self.execute("SELECT symbol, timestamp FROM stock_latest_prices"))
        latest: Dict[str, Optional[datetime]] = {}
        for row in rows:
            latest[row["symbol"]] = self._normalize_datetime(row.get("timestamp"))
        return latest

    def clickhouse_symbol_set(self, query: str) -> Set[str]:
        return {row[0] for row in self.ch_client.query(query).result_rows}

    def clickhouse_raw_symbols(self) -> Set[str]:
        return self.clickhouse_symbol_set("SELECT DISTINCT symbol FROM stock_ticks")

    def clickhouse_raw_max_map(self) -> Dict[str, Optional[datetime]]:
        rows = self.ch_client.query(
            "SELECT symbol, max(event_time) FROM stock_ticks GROUP BY symbol"
        ).result_rows
        latest: Dict[str, Optional[datetime]] = {}
        for symbol, event_time in rows:
            latest[symbol] = self._normalize_datetime(event_time)
        return latest

    def derived_layer_coverage(self) -> Dict[str, Dict[str, object]]:
        registry = self.registry_symbols()
        scylla_agg = self.scylla_agg_symbols()
        scylla_daily = self.scylla_daily_symbols()
        coverage: Dict[str, Dict[str, object]] = {}
        for layer, query in DERIVED_LAYER_QUERIES.items():
            if layer == "daily":
                scylla_symbols = scylla_daily
            else:
                scylla_symbols = scylla_agg.get(layer.split("_", 1)[1], set())
            clickhouse_symbols = self.clickhouse_symbol_set(query)
            scylla_registry = scylla_symbols & registry
            clickhouse_registry = clickhouse_symbols & registry
            coverage[layer] = {
                "scylla_registry_count": len(scylla_registry),
                "clickhouse_registry_count": len(clickhouse_registry),
                "registry_missing_in_clickhouse_count": len(registry - clickhouse_registry),
                "registry_missing_in_clickhouse_sample": sorted(registry - clickhouse_registry)[:20],
                "registry_missing_in_scylla_count": len(registry - scylla_registry),
                "registry_missing_in_scylla_sample": sorted(registry - scylla_registry)[:20],
            }
        return coverage

    def audit(self, lag_grace_seconds: int = 60) -> Dict:
        registry_rows = self.registry_rows()
        registry = {symbol for symbol, _ in registry_rows}
        scylla_raw = self.scylla_raw_symbols()
        clickhouse_raw = self.clickhouse_raw_symbols()
        scylla_latest = self.scylla_latest_map()
        clickhouse_latest = self.clickhouse_raw_max_map()

        symbol_rows = []
        missing_count = 0
        lagging_count = 0

        for symbol, market in registry_rows:
            sc_latest = scylla_latest.get(symbol)
            ch_latest = clickhouse_latest.get(symbol)
            lag_seconds = None
            if sc_latest and ch_latest:
                lag_seconds = (sc_latest - ch_latest).total_seconds()
            missing_in_clickhouse = symbol in scylla_raw and symbol not in clickhouse_raw
            is_lagging = lag_seconds is not None and lag_seconds > lag_grace_seconds
            needs_sync = missing_in_clickhouse or is_lagging
            if missing_in_clickhouse:
                missing_count += 1
            if is_lagging:
                lagging_count += 1
            symbol_rows.append(
                {
                    "symbol": symbol,
                    "market": market,
                    "scylla_has_raw": symbol in scylla_raw,
                    "clickhouse_has_raw": symbol in clickhouse_raw,
                    "scylla_latest": sc_latest.isoformat() if sc_latest else None,
                    "clickhouse_latest": ch_latest.isoformat() if ch_latest else None,
                    "lag_seconds": lag_seconds,
                    "missing_in_clickhouse": missing_in_clickhouse,
                    "needs_sync": needs_sync,
                }
            )

        derived = self.derived_layer_coverage()
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "registry_total": len(registry),
                "scylla_raw_registry_count": len(scylla_raw & registry),
                "clickhouse_raw_registry_count": len(clickhouse_raw & registry),
                "raw_missing_in_clickhouse_count": missing_count,
                "raw_lagging_in_clickhouse_count": lagging_count,
            },
            "layers": {
                "raw_ticks": {
                    "scylla_registry_count": len(scylla_raw & registry),
                    "clickhouse_registry_count": len(clickhouse_raw & registry),
                    "registry_missing_in_clickhouse_count": len(registry - (clickhouse_raw & registry)),
                    "registry_missing_in_clickhouse_sample": sorted(registry - (clickhouse_raw & registry))[:20],
                    "registry_missing_in_scylla_count": len(registry - (scylla_raw & registry)),
                    "registry_missing_in_scylla_sample": sorted(registry - (scylla_raw & registry))[:20],
                },
                **derived,
            },
            "symbols": symbol_rows,
        }
        return report

    def write_report(self, report: Dict, output: Path = REPORT_PATH) -> None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
        logger.info("Wrote ClickHouse sync report to %s", output)

    def symbols_needing_sync(self, audit_report: Dict) -> List[Tuple[str, str]]:
        market_by_symbol = {symbol: market for symbol, market in self.registry_rows()}
        selected = []
        for row in audit_report["symbols"]:
            if row.get("needs_sync"):
                selected.append((row["symbol"], market_by_symbol.get(row["symbol"], "")))
        return selected

    def clickhouse_event_keys(self, symbol: str) -> Set[str]:
        safe_symbol = symbol.replace("'", "''")
        rows = self.ch_client.query(
            f"SELECT event_time FROM stock_ticks WHERE symbol = '{safe_symbol}'"
        ).result_rows
        keys = set()
        for (event_time,) in rows:
            dt = self._normalize_datetime(event_time)
            if dt:
                keys.add(self._dt_key(dt))
        return keys

    def scylla_raw_rows(self, symbol: str) -> List[Dict]:
        query = """
            SELECT symbol, timestamp, price, exchange, quote_type, market_hours,
                   change_percent, day_volume, change, last_size, price_hint, producer_timestamp
            FROM stock_prices
            WHERE symbol = %s
        """
        return list(self.execute(query, [symbol]))

    def sync_symbol(self, symbol: str, batch_size: int = 2000) -> Dict:
        existing_keys = self.clickhouse_event_keys(symbol)
        scylla_rows = self.scylla_raw_rows(symbol)

        to_insert = []
        invalid_rows = 0
        for row in scylla_rows:
            event_time = self._parse_scylla_timestamp(row.get("timestamp"))
            if not event_time:
                invalid_rows += 1
                continue
            key = self._dt_key(event_time)
            if key in existing_keys:
                continue

            received_at = self._parse_epoch_ms(row.get("producer_timestamp")) or event_time
            price = row.get("price")
            if price is None:
                invalid_rows += 1
                continue

            to_insert.append(
                (
                    row["symbol"],
                    float(price),
                    event_time,
                    row.get("exchange"),
                    self._maybe_int(row.get("quote_type")),
                    self._maybe_int(row.get("market_hours")),
                    self._maybe_float(row.get("change_percent")),
                    self._maybe_float(row.get("change")),
                    row.get("price_hint"),
                    received_at,
                    self._maybe_int(row.get("day_volume")),
                    self._maybe_int(row.get("last_size")),
                )
            )

        to_insert.sort(key=lambda item: item[2])
        inserted = 0
        for offset in range(0, len(to_insert), batch_size):
            chunk = to_insert[offset: offset + batch_size]
            if not chunk:
                continue
            self.ch_client.insert("stock_ticks", chunk, column_names=INSERT_COLUMNS)
            inserted += len(chunk)

        latest_inserted = to_insert[-1][2].isoformat() if to_insert else None
        return {
            "symbol": symbol,
            "status": "ok",
            "inserted_rows": inserted,
            "existing_rows": len(existing_keys),
            "scylla_rows": len(scylla_rows),
            "invalid_rows": invalid_rows,
            "latest_inserted_event_time": latest_inserted,
        }

    def _normalize_datetime(self, value) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        return self._parse_scylla_timestamp(value)

    def _parse_epoch_ms(self, value) -> Optional[datetime]:
        if value is None:
            return None
        try:
            raw = int(value)
        except (TypeError, ValueError):
            return None
        if raw > 10**12:
            return datetime.fromtimestamp(raw / 1000.0, tz=timezone.utc)
        if raw > 10**9:
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        return None

    def _parse_scylla_timestamp(self, value) -> Optional[datetime]:
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
            epoch_dt = self._parse_epoch_ms(int(raw))
            if epoch_dt:
                return epoch_dt
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _dt_key(self, value: datetime) -> str:
        dt = self._normalize_datetime(value)
        return dt.isoformat(timespec="milliseconds") if dt else ""

    @staticmethod
    def _maybe_int(value):
        if value is None:
            return None
        return int(value)

    @staticmethod
    def _maybe_float(value):
        if value is None:
            return None
        return float(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and sync ClickHouse stock_ticks from Scylla raw data")
    sub = parser.add_subparsers(dest="command", required=True)

    audit = sub.add_parser("audit", help="Audit ClickHouse coverage against Scylla and registry")
    audit.add_argument("--write-report", action="store_true", help="Write JSON report to log dir")
    audit.add_argument("--lag-grace-seconds", type=int, default=300, help="Lag threshold before a symbol is marked stale")

    sync = sub.add_parser("sync", help="Sync missing/lagging Scylla raw rows into ClickHouse")
    sync.add_argument("--only-missing", action="store_true", help="Only sync symbols that audit marks as missing/lagging")
    sync.add_argument("--symbols", type=str, default="", help="Comma-separated symbol list to sync explicitly")
    sync.add_argument("--max-symbols", type=int, default=0, help="Limit number of symbols to process")
    sync.add_argument("--batch-size", type=int, default=2000, help="Insert batch size for ClickHouse")
    sync.add_argument("--write-report", action="store_true", help="Write JSON audit/run reports to log dir")
    sync.add_argument("--lag-grace-seconds", type=int, default=300, help="Lag threshold before a symbol is marked stale")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tool = ClickHouseSyncTool()
    tool.connect()

    try:
        if args.command == "audit":
            report = tool.audit(lag_grace_seconds=args.lag_grace_seconds)
            logger.info("ClickHouse sync audit summary: %s", report["summary"])
            if args.write_report:
                tool.write_report(report)
            print(json.dumps(report["summary"], ensure_ascii=True, indent=2))
            return 0

        report_before = tool.audit(lag_grace_seconds=args.lag_grace_seconds)
        targets = tool.registry_rows()
        if args.only_missing:
            targets = tool.symbols_needing_sync(report_before)
        if args.symbols:
            allowed = {part.strip().upper() for part in args.symbols.split(",") if part.strip()}
            targets = [row for row in targets if row[0].upper() in allowed]
        if args.max_symbols > 0:
            targets = targets[: args.max_symbols]

        logger.info("Syncing %d symbols into ClickHouse", len(targets))
        results = []
        for symbol, _market in targets:
            try:
                result = tool.sync_symbol(symbol, batch_size=args.batch_size)
                logger.info("ClickHouse sync result %s", result)
                results.append(result)
            except Exception as exc:
                logger.exception("ClickHouse sync failed for %s: %s", symbol, exc)
                results.append(
                    {
                        "symbol": symbol,
                        "status": "error",
                        "error": str(exc),
                    }
                )

        report_after = tool.audit(lag_grace_seconds=args.lag_grace_seconds)
        payload = {
            "sync_finished_at": datetime.now(timezone.utc).isoformat(),
            "before": report_before["summary"],
            "after": report_after["summary"],
            "results": results,
        }
        if args.write_report:
            tool.write_report(report_after)
            REPORT_PATH.with_name("clickhouse_sync_run.json").write_text(
                json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
                encoding="utf-8",
            )
            logger.info("Wrote ClickHouse sync run payload to %s", REPORT_PATH.with_name("clickhouse_sync_run.json"))

        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    finally:
        tool.close()


if __name__ == "__main__":
    raise SystemExit(main())
