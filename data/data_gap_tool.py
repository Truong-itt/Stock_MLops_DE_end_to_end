"""
Audit and backfill missing stock data for the configured symbol registry.

Why this exists:
- the UI can show symbols that do not yet have data in Scylla
- some Yahoo symbols for Vietnam need provider aliases such as `.VN`
- we want a repeatable tool in `data/` to audit coverage and optionally
  backfill missing layers from Yahoo Finance historical endpoints

This tool focuses on the layers the current application reads directly:
- stock_prices
- stock_latest_prices
- stock_prices_agg
- stock_daily_summary
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import yfinance as yf
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from logger import get_logger
from symbol_registry import SymbolRegistry


logger = get_logger(log_filename="data_gap_tool.log")

SCYLLA_HOSTS = ["scylla-node1", "scylla-node2", "scylla-node3"]
SCYLLA_PORT = 9042
SCYLLA_KS = "stock_data"
REPORT_PATH = Path("/var/log/stockraw/data_gap_report.json")

INTRADAY_INTERVALS = ("5m", "1h", "3h", "6h")


@dataclass
class SymbolRecord:
    symbol: str
    market: str


class DataGapTool:
    def __init__(self):
        self.registry = SymbolRegistry()
        self.cluster = None
        self.session = None
        self._provider_cache: Dict[Tuple[str, str], Optional[str]] = {}

    def connect(self) -> None:
        logger.info("Connecting to ScyllaDB for audit/backfill")
        self.cluster = Cluster(contact_points=SCYLLA_HOSTS, port=SCYLLA_PORT, protocol_version=4)
        self.session = self.cluster.connect(SCYLLA_KS)
        self.session.row_factory = dict_factory

    def close(self) -> None:
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Closed ScyllaDB connection")

    def execute(self, query: str, params: Optional[Sequence] = None):
        return self.session.execute(query, params or [])

    def registry_symbols(self) -> List[SymbolRecord]:
        data = self.registry.load()
        rows: List[SymbolRecord] = []
        for market, meta in data["markets"].items():
            for symbol in meta.get("symbols", []):
                rows.append(SymbolRecord(symbol=symbol, market=market))
        return rows

    def _coverage_sets(self) -> Dict[str, object]:
        latest = {row["symbol"] for row in self.execute("SELECT symbol FROM stock_latest_prices")}
        daily = {row["symbol"] for row in self.execute("SELECT symbol FROM stock_daily_summary")}
        ticks = {row["symbol"] for row in self.execute("SELECT symbol FROM stock_prices")}
        agg_rows = list(self.execute("SELECT symbol, interval FROM stock_prices_agg"))
        agg_by_interval = {
            interval: {row["symbol"] for row in agg_rows if row.get("interval") == interval}
            for interval in INTRADAY_INTERVALS
        }
        return {
            "latest": latest,
            "daily": daily,
            "ticks": ticks,
            "agg": agg_by_interval,
        }

    def audit(self) -> Dict:
        registry_rows = self.registry_symbols()
        coverage = self._coverage_sets()
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {},
            "missing_symbols": {},
            "symbols": [],
        }

        latest = coverage["latest"]
        daily = coverage["daily"]
        ticks = coverage["ticks"]
        agg = coverage["agg"]

        missing_counts = {
            "latest": 0,
            "daily": 0,
            "ticks": 0,
            **{f"agg_{interval}": 0 for interval in INTRADAY_INTERVALS},
        }
        missing_symbols = {key: [] for key in missing_counts}

        for item in registry_rows:
            missing = {
                "latest": item.symbol not in latest,
                "daily": item.symbol not in daily,
                "ticks": item.symbol not in ticks,
                **{
                    f"agg_{interval}": item.symbol not in agg[interval]
                    for interval in INTRADAY_INTERVALS
                },
            }
            for key, flag in missing.items():
                if flag:
                    missing_counts[key] += 1
                    missing_symbols[key].append({"symbol": item.symbol, "market": item.market})
            report["symbols"].append(
                {
                    "symbol": item.symbol,
                    "market": item.market,
                    "missing": missing,
                }
            )

        report["summary"] = {
            "registry_total": len(registry_rows),
            "missing_counts": missing_counts,
        }
        report["missing_symbols"] = missing_symbols
        return report

    def write_report(self, report: Dict, output: Path = REPORT_PATH) -> None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
        logger.info("Wrote audit report to %s", output)

    def provider_candidates(self, symbol: str, market: str) -> List[str]:
        sym = symbol.upper()
        if market == "vn":
            return [f"{sym}.VN", sym, f"{sym}.HN", f"{sym}.HM"]
        return [sym]

    def resolve_provider_symbol(self, symbol: str, market: str) -> Optional[str]:
        cache_key = (symbol.upper(), market)
        if cache_key in self._provider_cache:
            return self._provider_cache[cache_key]

        for candidate in self.provider_candidates(symbol, market):
            try:
                history = yf.Ticker(candidate).history(period="5d", interval="1d", auto_adjust=False)
                if not history.empty:
                    self._provider_cache[cache_key] = candidate
                    logger.info("Resolved %s/%s -> %s", symbol, market, candidate)
                    return candidate
            except Exception as exc:
                logger.debug("resolve candidate failed %s -> %s", candidate, exc)

        self._provider_cache[cache_key] = None
        logger.warning("Could not resolve provider symbol for %s/%s", symbol, market)
        return None

    def fetch_history(self, provider_symbol: str, interval: str, period: str) -> pd.DataFrame:
        df = yf.Ticker(provider_symbol).history(period=period, interval=interval, auto_adjust=False)
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.rename(columns={c: c.lower() for c in df.columns})
        keep_cols = [c for c in ("open", "high", "low", "close", "volume") if c in df.columns]
        df = df[keep_cols].copy()
        df = df.dropna(subset=["close"])
        if df.empty:
            return df
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
        return df

    def _resample(self, df: pd.DataFrame, rule: str) -> pd.DataFrame:
        if df.empty:
            return df
        grouped = df.resample(rule).agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        grouped = grouped.dropna(subset=["close"])
        return grouped

    def _vwap(self, open_p, high_p, low_p, close_p) -> Optional[float]:
        if any(v is None or (isinstance(v, float) and math.isnan(v)) for v in (open_p, high_p, low_p, close_p)):
            return None
        return float(open_p + high_p + low_p + close_p) / 4.0

    def _prepare_intraday_frames(self, provider_symbol: str) -> Dict[str, pd.DataFrame]:
        frames: Dict[str, pd.DataFrame] = {}

        base_5m = self.fetch_history(provider_symbol, interval="5m", period="5d")
        if not base_5m.empty:
            frames["5m"] = base_5m
            frames["1h"] = self._resample(base_5m, "1h")
            frames["3h"] = self._resample(base_5m, "3h")
            frames["6h"] = self._resample(base_5m, "6h")
            return frames

        base_1h = self.fetch_history(provider_symbol, interval="1h", period="30d")
        if not base_1h.empty:
            frames["1h"] = base_1h
            frames["3h"] = self._resample(base_1h, "3h")
            frames["6h"] = self._resample(base_1h, "6h")
        return frames

    def _insert_stock_prices(self, symbol: str, market: str, frame: pd.DataFrame) -> int:
        if frame.empty:
            return 0

        query = """
            INSERT INTO stock_prices (
                symbol, timestamp, price, exchange, quote_type, market_hours,
                change_percent, day_volume, change, last_size, price_hint, producer_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        prepared = self.session.prepare(query)

        count = 0
        previous_close = None
        for ts, row in frame.iterrows():
            close_p = float(row["close"])
            open_p = float(row["open"]) if not pd.isna(row["open"]) else close_p
            volume = int(row["volume"]) if not pd.isna(row["volume"]) else 0
            change_val = close_p - (previous_close if previous_close is not None else open_p)
            base = previous_close if previous_close not in (None, 0) else open_p
            change_pct = (change_val / base * 100.0) if base else 0.0
            ts_utc = ts.to_pydatetime().astimezone(timezone.utc)
            ts_text = ts_utc.isoformat()
            producer_ts = int(ts_utc.timestamp() * 1000)

            self.session.execute(
                prepared,
                (
                    symbol,
                    ts_text,
                    close_p,
                    market.upper(),
                    None,
                    None,
                    change_pct,
                    volume,
                    change_val,
                    volume,
                    "2",
                    producer_ts,
                ),
            )
            previous_close = close_p
            count += 1
        return count

    def _insert_stock_latest(self, symbol: str, market: str, daily_frame: pd.DataFrame, intraday_frames: Dict[str, pd.DataFrame]) -> int:
        source = None
        if intraday_frames.get("5m") is not None and not intraday_frames["5m"].empty:
            source = intraday_frames["5m"].iloc[-1], intraday_frames["5m"].index[-1]
        elif intraday_frames.get("1h") is not None and not intraday_frames["1h"].empty:
            source = intraday_frames["1h"].iloc[-1], intraday_frames["1h"].index[-1]
        elif not daily_frame.empty:
            source = daily_frame.iloc[-1], daily_frame.index[-1]

        if source is None:
            return 0

        row, ts = source
        close_p = float(row["close"])
        open_p = float(row["open"]) if not pd.isna(row["open"]) else close_p
        volume = int(row["volume"]) if not pd.isna(row["volume"]) else 0
        change_val = close_p - open_p
        change_pct = (change_val / open_p * 100.0) if open_p else 0.0
        ts_utc = ts.to_pydatetime().astimezone(timezone.utc)
        producer_ts = int(ts_utc.timestamp() * 1000)

        query = """
            INSERT INTO stock_latest_prices (
                symbol, price, timestamp, exchange, quote_type, market_hours,
                change_percent, day_volume, change, last_size, price_hint, producer_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        prepared = self.session.prepare(query)
        self.session.execute(
            prepared,
            (
                symbol,
                close_p,
                ts_utc,
                market.upper(),
                None,
                None,
                change_pct,
                volume,
                change_val,
                volume,
                "2",
                producer_ts,
            ),
        )
        return 1

    def _insert_daily_summary(self, symbol: str, market: str, daily_frame: pd.DataFrame) -> int:
        if daily_frame.empty:
            return 0
        query = """
            INSERT INTO stock_daily_summary (
                symbol, trade_date, open, high, low, close, volume,
                change, change_percent, vwap, exchange, quote_type, market_hours
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        prepared = self.session.prepare(query)
        count = 0
        for ts, row in daily_frame.iterrows():
            open_p = float(row["open"]) if not pd.isna(row["open"]) else None
            high_p = float(row["high"]) if not pd.isna(row["high"]) else None
            low_p = float(row["low"]) if not pd.isna(row["low"]) else None
            close_p = float(row["close"]) if not pd.isna(row["close"]) else None
            volume = int(row["volume"]) if not pd.isna(row["volume"]) else 0
            change_val = (close_p - open_p) if open_p is not None and close_p is not None else None
            change_pct = ((change_val / open_p) * 100.0) if open_p not in (None, 0) and change_val is not None else None
            vwap = self._vwap(open_p, high_p, low_p, close_p)
            trade_date = ts.to_pydatetime().date()

            self.session.execute(
                prepared,
                (
                    symbol,
                    trade_date,
                    open_p,
                    high_p,
                    low_p,
                    close_p,
                    volume,
                    change_val,
                    change_pct,
                    vwap,
                    market.upper(),
                    None,
                    None,
                ),
            )
            count += 1
        return count

    def _insert_agg(self, symbol: str, frames: Dict[str, pd.DataFrame]) -> int:
        query = """
            INSERT INTO stock_prices_agg (
                symbol, bucket_date, interval, ts, open, high, low, close, volume, vwap
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        prepared = self.session.prepare(query)
        count = 0

        for interval, frame in frames.items():
            if interval not in INTRADAY_INTERVALS or frame.empty:
                continue
            for ts, row in frame.iterrows():
                ts_utc = ts.to_pydatetime().astimezone(timezone.utc)
                open_p = float(row["open"]) if not pd.isna(row["open"]) else None
                high_p = float(row["high"]) if not pd.isna(row["high"]) else None
                low_p = float(row["low"]) if not pd.isna(row["low"]) else None
                close_p = float(row["close"]) if not pd.isna(row["close"]) else None
                volume = int(row["volume"]) if not pd.isna(row["volume"]) else 0
                vwap = self._vwap(open_p, high_p, low_p, close_p)

                self.session.execute(
                    prepared,
                    (
                        symbol,
                        ts_utc.date(),
                        interval,
                        ts_utc,
                        open_p,
                        high_p,
                        low_p,
                        close_p,
                        volume,
                        vwap,
                    ),
                )
                count += 1
        return count

    def backfill_symbol(self, item: SymbolRecord) -> Dict:
        provider_symbol = self.resolve_provider_symbol(item.symbol, item.market)
        if not provider_symbol:
            return {
                "symbol": item.symbol,
                "market": item.market,
                "status": "unresolved",
            }

        daily_frame = self.fetch_history(provider_symbol, interval="1d", period="60d")
        intraday_frames = self._prepare_intraday_frames(provider_symbol)

        raw_count = 0
        if intraday_frames.get("5m") is not None and not intraday_frames["5m"].empty:
            raw_count = self._insert_stock_prices(item.symbol, item.market, intraday_frames["5m"])

        agg_count = self._insert_agg(item.symbol, intraday_frames)
        daily_count = self._insert_daily_summary(item.symbol, item.market, daily_frame)
        latest_count = self._insert_stock_latest(item.symbol, item.market, daily_frame, intraday_frames)

        return {
            "symbol": item.symbol,
            "market": item.market,
            "provider_symbol": provider_symbol,
            "status": "ok",
            "inserted": {
                "stock_prices": raw_count,
                "stock_prices_agg": agg_count,
                "stock_daily_summary": daily_count,
                "stock_latest_prices": latest_count,
            },
        }

    def symbols_needing_backfill(self, audit_report: Dict) -> List[SymbolRecord]:
        registry_rows = {row.symbol: row for row in self.registry_symbols()}
        selected: List[SymbolRecord] = []
        for row in audit_report["symbols"]:
            missing = row["missing"]
            if any(missing.values()):
                selected.append(registry_rows[row["symbol"]])
        return selected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit and backfill missing stock data")
    sub = parser.add_subparsers(dest="command", required=True)

    audit = sub.add_parser("audit", help="Audit data coverage against registry")
    audit.add_argument("--write-report", action="store_true", help="Write JSON report to log dir")

    backfill = sub.add_parser("backfill", help="Backfill missing symbols from Yahoo history")
    backfill.add_argument("--only-missing", action="store_true", help="Only backfill symbols missing key layers")
    backfill.add_argument("--max-symbols", type=int, default=0, help="Limit number of symbols to process")
    backfill.add_argument(
        "--symbols",
        type=str,
        default="",
        help="Comma-separated symbol list to backfill explicitly",
    )
    backfill.add_argument("--write-report", action="store_true", help="Write post-backfill audit report")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tool = DataGapTool()
    tool.connect()

    try:
        if args.command == "audit":
            report = tool.audit()
            logger.info("Audit summary: %s", report["summary"])
            if args.write_report:
                tool.write_report(report)
            print(json.dumps(report["summary"], ensure_ascii=True, indent=2))
            return 0

        report_before = tool.audit()
        targets = tool.registry_symbols()
        if args.only_missing:
            targets = tool.symbols_needing_backfill(report_before)
        if args.symbols:
            allowed = {part.strip().upper() for part in args.symbols.split(",") if part.strip()}
            targets = [row for row in targets if row.symbol.upper() in allowed]

        if args.max_symbols > 0:
            targets = targets[: args.max_symbols]

        logger.info("Backfilling %d symbols", len(targets))
        results = []
        for item in targets:
            try:
                result = tool.backfill_symbol(item)
                logger.info("Backfill result %s", result)
                results.append(result)
            except Exception as exc:
                logger.exception("Backfill failed for %s: %s", item.symbol, exc)
                results.append(
                    {
                        "symbol": item.symbol,
                        "market": item.market,
                        "status": "error",
                        "error": str(exc),
                    }
                )

        report_after = tool.audit()
        payload = {
            "backfill_finished_at": datetime.now(timezone.utc).isoformat(),
            "before": report_before["summary"],
            "after": report_after["summary"],
            "results": results,
        }
        if args.write_report:
            tool.write_report(report_after)
            REPORT_PATH.with_name("data_gap_backfill.json").write_text(
                json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
                encoding="utf-8",
            )
            logger.info("Wrote backfill payload to %s", REPORT_PATH.with_name("data_gap_backfill.json"))

        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    finally:
        tool.close()


if __name__ == "__main__":
    raise SystemExit(main())
