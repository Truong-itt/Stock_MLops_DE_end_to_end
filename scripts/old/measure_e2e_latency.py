#!/usr/bin/env python3
"""
Simple end-to-end latency measurement for this stock data pipeline.

Measured stages:
1) ClickHouse (stock_ticks):
   - ch_event_to_received_ms      = received_at - event_time
   - ch_received_to_inserted_ms   = inserted_at - received_at
   - ch_event_to_inserted_ms      = inserted_at - event_time
   - ch_event_to_now_ms           = now - event_time
2) Scylla (stock_latest_prices):
   - scylla_producer_to_write_ms  = WRITETIME(price) - producer_timestamp
   - scylla_event_to_write_ms     = WRITETIME(price) - timestamp
   - scylla_event_to_now_ms       = now - timestamp
3) API (/api/stocks/latest):
   - api_event_to_now_ms          = now - timestamp (user-facing freshness)

Outputs:
- scripts/e2e_latency_stage_stats.csv  (one row per stage)
- scripts/e2e_latency_summary.csv      (key-value metrics for slides)

Run:
  python3 scripts/measure_e2e_latency.py
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.error import URLError, HTTPError
from urllib.request import urlopen

import clickhouse_connect
from cassandra.cluster import Cluster


DAY_MS = 24 * 60 * 60 * 1000
WEEK_MS = 7 * DAY_MS
logging.getLogger("clickhouse_connect").setLevel(logging.CRITICAL)


def _to_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        fv = float(v)
        if not math.isfinite(fv):
            return None
        return fv
    except Exception:
        return None


def _to_epoch_ms(v) -> Optional[int]:
    """Parse int/float/datetime/iso-string into epoch milliseconds."""
    if v is None:
        return None

    if isinstance(v, datetime):
        dt = v
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp() * 1000)

    if isinstance(v, (int, float)):
        iv = int(v)
        if iv < 10_000_000_000:
            iv *= 1000
        return iv

    s = str(v).strip()
    if not s:
        return None

    if s.isdigit():
        iv = int(s)
        if iv < 10_000_000_000:
            iv *= 1000
        return iv

    try:
        normalized = s[:-1] + "+00:00" if s.endswith("Z") else s
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _percentile(sorted_values: List[float], q: float) -> Optional[float]:
    if not sorted_values:
        return None
    if q <= 0:
        return sorted_values[0]
    if q >= 1:
        return sorted_values[-1]
    pos = (len(sorted_values) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_values[lo]
    frac = pos - lo
    return sorted_values[lo] * (1 - frac) + sorted_values[hi] * frac


def _describe(values: Iterable[float]) -> Optional[Dict[str, float]]:
    xs = [float(v) for v in values if v is not None]
    if not xs:
        return None
    xs.sort()
    n = len(xs)
    return {
        "n": n,
        "mean_ms": sum(xs) / n,
        "p50_ms": _percentile(xs, 0.50),
        "p90_ms": _percentile(xs, 0.90),
        "p95_ms": _percentile(xs, 0.95),
        "p99_ms": _percentile(xs, 0.99),
        "min_ms": xs[0],
        "max_ms": xs[-1],
    }


def _clean(values: Iterable[float], abs_cap_ms: int) -> List[float]:
    out = []
    cap = float(abs_cap_ms)
    for v in values:
        fv = _to_float(v)
        if fv is None:
            continue
        if abs(fv) > cap:
            continue
        out.append(fv)
    return out


def _try_clickhouse_client(
    hosts: List[str],
    port: int,
    database: str,
    username: str,
    password: str,
):
    last_err = None
    for host in hosts:
        try:
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
            )
            client.query("SELECT 1")
            return client, host
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"Cannot connect ClickHouse via hosts={hosts}: {last_err}")


def collect_clickhouse_metrics(
    stages: Dict[str, List[float]],
    window_minutes: int,
    sample_limit: int,
) -> Tuple[bool, str]:
    hosts_env = os.getenv("CLICKHOUSE_HOSTS", "").strip()
    if hosts_env:
        hosts = [h.strip() for h in hosts_env.split(",") if h.strip()]
    else:
        primary = os.getenv("CLICKHOUSE_HOST", "clickhouse").strip()
        hosts = [primary, "localhost", "127.0.0.1", "clickhouse"]
    # Preserve order while removing duplicates
    seen = set()
    uniq_hosts = []
    for h in hosts:
        if h not in seen:
            seen.add(h)
            uniq_hosts.append(h)

    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    db = os.getenv("CLICKHOUSE_DB", "stock_warehouse")
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "truongittstock")

    client, chosen_host = _try_clickhouse_client(uniq_hosts, port, db, user, password)
    try:
        safe_window = max(1, int(window_minutes))
        safe_limit = max(1, int(sample_limit))
        sql = f"""
        SELECT
          toInt64(dateDiff('millisecond', event_time, received_at)) AS event_to_received_ms,
          toInt64(dateDiff('millisecond', received_at, inserted_at)) AS received_to_inserted_ms,
          toInt64(dateDiff('millisecond', event_time, inserted_at)) AS event_to_inserted_ms,
          toInt64(dateDiff('millisecond', event_time, now64(3))) AS event_to_now_ms
        FROM stock_warehouse.stock_ticks
        WHERE inserted_at >= now64(3) - INTERVAL {safe_window} MINUTE
        ORDER BY inserted_at DESC
        LIMIT {safe_limit}
        """
        rows = client.query(sql).result_rows

        for row in rows:
            if not row:
                continue
            stages["ch_event_to_received_ms"].append(row[0])
            stages["ch_received_to_inserted_ms"].append(row[1])
            stages["ch_event_to_inserted_ms"].append(row[2])
            stages["ch_event_to_now_ms"].append(row[3])
    finally:
        client.close()
    return True, chosen_host


def collect_scylla_metrics(
    stages: Dict[str, List[float]],
    max_age_minutes: int,
) -> Tuple[bool, str]:
    hosts_env = os.getenv("SCYLLA_CONTACT_POINTS", "").strip()
    if hosts_env:
        hosts = [h.strip() for h in hosts_env.split(",") if h.strip()]
    else:
        hosts = ["localhost", "127.0.0.1", "scylla-node1", "scylla-node2", "scylla-node3"]

    port = int(os.getenv("SCYLLA_PORT", "9042"))
    keyspace = os.getenv("SCYLLA_KEYSPACE", "stock_data")

    cluster = Cluster(hosts, port=port)
    session = cluster.connect(keyspace)
    rows = session.execute(
        "SELECT symbol, timestamp, producer_timestamp, WRITETIME(price) AS wt_price "
        "FROM stock_latest_prices"
    )

    now_ms = int(time.time() * 1000)
    max_age_ms = max(1, int(max_age_minutes)) * 60 * 1000

    for row in rows:
        event_ms = _to_epoch_ms(getattr(row, "timestamp", None))
        if event_ms is None:
            continue

        age_ms = now_ms - event_ms
        # Ignore very old/stale rows to focus on current pipeline latency.
        if age_ms < 0 or age_ms > max_age_ms:
            continue

        stages["scylla_event_to_now_ms"].append(age_ms)

        wt_us = getattr(row, "wt_price", None)
        wt_ms = _to_float(wt_us)
        if wt_ms is None:
            continue
        wt_ms /= 1000.0  # WRITETIME is microseconds

        stages["scylla_event_to_write_ms"].append(wt_ms - event_ms)

        producer_ms = _to_epoch_ms(getattr(row, "producer_timestamp", None))
        if producer_ms is not None:
            stages["scylla_producer_to_write_ms"].append(wt_ms - producer_ms)

    session.shutdown()
    cluster.shutdown()
    return True, ",".join(hosts)


def collect_api_metrics(
    stages: Dict[str, List[float]],
    api_base: str,
    max_age_minutes: int,
) -> Tuple[bool, str]:
    url = api_base.rstrip("/") + "/api/stocks/latest"
    max_age_ms = max(1, int(max_age_minutes)) * 60 * 1000
    now_ms = int(time.time() * 1000)

    req = urlopen(url, timeout=8)
    payload = json.loads(req.read().decode("utf-8"))
    rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return False, "unexpected_response"

    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("is_placeholder"):
            continue
        event_ms = _to_epoch_ms(row.get("timestamp") or row.get("trade_date") or row.get("date"))
        if event_ms is None:
            continue
        age_ms = now_ms - event_ms
        if age_ms < 0 or age_ms > max_age_ms:
            continue
        stages["api_event_to_now_ms"].append(age_ms)
    return True, url


def write_stage_csv(path: str, stage_stats: Dict[str, Dict[str, float]]) -> None:
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["stage", "n", "mean_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "min_ms", "max_ms"])
        for stage in sorted(stage_stats.keys()):
            s = stage_stats[stage]
            writer.writerow([
                stage,
                int(s["n"]),
                round(s["mean_ms"], 3),
                round(s["p50_ms"], 3),
                round(s["p90_ms"], 3),
                round(s["p95_ms"], 3),
                round(s["p99_ms"], 3),
                round(s["min_ms"], 3),
                round(s["max_ms"], 3),
            ])


def write_summary_csv(path: str, summary: Dict[str, object]) -> None:
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["metric", "value"])
        for k in sorted(summary.keys()):
            writer.writerow([k, summary[k]])


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure simple end-to-end latency for the stock data pipeline.")
    parser.add_argument("--window-minutes", type=int, default=1440, help="ClickHouse sample window in minutes (default: 1440 = 1 day)")
    parser.add_argument("--sample-limit", type=int, default=200000, help="Max ClickHouse rows sampled (default: 200000)")
    parser.add_argument("--max-age-minutes", type=int, default=10080, help="Drop rows older than this age in minutes (default: 10080 = 7 days)")
    parser.add_argument("--abs-cap-ms", type=int, default=WEEK_MS, help="Absolute cap for latency values in ms (default: 7 days)")
    parser.add_argument("--api-base", default=os.getenv("API_BASE", "http://localhost:8020"), help="Backend base URL")
    parser.add_argument("--stage-csv", default="scripts/e2e_latency_stage_stats.csv", help="Output CSV per stage")
    parser.add_argument("--summary-csv", default="scripts/e2e_latency_summary.csv", help="Output summary key-value CSV")
    args = parser.parse_args()

    stages: Dict[str, List[float]] = {
        "ch_event_to_received_ms": [],
        "ch_received_to_inserted_ms": [],
        "ch_event_to_inserted_ms": [],
        "ch_event_to_now_ms": [],
        "scylla_producer_to_write_ms": [],
        "scylla_event_to_write_ms": [],
        "scylla_event_to_now_ms": [],
        "api_event_to_now_ms": [],
    }

    sources = {}

    # ClickHouse
    try:
        ok, detail = collect_clickhouse_metrics(stages, args.window_minutes, args.sample_limit)
        sources["clickhouse"] = f"ok ({detail})" if ok else f"failed ({detail})"
    except Exception as exc:
        sources["clickhouse"] = f"failed ({exc})"

    # Scylla
    try:
        ok, detail = collect_scylla_metrics(stages, args.max_age_minutes)
        sources["scylla"] = f"ok ({detail})" if ok else f"failed ({detail})"
    except Exception as exc:
        sources["scylla"] = f"failed ({exc})"

    # API
    try:
        ok, detail = collect_api_metrics(stages, args.api_base, args.max_age_minutes)
        sources["api"] = f"ok ({detail})" if ok else f"failed ({detail})"
    except (URLError, HTTPError, TimeoutError, json.JSONDecodeError, Exception) as exc:
        sources["api"] = f"failed ({exc})"

    # Clean & describe
    cleaned = {stage: _clean(values, abs_cap_ms=max(1, int(args.abs_cap_ms))) for stage, values in stages.items()}
    stage_stats = {}
    for stage, values in cleaned.items():
        s = _describe(values)
        if s:
            stage_stats[stage] = s

    if not stage_stats:
        print("No latency samples available from ClickHouse/Scylla/API.")
        print("Source status:", sources)
        return 2

    write_stage_csv(args.stage_csv, stage_stats)

    summary = {
        "window_minutes": args.window_minutes,
        "sample_limit": args.sample_limit,
        "max_age_minutes": args.max_age_minutes,
        "abs_cap_ms": args.abs_cap_ms,
        "source_clickhouse": sources.get("clickhouse", ""),
        "source_scylla": sources.get("scylla", ""),
        "source_api": sources.get("api", ""),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    for stage, s in stage_stats.items():
        summary[f"{stage}.n"] = int(s["n"])
        summary[f"{stage}.mean_ms"] = round(s["mean_ms"], 3)
        summary[f"{stage}.p50_ms"] = round(s["p50_ms"], 3)
        summary[f"{stage}.p90_ms"] = round(s["p90_ms"], 3)
        summary[f"{stage}.p95_ms"] = round(s["p95_ms"], 3)
        summary[f"{stage}.p99_ms"] = round(s["p99_ms"], 3)
        summary[f"{stage}.max_ms"] = round(s["max_ms"], 3)

    write_summary_csv(args.summary_csv, summary)

    print("\n=== E2E Latency Summary (ms) ===")
    print(f"Sources: {sources}")
    for stage in sorted(stage_stats.keys()):
        s = stage_stats[stage]
        print(
            f"{stage:30s} n={int(s['n']):5d} "
            f"mean={s['mean_ms']:.1f} p50={s['p50_ms']:.1f} "
            f"p90={s['p90_ms']:.1f} p95={s['p95_ms']:.1f} p99={s['p99_ms']:.1f} max={s['max_ms']:.1f}"
        )

    print(f"\nSaved stage stats: {args.stage_csv}")
    print(f"Saved summary CSV: {args.summary_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
