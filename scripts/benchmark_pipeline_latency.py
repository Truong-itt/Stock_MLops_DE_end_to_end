#!/usr/bin/env python3
"""
benchmark_pipeline_latency.py
─────────────────────────────
Đo latency từng chặng và full end-to-end cho 2 luồng dữ liệu:

    WS → Producer → Kafka → Flink → ClickHouse (stock_ticks)
    WS → Producer → Kafka → Flink → ScyllaDB  (stock_prices)

Cách đo:
  1) Chạy 1 Kafka consumer Avro song song để bắt:
       - event_time      (record.time           — WS gửi)
       - producer_recv   (record.received_at    — producer nhận từ WS)
       - kafka_ts        (ConsumerRecord ts     — Kafka log append/create time)
       - consumer_recv   (now()                 — script nhận từ Kafka)
  2) Đợi Flink ghi xuống DB, query lại:
       - ClickHouse: stock_ticks.inserted_at
       - ScyllaDB:   WRITETIME(price)/1000 trên stock_prices
  3) Join theo (symbol, event_time) → compute delta từng chặng.

Stages đo được (đơn vị ms):
  ws_to_producer_ms          = producer_recv - event_time
  producer_to_kafka_ms       = kafka_ts      - producer_recv
  kafka_to_consumer_ms       = consumer_recv - kafka_ts        (tham chiếu)
  kafka_to_ch_inserted_ms    = ch_inserted   - kafka_ts        (Flink+CH sink)
  kafka_to_scylla_write_ms   = scylla_wt     - kafka_ts        (Flink+Scylla sink)

Full E2E (ms):
  e2e_ch_from_event_ms       = ch_inserted   - event_time
  e2e_ch_from_producer_ms    = ch_inserted   - producer_recv
  e2e_scylla_from_event_ms   = scylla_wt     - event_time
  e2e_scylla_from_producer_ms= scylla_wt     - producer_recv

Outputs:
  scripts/benchmark_stage_stats.csv   — thống kê từng stage
  scripts/benchmark_raw_samples.csv   — delta từng record (giới hạn rows)
  scripts/benchmark_summary.csv       — key-value summary

Run (host):
  python3 scripts/benchmark_pipeline_latency.py --duration 120

Run (trong stock-network):
  KAFKA_BOOTSTRAP=kafka-1:29092 \
  SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
  CLICKHOUSE_HOST=clickhouse \
  SCYLLA_CONTACT_POINTS=scylla-node1,scylla-node2,scylla-node3 \
  python3 scripts/benchmark_pipeline_latency.py --duration 120
"""

from __future__ import annotations

import argparse
import csv
import logging
import math
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

logging.getLogger("clickhouse_connect").setLevel(logging.CRITICAL)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bench")


# ════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ════════════════════════════════════════════════════════════════════
@dataclass
class KafkaSample:
    """Timestamp đo được từ phía consumer (cho 1 tick)."""
    symbol: str
    event_time_ms: int
    producer_recv_ms: Optional[int]
    kafka_ts_ms: int
    consumer_recv_ms: int


# (symbol, event_time_ms) -> KafkaSample
KafkaIndex = Dict[Tuple[str, int], KafkaSample]


# ════════════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════════════
def _percentile(sorted_vals: List[float], q: float) -> Optional[float]:
    if not sorted_vals:
        return None
    if q <= 0:
        return sorted_vals[0]
    if q >= 1:
        return sorted_vals[-1]
    pos = (len(sorted_vals) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def _describe(values: Iterable[float], abs_cap_ms: float) -> Optional[Dict[str, float]]:
    xs: List[float] = []
    for v in values:
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(fv):
            continue
        if abs(fv) > abs_cap_ms:
            continue
        xs.append(fv)
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


def _split_hosts(env_val: str, fallback: List[str]) -> List[str]:
    s = (env_val or "").strip()
    if not s:
        return list(fallback)
    return [h.strip() for h in s.split(",") if h.strip()]


# ════════════════════════════════════════════════════════════════════
# KAFKA CONSUMER (background thread)
# ════════════════════════════════════════════════════════════════════
def run_kafka_consumer(
    bootstrap: str,
    schema_registry_url: str,
    topics: List[str],
    duration_sec: int,
    stop_event: threading.Event,
    out_index: KafkaIndex,
    out_index_lock: threading.Lock,
    from_earliest: bool = False,
    heartbeat_sec: int = 5,
) -> Dict[str, int]:
    from confluent_kafka import DeserializingConsumer, KafkaException
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import StringDeserializer

    sr_client = SchemaRegistryClient({"url": schema_registry_url})
    avro_deser = AvroDeserializer(sr_client)

    group_id = f"benchmark-latency-{int(time.time())}"
    consumer = DeserializingConsumer({
        "bootstrap.servers":      bootstrap,
        "group.id":               group_id,
        "key.deserializer":       StringDeserializer("utf_8"),
        "value.deserializer":     avro_deser,
        "auto.offset.reset":      "earliest" if from_earliest else "latest",
        "enable.auto.commit":     False,
        "session.timeout.ms":     10_000,
        "fetch.min.bytes":        1,
    })
    consumer.subscribe(topics)
    log.info(
        "Kafka consumer started: topics=%s group=%s offset_reset=%s",
        topics, group_id, "earliest" if from_earliest else "latest",
    )

    # ── Pre-flight diagnostics ──────────────────────────────────────
    try:
        md = consumer.list_topics(timeout=10)
        brokers = ",".join(f"{b.id}@{b.host}:{b.port}" for b in md.brokers.values())
        log.info("Kafka brokers visible: %s", brokers or "(none)")
        for t in topics:
            tmd = md.topics.get(t)
            if tmd is None or tmd.error is not None:
                log.warning("Topic '%s' not found or error: %s", t, getattr(tmd, "error", "missing"))
            else:
                log.info("Topic '%s': %d partitions", t, len(tmd.partitions))
    except Exception as e:
        log.warning("Cannot fetch Kafka metadata: %s", e)

    received = 0
    skipped_no_event_time = 0
    deadline = time.monotonic() + duration_sec
    last_heartbeat = time.monotonic()

    try:
        while not stop_event.is_set() and time.monotonic() < deadline:
            now = time.monotonic()
            if heartbeat_sec > 0 and (now - last_heartbeat) >= heartbeat_sec:
                remaining = max(0, int(deadline - now))
                log.info("… heartbeat: recorded=%d skipped=%d (còn %ds)", received, skipped_no_event_time, remaining)
                last_heartbeat = now

            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                log.warning("Kafka error: %s", msg.error())
                continue

            now_ms = int(time.time() * 1000)
            record = msg.value()
            if not isinstance(record, dict):
                continue

            event_time = record.get("time")
            producer_recv = record.get("received_at")
            symbol = record.get("id")
            if symbol is None or event_time is None:
                skipped_no_event_time += 1
                continue

            try:
                event_time_ms = int(event_time)
                if event_time_ms < 10_000_000_000:
                    event_time_ms *= 1000
            except (TypeError, ValueError):
                skipped_no_event_time += 1
                continue

            try:
                producer_recv_ms = int(producer_recv) if producer_recv is not None else None
                if producer_recv_ms is not None and producer_recv_ms < 10_000_000_000:
                    producer_recv_ms *= 1000
            except (TypeError, ValueError):
                producer_recv_ms = None

            # ConsumerRecord.timestamp() returns (type, ms)
            ts_type, kafka_ts_ms = msg.timestamp()
            if not kafka_ts_ms or kafka_ts_ms <= 0:
                kafka_ts_ms = now_ms

            sample = KafkaSample(
                symbol=str(symbol).strip().upper(),
                event_time_ms=event_time_ms,
                producer_recv_ms=producer_recv_ms,
                kafka_ts_ms=int(kafka_ts_ms),
                consumer_recv_ms=now_ms,
            )
            with out_index_lock:
                # Keep the first observation for a given (symbol, event_time).
                key = (sample.symbol, sample.event_time_ms)
                if key not in out_index:
                    out_index[key] = sample
                    received += 1
    except KafkaException as e:
        log.error("Kafka exception: %s", e)
    finally:
        try:
            consumer.close()
        except Exception:
            pass

    log.info("Kafka consumer stopped. recorded=%d skipped=%d", received, skipped_no_event_time)
    return {"recorded": received, "skipped": skipped_no_event_time}


# ════════════════════════════════════════════════════════════════════
# CLICKHOUSE: fetch inserted_at for the window
# ════════════════════════════════════════════════════════════════════
def fetch_clickhouse_rows(
    hosts: List[str],
    port: int,
    database: str,
    username: str,
    password: str,
    event_min_ms: int,
    event_max_ms: int,
    symbols: List[str],
) -> Tuple[List[Dict], Optional[str]]:
    import clickhouse_connect

    last_err: Optional[Exception] = None
    client = None
    chosen = None
    for h in hosts:
        try:
            client = clickhouse_connect.get_client(
                host=h, port=port,
                database=database,
                username=username, password=password,
            )
            client.query("SELECT 1")
            chosen = h
            break
        except Exception as e:
            last_err = e
            client = None

    if client is None:
        log.warning("ClickHouse: cannot connect to any of %s (%s)", hosts, last_err)
        return [], None

    # Convert ms → datetime literal (UTC) — small margin to be safe.
    pad_min = 1
    t_from = datetime.fromtimestamp((event_min_ms / 1000) - pad_min * 60, tz=timezone.utc)
    t_to = datetime.fromtimestamp((event_max_ms / 1000) + pad_min * 60, tz=timezone.utc)
    from_str = t_from.strftime("%Y-%m-%d %H:%M:%S")
    to_str = t_to.strftime("%Y-%m-%d %H:%M:%S")

    # Use IN if symbol set is small; otherwise skip filter.
    symbol_filter = ""
    if symbols and len(symbols) <= 200:
        in_list = ",".join("'" + s.replace("'", "''") + "'" for s in symbols)
        symbol_filter = f"AND symbol IN ({in_list})"

    sql = f"""
    SELECT
      symbol,
      toUnixTimestamp64Milli(event_time)  AS event_ms,
      toUnixTimestamp64Milli(received_at) AS recv_ms,
      toUnixTimestamp64Milli(inserted_at) AS ins_ms
    FROM stock_warehouse.stock_ticks
    WHERE event_time BETWEEN toDateTime64('{from_str}', 3, 'UTC')
                         AND toDateTime64('{to_str}',   3, 'UTC')
      {symbol_filter}
    ORDER BY event_time
    """
    try:
        rows = client.query(sql).result_rows
    finally:
        client.close()

    out: List[Dict] = []
    for r in rows:
        if not r:
            continue
        out.append({
            "symbol":   str(r[0]).strip().upper(),
            "event_ms": int(r[1]),
            "recv_ms":  int(r[2]) if r[2] is not None else None,
            "ins_ms":   int(r[3]) if r[3] is not None else None,
        })
    log.info("ClickHouse: fetched %d rows (host=%s)", len(out), chosen)
    return out, chosen


# ════════════════════════════════════════════════════════════════════
# SCYLLA: fetch WRITETIME for the window (use stock_prices for per-tick)
# ════════════════════════════════════════════════════════════════════
def fetch_scylla_rows(
    hosts: List[str],
    port: int,
    keyspace: str,
    event_min_ms: int,
    event_max_ms: int,
    symbols: List[str],
) -> Tuple[List[Dict], Optional[str]]:
    from cassandra.cluster import Cluster

    try:
        cluster = Cluster(hosts, port=port)
        session = cluster.connect(keyspace)
    except Exception as e:
        log.warning("Scylla: cannot connect (%s): %s", hosts, e)
        return [], None

    out: List[Dict] = []
    chosen = ",".join(hosts)
    pad_ms = 60_000
    lo, hi = event_min_ms - pad_ms, event_max_ms + pad_ms

    try:
        # PRIMARY KEY (symbol, timestamp text). Per-symbol slice avoids full scan.
        select_stmt = session.prepare(
            "SELECT symbol, timestamp, producer_timestamp, WRITETIME(price) AS wt "
            "FROM stock_prices "
            "WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?"
        )
        # timestamp column is TEXT (epoch ms as string) — use ms as text.
        lo_s = str(lo)
        hi_s = str(hi)

        for sym in symbols:
            try:
                rows = session.execute(select_stmt, (sym, lo_s, hi_s))
            except Exception as e:
                log.debug("Scylla query failed for %s: %s", sym, e)
                continue
            for r in rows:
                try:
                    event_ms = int(r.timestamp)
                except (TypeError, ValueError):
                    continue
                wt_us = getattr(r, "wt", None)
                producer_ts = getattr(r, "producer_timestamp", None)
                out.append({
                    "symbol": str(r.symbol).strip().upper(),
                    "event_ms": event_ms,
                    "producer_ms": int(producer_ts) if producer_ts is not None else None,
                    "wt_ms": (int(wt_us) / 1000.0) if wt_us is not None else None,
                })
    finally:
        try:
            session.shutdown()
            cluster.shutdown()
        except Exception:
            pass

    log.info("Scylla: fetched %d rows (hosts=%s)", len(out), chosen)
    return out, chosen


# ════════════════════════════════════════════════════════════════════
# JOIN + STAGE COMPUTATION
# ════════════════════════════════════════════════════════════════════
@dataclass
class JoinedRow:
    symbol: str
    event_time_ms: int
    producer_recv_ms: Optional[int]
    kafka_ts_ms: Optional[int]
    consumer_recv_ms: Optional[int]
    ch_inserted_ms: Optional[int] = None
    scylla_wt_ms: Optional[float] = None


STAGES: List[str] = [
    "ws_to_producer_ms",
    "producer_to_kafka_ms",
    "kafka_to_consumer_ms",
    "kafka_to_ch_inserted_ms",
    "kafka_to_scylla_write_ms",
    "e2e_ch_from_event_ms",
    "e2e_ch_from_producer_ms",
    "e2e_scylla_from_event_ms",
    "e2e_scylla_from_producer_ms",
]


def compute_stages(rows: List[JoinedRow]) -> Dict[str, List[float]]:
    out: Dict[str, List[float]] = {s: [] for s in STAGES}
    for r in rows:
        et = r.event_time_ms
        prod = r.producer_recv_ms
        kt = r.kafka_ts_ms
        cons = r.consumer_recv_ms

        if prod is not None and et is not None:
            out["ws_to_producer_ms"].append(prod - et)
        if kt is not None and prod is not None:
            out["producer_to_kafka_ms"].append(kt - prod)
        if cons is not None and kt is not None:
            out["kafka_to_consumer_ms"].append(cons - kt)

        if r.ch_inserted_ms is not None:
            if kt is not None:
                out["kafka_to_ch_inserted_ms"].append(r.ch_inserted_ms - kt)
            if et is not None:
                out["e2e_ch_from_event_ms"].append(r.ch_inserted_ms - et)
            if prod is not None:
                out["e2e_ch_from_producer_ms"].append(r.ch_inserted_ms - prod)

        if r.scylla_wt_ms is not None:
            if kt is not None:
                out["kafka_to_scylla_write_ms"].append(r.scylla_wt_ms - kt)
            if et is not None:
                out["e2e_scylla_from_event_ms"].append(r.scylla_wt_ms - et)
            if prod is not None:
                out["e2e_scylla_from_producer_ms"].append(r.scylla_wt_ms - prod)
    return out


# ════════════════════════════════════════════════════════════════════
# CSV WRITERS
# ════════════════════════════════════════════════════════════════════
def write_stage_csv(path: str, stats: Dict[str, Dict[str, float]]) -> None:
    cols = ["stage", "n", "mean_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "min_ms", "max_ms"]
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        for stage in STAGES:
            s = stats.get(stage)
            if not s:
                w.writerow([stage, 0, "", "", "", "", "", "", ""])
                continue
            w.writerow([
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
        w = csv.writer(fh)
        w.writerow(["metric", "value"])
        for k in sorted(summary.keys()):
            w.writerow([k, summary[k]])


def write_raw_csv(path: str, joined: List[JoinedRow], limit: int) -> None:
    cols = [
        "symbol", "event_time_ms", "producer_recv_ms", "kafka_ts_ms",
        "consumer_recv_ms", "ch_inserted_ms", "scylla_wt_ms",
        "ws_to_producer_ms", "producer_to_kafka_ms", "kafka_to_consumer_ms",
        "kafka_to_ch_inserted_ms", "kafka_to_scylla_write_ms",
        "e2e_ch_from_event_ms", "e2e_ch_from_producer_ms",
        "e2e_scylla_from_event_ms", "e2e_scylla_from_producer_ms",
    ]
    rows_out = []
    for r in joined:
        et, prod, kt, cons = r.event_time_ms, r.producer_recv_ms, r.kafka_ts_ms, r.consumer_recv_ms
        ch = r.ch_inserted_ms
        sc = r.scylla_wt_ms

        def diff(a, b):
            if a is None or b is None:
                return ""
            return round(a - b, 3)

        rows_out.append([
            r.symbol, et, prod, kt, cons,
            ch, sc,
            diff(prod, et),
            diff(kt, prod),
            diff(cons, kt),
            diff(ch, kt),
            diff(sc, kt),
            diff(ch, et),
            diff(ch, prod),
            diff(sc, et),
            diff(sc, prod),
        ])

    rows_out = rows_out[:limit] if limit > 0 else rows_out
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        w.writerows(rows_out)


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════
def main() -> int:
    p = argparse.ArgumentParser(
        description="Benchmark latency per-stage + e2e for Kafka→Flink→ClickHouse / ScyllaDB."
    )
    p.add_argument("--duration", type=int, default=120,
                   help="Số giây consumer Kafka chạy (default 120)")
    p.add_argument("--db-grace-seconds", type=int, default=15,
                   help="Đợi thêm bao nhiêu giây sau khi consumer dừng để Flink hoàn tất ghi DB (default 15)")
    p.add_argument("--max-samples", type=int, default=200_000,
                   help="Giới hạn số sample lưu trong RAM")
    p.add_argument("--raw-csv-limit", type=int, default=5000,
                   help="Số dòng tối đa ghi vào raw CSV (default 5000)")
    p.add_argument("--abs-cap-ms", type=int, default=60 * 60 * 1000,
                   help="Loại bỏ delta vượt cap để chống outlier do skew clock (default 1h)")
    p.add_argument("--out-dir", default="scripts",
                   help="Thư mục output CSV (default scripts/)")
    p.add_argument("--topics", default="stock_price_vn,stock_price_dif",
                   help="Comma-separated Kafka topics")
    p.add_argument("--from-earliest", action="store_true",
                   help="Consume từ offset cũ nhất (dùng cho ngoài giờ giao dịch / debug)")
    p.add_argument("--heartbeat-sec", type=int, default=5,
                   help="In log nhịp tim mỗi N giây (default 5, 0=off)")
    args = p.parse_args()

    out_dir = args.out_dir
    os.makedirs(out_dir, exist_ok=True)
    stage_csv = os.path.join(out_dir, "benchmark_stage_stats.csv")
    summary_csv = os.path.join(out_dir, "benchmark_summary.csv")
    raw_csv = os.path.join(out_dir, "benchmark_raw_samples.csv")

    # ── Connection params ────────────────────────────────────────────
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    topics = [t.strip() for t in args.topics.split(",") if t.strip()]

    ch_hosts = _split_hosts(
        os.getenv("CLICKHOUSE_HOSTS", ""),
        fallback=[os.getenv("CLICKHOUSE_HOST", "localhost"), "127.0.0.1", "clickhouse"],
    )
    ch_port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    ch_db = os.getenv("CLICKHOUSE_DB", "stock_warehouse")
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_pass = os.getenv("CLICKHOUSE_PASSWORD", "truongittstock")

    scylla_hosts = _split_hosts(
        os.getenv("SCYLLA_CONTACT_POINTS", ""),
        fallback=["localhost", "127.0.0.1", "scylla-node1", "scylla-node2", "scylla-node3"],
    )
    scylla_port = int(os.getenv("SCYLLA_PORT", "9042"))
    scylla_ks = os.getenv("SCYLLA_KEYSPACE", "stock_data")

    log.info("Kafka:   %s  topics=%s", kafka_bootstrap, topics)
    log.info("Schema:  %s", schema_registry_url)
    log.info("CH:      hosts=%s port=%s db=%s", ch_hosts, ch_port, ch_db)
    log.info("Scylla:  hosts=%s port=%s ks=%s", scylla_hosts, scylla_port, scylla_ks)

    # ── Stage 1: run Kafka consumer ─────────────────────────────────
    kafka_index: KafkaIndex = {}
    idx_lock = threading.Lock()
    stop_event = threading.Event()

    def _sig(*_):
        log.info("Received signal, stopping consumer ...")
        stop_event.set()
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    t0 = time.time()
    counts = run_kafka_consumer(
        bootstrap=kafka_bootstrap,
        schema_registry_url=schema_registry_url,
        topics=topics,
        duration_sec=args.duration,
        stop_event=stop_event,
        out_index=kafka_index,
        out_index_lock=idx_lock,
        from_earliest=args.from_earliest,
        heartbeat_sec=args.heartbeat_sec,
    )
    t_consumer_end = time.time()

    if not kafka_index:
        log.error("Không thu được message nào từ Kafka. Kiểm tra producer + connectivity.")
        return 2

    # Cap samples to avoid huge memory.
    if len(kafka_index) > args.max_samples:
        items = list(kafka_index.items())[: args.max_samples]
        kafka_index = dict(items)

    event_times = [s.event_time_ms for s in kafka_index.values()]
    e_min, e_max = min(event_times), max(event_times)
    symbols = sorted({s.symbol for s in kafka_index.values()})
    log.info(
        "Captured %d unique (symbol, event_time). Window=[%s, %s] symbols=%d",
        len(kafka_index),
        datetime.fromtimestamp(e_min / 1000, tz=timezone.utc).isoformat(),
        datetime.fromtimestamp(e_max / 1000, tz=timezone.utc).isoformat(),
        len(symbols),
    )

    # ── Grace period for Flink → DB to flush ────────────────────────
    if args.db_grace_seconds > 0:
        log.info("Đợi %ds để Flink → DB flush ...", args.db_grace_seconds)
        time.sleep(args.db_grace_seconds)

    # ── Stage 2: fetch DB rows in parallel ──────────────────────────
    ch_rows: List[Dict] = []
    scylla_rows: List[Dict] = []
    ch_host_used: Optional[str] = None
    scylla_host_used: Optional[str] = None

    ch_err: Optional[str] = None
    scylla_err: Optional[str] = None

    def _ch_job():
        nonlocal ch_rows, ch_host_used, ch_err
        try:
            ch_rows, ch_host_used = fetch_clickhouse_rows(
                ch_hosts, ch_port, ch_db, ch_user, ch_pass,
                e_min, e_max, symbols,
            )
        except Exception as exc:
            ch_err = repr(exc)
            log.warning("ClickHouse fetch error: %s", exc)

    def _scylla_job():
        nonlocal scylla_rows, scylla_host_used, scylla_err
        try:
            scylla_rows, scylla_host_used = fetch_scylla_rows(
                scylla_hosts, scylla_port, scylla_ks,
                e_min, e_max, symbols,
            )
        except Exception as exc:
            scylla_err = repr(exc)
            log.warning("Scylla fetch error: %s", exc)

    t_ch = threading.Thread(target=_ch_job, name="ch-fetch")
    t_sc = threading.Thread(target=_scylla_job, name="scylla-fetch")
    t_ch.start()
    t_sc.start()
    t_ch.join()
    t_sc.join()

    # ── Stage 3: join ───────────────────────────────────────────────
    ch_by_key: Dict[Tuple[str, int], Dict] = {(r["symbol"], r["event_ms"]): r for r in ch_rows}
    scylla_by_key: Dict[Tuple[str, int], Dict] = {(r["symbol"], r["event_ms"]): r for r in scylla_rows}

    joined: List[JoinedRow] = []
    for key, ks in kafka_index.items():
        jr = JoinedRow(
            symbol=ks.symbol,
            event_time_ms=ks.event_time_ms,
            producer_recv_ms=ks.producer_recv_ms,
            kafka_ts_ms=ks.kafka_ts_ms,
            consumer_recv_ms=ks.consumer_recv_ms,
        )
        ch_r = ch_by_key.get(key)
        if ch_r and ch_r.get("ins_ms") is not None:
            jr.ch_inserted_ms = ch_r["ins_ms"]
        sc_r = scylla_by_key.get(key)
        if sc_r and sc_r.get("wt_ms") is not None:
            jr.scylla_wt_ms = sc_r["wt_ms"]
        joined.append(jr)

    matched_ch = sum(1 for r in joined if r.ch_inserted_ms is not None)
    matched_scylla = sum(1 for r in joined if r.scylla_wt_ms is not None)
    log.info(
        "Joined %d kafka samples — matched_ch=%d (%.1f%%) matched_scylla=%d (%.1f%%)",
        len(joined),
        matched_ch, 100.0 * matched_ch / max(1, len(joined)),
        matched_scylla, 100.0 * matched_scylla / max(1, len(joined)),
    )

    # ── Stage 4: compute + write CSV ────────────────────────────────
    stages = compute_stages(joined)
    stage_stats: Dict[str, Dict[str, float]] = {}
    for name, vals in stages.items():
        s = _describe(vals, abs_cap_ms=float(args.abs_cap_ms))
        if s:
            stage_stats[name] = s

    write_stage_csv(stage_csv, stage_stats)
    write_raw_csv(raw_csv, joined, args.raw_csv_limit)

    summary: Dict[str, object] = {
        "generated_at_utc":         datetime.now(timezone.utc).isoformat(),
        "duration_sec":             args.duration,
        "db_grace_seconds":         args.db_grace_seconds,
        "abs_cap_ms":               args.abs_cap_ms,
        "kafka_bootstrap":          kafka_bootstrap,
        "schema_registry_url":      schema_registry_url,
        "topics":                   ",".join(topics),
        "clickhouse_host_used":     ch_host_used or "",
        "scylla_host_used":         scylla_host_used or "",
        "clickhouse_error":         ch_err or "",
        "scylla_error":             scylla_err or "",
        "kafka_recorded":           counts.get("recorded", 0),
        "kafka_skipped":            counts.get("skipped", 0),
        "kafka_unique_keys":        len(kafka_index),
        "symbols_count":            len(symbols),
        "ch_rows_fetched":          len(ch_rows),
        "scylla_rows_fetched":      len(scylla_rows),
        "matched_ch":               matched_ch,
        "matched_scylla":           matched_scylla,
        "event_window_from_utc":    datetime.fromtimestamp(e_min / 1000, tz=timezone.utc).isoformat(),
        "event_window_to_utc":      datetime.fromtimestamp(e_max / 1000, tz=timezone.utc).isoformat(),
    }
    for stage, s in stage_stats.items():
        summary[f"{stage}.n"] = int(s["n"])
        summary[f"{stage}.mean_ms"] = round(s["mean_ms"], 3)
        summary[f"{stage}.p50_ms"] = round(s["p50_ms"], 3)
        summary[f"{stage}.p90_ms"] = round(s["p90_ms"], 3)
        summary[f"{stage}.p95_ms"] = round(s["p95_ms"], 3)
        summary[f"{stage}.p99_ms"] = round(s["p99_ms"], 3)
        summary[f"{stage}.max_ms"] = round(s["max_ms"], 3)
    write_summary_csv(summary_csv, summary)

    # ── Console pretty print ────────────────────────────────────────
    print("\n=== Pipeline Latency Benchmark (ms) ===")
    print(f"Kafka samples: {len(kafka_index)}  matched_ch={matched_ch}  matched_scylla={matched_scylla}")
    for stage in STAGES:
        s = stage_stats.get(stage)
        if not s:
            print(f"{stage:30s} n=0 (no data)")
            continue
        print(
            f"{stage:30s} n={int(s['n']):6d} "
            f"mean={s['mean_ms']:9.1f}  p50={s['p50_ms']:9.1f}  "
            f"p90={s['p90_ms']:9.1f}  p95={s['p95_ms']:9.1f}  "
            f"p99={s['p99_ms']:9.1f}  max={s['max_ms']:9.1f}"
        )
    print(f"\nStage CSV:   {stage_csv}")
    print(f"Summary CSV: {summary_csv}")
    print(f"Raw CSV:     {raw_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
