#!/usr/bin/env python3
"""
Measure pipeline latency:
- Try ClickHouse: compute now64(3) - received_at on `stock_ticks`
- If ClickHouse unavailable, try Scylla: compute write_time - producer_timestamp (best-effort)
- Fallback: parse `data/recordings/*.jsonl` and compute received_at_ms - time

Outputs p50/p90/p99, mean, std, count and simple histogram.
"""
import sys
import os
import glob
import json
import statistics
from math import ceil

def stats_from_list(xs):
    if not xs:
        return None
    xs_sorted = sorted(xs)
    n = len(xs)
    def pct(p):
        idx = min(n-1, max(0, int(p*n)))
        return xs_sorted[idx]
    return {
        'n': n,
        'mean_ms': sum(xs)/n,
        'std_ms': statistics.pstdev(xs) if n>1 else 0.0,
        'p50_ms': pct(0.5),
        'p90_ms': pct(0.9),
        'p99_ms': pct(0.99),
        'min_ms': xs_sorted[0],
        'max_ms': xs_sorted[-1],
    }

# 1) Try ClickHouse
try:
    import clickhouse_connect
    CH_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
    CH_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))
    CH_DB = os.getenv('CLICKHOUSE_DB', 'stock_warehouse')
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, database=CH_DB)
    print('Connected to ClickHouse at', CH_HOST)
    q = """
    SELECT toInt64(now64(3) - received_at) AS delta_ms
    FROM stock_warehouse.stock_ticks
    WHERE received_at > now64() - INTERVAL 1 HOUR
    LIMIT 200000
    """
    res = client.query(q)
    rows = res.result_rows
    deltas = [int(r[0]) for r in rows if r and r[0] is not None]
    s = stats_from_list(deltas)
    if s:
        print('Source: ClickHouse stock_ticks (last 1h sample)')
        print(s)
        sys.exit(0)
    else:
        print('ClickHouse returned no rows, falling back')
except Exception as e:
    print('ClickHouse access failed:', e)

# 2) Try Scylla (best-effort)
try:
    from cassandra.cluster import Cluster
    SCYLLA_HOSTS = os.getenv('SCYLLA_CONTACT_POINTS', 'scylla-node1,scylla-node2,scylla-node3').split(',')
    SCYLLA_KS = os.getenv('SCYLLA_KEYSPACE', 'stock_data')
    cluster = Cluster([h.strip() for h in SCYLLA_HOSTS])
    session = cluster.connect(SCYLLA_KS)
    print('Connected to Scylla at', SCYLLA_HOSTS)
    # Best-effort: try to read recent rows from stock_prices if exists
    q = "SELECT symbol, producer_timestamp, writetime(price) FROM stock_prices LIMIT 200000"
    try:
        rows = session.execute(q)
        deltas = []
        for r in rows:
            try:
                producer_ts = r.producer_timestamp
                wt = r.writetime_price if hasattr(r, 'writetime_price') else None
            except Exception:
                producer_ts = None
                wt = None
            # Note: writetime returns microseconds since unix epoch in cassandra? If unavailable skip
            if producer_ts and wt:
                # Normalize producer_ts
                try:
                    pts = int(producer_ts)
                    if pts < 10_000_000_000:
                        pts *= 1000
                except Exception:
                    continue
                # Many drivers return writetime in microseconds relative to partition? Hard to generalize.
                # We'll skip if format unknown.
                continue
        print('Scylla query executed but cannot compute reliable deltas automatically, falling back')
    except Exception as e:
        print('Scylla query failed or table missing:', e)
except Exception as e:
    print('Scylla access failed:', e)

# 3) Fallback: parse local recordings
print('Falling back to local recordings in data/recordings/*.jsonl')
deltas = []
for path in glob.glob('data/recordings/*.jsonl'):
    try:
        with open(path,'r') as fh:
            for line in fh:
                line=line.strip()
                if not line:
                    continue
                try:
                    r=json.loads(line)
                except Exception:
                    continue
                t = r.get('time') or r.get('time_ms') or r.get('timestamp')
                recv = r.get('received_at_ms') or r.get('received_at') or r.get('receivedAt')
                if t is None or recv is None:
                    continue
                try:
                    t=int(t)
                    recv=int(recv)
                    # Normalize seconds->ms
                    if t < 10_000_000_000:
                        t *= 1000
                    if recv < 10_000_000_000:
                        recv *= 1000
                    delta = recv - t
                    deltas.append(delta)
                except Exception:
                    continue
    except Exception as e:
        print('Failed to read',path, e)

s = stats_from_list(deltas)
if not s:
    print('No samples found in recordings')
    sys.exit(2)
print('Source: local recordings (data/recordings)')
print(s)

# Simple histogram
bins = [0,1,5,10,20,50,100,200,500,1000,5000]
hist = {b:0 for b in bins}
for v in deltas:
    for b in bins:
        if v <= b:
            hist[b]+=1
            break
print('Histogram (ms <= bin):')
for b in bins:
    print(f'  <={b} ms: {hist[b]}')

sys.exit(0)
