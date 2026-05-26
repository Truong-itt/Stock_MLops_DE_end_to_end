#!/usr/bin/env python3
"""
Advanced latency analysis on local recordings.
- Loads `data/recordings/*.jsonl`
- Computes raw stats, filtered stats (remove deltas > 1 hour)
- Bootstraps 95% CI for p50, p90, p99
- Exports `scripts/latency_summary.csv` with key metrics

Run: python3 scripts/measure_latency_analysis.py
"""
import glob
import json
import random
import statistics
import csv
from math import ceil

def load_deltas(paths):
    deltas = []
    for path in paths:
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
                        if t < 10_000_000_000:
                            t *= 1000
                        if recv < 10_000_000_000:
                            recv *= 1000
                        delta = recv - t
                        deltas.append(delta)
                    except Exception:
                        continue
        except Exception:
            continue
    return deltas

def quantile(xs, p):
    if not xs:
        return None
    xs_sorted = sorted(xs)
    n = len(xs_sorted)
    idx = min(n-1, max(0, int(p * n)))
    return xs_sorted[idx]

def stats(xs):
    if not xs:
        return {}
    xs_s = sorted(xs)
    return {
        'n': len(xs),
        'mean_ms': sum(xs)/len(xs),
        'std_ms': statistics.pstdev(xs) if len(xs)>1 else 0.0,
        'p50_ms': quantile(xs, 0.5),
        'p90_ms': quantile(xs, 0.9),
        'p99_ms': quantile(xs, 0.99),
        'min_ms': xs_s[0],
        'max_ms': xs_s[-1],
    }

def bootstrap_ci(xs, p, iterations=2000, seed=42):
    random.seed(seed)
    n = len(xs)
    if n == 0:
        return (None, None)
    estimates = []
    for _ in range(iterations):
        sample = [random.choice(xs) for _ in range(n)]
        estimates.append(quantile(sample, p))
    estimates.sort()
    lo = estimates[int(0.025 * iterations)]
    hi = estimates[int(0.975 * iterations)]
    return (lo, hi)

def write_summary_csv(path, rows):
    keys = ['metric','value']
    with open(path,'w',newline='') as fh:
        w = csv.writer(fh)
        w.writerow(keys)
        for k,v in rows.items():
            w.writerow([k,v])

def main():
    paths = glob.glob('data/recordings/*.jsonl')
    deltas = load_deltas(paths)
    if not deltas:
        print('No samples found in data/recordings')
        return

    raw = stats(deltas)

    # Filter out very large deltas > 1 hour (3600000 ms) for robust metrics
    cutoff_ms = 3600 * 1000
    filtered = [d for d in deltas if 0 <= d <= cutoff_ms]
    filt = stats(filtered)

    # Bootstrap CIs on filtered data
    ci = {}
    for p_label, p in [('p50',0.5), ('p90',0.9), ('p99',0.99)]:
        lo, hi = bootstrap_ci(filtered, p, iterations=2000)
        ci[f'{p_label}_ci_lo_ms'] = lo
        ci[f'{p_label}_ci_hi_ms'] = hi

    # Print human-readable
    print('\nLatency analysis (local recordings)')
    print('Samples (raw):', raw.get('n'))
    print('Raw mean(ms):', round(raw.get('mean_ms',0),2), 'p50/p90/p99(ms):', raw.get('p50_ms'), raw.get('p90_ms'), raw.get('p99_ms'))
    print('\nFiltered (0 <= delta <= 1h) samples:', filt.get('n'))
    print('Filtered mean(ms):', round(filt.get('mean_ms',0),2))
    print('Filtered p50/p90/p99(ms):', filt.get('p50_ms'), filt.get('p90_ms'), filt.get('p99_ms'))
    print('\n95% bootstrap CIs (filtered):')
    print('p50 CI:', ci['p50_ci_lo_ms'], '-', ci['p50_ci_hi_ms'])
    print('p90 CI:', ci['p90_ci_lo_ms'], '-', ci['p90_ci_hi_ms'])
    print('p99 CI:', ci['p99_ci_lo_ms'], '-', ci['p99_ci_hi_ms'])

    # Save summary CSV
    rows = {
        'raw_n': raw.get('n'),
        'raw_mean_ms': raw.get('mean_ms'),
        'raw_p50_ms': raw.get('p50_ms'),
        'raw_p90_ms': raw.get('p90_ms'),
        'raw_p99_ms': raw.get('p99_ms'),
        'filtered_n': filt.get('n'),
        'filtered_mean_ms': filt.get('mean_ms'),
        'filtered_p50_ms': filt.get('p50_ms'),
        'filtered_p90_ms': filt.get('p90_ms'),
        'filtered_p99_ms': filt.get('p99_ms'),
    }
    rows.update(ci)
    write_summary_csv('scripts/latency_summary.csv', rows)
    print('\nSummary written to scripts/latency_summary.csv')

if __name__ == '__main__':
    main()
