"""Benchmarks and experiments for the report. Results go to results/*.csv (+ PNG plots if
matplotlib is installed).

  python benchmark.py --experiment single        # one run with the given settings
  python benchmark.py --experiment quorum        # vary (R, W) with N=3
  python benchmark.py --experiment scale         # vary cluster size 3,4,5
  python benchmark.py --experiment consistency   # stale-read rate vs (R, W)
"""
import argparse
import csv
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor

from client import DKVClient, QuorumError
from cluster import Cluster

RESULTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
BASE_PORT = 8201


def pct(sorted_vals, p):
    return sorted_vals[min(len(sorted_vals) - 1, int(len(sorted_vals) * p / 100))] if sorted_vals else 0.0


def run_workload(endpoints, ops, threads, read_ratio, keyspace=200):
    keys = [f"key{i}" for i in range(keyspace)]
    value = "x" * 64
    pre = DKVClient(endpoints)
    for k in keys:
        pre.put(k, value)
    per_thread = ops // threads

    def worker(seed):
        rnd, cl, lat, err = random.Random(seed), DKVClient(endpoints), [], 0
        for _ in range(per_thread):
            k = rnd.choice(keys)
            t = time.perf_counter()
            try:
                if rnd.random() < read_ratio:
                    cl.get(k)
                else:
                    cl.put(k, value)
                lat.append((time.perf_counter() - t) * 1000)
            except (QuorumError, ConnectionError):
                err += 1
        return lat, err

    t0 = time.perf_counter()
    with ThreadPoolExecutor(threads) as ex:
        results = list(ex.map(worker, range(threads)))
    elapsed = time.perf_counter() - t0
    lat = sorted(x for r in results for x in r[0])
    errors = sum(r[1] for r in results)
    return {"ops": len(lat), "errors": errors, "throughput_ops_s": round(len(lat) / elapsed, 1),
            "avg_ms": round(sum(lat) / len(lat), 2) if lat else 0,
            "p50_ms": round(pct(lat, 50), 2), "p95_ms": round(pct(lat, 95), 2),
            "p99_ms": round(pct(lat, 99), 2)}


def run_configs(configs, args):
    rows = []
    for cfg in configs:
        label = f"nodes={cfg['num_nodes']} N={cfg['N']} R={cfg['R']} W={cfg['W']}"
        print(f"running {label} ...", flush=True)
        with Cluster(base_port=BASE_PORT, **cfg) as cl:
            res = run_workload(cl.endpoints, args.ops, args.threads, args.read_ratio)
        row = {"label": label, **cfg, **res}
        print("   ", {k: row[k] for k in ("throughput_ops_s", "avg_ms", "p95_ms", "errors")})
        rows.append(row)
    return rows


def stale_read_test(cfg, iterations, delay_ms):
    """Write via node A, immediately read via node B; count reads that miss the new value."""
    stale = 0
    with Cluster(base_port=BASE_PORT, replica_delay_ms=delay_ms, **cfg) as cl:
        a = DKVClient([cl.endpoints[0]])
        b = DKVClient([cl.endpoints[-1]])
        for i in range(iterations):
            key, val = f"k{i}", f"v{i}"
            a.put(key, "old")
            time.sleep(delay_ms / 1000.0 * 2)         # let the 'old' value fully propagate
            a.put(key, val)
            if b.get(key) != val:
                stale += 1
    return stale


def save_csv(name, rows):
    os.makedirs(RESULTS, exist_ok=True)
    path = os.path.join(RESULTS, f"{name}.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print("saved", path)


def plot(name, rows, ycol, ylabel, title):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("(matplotlib not installed - skipping plot; pip install matplotlib)")
        return
    plt.figure(figsize=(8, 4))
    plt.bar([r["label"] for r in rows], [r[ycol] for r in rows])
    plt.ylabel(ylabel)
    plt.title(title)
    plt.xticks(rotation=20, ha="right")
    plt.tight_layout()
    path = os.path.join(RESULTS, f"{name}_{ycol}.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print("saved", path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--experiment", choices=["single", "quorum", "scale", "consistency"],
                    default="single")
    ap.add_argument("--ops", type=int, default=2000)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--read-ratio", type=float, default=0.5)
    ap.add_argument("--nodes", type=int, default=5)
    ap.add_argument("--N", type=int, default=3)
    ap.add_argument("--R", type=int, default=2)
    ap.add_argument("--W", type=int, default=2)
    ap.add_argument("--iterations", type=int, default=100, help="for consistency experiment")
    ap.add_argument("--delay-ms", type=int, default=40, help="artificial replica lag (consistency)")
    a = ap.parse_args()

    if a.experiment == "single":
        rows = run_configs([dict(num_nodes=a.nodes, N=a.N, R=a.R, W=a.W)], a)
        save_csv("single", rows)
    elif a.experiment == "quorum":
        cfgs = [dict(num_nodes=5, N=3, R=r, W=w) for r, w in [(1, 1), (1, 3), (3, 1), (2, 2), (3, 3)]]
        rows = run_configs(cfgs, a)
        save_csv("quorum", rows)
        plot("quorum", rows, "throughput_ops_s", "ops/sec", "Throughput vs (R, W), N=3")
        plot("quorum", rows, "p95_ms", "p95 latency (ms)", "p95 latency vs (R, W), N=3")
    elif a.experiment == "scale":
        rows = run_configs([dict(num_nodes=n, N=3, R=2, W=2) for n in (3, 4, 5)], a)
        save_csv("scale", rows)
        plot("scale", rows, "throughput_ops_s", "ops/sec", "Throughput vs cluster size")
    else:
        rows = []
        for r, w in [(1, 1), (1, 2), (2, 1), (2, 2), (3, 3)]:
            cfg = dict(num_nodes=3, N=3, R=r, W=w)
            print(f"consistency test N=3 R={r} W={w} (R+W={r + w}) ...", flush=True)
            stale = stale_read_test(cfg, a.iterations, a.delay_ms)
            rows.append({"label": f"R={r} W={w} (R+W={r + w})", "N": 3, "R": r, "W": w,
                         "iterations": a.iterations, "stale_reads": stale,
                         "stale_percent": round(100 * stale / a.iterations, 1)})
            print("    stale reads:", stale, "/", a.iterations)
        save_csv("consistency", rows)
        plot("consistency", rows, "stale_percent", "stale reads (%)",
             "Stale reads vs (R, W) with replica lag")


if __name__ == "__main__":
    main()
