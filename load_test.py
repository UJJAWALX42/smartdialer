"""
Basic load test targeting the actual question the brief asks:

    "We just went from 1,000 to 100,000 agents. What breaks first?"

This does NOT attempt to place real calls at scale (the brief explicitly
says not to). Instead it isolates the one operation that every worker,
at any scale, must perform once per call attempt: a CAS reservation
write against the shared store (db.reserve_agent). That single-writer
SQLite file is exactly the piece this project's architecture would need
to replace first at high scale (see ADR.md) - this test produces the
numbers that justify that claim instead of asserting it.

Usage:
    python load_test.py --agents 100 1000 10000 --workers 8 --ops-per-worker 200
"""
from __future__ import annotations
import argparse
import os
import statistics
import threading
import time

from smartdialer import db as dbmod


def run_load_point(num_agents: int, num_workers: int, ops_per_worker: int, db_path: str) -> dict:
    if os.path.exists(db_path):
        os.remove(db_path)
    conn = dbmod.connect(db_path)
    dbmod.init_db(conn)
    agent_ids = dbmod.seed_agents(conn, "load-camp", num_agents)
    conn.close()

    latencies = []
    successes = [0]
    lock = threading.Lock()

    def worker(worker_id: str):
        conn = dbmod.connect(db_path)
        local_latencies = []
        local_success = 0
        for i in range(ops_per_worker):
            agent_id = agent_ids[(hash(worker_id) + i) % len(agent_ids)]
            t0 = time.perf_counter()
            ok = dbmod.reserve_agent(conn, agent_id, worker_id, lease_seconds=0.001)
            dt = time.perf_counter() - t0
            local_latencies.append(dt)
            if ok:
                local_success += 1
                dbmod.release_agent(conn, agent_id)  # free it immediately so contention stays realistic
        conn.close()
        with lock:
            latencies.extend(local_latencies)
            successes[0] += local_success

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(num_workers)]
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_time = time.perf_counter() - start

    total_ops = num_workers * ops_per_worker
    latencies.sort()
    p50 = latencies[len(latencies) // 2] if latencies else 0
    p99 = latencies[int(len(latencies) * 0.99)] if latencies else 0

    return {
        "num_agents": num_agents,
        "num_workers": num_workers,
        "total_ops": total_ops,
        "total_time_sec": round(total_time, 3),
        "ops_per_sec": round(total_ops / total_time, 1) if total_time > 0 else 0,
        "p50_latency_ms": round(p50 * 1000, 3),
        "p99_latency_ms": round(p99 * 1000, 3),
        "mean_latency_ms": round(statistics.mean(latencies) * 1000, 3) if latencies else 0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--agents", type=int, nargs="+", default=[100, 1000, 10000])
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--ops-per-worker", type=int, default=200)
    ap.add_argument("--out-dir", default="run_artifacts")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    print(f"{'agents':>8} {'workers':>8} {'ops':>8} {'time(s)':>9} {'ops/s':>10} "
          f"{'p50(ms)':>9} {'p99(ms)':>9}")
    for n in args.agents:
        db_path = os.path.join(args.out_dir, f"load_{n}.db")
        result = run_load_point(n, args.workers, args.ops_per_worker, db_path)
        print(f"{result['num_agents']:>8} {result['num_workers']:>8} {result['total_ops']:>8} "
              f"{result['total_time_sec']:>9} {result['ops_per_sec']:>10} "
              f"{result['p50_latency_ms']:>9} {result['p99_latency_ms']:>9}")

    print("\nInterpretation: ops/sec should stay roughly flat as `agents` grows (each CAS write is "
          "O(1) - an indexed UPDATE), but p99 latency should climb with `workers` because BEGIN "
          "IMMEDIATE serializes all writers on the single SQLite file. That serialization - not the "
          "agent count - is the actual bottleneck, and is what needs to change first at real scale. "
          "See ADR.md 'Scale analysis'.")


if __name__ == "__main__":
    main()
