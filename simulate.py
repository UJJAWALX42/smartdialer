"""
Runs a SmartDialer simulation end-to-end (real threads, real SQLite CAS
layer, real mock providers) and reports the metrics the assignment asks
for: agent utilization, calls initiated/connected, pacing behaviour, and
Safety Controller decisions.

Usage:
    python simulate.py --scenario A --mode predictive --agents 30 --borrowers 400 --provider a --seconds 20
    python simulate.py --scenario D --mode predictive --agents 30 --borrowers 400 --provider mixed --seconds 30
    python simulate.py --all   # runs A, B, C, D for both modes and writes a comparison table

Scenarios (answer_rate, avg_talk_time_seconds), matching the brief:
    A: 20%, 120s      B: 50%, 90s      C: 70%, 180s      D: changing over time
"""
from __future__ import annotations
import argparse
import csv
import os
import random
import threading
import time

from smartdialer import db as dbmod
from smartdialer.campaign import setup_campaign
from smartdialer.providers.provider_a import ProviderA
from smartdialer.providers.provider_b import ProviderB
from smartdialer.call_allocator import CallAllocator
from smartdialer.metrics import Metrics
from smartdialer.safety_controller import SafetyController
from smartdialer.progressive_dialer import ProgressiveDialer
from smartdialer.predictive_engine import PredictiveEngine
from smartdialer.worker import Worker

SCENARIOS = {
    "A": {"answer_rate": 0.20, "talk_time": 120},
    "B": {"answer_rate": 0.50, "talk_time": 90},
    "C": {"answer_rate": 0.70, "talk_time": 180},
    "D": {"answer_rate": None, "talk_time": None},  # changing - handled specially
}


def make_provider(kind: str, answer_rate: float, seed: int):
    failure_rate = max(0.01, 1.0 - answer_rate) * 0.15  # loosely couples "answer rate" to failure rate for realism
    if kind == "a":
        return ProviderA(failure_rate=min(failure_rate, 0.2), seed=seed)
    if kind == "b":
        return ProviderB(failure_rate=min(failure_rate, 0.3), seed=seed)
    raise ValueError(kind)


class DriftingAnswerRate:
    """Backs Scenario D: answer rate and provider health change mid-run."""

    def __init__(self, rng: random.Random):
        self.rng = rng
        self.value = 0.5

    def step(self):
        self.value = min(0.9, max(0.05, self.value + self.rng.uniform(-0.08, 0.08)))
        return self.value


def run_scenario(scenario: str, mode: str, num_agents: int, num_borrowers: int,
                  provider_kind: str, seconds: float, num_workers: int, db_path: str,
                  seed: int = 42) -> dict:
    campaign_id = f"sim-{scenario}-{mode}-{provider_kind}"
    conn = dbmod.connect(db_path)
    setup_campaign(conn, campaign_id, num_agents, num_borrowers)
    conn.close()

    rng = random.Random(seed)
    spec = SCENARIOS[scenario]
    drifting = DriftingAnswerRate(rng) if scenario == "D" else None
    answer_rate = drifting.value if drifting else spec["answer_rate"]

    provider = make_provider(provider_kind if provider_kind != "mixed" else rng.choice(["a", "b"]),
                              answer_rate, seed)

    metrics = Metrics()
    allocator = CallAllocator(db_path, provider, metrics)
    safety = SafetyController()
    pacing = ProgressiveDialer() if mode == "progressive" else PredictiveEngine()
    if mode == "predictive":
        pacing.observe_answer_rate(answer_rate)

    stop_flag = {"stop": False}

    def outage_or_drift_loop():
        t0 = time.time()
        while not stop_flag["stop"] and time.time() - t0 < seconds:
            time.sleep(1.0)
            if drifting:
                new_rate = drifting.step()
                if mode == "predictive":
                    pacing.observe_answer_rate(new_rate)
            if scenario in ("B", "C") and rng.random() < 0.03:
                provider.set_outage(True)
            elif provider.in_outage and rng.random() < 0.3:
                provider.set_outage(False)

    drift_thread = threading.Thread(target=outage_or_drift_loop, daemon=True)
    drift_thread.start()

    workers = [
        Worker(f"worker-{i}", db_path, campaign_id, allocator, pacing, safety, metrics,
               provider_error_rate_fn=provider.current_error_rate)
        for i in range(num_workers)
    ]
    threads = [threading.Thread(target=w.run, daemon=True) for w in workers]
    start = time.time()
    for t in threads:
        t.start()

    time.sleep(seconds)
    stop_flag["stop"] = True
    for w in workers:
        w.stop()

    elapsed = time.time() - start
    m = metrics.snapshot()
    conn = dbmod.connect(db_path)
    agent_counts = dbmod.count_agents_by_state(conn, campaign_id)
    conn.close()

    utilization = 1.0 - (agent_counts.get("AVAILABLE", 0) / max(1, num_agents))
    result = {
        "scenario": scenario, "mode": mode, "provider": provider_kind,
        "agents": num_agents, "elapsed_sec": round(elapsed, 1),
        "calls_initiated": m.get("calls_initiated", 0),
        "calls_connected": m.get("calls_connected", 0),
        "calls_completed": m.get("calls_completed", 0),
        "calls_failed": m.get("calls_failed", 0),
        "events_ignored_dup_or_stale": m.get("events_ignored_duplicate_or_stale", 0),
        "final_utilization_pct": round(utilization * 100, 1),
        "safety_decisions": len(metrics.decisions),
        "safety_reduce_or_fallback": sum(
            1 for d in metrics.decisions if d["action"] in ("REDUCE", "FALLBACK_PROGRESSIVE")
        ),
    }
    return result, metrics


def write_csv(rows: list[dict], path: str):
    if not rows:
        return
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def maybe_plot(rows: list[dict], out_path: str):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed - skipping chart")
        return
    labels = [f"{r['scenario']}/{r['mode']}" for r in rows]
    util = [r["final_utilization_pct"] for r in rows]
    connected = [r["calls_connected"] for r in rows]

    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax1.bar(labels, util, color="#3b82f6", alpha=0.7, label="Utilization %")
    ax1.set_ylabel("Final agent utilization (%)")
    ax1.set_ylim(0, 100)
    ax2 = ax1.twinx()
    ax2.plot(labels, connected, color="#ef4444", marker="o", label="Calls connected")
    ax2.set_ylabel("Calls connected")
    fig.suptitle("SmartDialer simulation: utilization vs. calls connected")
    fig.tight_layout()
    plt.savefig(out_path, dpi=130)
    print(f"chart written to {out_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenario", choices=list(SCENARIOS.keys()), default="B")
    ap.add_argument("--mode", choices=["progressive", "predictive"], default="predictive")
    ap.add_argument("--agents", type=int, default=20)
    ap.add_argument("--borrowers", type=int, default=300)
    ap.add_argument("--provider", choices=["a", "b", "mixed"], default="a")
    ap.add_argument("--seconds", type=float, default=15.0)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--out", default="sim_results.csv")
    ap.add_argument("--chart", default="sim_chart.png")
    ap.add_argument("--all", action="store_true", help="run all 4 scenarios x both modes")
    args = ap.parse_args()

    os.makedirs("run_artifacts", exist_ok=True)
    rows = []

    if args.all:
        for scenario in SCENARIOS:
            for mode in ("progressive", "predictive"):
                db_path = f"run_artifacts/sim_{scenario}_{mode}.db"
                if os.path.exists(db_path):
                    os.remove(db_path)
                result, _ = run_scenario(scenario, mode, args.agents, args.borrowers,
                                          args.provider, args.seconds, args.workers, db_path)
                rows.append(result)
                print(result)
    else:
        db_path = f"run_artifacts/sim_{args.scenario}_{args.mode}.db"
        if os.path.exists(db_path):
            os.remove(db_path)
        result, metrics = run_scenario(args.scenario, args.mode, args.agents, args.borrowers,
                                        args.provider, args.seconds, args.workers, db_path)
        rows.append(result)
        print(result)
        print("\nSample of safety-controller decisions (why N calls were started):")
        for d in metrics.decisions[:8]:
            print(f"  [{d['action']:>20}] requested={d['requested']:<4} approved={d['approved']:<4} "
                  f"| {d['pacing_reason']} | {d['safety_reason']}")

    write_csv(rows, os.path.join("run_artifacts", args.out))
    maybe_plot(rows, os.path.join("run_artifacts", args.chart))
    print(f"\nresults written to run_artifacts/{args.out}")


if __name__ == "__main__":
    main()
