from __future__ import annotations
import time
import uuid

from . import db
from .models import SystemSnapshot
from .safety_controller import SafetyController
from .call_allocator import CallAllocator
from .metrics import Metrics

TICK_SECONDS = 0.5
IN_FLIGHT_WINDOW_SECONDS = 20.0  # rough window used to estimate "ringing/dialing" count from metrics


def build_snapshot(conn, campaign_id: str, metrics: Metrics, provider_error_rate: float) -> SystemSnapshot:
    agent_counts = db.count_agents_by_state(conn, campaign_id)
    m = metrics.snapshot()
    calls_initiated = m.get("calls_initiated", 0)
    calls_completed = m.get("calls_completed", 0)
    calls_failed = m.get("calls_failed", 0)
    calls_connected = m.get("calls_connected", 0)
    in_flight = max(0, calls_initiated - calls_completed - calls_failed)

    answered = m.get("call_state_answered", 0)
    ringing = m.get("call_state_ringing", 0)
    denom = max(1, ringing + answered)
    recent_answer_rate = answered / denom if denom else 0.3

    return SystemSnapshot(
        campaign_id=campaign_id,
        available_agents=agent_counts.get("AVAILABLE", 0),
        agents_dialing_or_ringing=in_flight,
        calls_connected=calls_connected,
        calls_ringing=m.get("call_state_ringing", 0),
        recent_answer_rate=recent_answer_rate,
        recent_avg_talk_time=0.0,
        recent_avg_setup_time=0.0,
        provider_error_rate=provider_error_rate,
        recent_abandon_count=m.get("abandoned_connects", 0),
        timestamp=time.time(),
    )


class Worker:
    """One dialer worker. Run several of these concurrently (threads in
    this demo; separate processes/hosts in production - the safety
    properties are identical either way because they are enforced by the
    SQLite CAS layer in db.py, not by anything worker-local)."""

    def __init__(self, worker_id: str, db_path: str, campaign_id: str, allocator: CallAllocator,
                 pacing_engine, safety_controller: SafetyController, metrics: Metrics,
                 provider_error_rate_fn):
        self.worker_id = worker_id
        self.db_path = db_path
        self.campaign_id = campaign_id
        self.allocator = allocator
        self.pacing_engine = pacing_engine
        self.safety_controller = safety_controller
        self.metrics = metrics
        self.provider_error_rate_fn = provider_error_rate_fn
        self._stop = False

    def stop(self):
        self._stop = True

    def run(self, max_ticks: int | None = None) -> None:
        conn = db.connect(self.db_path)
        ticks = 0
        try:
            while not self._stop:
                self.tick(conn)
                ticks += 1
                if max_ticks is not None and ticks >= max_ticks:
                    break
                time.sleep(TICK_SECONDS)
        finally:
            conn.close()

    def tick(self, conn) -> None:
        snapshot = build_snapshot(conn, self.campaign_id, self.metrics, self.provider_error_rate_fn())
        requested, reason = self.pacing_engine.compute_request(snapshot)
        decision = self.safety_controller.evaluate(requested, snapshot)
        self.metrics.log_decision({
            "worker": self.worker_id,
            "ts": time.time(),
            "requested": decision.requested,
            "approved": decision.approved,
            "action": decision.action.value,
            "pacing_reason": reason,
            "safety_reason": decision.reason,
        })

        started = 0
        for _ in range(decision.approved):
            agent_id = db.pick_available_agent(conn, self.campaign_id)
            if agent_id is None:
                break
            if not db.reserve_agent(conn, agent_id, self.worker_id):
                # lost the race to another worker - try a different agent next loop
                continue

            borrower_id = db.reserve_next_borrower(conn, self.campaign_id, self.worker_id)
            if borrower_id is None:
                db.release_agent(conn, agent_id)  # nobody left to call - give the agent back
                break

            attempt = db.get_borrower_attempt_count(conn, borrower_id)
            idempotency_key = f"{borrower_id}-attempt-{attempt}"
            phone = f"+1555{borrower_id[-6:]}"
            self.allocator.place_call(self.campaign_id, borrower_id, agent_id, phone, idempotency_key)
            started += 1

        if started < decision.approved:
            self.metrics.incr("ticks_capacity_starved")
