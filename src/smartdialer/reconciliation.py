from __future__ import annotations
from . import db
from .metrics import Metrics


def sweep(conn, metrics: Metrics | None = None) -> dict[str, int]:
    """
    Every reservation (agent or call) is written with a lease_expires_at
    timestamp at creation time (see db.reserve_agent / db.create_call).
    A worker that crashes mid-flow simply stops renewing/advancing that
    row - it does not need to explicitly signal failure. This sweep is
    the ONLY thing that needs to run after a crash: it finds leases that
    expired with no forward progress, releases the agent back to
    AVAILABLE, fails the call, and requeues the borrower for another
    attempt.

    Run this periodically (e.g. every few seconds) from any single
    worker, or from a dedicated reconciliation loop - it is itself CAS-
    safe (via db.release_agent / db.fail_call / db.requeue_borrower), so
    running it from multiple workers concurrently is harmless; whichever
    one gets there first wins, the rest are no-ops.
    """
    reclaimed_agents = 0
    failed_calls = 0

    for agent_id in db.find_expired_agent_reservations(conn):
        db.release_agent(conn, agent_id)
        reclaimed_agents += 1
        if metrics:
            metrics.incr("reconciled_agents_reclaimed")

    for call_id in db.find_expired_calls(conn):
        call = db.get_call(conn, call_id)
        if call is None:
            continue
        db.fail_call(conn, call_id)
        db.requeue_borrower(conn, call["borrower_id"])
        if call["agent_id"]:
            db.release_agent(conn, call["agent_id"])
        failed_calls += 1
        if metrics:
            metrics.incr("reconciled_calls_failed")

    return {"reclaimed_agents": reclaimed_agents, "failed_calls": failed_calls}
