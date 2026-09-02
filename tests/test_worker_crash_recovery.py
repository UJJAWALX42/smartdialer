import time
from smartdialer import db as dbmod
from smartdialer.campaign import setup_campaign
from smartdialer import reconciliation
from smartdialer.metrics import Metrics


def test_worker_crash_after_call_initiated_is_reconciled(db_path):
    conn = dbmod.connect(db_path)
    setup_campaign(conn, "camp-crash", num_agents=2, num_borrowers=2)

    # --- simulate the crashing worker's partial work ---
    agent_id = dbmod.pick_available_agent(conn, "camp-crash")
    assert dbmod.reserve_agent(conn, agent_id, "worker-doomed")

    borrower_id = dbmod.reserve_next_borrower(conn, "camp-crash", "worker-doomed")
    assert borrower_id is not None

    call_id = dbmod.create_call(conn, "camp-crash", borrower_id, agent_id, "provider_a", "crash-key")
    dbmod.advance_agent(conn, agent_id, dbmod.AgentState.DIALING, lease_seconds=5.0)
    # worker crashes right here - never calls the provider, never advances the call further

    # sanity: system is "stuck" mid-flow
    row = dbmod.get_call(conn, call_id)
    assert row["state"] == "RESERVED"
    agent_row = conn.execute("SELECT state FROM agents WHERE id=?", (agent_id,)).fetchone()
    assert agent_row["state"] == "DIALING"

    # --- "system comes back": force leases to look expired (simulating
    # time having passed) and run the reconciliation sweep, as a fresh
    # process/worker would on startup ---
    conn.execute("UPDATE agents SET lease_expires_at = ? WHERE id = ?", (time.time() - 1, agent_id))
    conn.execute("UPDATE calls SET lease_expires_at = ? WHERE id = ?", (time.time() - 1, call_id))

    metrics = Metrics()
    result = reconciliation.sweep(conn, metrics)

    assert result["reclaimed_agents"] >= 1
    assert result["failed_calls"] == 1

    agent_row = conn.execute("SELECT state FROM agents WHERE id=?", (agent_id,)).fetchone()
    assert agent_row["state"] == "AVAILABLE"

    call_row = dbmod.get_call(conn, call_id)
    assert call_row["state"] == "FAILED"

    borrower_row = conn.execute("SELECT state, attempt_count FROM borrowers WHERE id=?", (borrower_id,)).fetchone()
    assert borrower_row["state"] == "QUEUED", "borrower must be requeued so a healthy worker retries them"
    assert borrower_row["attempt_count"] == 1

    conn.close()


def test_reconciliation_sweep_is_a_noop_for_healthy_in_flight_work(db_path):
    conn = dbmod.connect(db_path)
    setup_campaign(conn, "camp-healthy", num_agents=1, num_borrowers=1)

    agent_id = dbmod.pick_available_agent(conn, "camp-healthy")
    dbmod.reserve_agent(conn, agent_id, "worker-1", lease_seconds=60.0)  # lease far in the future

    result = reconciliation.sweep(conn)
    assert result == {"reclaimed_agents": 0, "failed_calls": 0}

    agent_row = conn.execute("SELECT state FROM agents WHERE id=?", (agent_id,)).fetchone()
    assert agent_row["state"] == "RESERVED", "a healthy, non-expired reservation must not be touched"
    conn.close()
