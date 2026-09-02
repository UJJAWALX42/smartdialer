import time
from smartdialer import db as dbmod
from smartdialer.campaign import setup_campaign
from smartdialer.providers.provider_b import ProviderB
from smartdialer.call_allocator import CallAllocator
from smartdialer.metrics import Metrics


def test_provider_outage_produces_no_terminal_event_and_is_recoverable_via_reconciliation(db_path, tmp_path):
    conn = dbmod.connect(db_path)
    setup_campaign(conn, "camp-outage", num_agents=3, num_borrowers=3)
    conn.close()

    provider = ProviderB(seed=1)
    provider.set_outage(True)
    metrics = Metrics()
    allocator = CallAllocator(db_path, provider, metrics)

    conn = dbmod.connect(db_path)
    agent_id = dbmod.pick_available_agent(conn, "camp-outage")
    dbmod.reserve_agent(conn, agent_id, "w1")
    borrower_id = dbmod.reserve_next_borrower(conn, "camp-outage", "w1")
    call_id = allocator.place_call("camp-outage", borrower_id, agent_id, "+15550000000", "k1")
    conn.close()

    time.sleep(0.3)  # give the (silent, outage) provider thread a moment - it will emit nothing

    conn = dbmod.connect(db_path)
    row = dbmod.get_call(conn, call_id)
    assert row["state"] == "RESERVED", "outage provider should never advance the call on its own"

    # force the lease to look expired, then run reconciliation - this is
    # exactly what happens after a real timeout, just sped up for the test
    conn.execute("UPDATE calls SET lease_expires_at = ? WHERE id = ?", (time.time() - 1, call_id))
    from smartdialer import reconciliation
    result = reconciliation.sweep(conn, metrics)
    conn.close()

    assert result["failed_calls"] == 1

    conn = dbmod.connect(db_path)
    row = dbmod.get_call(conn, call_id)
    assert row["state"] == "FAILED"
    agent_row = conn.execute("SELECT state FROM agents WHERE id=?", (agent_id,)).fetchone()
    assert agent_row["state"] == "AVAILABLE", "agent must be freed back up after the failed/timed-out call"
    borrower_row = conn.execute("SELECT state, attempt_count FROM borrowers WHERE id=?", (borrower_id,)).fetchone()
    assert borrower_row["state"] == "QUEUED"
    assert borrower_row["attempt_count"] == 1
    conn.close()


def test_provider_error_rate_is_visible_for_safety_controller_input():
    provider = ProviderB(failure_rate=1.0, timeout_rate=0.0, seed=2)
    events = []
    done = []

    def on_event(call_id, event_id, state):
        events.append(state)
        if state.value in ("COMPLETED", "FAILED"):
            done.append(True)

    provider.start_call("call-x", "+15550000000", on_event)
    for _ in range(50):
        if done:
            break
        time.sleep(0.1)

    assert provider.current_error_rate() > 0.0
