from smartdialer import db as dbmod
from smartdialer.state_machines import CallState


def _new_call(conn, campaign):
    return dbmod.create_call(conn, campaign, "borrower-000001", "agent-00001", "provider_b", "ooo-key")


def test_answered_answered_answered_completed(db_path, campaign):
    """Brief's example #1: ANSWERED, ANSWERED, ANSWERED, COMPLETED."""
    conn = dbmod.connect(db_path)
    call_id = _new_call(conn, campaign)

    dbmod.apply_call_event(conn, call_id, "e1", CallState.ANSWERED)
    dbmod.apply_call_event(conn, call_id, "e2", CallState.ANSWERED)
    dbmod.apply_call_event(conn, call_id, "e3", CallState.ANSWERED)
    final = dbmod.apply_call_event(conn, call_id, "e4", CallState.COMPLETED)

    assert final == CallState.COMPLETED
    row = dbmod.get_call(conn, call_id)
    assert row["state"] == "COMPLETED"
    assert row["version"] == 2  # one real transition to ANSWERED, one to COMPLETED
    conn.close()


def test_completed_answered_ringing(db_path, campaign):
    """Brief's example #2: COMPLETED, ANSWERED, RINGING - must not crash
    and must not un-complete the call."""
    conn = dbmod.connect(db_path)
    call_id = _new_call(conn, campaign)

    s1 = dbmod.apply_call_event(conn, call_id, "e1", CallState.COMPLETED)
    s2 = dbmod.apply_call_event(conn, call_id, "e2", CallState.ANSWERED)
    s3 = dbmod.apply_call_event(conn, call_id, "e3", CallState.RINGING)

    assert s1 == CallState.COMPLETED
    assert s2 == CallState.COMPLETED, "ANSWERED after COMPLETED must be ignored, not applied"
    assert s3 == CallState.COMPLETED, "RINGING after COMPLETED must be ignored, not applied"

    row = dbmod.get_call(conn, call_id)
    assert row["state"] == "COMPLETED"
    conn.close()


def test_worker_crash_right_after_answered_then_completed_arrives(db_path, campaign):
    """Simulates: provider sends ANSWERED, the worker handling it 'crashes'
    (i.e. we simply don't do anything else), then later COMPLETED arrives
    from a fresh worker/connection. The call should still land COMPLETED."""
    conn1 = dbmod.connect(db_path)
    call_id = _new_call(conn1, campaign)
    dbmod.apply_call_event(conn1, call_id, "e1", CallState.ANSWERED)
    conn1.close()  # simulated crash - connection dropped, no cleanup

    conn2 = dbmod.connect(db_path)  # a different worker/connection picks up the next event
    final = dbmod.apply_call_event(conn2, call_id, "e2", CallState.COMPLETED)
    assert final == CallState.COMPLETED
    conn2.close()
