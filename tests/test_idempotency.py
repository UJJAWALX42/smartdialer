from smartdialer import db as dbmod
from smartdialer.state_machines import CallState


def test_duplicate_event_id_is_a_noop(db_path, campaign):
    conn = dbmod.connect(db_path)
    call_id = dbmod.create_call(conn, campaign, "borrower-000000", "agent-00000", "provider_a", "key-1")

    s1 = dbmod.apply_call_event(conn, call_id, "evt-answered-1", CallState.ANSWERED)
    s2 = dbmod.apply_call_event(conn, call_id, "evt-answered-1", CallState.ANSWERED)  # same event_id again

    assert s1 == CallState.ANSWERED
    assert s2 == CallState.ANSWERED

    row = dbmod.get_call(conn, call_id)
    # version should have incremented exactly once (RESERVED->ANSWERED is 1 transition), not twice
    assert row["version"] == 1
    conn.close()


def test_duplicate_call_creation_is_idempotent_by_key(db_path, campaign):
    conn = dbmod.connect(db_path)
    id1 = dbmod.create_call(conn, campaign, "borrower-000000", "agent-00000", "provider_a", "same-key")
    id2 = dbmod.create_call(conn, campaign, "borrower-000000", "agent-00000", "provider_a", "same-key")
    assert id1 == id2, "creating a call twice with the same idempotency key must not create two rows"

    count = conn.execute("SELECT COUNT(*) c FROM calls WHERE idempotency_key='same-key'").fetchone()["c"]
    assert count == 1
    conn.close()
