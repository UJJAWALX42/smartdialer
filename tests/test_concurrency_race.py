import threading
from smartdialer import db as dbmod


def test_only_one_worker_can_reserve_the_same_agent(db_path, campaign):
    conn_main = dbmod.connect(db_path)
    dbmod.seed_agents(conn_main, campaign, 1)  # overwrite: exactly 1 agent for a tight race
    agent_id = "agent-00000"
    conn_main.close()

    results = []
    lock = threading.Lock()

    def worker(worker_id: str):
        conn = dbmod.connect(db_path)
        ok = dbmod.reserve_agent(conn, agent_id, worker_id)
        with lock:
            results.append(ok)
        conn.close()

    threads = [threading.Thread(target=worker, args=(f"worker-{i}",)) for i in range(25)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(True) == 1, "exactly one worker must win the reservation race"
    assert results.count(False) == 24


def test_only_one_worker_can_reserve_the_same_borrower(db_path):
    conn = dbmod.connect(db_path)
    dbmod.seed_borrowers(conn, "camp-x", 1)
    conn.close()

    claimed = []
    lock = threading.Lock()

    def worker(worker_id: str):
        conn = dbmod.connect(db_path)
        bid = dbmod.reserve_next_borrower(conn, "camp-x", worker_id)
        if bid is not None:
            with lock:
                claimed.append((worker_id, bid))
        conn.close()

    threads = [threading.Thread(target=worker, args=(f"worker-{i}",)) for i in range(25)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(claimed) == 1, f"exactly one worker should have claimed the borrower, got {claimed}"


def test_agent_state_never_goes_negative_under_load(db_path, campaign):
    """25 workers race for 5 agents - no more than 5 reservations should
    ever succeed, and the count of AVAILABLE agents afterwards must be
    consistent (0)."""
    conn = dbmod.connect(db_path)
    agent_ids = [f"agent-{i:05d}" for i in range(5)]
    conn.close()

    wins = []
    lock = threading.Lock()

    def worker(worker_id: str):
        conn = dbmod.connect(db_path)
        local_wins = 0
        for aid in agent_ids:
            if dbmod.reserve_agent(conn, aid, worker_id):
                local_wins += 1
        with lock:
            wins.append(local_wins)
        conn.close()

    threads = [threading.Thread(target=worker, args=(f"worker-{i}",)) for i in range(25)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(wins) == 5

    conn = dbmod.connect(db_path)
    counts = dbmod.count_agents_by_state(conn, campaign)
    conn.close()
    assert counts.get("AVAILABLE", 0) == 0
    assert counts.get("RESERVED", 0) == 5
