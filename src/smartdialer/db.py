"""
Storage layer.

Design choice: SQLite in WAL mode, with every write wrapped in a manual
`BEGIN IMMEDIATE` transaction.

Why this gives us real cross-worker safety (not just in-process locks):
`BEGIN IMMEDIATE` acquires SQLite's RESERVED lock immediately, so if two
threads/processes race to run `reserve_agent()` on the same agent, the
second writer physically blocks until the first COMMITs, then re-reads
the row and finds `state != AVAILABLE`, so its own UPDATE affects 0 rows
and it correctly reports failure. This is the exact same pattern as
`UPDATE agents SET state=... WHERE id=? AND state='AVAILABLE'` in
Postgres/MySQL - a single-statement compare-and-swap. We are not using
an application-level lock (e.g. asyncio.Lock/threading.Lock) anywhere,
on purpose: an in-process lock would NOT protect you once you have
multiple worker processes/hosts, and this project explicitly needs to
answer "what happens with multiple dialer workers". The CAS pattern
here generalizes directly to a real multi-node deployment; an
in-process lock would not, so using one would have been a shortcut that
quietly breaks in production. See ADR.md.
"""
from __future__ import annotations
import sqlite3
import time
import uuid
from contextlib import contextmanager

from .state_machines import AgentState, CallState, call_event_is_forward_progress

SCHEMA = """
CREATE TABLE IF NOT EXISTS agents (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    state TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    reserved_by TEXT,
    lease_expires_at REAL
);

CREATE TABLE IF NOT EXISTS borrowers (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'QUEUED',
    version INTEGER NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    reserved_by TEXT
);

CREATE TABLE IF NOT EXISTS calls (
    id TEXT PRIMARY KEY,
    campaign_id TEXT NOT NULL,
    borrower_id TEXT NOT NULL,
    agent_id TEXT,
    provider TEXT NOT NULL,
    state TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 0,
    idempotency_key TEXT UNIQUE NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    lease_expires_at REAL
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    call_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    processed_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS metrics_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    event TEXT NOT NULL,
    detail TEXT
);
"""


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30, isolation_level=None, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


@contextmanager
def immediate_tx(conn: sqlite3.Connection):
    """A write transaction that acquires SQLite's write lock up front,
    giving us CAS semantics across threads/processes sharing this DB file."""
    conn.execute("BEGIN IMMEDIATE;")
    try:
        yield conn
        conn.execute("COMMIT;")
    except Exception:
        conn.execute("ROLLBACK;")
        raise


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------
def seed_agents(conn: sqlite3.Connection, campaign_id: str, count: int) -> list[str]:
    ids = [f"agent-{i:05d}" for i in range(count)]
    with immediate_tx(conn):
        conn.executemany(
            "INSERT OR REPLACE INTO agents (id, campaign_id, state, version) VALUES (?, ?, ?, 0)",
            [(aid, campaign_id, AgentState.AVAILABLE.value) for aid in ids],
        )
    return ids


def seed_borrowers(conn: sqlite3.Connection, campaign_id: str, count: int) -> list[str]:
    ids = [f"borrower-{i:06d}" for i in range(count)]
    with immediate_tx(conn):
        conn.executemany(
            "INSERT OR REPLACE INTO borrowers (id, campaign_id, phone, state, version) "
            "VALUES (?, ?, ?, 'QUEUED', 0)",
            [(bid, campaign_id, f"+1555{i:07d}", ) for i, bid in enumerate(ids)],
        )
    return ids


# ---------------------------------------------------------------------------
# Agent reservation (CAS)
# ---------------------------------------------------------------------------
def reserve_agent(conn: sqlite3.Connection, agent_id: str, worker_id: str,
                   lease_seconds: float = 15.0) -> bool:
    """Atomically flips one AVAILABLE agent to RESERVED. Returns False if
    another worker already claimed it (or it isn't AVAILABLE). This is the
    function that answers: 'two workers see the same agent at the same
    time - both must not be able to reserve it.'"""
    now = time.time()
    with immediate_tx(conn):
        cur = conn.execute(
            "UPDATE agents SET state=?, version=version+1, reserved_by=?, lease_expires_at=? "
            "WHERE id=? AND state=?",
            (AgentState.RESERVED.value, worker_id, now + lease_seconds, agent_id, AgentState.AVAILABLE.value),
        )
        return cur.rowcount == 1


def advance_agent(conn: sqlite3.Connection, agent_id: str, to_state: AgentState,
                   lease_seconds: float | None = None) -> bool:
    now = time.time()
    lease = (now + lease_seconds) if lease_seconds else None
    with immediate_tx(conn):
        cur = conn.execute(
            "UPDATE agents SET state=?, version=version+1, lease_expires_at=? WHERE id=?",
            (to_state.value, lease, agent_id),
        )
        return cur.rowcount == 1


def release_agent(conn: sqlite3.Connection, agent_id: str) -> None:
    with immediate_tx(conn):
        conn.execute(
            "UPDATE agents SET state=?, version=version+1, reserved_by=NULL, lease_expires_at=NULL WHERE id=?",
            (AgentState.AVAILABLE.value, agent_id),
        )


def pick_available_agent(conn: sqlite3.Connection, campaign_id: str) -> str | None:
    row = conn.execute(
        "SELECT id FROM agents WHERE campaign_id=? AND state=? ORDER BY id LIMIT 1",
        (campaign_id, AgentState.AVAILABLE.value),
    ).fetchone()
    return row["id"] if row else None


def get_borrower_attempt_count(conn: sqlite3.Connection, borrower_id: str) -> int:
    row = conn.execute("SELECT attempt_count FROM borrowers WHERE id=?", (borrower_id,)).fetchone()
    return row["attempt_count"] if row else 0


def count_agents_by_state(conn: sqlite3.Connection, campaign_id: str) -> dict[str, int]:
    rows = conn.execute(
        "SELECT state, COUNT(*) c FROM agents WHERE campaign_id=? GROUP BY state", (campaign_id,)
    ).fetchall()
    return {r["state"]: r["c"] for r in rows}


# ---------------------------------------------------------------------------
# Borrower reservation (CAS)
# ---------------------------------------------------------------------------
def reserve_next_borrower(conn: sqlite3.Connection, campaign_id: str, worker_id: str) -> str | None:
    """Picks one QUEUED borrower and atomically claims it. Uses the same
    CAS pattern as reserve_agent so two workers can never grab the same
    borrower."""
    row = conn.execute(
        "SELECT id FROM borrowers WHERE campaign_id=? AND state='QUEUED' ORDER BY attempt_count ASC, id ASC LIMIT 1",
        (campaign_id,),
    ).fetchone()
    if row is None:
        return None
    borrower_id = row["id"]
    with immediate_tx(conn):
        cur = conn.execute(
            "UPDATE borrowers SET state='RESERVED', version=version+1, reserved_by=? "
            "WHERE id=? AND state='QUEUED'",
            (worker_id, borrower_id),
        )
        if cur.rowcount == 1:
            return borrower_id
    return None  # someone else grabbed it between SELECT and UPDATE - caller just retries


def requeue_borrower(conn: sqlite3.Connection, borrower_id: str) -> None:
    with immediate_tx(conn):
        conn.execute(
            "UPDATE borrowers SET state='QUEUED', version=version+1, reserved_by=NULL, "
            "attempt_count=attempt_count+1 WHERE id=?",
            (borrower_id,),
        )


def complete_borrower(conn: sqlite3.Connection, borrower_id: str) -> None:
    with immediate_tx(conn):
        conn.execute("UPDATE borrowers SET state='DONE', version=version+1 WHERE id=?", (borrower_id,))


# ---------------------------------------------------------------------------
# Calls
# ---------------------------------------------------------------------------
def create_call(conn: sqlite3.Connection, campaign_id: str, borrower_id: str, agent_id: str | None,
                 provider: str, idempotency_key: str) -> str:
    """Idempotent by idempotency_key: if a call with this key already
    exists (e.g. a duplicate job pickup, or a retried request), we return
    the EXISTING call id instead of creating a second call."""
    now = time.time()
    call_id = f"call-{uuid.uuid4().hex[:12]}"
    with immediate_tx(conn):
        try:
            conn.execute(
                "INSERT INTO calls (id, campaign_id, borrower_id, agent_id, provider, state, version, "
                "idempotency_key, created_at, updated_at, lease_expires_at) "
                "VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
                (call_id, campaign_id, borrower_id, agent_id, provider, CallState.RESERVED.value,
                 idempotency_key, now, now, now + 30.0),
            )
            return call_id
        except sqlite3.IntegrityError:
            row = conn.execute(
                "SELECT id FROM calls WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            return row["id"]


def get_call(conn: sqlite3.Connection, call_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM calls WHERE id=?", (call_id,)).fetchone()


def apply_call_event(conn: sqlite3.Connection, call_id: str, event_id: str,
                      incoming_state: CallState) -> CallState:
    """
    Idempotent, out-of-order-tolerant event application.

    - Duplicate event_id -> no-op, returns current state.
    - Event that doesn't represent forward progress (stale/out-of-order,
      or arriving after a terminal state) -> no-op, returns current state.
    - Otherwise -> applies the transition and returns the new state.

    This is deliberately NOT validated against the strict adjacency graph
    in state_machines.CALL_TRANSITIONS, because provider events can
    legitimately skip states we'd never skip ourselves (e.g. a provider
    that doesn't emit RINGING before ANSWERED). We only require that the
    event moves the call forward and that we haven't already finished.
    """
    now = time.time()
    with immediate_tx(conn):
        dup = conn.execute("SELECT 1 FROM processed_events WHERE event_id=?", (event_id,)).fetchone()
        if dup:
            row = conn.execute("SELECT state FROM calls WHERE id=?", (call_id,)).fetchone()
            return CallState(row["state"])

        conn.execute(
            "INSERT INTO processed_events (event_id, call_id, event_type, processed_at) VALUES (?, ?, ?, ?)",
            (event_id, call_id, incoming_state.value, now),
        )

        row = conn.execute("SELECT state FROM calls WHERE id=?", (call_id,)).fetchone()
        current = CallState(row["state"])

        if not call_event_is_forward_progress(current, incoming_state):
            return current  # stale/duplicate/out-of-order relative to current state - ignored

        conn.execute(
            "UPDATE calls SET state=?, version=version+1, updated_at=? WHERE id=?",
            (incoming_state.value, now, call_id),
        )
        return incoming_state


# ---------------------------------------------------------------------------
# Reconciliation support
# ---------------------------------------------------------------------------
def find_expired_agent_reservations(conn: sqlite3.Connection, now: float | None = None) -> list[str]:
    now = now if now is not None else time.time()
    rows = conn.execute(
        "SELECT id FROM agents WHERE state IN (?, ?) AND lease_expires_at IS NOT NULL AND lease_expires_at < ?",
        (AgentState.RESERVED.value, AgentState.DIALING.value, now),
    ).fetchall()
    return [r["id"] for r in rows]


def find_expired_calls(conn: sqlite3.Connection, now: float | None = None) -> list[str]:
    now = now if now is not None else time.time()
    rows = conn.execute(
        "SELECT id FROM calls WHERE state NOT IN (?, ?, ?) AND lease_expires_at IS NOT NULL "
        "AND lease_expires_at < ?",
        (CallState.COMPLETED.value, CallState.FAILED.value, CallState.CANCELLED.value, now),
    ).fetchall()
    return [r["id"] for r in rows]


def fail_call(conn: sqlite3.Connection, call_id: str) -> None:
    now = time.time()
    with immediate_tx(conn):
        conn.execute(
            "UPDATE calls SET state=?, version=version+1, updated_at=? WHERE id=? AND state NOT IN (?,?,?)",
            (CallState.FAILED.value, now, call_id, CallState.COMPLETED.value, CallState.FAILED.value,
             CallState.CANCELLED.value),
        )


def log_metric(conn: sqlite3.Connection, event: str, detail: str = "") -> None:
    with immediate_tx(conn):
        conn.execute(
            "INSERT INTO metrics_log (ts, event, detail) VALUES (?, ?, ?)", (time.time(), event, detail)
        )
