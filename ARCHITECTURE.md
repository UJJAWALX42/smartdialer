# Architecture

## 1. Pipeline

```mermaid
flowchart LR
    C[Campaign] --> PE
    subgraph PE[Pacing Engine]
        direction TB
        PD[Progressive Dialer]
        PR[Predictive Engine]
    end
    PE -- "requested: N calls + why" --> SC[Safety Controller]
    SC -- "approved: M calls (M <= available agents, M <= N)" --> CA[Call Allocator]
    CA -- "start_call(), receives events" --> TP[(Telecom Provider\nA or B, behind one interface)]
    CA --> DB[(SQLite: agents / borrowers / calls\nCAS reservations + idempotent events)]
    RC[Reconciliation Sweep] -.->|reclaims expired leases| DB
```

Key property: **the pacing engines have no reference to the Call
Allocator or the Provider at all** (see `worker.py` - `pacing_engine`
and `safety_controller` are separate objects; only the Safety
Controller's *approved* count ever reaches the allocation loop). A
predictive engine that wanted to bypass safety would have to be handed
a capability it structurally does not have.

## 2. Agent state machine

```mermaid
stateDiagram-v2
    [*] --> OFFLINE
    OFFLINE --> AVAILABLE
    AVAILABLE --> RESERVED: worker wins CAS reservation
    AVAILABLE --> PAUSED
    AVAILABLE --> OFFLINE
    RESERVED --> DIALING: call placed with provider
    RESERVED --> AVAILABLE: setup failed / reservation lease expired
    DIALING --> CONNECTED: call reached CONNECTED
    DIALING --> AVAILABLE: call failed before connecting
    CONNECTED --> WRAP_UP: call COMPLETED
    WRAP_UP --> AVAILABLE
    WRAP_UP --> PAUSED
    WRAP_UP --> OFFLINE
    PAUSED --> AVAILABLE
    PAUSED --> OFFLINE
```

## 3. Call state machine

```mermaid
stateDiagram-v2
    [*] --> QUEUED
    QUEUED --> RESERVED: borrower + agent CAS-reserved together
    QUEUED --> CANCELLED
    RESERVED --> INITIATED: allocator calls provider
    RESERVED --> FAILED
    RESERVED --> CANCELLED
    INITIATED --> RINGING
    INITIATED --> FAILED
    RINGING --> ANSWERED
    RINGING --> FAILED
    ANSWERED --> CONNECTED
    ANSWERED --> FAILED
    CONNECTED --> COMPLETED
    COMPLETED --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```

Provider events are **not** validated against this graph directly (see
`state_machines.call_event_is_forward_progress`). A real provider will
skip states, duplicate events, and reorder them, so the event-ingest
path uses a simpler, more robust rule: apply an incoming event only if
it represents forward progress and the call hasn't already reached a
terminal state. The graph above documents intent; the rank+terminal
guard is what actually survives contact with a messy provider.

## 4. Two workers race for the same agent

```mermaid
sequenceDiagram
    participant W1 as Worker 1
    participant W2 as Worker 2
    participant DB as SQLite (agents table)

    W1->>DB: BEGIN IMMEDIATE
    Note over DB: write lock acquired by W1
    W2->>DB: BEGIN IMMEDIATE (blocks)
    W1->>DB: UPDATE agents SET state='RESERVED' WHERE id=? AND state='AVAILABLE'
    DB-->>W1: 1 row affected
    W1->>DB: COMMIT
    Note over DB: lock released
    DB-->>W2: write lock granted
    W2->>DB: UPDATE agents SET state='RESERVED' WHERE id=? AND state='AVAILABLE'
    DB-->>W2: 0 rows affected (state is already RESERVED)
    W2->>DB: COMMIT (no-op)
    Note over W2: reserve_agent() returns False - W2 tries a different agent
```

No application-level lock is involved. `BEGIN IMMEDIATE` plus a
`WHERE state = 'AVAILABLE'` guard on the UPDATE *is* the compare-and-
swap; it is enforced by SQLite's own locking, which is why the same
pattern (as a single `UPDATE ... WHERE` on Postgres/MySQL, or a
version-column CAS) works identically with real multi-process,
multi-host workers. See `tests/test_concurrency_race.py`, which runs
this exact scenario with 25 real OS threads.

## 5. Worker crash and recovery

```mermaid
sequenceDiagram
    participant W as Worker (crashes)
    participant DB as SQLite
    participant RC as Reconciliation Sweep

    W->>DB: reserve_agent() -> RESERVED, lease=now+15s
    W->>DB: reserve_next_borrower() -> RESERVED
    W->>DB: create_call() -> call state=RESERVED, lease=now+30s
    W->>DB: advance_agent(DIALING)
    Note over W: *** process crashes here, before calling the provider ***
    Note over DB: agent stuck DIALING, call stuck RESERVED,\nborrower stuck RESERVED - but both leases will expire

    loop every few seconds, any live worker
        RC->>DB: find_expired_agent_reservations() / find_expired_calls()
        DB-->>RC: [agent, call] past lease_expires_at
        RC->>DB: release_agent() -> AVAILABLE
        RC->>DB: fail_call() -> FAILED
        RC->>DB: requeue_borrower() -> QUEUED, attempt_count+1
    end
    Note over DB: system is consistent again - no manual intervention,\nno special "was this worker alive" check needed
```

The design choice here: **recovery does not try to detect that a
specific worker died.** It never asks "is worker W1 still alive?" -
that question is genuinely hard in a distributed system (see: every
paper ever written about failure detectors). Instead every reservation
carries a lease, and *not renewing a lease* is indistinguishable from,
and handled identically to, an actual crash. See
`tests/test_worker_crash_recovery.py`.

## 6. Multiple workers, one campaign

```mermaid
flowchart TB
    subgraph Workers
        W1[Worker 1]
        W2[Worker 2]
        W3[Worker N]
    end
    W1 & W2 & W3 --> DB[(Shared SQLite file\nWAL mode)]
    RC[Reconciliation sweep\nrun by any worker] --> DB
    W1 & W2 & W3 -->|via CallAllocator| PA[Provider A]
    W1 & W2 & W3 -->|via CallAllocator| PB[Provider B]
```

All N workers run the identical `Worker.tick()` loop against the same
DB file. There is no leader/coordinator and no partitioning of agents
or borrowers between workers - correctness comes entirely from the CAS
layer, so adding or removing workers requires no coordination protocol.
This is deliberately the simplest architecture that satisfies the
requirements; see ADR.md for why Kafka/Redis/a coordinator were not
introduced, and for where this specific choice stops scaling.
