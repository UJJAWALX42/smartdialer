# Architecture Decision Document

## 1. Language: Python

**Why:** the hard part of this assignment is not the language, it's the
state machine / concurrency / safety-boundary reasoning, and that
reasoning needs to be visible and easy to walk through in a discussion.
Python's `sqlite3` module also gives real transactional CAS semantics
(`BEGIN IMMEDIATE`) out of the box, with zero extra services to install,
so the concurrency story is real, not simulated with in-process locks
that would stop being true the moment you had a second process.

**What it makes harder:** the GIL means the "multiple workers" demo
uses threads, which is an honest simulation of multiple processes for
I/O-bound work (every worker spends almost all its time waiting on
SQLite or the mock provider's `time.sleep`) but is not literally
multi-process. `tests/test_concurrency_race.py` and `load_test.py` still
exercise the real cross-process-shaped mechanism (SQLite's file lock),
so the safety property being tested - "does the storage layer serialize
conflicting writers correctly" - transfers to a real multi-host
deployment unchanged. Only the "are these truly separate OS processes"
detail doesn't.

## 2. Storage: SQLite (single file, WAL mode), not Postgres/Redis/Kafka

**What problem it solves:** every "don't let two workers do the same
thing twice" requirement in this assignment (agent reservation, borrower
reservation, idempotent call creation, idempotent event application)
reduces to the same primitive: **a single atomic
compare-and-swap write with a `WHERE` guard on current state.**
SQLite's `BEGIN IMMEDIATE` transaction gives exactly that, for free, with
one file and no network hop.

**Why not add Kafka/Redis/a queue:** the brief explicitly warns against
this ("don't add technology just because it sounds impressive"), and
none of the five distributed-systems problems in the brief (agent
allocation, borrower allocation, duplicate jobs, retries, stale state)
are actually solved by adding a queue - they're solved by CAS + leases +
idempotency keys, which a queue doesn't give you automatically anyway
(a message queue still needs an idempotency layer on top if a message
can be delivered twice, which Kafka/SQS/RabbitMQ can all do). Adding
Redis for "distributed locking" would have been strictly worse: it adds
a second source of truth that can disagree with the DB (the exact
"your database says AVAILABLE, your cache says RESERVED - which wins"
trap the brief warns about), for a coordination guarantee SQLite's
transactions already provide at this scale.

**What it makes harder:** SQLite has exactly one writer at a time
file-wide (not per-row). That is the single most important limitation
of this architecture and is the direct answer to the scale question
below - it is called out on purpose, not glossed over.

## 3. Concurrency model: optimistic CAS + leases, no application locks

Every mutable entity (`agents`, `borrowers`, `calls`) has a `version`
column that increments on every write, and reservations carry a
`lease_expires_at`. Three mechanisms, each solving a distinct problem
from the brief:

| Problem | Mechanism | Where |
|---|---|---|
| Two workers reserve the same agent | CAS: `UPDATE ... WHERE state='AVAILABLE'`, check `rowcount` | `db.reserve_agent` |
| Duplicate job pickup / retried call creation | Idempotency key with a `UNIQUE` constraint; on conflict, return the existing row | `db.create_call` |
| Duplicate provider event delivery | Dedupe by `event_id` in a `processed_events` table before applying | `db.apply_call_event` |
| Out-of-order provider events | Forward-progress rank check + terminal-state lock, independent of dedupe | `state_machines.call_event_is_forward_progress` |
| Worker crash mid-flow | Lease TTL on every reservation; a periodic sweep reclaims expired leases | `reconciliation.sweep` |

No `threading.Lock`/`asyncio.Lock` is used anywhere in the reservation
path, deliberately - an in-process lock would give a false sense of
safety that evaporates the moment there's a second process, which
directly contradicts the "assume multiple dialer workers" requirement.

## 4. Safety Controller as a structural boundary, not a policy switch

The predictive engine (`predictive_engine.py`) has no import of, and no
reference to, `call_allocator.py` or any provider. It returns a plain
`(int, str)` - a requested count and a human-readable reason. `worker.py`
is the only place that wires pacing engine -> safety controller ->
reservation loop, and it always calls the Safety Controller in between.
There is no configuration flag that lets the predictive engine skip the
Safety Controller, because there is no code path where it could call the
allocator even if it wanted to - it doesn't have a reference to it. This
was a deliberate choice over "add a `bypass_safety` flag and just don't
document it" - the constraint the brief describes ("the predictive
algorithm should not have a way to simply switch the safety mechanism
off") is much stronger if it's true by construction than if it's true by
convention.

The Safety Controller's own hard ceiling
(`approved <= available_agents`, see `safety_controller.py`) is a
constant, not a parameter - nothing in the codebase can raise it.

## 5. Predictive pacing formula

`desired_in_flight = available_agents / answer_rate_estimate` (EWMA of
recent answer rate), request = `desired_in_flight - already_in_flight`.
Plain-English version: *if only 1 in 3 calls gets answered, dial 3x the
number of agents you have "in flight" so that, statistically, about one
agent's worth of calls lands at a time.* This is intentionally the
simplest model that responds correctly to the "why did you dial 17 calls
instead of 10" question with a one-line, arithmetic answer instead of a
model card. It adapts within a few ticks via EWMA (`alpha=0.3`) rather
than using the whole-campaign average, so a real answer-rate collapse
(70% -> 10%) is reflected in the *request* within a handful of ticks -
and, independently, can never produce an *unsafe outcome* even before it
adapts, because the Safety Controller re-checks `available_agents` every
tick regardless of what the predictive engine asked for. Prediction
error degrades to "occasionally under-dial" (safe), never to
"over-connect" (unsafe).

## 6. Failure handling summary

| Scenario | Behavior |
|---|---|
| Worker crash after `reserve → reserve → initiate` | Leases expire; `reconciliation.sweep` releases the agent, fails the call, requeues the borrower. No crash detection needed - see `tests/test_worker_crash_recovery.py`. |
| Provider outage | `provider.current_error_rate()` feeds into `SystemSnapshot`; Safety Controller detects `error_rate >= 0.6` and returns `FALLBACK_PROGRESSIVE`, capping new calls at `available_agents` (same as progressive mode) until the provider recovers. In-flight calls simply never get a terminal event and are cleaned up by the same lease-expiry sweep. See `tests/test_provider_failure.py`. |
| 100 agents, 40 disappear in seconds | Reaction time is bounded by tick interval (`TICK_SECONDS = 0.5` in `worker.py`): every worker re-reads `available_agents` fresh from the DB every tick, so the very next tick after the drop naturally requests fewer calls. No explicit "agent disappeared" event handling is needed because the pacing engines never cache agent counts between ticks. |
| Duplicate provider events | Deduped by `event_id`; second delivery is a verified no-op (`tests/test_idempotency.py`). |
| Out-of-order events | Handled by the forward-progress rank + terminal-state lock (`tests/test_out_of_order.py`), including the exact `ANSWERED×3, COMPLETED` and `COMPLETED, ANSWERED, RINGING` sequences from the brief. |

## 7. Scale analysis: 100 -> 1,000 -> 10,000 agents

**What breaks first, with numbers, not a guess:** `load_test.py` isolates
the one operation every worker performs once per call attempt - a CAS
agent reservation - and measures it directly.

```
agents   workers   ops    time(s)   ops/s     p99(ms)
   100        8    1200    0.665    1804.6     0.436
  1000        8    1200    0.460    2610.0     0.544
  5000        8    1200    0.561    2137.8     0.360

agents=1000, varying workers:
  workers=2    ops/s=2842.6
  workers=8    ops/s=2098.7
  workers=32   ops/s=2984.3
```

Two things fall out of this data:

1. **Throughput does not depend on the number of agents** (flat ~2,000-
   3,000 ops/sec whether there are 100 or 5,000 agents) - each CAS write
   is an indexed `UPDATE` on a single row, so agent count alone is not
   the bottleneck.
2. **Throughput does not meaningfully improve with more worker threads
   either** - it plateaus around ~2,000-3,000 ops/sec regardless of 2,
   8, or 32 concurrent workers, because `BEGIN IMMEDIATE` serializes
   *all* writers on the single SQLite file, no matter how many threads
   are waiting.

So: **the single-writer SQLite file is what breaks first**, not at
10,000 agents specifically, but at whatever total reservation-throughput
the campaign needs once total workers × calls-per-second exceeds
roughly 2-3k ops/sec. At 1,000 agents doing even a modest 1 call/agent/
minute, that's ~17 CAS ops/sec - nowhere close to the ceiling. At 10,000
agents with tighter pacing loops and multiple campaigns sharing
infrastructure, it's a realistic wall.

**The fix, and why it's not "add more servers":** the constraint is a
single writer per SQLite *file*, not per row. The fix is to shard the
write path so unrelated writes stop contending for the same lock:

- **Partition by campaign** (or by agent-id hash) across multiple
  SQLite files / multiple Postgres databases, since agent reservations
  for campaign A never need to serialize against campaign B's. This
  alone removes cross-campaign contention entirely and requires no new
  infrastructure concept - just N stores instead of 1.
- **Move to Postgres for a single large campaign** that itself needs to
  exceed one file's throughput: Postgres's row-level locking (vs
  SQLite's file-level lock) lets independent agent rows commit
  concurrently instead of serializing behind each other, which is the
  actual thing SQLite is giving up at scale.
- **What doesn't need to change:** the CAS *pattern* itself
  (`UPDATE ... WHERE state = ?`) is identical in Postgres - this is a
  storage-engine swap, not an architecture rewrite. `db.py` is the only
  file that would need to change.

The secondary bottleneck, once storage is sharded: each `Worker.tick()`
does a handful of small queries to build a `SystemSnapshot`
(`count_agents_by_state`, metrics aggregation). At 10,000 agents with
many workers polling every 0.5s, this read load becomes non-trivial
before the CAS writes do - the fix there is a cached/pushed snapshot
(e.g. one process aggregates state once per tick and workers read that)
rather than every worker re-aggregating independently, which is a
read-path optimization, not a correctness one.

## 8. What I'd do differently with another week

- Replace the SQLite `run_load_point` per-agent hashing in `load_test.py`
  with a proper closed-loop load generator (fixed request rate rather
  than "as fast as possible") to get latency-under-target-load numbers,
  which are more meaningful than max-throughput numbers for a system
  that should mostly run well below saturation.
- Add a real (free-tier) provider integration behind the same
  `Provider` interface, per the "cherry on the cake" suggestion, to
  validate the interface boundary against a real webhook delivery model
  (retries, signature verification, actual out-of-order behavior)
  instead of only the mocked version of it.
- Partition agents/borrowers by campaign in the schema now, even at
  small scale, so the sharding story in the scale analysis is
  demonstrated rather than just described.
- Add a small Grafana-style live view (even a terminal dashboard) driven
  off `metrics.py` so a reviewer can watch pacing/safety decisions happen
  in real time instead of reading them after the run.
