# SmartDialer

A working prototype of a compliance-safe outbound dialer supporting both
**Progressive** and **Predictive** dialing modes, built around a hard
architectural rule: the predictive pacing algorithm can *request* calls,
but only the **Safety Controller** can authorize them, and it cannot be
bypassed.

**[View the results page →](https://ujjawalx42.github.io/smartdialer/)**
— pipeline diagram, state machines, real captured Safety Controller
decisions, simulation output, and load-test numbers, generated entirely
from this repo's own output. It's a static summary, not a hosted
version of the tool: this is a CLI prototype, run locally per the setup
below.

```
Campaign -> Pacing Engine (Progressive | Predictive) -> Safety Controller -> Call Allocator -> Telecom Provider
```

See `ARCHITECTURE.md` for diagrams and `ADR.md` for the reasoning behind
every major decision (language, storage, concurrency model, what would
change at 10,000 agents). This file is just setup + how to run things.

## Requirements

- Python 3.10+
- No external services. Storage is a single SQLite file (see ADR.md for why).

## Setup

```bash
cd smartdialer
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run the tests

```bash
python -m pytest -v
```

26 tests cover: state machine legality, concurrent-reservation races
(the "two workers, one agent" problem), idempotent call creation,
duplicate/out-of-order provider event handling (the exact
`ANSWERED,ANSWERED,ANSWERED,COMPLETED` and `COMPLETED,ANSWERED,RINGING`
sequences from the brief), Safety Controller approve/reduce/reject/
fallback logic, provider outage handling, and worker-crash recovery.

## Run a simulation

```bash
# One scenario:
python simulate.py --scenario B --mode predictive --agents 20 --borrowers 300 \
    --provider a --seconds 15 --workers 3

# All 4 scenarios (A/B/C/D) x both modes, for comparison:
python simulate.py --all --agents 20 --borrowers 300 --provider mixed --seconds 15
```

Scenarios, matching the brief:

| Scenario | Answer rate | Avg talk time |
|---|---|---|
| A | 20% | 120s |
| B | 50% | 90s |
| C | 70% | 180s |
| D | drifts over time | drifts over time |

Output: a results table printed to stdout, a sample of Safety Controller
decisions with their reasons, a CSV (`run_artifacts/sim_results.csv`),
and a utilization-vs-connected-calls chart
(`run_artifacts/sim_chart.png`).

`--provider a` uses the fast/reliable mock; `--provider b` uses the
slow one with timeouts, duplicate events, and out-of-order events;
`--provider mixed` randomizes per run.

## Run the load test

```bash
python load_test.py --agents 100 1000 10000 --workers 8 --ops-per-worker 200
```

Measures throughput/latency of the one operation every worker performs
per call attempt - the CAS agent reservation - as the agent pool grows.
This is what the "100 -> 1,000 -> 10,000 agents, what breaks first"
answer in ADR.md is based on, not a guess.

## Project layout

```
src/smartdialer/
  state_machines.py     Agent + Call state machines (explicit transition graphs)
  models.py              Agent, Borrower, Call, SystemSnapshot dataclasses
  db.py                  SQLite storage layer: CAS reservations, idempotent
                          call creation, idempotent/out-of-order-safe event application
  providers/
    base.py               Provider interface
    provider_a.py         Mock: fast, reliable, low failure rate
    provider_b.py         Mock: slow, timeouts, duplicate + out-of-order events
  call_allocator.py       Talks to the Provider; normalizes events into state transitions
  safety_controller.py    Approve / reduce / reject / fallback-to-progressive
  progressive_dialer.py   Progressive pacing engine
  predictive_engine.py    Predictive pacing engine (rule-based, explainable)
  worker.py               One dialer worker; run N of these concurrently
  reconciliation.py       Lease-expiry sweep -> crash recovery
  metrics.py              Counters + decision log used by the simulator
  campaign.py             Seeding helper

tests/                    26 tests (state machines, concurrency races,
                           idempotency, out-of-order events, safety
                           controller, provider failure, crash recovery)
simulate.py                Scenario simulator (A/B/C/D x progressive/predictive)
load_test.py                Load test / scale analysis
ARCHITECTURE.md             Diagrams: pipeline, state machines, sequence
                             diagrams for the race and crash-recovery cases
ADR.md                      Architecture decisions + scale analysis
```

## The short answer to the brief's closing question

> How would you build a SmartDialer that gets as much of the utilization
> benefit of predictive dialing as possible, while retaining the
> deterministic safety characteristics of progressive dialing?

Don't build two dialers - build one reservation primitive and two
*pacing* strategies that both feed it. Progressive dialing's safety
comes entirely from one invariant: **a call is never allowed to reach
CONNECTED unless an agent was reserved for it before the call was
placed.** That invariant doesn't care how many calls you *start* per
tick, only that starting a call and reserving an agent are the same
atomic decision. So: let the predictive engine be as aggressive as it
wants about *how many calls to request*; run every one of those
requests through a Safety Controller that enforces "requested calls
this tick <= agents actually free right now" as a hard, unconditional
ceiling, with no parameter the predictive engine can raise. Predictive
dialing then only ever wins you the *idle time between one agent
finishing a call and the next borrower answering* - which is exactly
the utilization gap progressive dialing leaves on the table - without
ever being allowed to create a connected call with nowhere to route it.
Get the prediction wrong, and the system just under-dials for a few
ticks (safe, wasteful) instead of over-connecting (unsafe, a compliance
event). The full reasoning and the code that implements this are in
`safety_controller.py` and `ADR.md`.
