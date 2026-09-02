from __future__ import annotations
import time

from . import db
from .state_machines import AgentState, CallState
from .providers.base import Provider
from .metrics import Metrics


class CallAllocator:
    """The only component that talks to a Provider. Everything above it
    (Progressive Dialer, Predictive Engine, Safety Controller) only ever
    deals with abstract 'start N calls' decisions; this is where a
    decision becomes an actual outbound call attempt, and where provider
    events flow back in and get normalized into call/agent state
    transitions. Provider-specific behavior (retries, timeouts, dup/
    out-of-order events) never leaks above this layer."""

    def __init__(self, db_path: str, provider: Provider, metrics: Metrics):
        self.db_path = db_path
        self.provider = provider
        self.metrics = metrics

    def place_call(self, campaign_id: str, borrower_id: str, agent_id: str, phone: str,
                    idempotency_key: str) -> str:
        conn = db.connect(self.db_path)
        try:
            call_id = db.create_call(conn, campaign_id, borrower_id, agent_id,
                                      self.provider.name, idempotency_key)
        finally:
            conn.close()

        db.advance_agent(db.connect(self.db_path), agent_id, AgentState.DIALING, lease_seconds=30.0)
        self.metrics.incr("calls_initiated")

        def on_event(cid: str, event_id: str, state: CallState) -> None:
            self._handle_event(campaign_id, cid, event_id, state, agent_id, borrower_id)

        self.provider.start_call(call_id, phone, on_event)
        return call_id

    def _handle_event(self, campaign_id: str, call_id: str, event_id: str, state: CallState,
                       agent_id: str, borrower_id: str) -> None:
        conn = db.connect(self.db_path)
        try:
            applied_state = db.apply_call_event(conn, call_id, event_id, state)
            if applied_state != state:
                # event was a duplicate/out-of-order no-op
                self.metrics.incr("events_ignored_duplicate_or_stale")
                return

            self.metrics.incr(f"call_state_{applied_state.value.lower()}")

            if applied_state == CallState.CONNECTED:
                db.advance_agent(conn, agent_id, AgentState.CONNECTED)
                self.metrics.incr("calls_connected")

            elif applied_state == CallState.COMPLETED:
                db.advance_agent(conn, agent_id, AgentState.WRAP_UP, lease_seconds=5.0)
                db.complete_borrower(conn, borrower_id)
                self.metrics.incr("calls_completed")
                # short wrap-up, then back to available
                db.advance_agent(conn, agent_id, AgentState.AVAILABLE)

            elif applied_state in (CallState.FAILED, CallState.CANCELLED):
                db.release_agent(conn, agent_id)
                db.requeue_borrower(conn, borrower_id)
                self.metrics.incr("calls_failed")
        finally:
            conn.close()
