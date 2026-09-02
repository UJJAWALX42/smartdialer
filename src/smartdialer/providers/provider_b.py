from __future__ import annotations
import random
import threading
import time
import uuid
from collections import deque

from .base import Provider, OnEvent
from ..state_machines import CallState


class ProviderB(Provider):
    """Slower and messier on purpose:
    - higher/variable failure rate
    - some calls simply time out (no terminal event ever arrives - the
      lease-expiry reconciliation sweep is what cleans these up)
    - some events are delivered twice (duplicate webhook delivery)
    - some events are delivered out of order (network reordering)

    The dialer code is never told any of this - it only sees
    (call_id, event_id, state) events via `on_event`, same as Provider A.
    """

    name = "provider_b"

    def __init__(self, failure_rate: float = 0.12, timeout_rate: float = 0.08,
                 duplicate_rate: float = 0.15, reorder_rate: float = 0.15, seed: int | None = None):
        self.failure_rate = failure_rate
        self.timeout_rate = timeout_rate
        self.duplicate_rate = duplicate_rate
        self.reorder_rate = reorder_rate
        self._rng = random.Random(seed)
        self._outage = False
        self._recent = deque(maxlen=100)

    def set_outage(self, active: bool) -> None:
        self._outage = active

    @property
    def in_outage(self) -> bool:
        return self._outage

    def current_error_rate(self) -> float:
        if not self._recent:
            return 0.0
        return sum(self._recent) / len(self._recent)

    def start_call(self, call_id: str, phone: str, on_event: OnEvent) -> None:
        t = threading.Thread(target=self._run, args=(call_id, on_event), daemon=True)
        t.start()

    def _emit(self, on_event: OnEvent, call_id: str, state: CallState, event_id: str | None = None) -> None:
        on_event(call_id, event_id or uuid.uuid4().hex, state)
        if self._rng.random() < self.duplicate_rate:
            # re-deliver the SAME event_id -> exercises dedupe-by-event_id
            time.sleep(self._rng.uniform(0.01, 0.2))
            on_event(call_id, event_id or uuid.uuid4().hex, state)

    def _run(self, call_id: str, on_event: OnEvent) -> None:
        if self._outage or self._rng.random() < self.timeout_rate:
            self._recent.append(1)
            return  # simulated timeout: nothing ever arrives

        time.sleep(self._rng.uniform(0.2, 0.8))
        seq = [CallState.INITIATED, CallState.RINGING]

        if self._rng.random() < self.failure_rate:
            self._recent.append(1)
            seq.append(CallState.FAILED)
        else:
            self._recent.append(0)
            seq += [CallState.ANSWERED, CallState.CONNECTED, CallState.COMPLETED]

        # Occasionally reorder two adjacent non-terminal events to simulate
        # network reordering (e.g. RINGING arriving after ANSWERED).
        if len(seq) >= 3 and self._rng.random() < self.reorder_rate:
            i = self._rng.randrange(0, len(seq) - 2)
            seq[i], seq[i + 1] = seq[i + 1], seq[i]

        for state in seq:
            time.sleep(self._rng.uniform(0.3, 1.8))
            self._emit(on_event, call_id, state)
