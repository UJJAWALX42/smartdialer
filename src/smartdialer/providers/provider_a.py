from __future__ import annotations
import random
import threading
import time
import uuid
from collections import deque

from .base import Provider, OnEvent
from ..state_machines import CallState


class ProviderA(Provider):
    """Fast, reliable, low failure rate. Well-behaved: events arrive once,
    in order, with small realistic delays."""

    name = "provider_a"

    def __init__(self, failure_rate: float = 0.03, seed: int | None = None):
        self.failure_rate = failure_rate
        self._rng = random.Random(seed)
        self._outage = False
        self._recent = deque(maxlen=100)  # 1 = error/timeout, 0 = ok

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

    def _emit(self, on_event: OnEvent, call_id: str, state: CallState) -> None:
        on_event(call_id, uuid.uuid4().hex, state)

    def _run(self, call_id: str, on_event: OnEvent) -> None:
        if self._outage:
            self._recent.append(1)
            return  # total outage: no events at all -> relies on lease expiry
        time.sleep(self._rng.uniform(0.05, 0.15))
        self._emit(on_event, call_id, CallState.INITIATED)
        time.sleep(self._rng.uniform(0.1, 0.3))
        self._emit(on_event, call_id, CallState.RINGING)
        time.sleep(self._rng.uniform(0.3, 1.2))

        if self._rng.random() < self.failure_rate:
            self._recent.append(1)
            self._emit(on_event, call_id, CallState.FAILED)
            return

        self._recent.append(0)
        self._emit(on_event, call_id, CallState.ANSWERED)
        time.sleep(self._rng.uniform(0.02, 0.05))
        self._emit(on_event, call_id, CallState.CONNECTED)
        time.sleep(self._rng.uniform(0.3, 1.5))  # simulated talk time (scaled down for demo speed)
        self._emit(on_event, call_id, CallState.COMPLETED)
