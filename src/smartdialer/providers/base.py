from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable
from ..state_machines import CallState

OnEvent = Callable[[str, str, CallState], None]  # (call_id, event_id, new_state) -> None


class Provider(ABC):
    """A telecom provider. The dialer only ever talks to this interface -
    it must not need to know provider-specific quirks (retries, timeouts,
    duplicate/out-of-order webhooks all happen 'inside' a provider and
    are exposed to us only as a stream of (call_id, event_id, state)
    events delivered via `on_event`)."""

    name: str

    @abstractmethod
    def start_call(self, call_id: str, phone: str, on_event: OnEvent) -> None:
        """Fire-and-forget: asynchronously begins dialing. Delivers zero or
        more events to `on_event` over time, from a background thread."""
        raise NotImplementedError

    @abstractmethod
    def current_error_rate(self) -> float:
        """Rolling error/timeout rate, used by the Safety Controller to
        detect an unhealthy provider and react (reduce pacing / fail over)."""
        raise NotImplementedError

    @abstractmethod
    def set_outage(self, active: bool) -> None:
        """Test/demo hook to simulate the provider suddenly failing."""
        raise NotImplementedError

    @property
    @abstractmethod
    def in_outage(self) -> bool:
        raise NotImplementedError
