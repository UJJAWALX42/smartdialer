from __future__ import annotations
from dataclasses import dataclass, field
from .state_machines import AgentState, CallState


@dataclass
class Agent:
    id: str
    campaign_id: str
    state: AgentState
    version: int
    reserved_by: str | None = None
    lease_expires_at: float | None = None


@dataclass
class Borrower:
    id: str
    campaign_id: str
    phone: str
    state: str  # "QUEUED" | "RESERVED" | "DONE"
    version: int
    attempt_count: int = 0
    reserved_by: str | None = None


@dataclass
class Call:
    id: str
    campaign_id: str
    borrower_id: str
    agent_id: str | None
    provider: str
    state: CallState
    version: int
    idempotency_key: str
    created_at: float
    updated_at: float
    lease_expires_at: float | None = None


@dataclass
class SystemSnapshot:
    """A point-in-time read used by the pacing engines and safety controller.
    Deliberately a plain, cheap aggregate query - not a distributed
    consensus read. Slightly-stale data here is acceptable because the
    Safety Controller's hard limits are enforced again at the point of
    actual reservation (CAS), which is the real source of truth.
    """
    campaign_id: str
    available_agents: int
    agents_dialing_or_ringing: int
    calls_connected: int
    calls_ringing: int
    recent_answer_rate: float
    recent_avg_talk_time: float
    recent_avg_setup_time: float
    provider_error_rate: float
    recent_abandon_count: int
    timestamp: float
