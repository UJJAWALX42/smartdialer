"""
Explicit state machines for Agent and Call lifecycles.

Two layers are used deliberately:

1. A legal-transition graph (`*_TRANSITIONS`) that documents which state
   changes the SYSTEM is allowed to *request*. This is the "textbook"
   state machine and is what you'd show in a design review.

2. A monotonic rank (`*_RANK`) + terminal-state guard, used only when
   applying events that originate OUTSIDE our control (telecom provider
   webhooks). External systems do not respect our transition graph -
   they duplicate events, reorder them, and sometimes skip states
   entirely. The rank guard answers a simpler, more robust question:
   "does this incoming event move the call forward, and have we already
   reached a terminal state?" That is what makes duplicate/out-of-order
   provider events a non-issue (see call_allocator.apply_provider_event).
"""
from __future__ import annotations
from enum import Enum


class AgentState(str, Enum):
    OFFLINE = "OFFLINE"
    AVAILABLE = "AVAILABLE"
    RESERVED = "RESERVED"
    DIALING = "DIALING"
    CONNECTED = "CONNECTED"
    WRAP_UP = "WRAP_UP"
    PAUSED = "PAUSED"


class CallState(str, Enum):
    QUEUED = "QUEUED"
    RESERVED = "RESERVED"
    INITIATED = "INITIATED"
    RINGING = "RINGING"
    ANSWERED = "ANSWERED"
    CONNECTED = "CONNECTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


# ---------------------------------------------------------------------------
# Agent transition graph
# ---------------------------------------------------------------------------
AGENT_TRANSITIONS: dict[AgentState, set[AgentState]] = {
    AgentState.OFFLINE: {AgentState.AVAILABLE},
    AgentState.AVAILABLE: {AgentState.RESERVED, AgentState.PAUSED, AgentState.OFFLINE},
    AgentState.RESERVED: {AgentState.DIALING, AgentState.AVAILABLE},  # AVAILABLE = release on failed setup
    AgentState.DIALING: {AgentState.CONNECTED, AgentState.AVAILABLE},  # AVAILABLE = call failed before connect
    AgentState.CONNECTED: {AgentState.WRAP_UP},
    AgentState.WRAP_UP: {AgentState.AVAILABLE, AgentState.PAUSED, AgentState.OFFLINE},
    AgentState.PAUSED: {AgentState.AVAILABLE, AgentState.OFFLINE},
}

AGENT_TERMINAL_STATES: set[AgentState] = set()  # agents cycle forever; nothing is terminal

# ---------------------------------------------------------------------------
# Call transition graph
# ---------------------------------------------------------------------------
CALL_TRANSITIONS: dict[CallState, set[CallState]] = {
    CallState.QUEUED: {CallState.RESERVED, CallState.CANCELLED},
    CallState.RESERVED: {CallState.INITIATED, CallState.CANCELLED, CallState.FAILED},
    CallState.INITIATED: {CallState.RINGING, CallState.FAILED},
    CallState.RINGING: {CallState.ANSWERED, CallState.FAILED},
    CallState.ANSWERED: {CallState.CONNECTED, CallState.FAILED},
    CallState.CONNECTED: {CallState.COMPLETED},
    CallState.COMPLETED: set(),
    CallState.FAILED: set(),
    CallState.CANCELLED: set(),
}

CALL_TERMINAL_STATES: set[CallState] = {CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED}

# Monotonic rank used ONLY for classifying inbound provider events as
# stale/duplicate/out-of-order. Terminal states are handled separately
# via CALL_TERMINAL_STATES (a terminal state is always "final" regardless
# of rank comparisons).
CALL_RANK: dict[CallState, int] = {
    CallState.QUEUED: 0,
    CallState.RESERVED: 1,
    CallState.INITIATED: 2,
    CallState.RINGING: 3,
    CallState.ANSWERED: 4,
    CallState.CONNECTED: 5,
    CallState.COMPLETED: 6,
    CallState.FAILED: 6,
    CallState.CANCELLED: 6,
}


def is_legal_agent_transition(src: AgentState, dst: AgentState) -> bool:
    return dst in AGENT_TRANSITIONS.get(src, set())


def is_legal_call_transition(src: CallState, dst: CallState) -> bool:
    return dst in CALL_TRANSITIONS.get(src, set())


def call_event_is_forward_progress(current: CallState, incoming: CallState) -> bool:
    """
    True if applying `incoming` on top of `current` represents forward
    progress and should be applied. False means: duplicate, stale, or
    arrived-after-terminal - drop it (idempotent no-op).
    """
    if current in CALL_TERMINAL_STATES:
        return False
    return CALL_RANK[incoming] > CALL_RANK[current] or (
        CALL_RANK[incoming] == CALL_RANK[current] and incoming in CALL_TERMINAL_STATES
        and current not in CALL_TERMINAL_STATES
    )
