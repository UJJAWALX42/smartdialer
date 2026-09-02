from smartdialer.state_machines import (
    AgentState, CallState, is_legal_agent_transition, is_legal_call_transition,
    call_event_is_forward_progress,
)


def test_agent_legal_transitions():
    assert is_legal_agent_transition(AgentState.AVAILABLE, AgentState.RESERVED)
    assert is_legal_agent_transition(AgentState.RESERVED, AgentState.DIALING)
    assert is_legal_agent_transition(AgentState.DIALING, AgentState.CONNECTED)
    assert is_legal_agent_transition(AgentState.WRAP_UP, AgentState.AVAILABLE)


def test_agent_illegal_transitions():
    assert not is_legal_agent_transition(AgentState.OFFLINE, AgentState.RESERVED)
    assert not is_legal_agent_transition(AgentState.AVAILABLE, AgentState.CONNECTED)
    assert not is_legal_agent_transition(AgentState.CONNECTED, AgentState.AVAILABLE)  # must pass through WRAP_UP


def test_call_legal_transitions():
    assert is_legal_call_transition(CallState.QUEUED, CallState.RESERVED)
    assert is_legal_call_transition(CallState.RINGING, CallState.ANSWERED)
    assert is_legal_call_transition(CallState.CONNECTED, CallState.COMPLETED)


def test_call_terminal_states_have_no_outgoing_transitions():
    for terminal in (CallState.COMPLETED, CallState.FAILED, CallState.CANCELLED):
        assert not is_legal_call_transition(terminal, CallState.RINGING)
        assert not is_legal_call_transition(terminal, CallState.QUEUED)


def test_forward_progress_guard_allows_normal_sequence():
    assert call_event_is_forward_progress(CallState.QUEUED, CallState.RESERVED)
    assert call_event_is_forward_progress(CallState.RINGING, CallState.ANSWERED)


def test_forward_progress_guard_rejects_stale_events():
    # duplicate ANSWERED after already ANSWERED
    assert not call_event_is_forward_progress(CallState.ANSWERED, CallState.ANSWERED)
    # RINGING arriving after we already saw ANSWERED (out of order)
    assert not call_event_is_forward_progress(CallState.ANSWERED, CallState.RINGING)


def test_forward_progress_guard_locks_terminal_states():
    assert not call_event_is_forward_progress(CallState.COMPLETED, CallState.ANSWERED)
    assert not call_event_is_forward_progress(CallState.COMPLETED, CallState.RINGING)
    assert not call_event_is_forward_progress(CallState.FAILED, CallState.CONNECTED)
