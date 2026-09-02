from smartdialer.safety_controller import SafetyController, SafetyAction
from smartdialer.models import SystemSnapshot
import time


def snap(**overrides) -> SystemSnapshot:
    base = dict(
        campaign_id="c1", available_agents=20, agents_dialing_or_ringing=0, calls_connected=0,
        calls_ringing=0, recent_answer_rate=0.4, recent_avg_talk_time=90, recent_avg_setup_time=3,
        provider_error_rate=0.02, recent_abandon_count=0, timestamp=time.time(),
    )
    base.update(overrides)
    return SystemSnapshot(**base)


def test_approve_when_request_fits_capacity():
    sc = SafetyController()
    d = sc.evaluate(15, snap(available_agents=20))
    assert d.action == SafetyAction.APPROVE
    assert d.approved == 15


def test_reduce_when_request_exceeds_available_agents():
    sc = SafetyController()
    d = sc.evaluate(50, snap(available_agents=20))
    assert d.action == SafetyAction.REDUCE
    assert d.approved == 20, "must never approve more than available agents"


def test_never_exceeds_available_agents_even_for_huge_predictive_request():
    sc = SafetyController()
    d = sc.evaluate(10_000, snap(available_agents=7))
    assert d.approved == 7


def test_reject_when_zero_requested():
    sc = SafetyController()
    d = sc.evaluate(0, snap())
    assert d.action == SafetyAction.REJECT
    assert d.approved == 0


def test_fallback_on_provider_outage():
    sc = SafetyController()
    d = sc.evaluate(30, snap(available_agents=10, provider_error_rate=0.9))
    assert d.action == SafetyAction.FALLBACK_PROGRESSIVE
    assert d.approved == 10


def test_fallback_on_recent_abandonment():
    sc = SafetyController()
    d = sc.evaluate(30, snap(available_agents=10, recent_abandon_count=2))
    assert d.action == SafetyAction.FALLBACK_PROGRESSIVE
    assert d.approved == 10


def test_reduced_pacing_under_elevated_but_not_outage_error_rate():
    sc = SafetyController()
    d = sc.evaluate(20, snap(available_agents=20, provider_error_rate=0.3))
    assert d.action in (SafetyAction.REDUCE,)
    assert d.approved < 20
    assert d.approved > 0
