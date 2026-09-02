from __future__ import annotations
from dataclasses import dataclass
from enum import Enum

from .models import SystemSnapshot

# Hard limits below are constants, not parameters the pacing engine can
# influence. The predictive engine can only ever *request* a number of
# calls; it has no code path that writes to agent/call state directly,
# and no parameter that raises these ceilings. That separation is the
# actual safety mechanism - not just "the number happens to be small".
MAX_ABANDON_RATE = 0.03          # >3% of connects with no agent -> force fallback
MAX_PROVIDER_ERROR_RATE = 0.25   # provider unhealthy -> reduce aggressiveness
PROVIDER_OUTAGE_ERROR_RATE = 0.6  # provider effectively down -> hard fallback to progressive


class SafetyAction(str, Enum):
    APPROVE = "APPROVE"
    REDUCE = "REDUCE"
    REJECT = "REJECT"
    FALLBACK_PROGRESSIVE = "FALLBACK_PROGRESSIVE"


@dataclass
class SafetyDecision:
    requested: int
    approved: int
    action: SafetyAction
    reason: str


class SafetyController:
    """The only component allowed to authorize how many NEW calls get
    started this tick. It sits structurally between the pacing engines
    (Progressive/Predictive) and the CallAllocator - neither pacing
    engine can call the allocator directly (see worker.py)."""

    def evaluate(self, requested: int, snapshot: SystemSnapshot) -> SafetyDecision:
        reasons = []

        # Hard invariant, always enforced regardless of mode: we can never
        # commit more agent-bound outbound calls than agents that are
        # actually free right now. This is what makes an "abandoned
        # connected call" structurally impossible from our side - a call
        # can only be marked CONNECTED if an agent was reserved for it
        # before we dialed.
        cap = max(0, snapshot.available_agents)

        # Provider outage: don't add more load onto a provider that's
        # failing almost everything. Fall back to progressive-equivalent
        # (1 new call slot per available agent, still capped by `cap`).
        if snapshot.provider_error_rate >= PROVIDER_OUTAGE_ERROR_RATE:
            approved = min(requested, cap)
            return SafetyDecision(requested, approved, SafetyAction.FALLBACK_PROGRESSIVE,
                                   f"provider_error_rate={snapshot.provider_error_rate:.2f} >= "
                                   f"{PROVIDER_OUTAGE_ERROR_RATE}: treating provider as down, "
                                   f"falling back to 1:1 progressive-style pacing")

        # Abandonment protection: if we've recently produced connects with
        # no agent available, something upstream mis-predicted badly.
        # Force conservative behaviour until it recovers.
        if snapshot.recent_abandon_count > 0:
            approved = min(requested, cap)
            return SafetyDecision(requested, approved, SafetyAction.FALLBACK_PROGRESSIVE,
                                   f"recent_abandon_count={snapshot.recent_abandon_count} > 0: "
                                   f"compliance risk detected, forcing progressive fallback")

        if requested <= 0:
            return SafetyDecision(requested, 0, SafetyAction.REJECT, "pacing engine requested 0 calls")

        approved = requested
        if snapshot.provider_error_rate >= MAX_PROVIDER_ERROR_RATE:
            # reduce aggressiveness proportionally instead of an all-or-nothing cut
            factor = max(0.25, 1.0 - snapshot.provider_error_rate)
            approved = int(requested * factor)
            reasons.append(f"provider_error_rate={snapshot.provider_error_rate:.2f} elevated, "
                            f"scaled request by {factor:.2f}")

        if approved > cap:
            reasons.append(f"requested {approved} exceeds available_agents cap {cap}")
            approved = cap

        approved = max(0, approved)

        if approved == 0:
            return SafetyDecision(requested, 0, SafetyAction.REJECT,
                                   "; ".join(reasons) or "no capacity available")
        if approved < requested:
            return SafetyDecision(requested, approved, SafetyAction.REDUCE, "; ".join(reasons))
        return SafetyDecision(requested, approved, SafetyAction.APPROVE,
                               "requested amount fits within safe capacity")
