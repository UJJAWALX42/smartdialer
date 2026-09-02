from __future__ import annotations
from .models import SystemSnapshot

MIN_ANSWER_RATE = 0.05  # floor to avoid divide-by-near-zero blowing up the request


class PredictiveEngine:
    """
    Rule-based predictive pacing (no ML - deliberately, per the brief).

    Core idea, in plain terms: we want the number of calls simultaneously
    "in flight" (ringing/dialing) to be roughly enough that, given our
    recent answer rate, the number that actually GET ANSWERED at any
    moment lands near the number of agents we have - not zero (wasted
    capacity) and not way more than agents (abandoned calls).

        desired_in_flight = available_agents / recent_answer_rate

    e.g. 50% answer rate, 20 available agents -> we want ~40 calls in
    flight so that about 20 of them answer around the same time.

    `requested_new = desired_in_flight - calls_currently_ringing_or_dialing`

    This number is a REQUEST, not a command - the Safety Controller
    (safety_controller.py) independently caps it at available_agents
    every single tick regardless of what this formula says, so a bad
    prediction (e.g. answer rate suddenly drops) cannot itself cause an
    abandoned call; it can only cause under-dialing (safe) until the
    rolling average catches up. That's the answer to "your model
    predicted 70%, it dropped to 10%, how does the system protect
    itself?": the protection isn't in this class at all, it's structural,
    one layer down.
    """

    def __init__(self, ewma_alpha: float = 0.3):
        self.ewma_alpha = ewma_alpha
        self._answer_rate_estimate: float | None = None

    def observe_answer_rate(self, latest_sample: float) -> None:
        """Feed in a fresh answer-rate sample (e.g. computed over the last
        N calls) to update the rolling estimate via EWMA, so the engine
        adapts within a few ticks instead of over the whole campaign
        history."""
        if self._answer_rate_estimate is None:
            self._answer_rate_estimate = latest_sample
        else:
            a = self.ewma_alpha
            self._answer_rate_estimate = a * latest_sample + (1 - a) * self._answer_rate_estimate

    def compute_request(self, snapshot: SystemSnapshot) -> tuple[int, str]:
        answer_rate = self._answer_rate_estimate
        if answer_rate is None:
            answer_rate = snapshot.recent_answer_rate
        answer_rate = max(answer_rate, MIN_ANSWER_RATE)

        desired_in_flight = snapshot.available_agents / answer_rate
        requested_new = round(desired_in_flight - snapshot.agents_dialing_or_ringing)
        requested_new = max(0, requested_new)

        reason = (
            f"predictive: available_agents={snapshot.available_agents}, "
            f"answer_rate_estimate={answer_rate:.2f} -> desired_in_flight={desired_in_flight:.1f}, "
            f"already_in_flight={snapshot.agents_dialing_or_ringing} -> requesting {requested_new} new calls"
        )
        return requested_new, reason
