from __future__ import annotations
from .models import SystemSnapshot


class ProgressiveDialer:
    """Progressive mode: the pacing decision is always exactly
    'available_agents' - one outbound call per free agent, never more.
    All the actual safety/reservation guarantees (an agent can't be
    double-booked, a vanished agent can't be dialed for) live in
    db.reserve_agent (CAS) and worker.py's reserve-then-dial sequencing,
    not here. This class's only job is the pacing NUMBER."""

    def compute_request(self, snapshot: SystemSnapshot) -> tuple[int, str]:
        n = snapshot.available_agents
        reason = f"progressive: 1 call per available agent ({snapshot.available_agents} available)"
        return n, reason
