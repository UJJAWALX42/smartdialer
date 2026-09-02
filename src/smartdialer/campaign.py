from __future__ import annotations
from . import db


def setup_campaign(conn, campaign_id: str, num_agents: int, num_borrowers: int) -> None:
    db.init_db(conn)
    db.seed_agents(conn, campaign_id, num_agents)
    db.seed_borrowers(conn, campaign_id, num_borrowers)
