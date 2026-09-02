import os
import tempfile
import pytest

from smartdialer import db as dbmod
from smartdialer.campaign import setup_campaign


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "test.db")
    conn = dbmod.connect(path)
    dbmod.init_db(conn)
    conn.close()
    return path


@pytest.fixture
def campaign(db_path):
    conn = dbmod.connect(db_path)
    setup_campaign(conn, "camp-1", num_agents=5, num_borrowers=20)
    conn.close()
    return "camp-1"
