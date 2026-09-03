# tests/conftest.py
import pytest

from content_intake.pipeline.db import connect, apply_schema


@pytest.fixture
def db_conn():
    conn = connect()
    apply_schema(conn)
    # start every test from a clean slate
    conn.execute("TRUNCATE item_attempts, annotations_cache, items, runs, stub_call_slots CASCADE")
    conn.commit()
    yield conn
    conn.close()
