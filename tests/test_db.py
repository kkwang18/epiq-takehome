# tests/test_db.py
from content_intake.pipeline.db import ensure_slots


def test_ensure_slots_creates_exact_count(db_conn):
    ensure_slots(db_conn, 2)
    rows = db_conn.execute("SELECT slot_id FROM stub_call_slots ORDER BY slot_id").fetchall()
    assert [r["slot_id"] for r in rows] == [1, 2]


def test_ensure_slots_shrinks_when_capacity_reduced(db_conn):
    ensure_slots(db_conn, 3)
    ensure_slots(db_conn, 1)
    rows = db_conn.execute("SELECT slot_id FROM stub_call_slots ORDER BY slot_id").fetchall()
    assert [r["slot_id"] for r in rows] == [1]


def test_schema_tables_exist(db_conn):
    tables = db_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    ).fetchall()
    names = {r["table_name"] for r in tables}
    assert {"runs", "items", "annotations_cache", "stub_call_slots", "item_attempts"} <= names
