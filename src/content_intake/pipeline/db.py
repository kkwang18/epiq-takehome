# src/content_intake/pipeline/db.py
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_dsn() -> str:
    return os.environ.get(
        "INTAKE_PG_DSN", "postgresql://intake:intake@localhost:5432/intake"
    )


def connect() -> psycopg.Connection:
    return psycopg.connect(get_dsn(), row_factory=dict_row, autocommit=False)


def apply_schema(conn: psycopg.Connection) -> None:
    conn.execute(_SCHEMA_PATH.read_text())
    conn.commit()


def ensure_slots(conn: psycopg.Connection, capacity: int) -> None:
    conn.execute(
        """
        INSERT INTO stub_call_slots (slot_id)
        SELECT g FROM generate_series(1, %s) AS g
        ON CONFLICT (slot_id) DO NOTHING
        """,
        (capacity,),
    )
    conn.execute("DELETE FROM stub_call_slots WHERE slot_id > %s", (capacity,))
    conn.commit()
