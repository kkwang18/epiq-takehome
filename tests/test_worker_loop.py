# tests/test_worker_loop.py
"""run_worker's outer loop: one bad item must not take the worker process down."""
import time
import uuid

import pytest

import content_intake.pipeline.worker as worker_mod


class _StopLoop(BaseException):
    """Escapes run_worker's `except Exception` so the test can end the infinite loop."""


def _insert_item(conn, tmp_path, order_index=0):
    run_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, %s, %s, %s)",
        (run_id, "c1", "tenant-a", str(tmp_path)),
    )
    conn.execute(
        """
        INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                            role, order_index, expects_annotation, state)
        VALUES (%s, %s, 'tenant-a', 'p', 'txt', 5, 'abc', 'original', %s, true, 'pending')
        """,
        (item_id, run_id, order_index),
    )
    conn.commit()
    return item_id


def _connection_count(conn) -> int:
    return conn.execute(
        "SELECT count(*) AS n FROM pg_stat_activity WHERE datname = current_database()"
    ).fetchone()["n"]


def test_run_worker_survives_an_exception_from_process_item(db_conn, tmp_path, monkeypatch):
    # Two claimable items. process_item blows up on the first one; the loop must go on to
    # claim the second rather than letting the exception kill the worker process. Without
    # this, a deterministically-failing item is a poison pill that kills every worker in
    # turn as each reclaims it after the previous one's lease expires.
    first_id = _insert_item(db_conn, tmp_path, order_index=0)
    _insert_item(db_conn, tmp_path, order_index=1)

    seen = []

    def boom(conn, connect_fn, item, *args, **kwargs):
        seen.append(item["item_id"])
        if len(seen) == 1:
            raise RuntimeError("simulated poison item")
        raise _StopLoop  # end the otherwise-infinite loop, without being caught

    monkeypatch.setattr(worker_mod, "process_item", boom)

    baseline_connections = _connection_count(db_conn)

    with pytest.raises(_StopLoop):
        worker_mod.run_worker(0)

    assert len(seen) == 2, "the loop stopped after the first exception instead of continuing"
    assert seen[0] == first_id
    assert seen[1] != first_id, "second iteration should have claimed the other item"

    # And the per-iteration connection was closed, not leaked, on the iteration that raised.
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and _connection_count(db_conn) > baseline_connections:
        time.sleep(0.1)
    assert _connection_count(db_conn) == baseline_connections
