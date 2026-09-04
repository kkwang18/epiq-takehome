# tests/test_scenario_fault.py
import time
import uuid

from content_intake.scenario.fault import (
    find_leasing_worker,
    worker_index_from_id,
    wait_for_item_terminal,
)


def _insert_run_and_item(conn, tmp_path, state="pending", leased_by=None):
    run_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, 'c1', 'tenant-a', %s)",
        (run_id, str(tmp_path)),
    )
    conn.execute(
        """
        INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                            role, order_index, expects_annotation, state, leased_by)
        VALUES (%s, %s, 'tenant-a', 'p', 'txt', 5, 'abc', 'original', 0, true, %s, %s)
        """,
        (item_id, run_id, state, leased_by),
    )
    conn.commit()
    return run_id, item_id


def test_find_leasing_worker_returns_none_when_nothing_in_progress(db_conn, tmp_path):
    _insert_run_and_item(db_conn, tmp_path, state="pending")
    assert find_leasing_worker(db_conn) is None


def test_find_leasing_worker_returns_a_worker_and_item(db_conn, tmp_path):
    run_id, item_id = _insert_run_and_item(db_conn, tmp_path, state="in_progress", leased_by="worker-2")
    result = find_leasing_worker(db_conn)
    assert result is not None
    assert result["leased_by"] == "worker-2"
    assert str(result["item_id"]) == item_id


def test_worker_index_from_id():
    assert worker_index_from_id("worker-0") == 0
    assert worker_index_from_id("worker-3") == 3


def test_wait_for_item_terminal_returns_time_once_terminal(db_conn, tmp_path):
    run_id, item_id = _insert_run_and_item(db_conn, tmp_path, state="in_progress", leased_by="worker-0")

    import threading

    def finalize_after_delay():
        time.sleep(0.3)
        from content_intake.pipeline.db import connect
        c = connect()
        c.execute("UPDATE items SET state = 'succeeded' WHERE item_id = %s", (item_id,))
        c.commit()
        c.close()

    t = threading.Thread(target=finalize_after_delay)
    t.start()
    recovered_at = wait_for_item_terminal(db_conn, item_id, timeout=2.0, interval=0.1)
    t.join()
    assert recovered_at is not None


def test_wait_for_item_terminal_returns_none_on_timeout(db_conn, tmp_path):
    run_id, item_id = _insert_run_and_item(db_conn, tmp_path, state="in_progress", leased_by="worker-0")
    result = wait_for_item_terminal(db_conn, item_id, timeout=0.3, interval=0.1)
    assert result is None
