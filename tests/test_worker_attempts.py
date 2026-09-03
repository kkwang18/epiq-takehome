# tests/test_worker_attempts.py
import uuid

import pytest
import psycopg

from content_intake.pipeline.worker import start_attempt, complete_attempt, find_completed_success


def _insert_item(conn, tmp_path, tenant="tenant-a"):
    run_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, %s, %s, %s)",
        (run_id, "c1", tenant, str(tmp_path)),
    )
    conn.execute(
        """
        INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                            role, expects_annotation, state)
        VALUES (%s, %s, %s, 'p', 'txt', 5, 'abc', 'original', true, 'in_progress')
        """,
        (item_id, run_id, tenant),
    )
    conn.commit()
    return item_id


def test_start_attempt_commits_before_returning(db_conn, tmp_path):
    item_id = _insert_item(db_conn, tmp_path)
    attempt_id, attempt_no = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    assert attempt_no == 1
    row = db_conn.execute("SELECT completed_at FROM item_attempts WHERE attempt_id = %s", (attempt_id,)).fetchone()
    assert row is not None
    assert row["completed_at"] is None


def test_attempt_numbers_increment_per_item(db_conn, tmp_path):
    item_id = _insert_item(db_conn, tmp_path)
    _, n1 = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    _, n2 = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    assert (n1, n2) == (1, 2)


def test_complete_attempt_records_outcome(db_conn, tmp_path):
    item_id = _insert_item(db_conn, tmp_path)
    attempt_id, _ = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    complete_attempt(db_conn, attempt_id, http_status=200, outcome="success")
    row = db_conn.execute(
        "SELECT completed_at, http_status, outcome FROM item_attempts WHERE attempt_id = %s", (attempt_id,)
    ).fetchone()
    assert row["completed_at"] is not None
    assert row["http_status"] == 200
    assert row["outcome"] == "success"


def test_find_completed_success_returns_none_when_absent(db_conn, tmp_path):
    item_id = _insert_item(db_conn, tmp_path)
    assert find_completed_success(db_conn, item_id) is None


def test_find_completed_success_returns_the_successful_attempt(db_conn, tmp_path):
    item_id = _insert_item(db_conn, tmp_path)
    attempt_id, _ = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    complete_attempt(db_conn, attempt_id, http_status=200, outcome="success")
    found = find_completed_success(db_conn, item_id)
    assert found["attempt_id"] == attempt_id


def test_find_completed_success_ignores_failed_attempts(db_conn, tmp_path):
    item_id = _insert_item(db_conn, tmp_path)
    attempt_id, _ = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    complete_attempt(db_conn, attempt_id, http_status=500, outcome="server_error")
    assert find_completed_success(db_conn, item_id) is None


def test_unique_constraint_prevents_duplicate_attempt_numbers(db_conn, tmp_path):
    item_id = _insert_item(db_conn, tmp_path)
    db_conn.execute(
        "INSERT INTO item_attempts (attempt_id, item_id, tenant, attempt_no, started_at, worker_id, slot_id) "
        "VALUES (%s, %s, 'tenant-a', 1, now(), 'worker-0', 1)",
        (str(uuid.uuid4()), item_id),
    )
    db_conn.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        db_conn.execute(
            "INSERT INTO item_attempts (attempt_id, item_id, tenant, attempt_no, started_at, worker_id, slot_id) "
            "VALUES (%s, %s, 'tenant-a', 1, now(), 'worker-1', 2)",
            (str(uuid.uuid4()), item_id),
        )
    db_conn.rollback()
