# tests/test_worker_claim.py
import time
import uuid

from content_intake.pipeline.db import connect
from content_intake.pipeline.worker import claim_item, LeaseRenewer


def _insert_run_and_item(conn, tmp_path, tenant="tenant-a"):
    run_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, %s, %s, %s)",
        (run_id, "c1", tenant, str(tmp_path)),
    )
    conn.execute(
        """
        INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                            role, order_index, expects_annotation, state)
        VALUES (%s, %s, %s, 'p', 'txt', 5, 'abc', 'original', 0, true, 'pending')
        """,
        (item_id, run_id, tenant),
    )
    conn.commit()
    return run_id, item_id


def test_claim_returns_pending_item(db_conn, tmp_path):
    run_id, item_id = _insert_run_and_item(db_conn, tmp_path)
    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    assert claimed["item_id"] == item_id
    assert claimed["state"] == "in_progress"
    assert claimed["leased_by"] == "worker-0"


def test_claim_returns_none_when_nothing_pending(db_conn):
    assert claim_item(db_conn, "worker-0", lease_seconds=5) is None


def test_two_workers_never_claim_the_same_item(db_conn, tmp_path):
    _insert_run_and_item(db_conn, tmp_path)
    c1 = claim_item(db_conn, "worker-0", lease_seconds=5)
    c2 = claim_item(db_conn, "worker-1", lease_seconds=5)
    assert c1 is not None
    assert c2 is None  # only one item existed


def test_expired_lease_becomes_reclaimable(db_conn, tmp_path):
    _run_id, item_id = _insert_run_and_item(db_conn, tmp_path)
    claim_item(db_conn, "worker-0", lease_seconds=0)  # expires immediately
    time.sleep(0.05)
    reclaimed = claim_item(db_conn, "worker-1", lease_seconds=5)
    assert reclaimed["item_id"] == item_id
    assert reclaimed["leased_by"] == "worker-1"


def test_lease_renewer_renews_while_running(db_conn, tmp_path):
    _run_id, item_id = _insert_run_and_item(db_conn, tmp_path)
    claim_item(db_conn, "worker-0", lease_seconds=1)
    renewer = LeaseRenewer(connect, item_id, "worker-0", lease_seconds=1, interval=1)
    renewer.start()
    time.sleep(2.2)
    assert renewer.lost() is False
    row = db_conn.execute("SELECT leased_until FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["leased_until"] is not None
    renewer.stop()


def test_lease_renewer_detects_stolen_lease(db_conn, tmp_path):
    _run_id, item_id = _insert_run_and_item(db_conn, tmp_path)
    claim_item(db_conn, "worker-0", lease_seconds=1)
    renewer = LeaseRenewer(connect, item_id, "worker-0", lease_seconds=1, interval=1)
    renewer.start()
    # simulate the original worker's lease expiring and another worker stealing it
    time.sleep(1.2)
    db_conn.execute(
        "UPDATE items SET leased_by = 'worker-1', leased_until = now() + interval '5 seconds' WHERE item_id = %s",
        (item_id,),
    )
    db_conn.commit()
    time.sleep(1.5)
    assert renewer.lost() is True
    renewer.stop()
