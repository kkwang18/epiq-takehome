# tests/test_worker_slots.py
import time

from content_intake.pipeline.db import ensure_slots
from content_intake.pipeline.worker import claim_slot, release_slot


def test_claim_slot_up_to_capacity(db_conn):
    ensure_slots(db_conn, 2)
    s1 = claim_slot(db_conn, "worker-0", lease_seconds=5)
    s2 = claim_slot(db_conn, "worker-1", lease_seconds=5)
    s3 = claim_slot(db_conn, "worker-2", lease_seconds=5)
    assert s1 is not None and s2 is not None
    assert {s1, s2} == {1, 2}
    assert s3 is None


def test_release_frees_the_slot(db_conn):
    ensure_slots(db_conn, 1)
    s1 = claim_slot(db_conn, "worker-0", lease_seconds=5)
    assert s1 == 1
    release_slot(db_conn, s1, "worker-0")
    s2 = claim_slot(db_conn, "worker-1", lease_seconds=5)
    assert s2 == 1


def test_release_by_wrong_worker_does_not_free_slot(db_conn):
    ensure_slots(db_conn, 1)
    s1 = claim_slot(db_conn, "worker-0", lease_seconds=5)
    release_slot(db_conn, s1, "worker-1")  # not the holder
    s2 = claim_slot(db_conn, "worker-1", lease_seconds=5)
    assert s2 is None


def test_expired_slot_lease_self_heals(db_conn):
    ensure_slots(db_conn, 1)
    claim_slot(db_conn, "worker-0", lease_seconds=0)
    time.sleep(0.05)
    s2 = claim_slot(db_conn, "worker-1", lease_seconds=5)
    assert s2 == 1
