import base64
import uuid
from pathlib import Path

import httpx

from content_intake.pipeline.db import connect, ensure_slots
from content_intake.pipeline.worker import claim_item, process_item


def _insert_item(conn, files_dir: Path, tenant="tenant-a", content=b"hello", extension="txt", role="original", expects_annotation=True, path="p.txt"):
    run_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    (files_dir / path).parent.mkdir(parents=True, exist_ok=True)
    (files_dir / path).write_bytes(content)
    import hashlib
    conn.execute(
        "INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, %s, %s, %s)",
        (run_id, "c1", tenant, str(files_dir)),
    )
    conn.execute(
        """
        INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                            role, expects_annotation, state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
        """,
        (item_id, run_id, tenant, path, extension, len(content), hashlib.sha256(content).hexdigest(), role, expects_annotation),
    )
    conn.commit()
    return item_id


def _fake_success_client():
    def handler(request):
        return httpx.Response(200, json={"sha256": "irrelevant-for-test", "result": 0.5})
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_successful_item_reaches_succeeded_with_annotation(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)
    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, _fake_success_client(), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state, annotation, extracted_text FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "succeeded"
    assert row["annotation"] is not None
    assert row["extracted_text"] == "hello"


def test_empty_content_never_calls_stub(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path, content=b"", expects_annotation=False)

    def handler(request):
        raise AssertionError("stub must not be called for empty_content")

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "empty_content"


def test_decode_failed_never_calls_stub(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    png_bytes = b"\x89PNG\r\n\x1a\nrest"
    item_id = _insert_item(db_conn, tmp_path, content=png_bytes, extension="json", path="p.json", expects_annotation=False)

    def handler(request):
        raise AssertionError("stub must not be called for decode_failed")

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "decode_failed"


def test_annotation_cache_hit_skips_stub_call_and_scopes_by_tenant(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    content = b"shared-bytes"
    import hashlib
    sha = hashlib.sha256(content).hexdigest()
    db_conn.execute(
        "INSERT INTO annotations_cache (tenant, sha256, annotation) VALUES ('tenant-a', %s, %s)",
        (sha, '{"cached": true}'),
    )
    db_conn.commit()

    item_id = _insert_item(db_conn, tmp_path, content=content, tenant="tenant-a")

    def handler(request):
        raise AssertionError("stub must not be called on a cache hit")

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state, annotation FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "succeeded"
    assert row["annotation"]["cached"] is True


def test_cache_is_not_shared_across_tenants(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    content = b"shared-bytes-2"
    import hashlib
    sha = hashlib.sha256(content).hexdigest()
    db_conn.execute(
        "INSERT INTO annotations_cache (tenant, sha256, annotation) VALUES ('tenant-a', %s, %s)",
        (sha, '{"cached": true}'),
    )
    db_conn.commit()

    item_id = _insert_item(db_conn, tmp_path, content=content, tenant="tenant-b", path="p2.txt")
    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, _fake_success_client(), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state, annotation FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "succeeded"
    assert row["annotation"].get("cached") is not True  # got a fresh annotation, not tenant-a's cached one


def test_400_terminates_immediately_as_invalid_request(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(400, json={"error": {"code": "invalid_request"}})

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state, reason FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "annotation_invalid_request"
    assert len(calls) == 1  # no retries


def test_exhausted_retries_lands_in_annotation_failed(db_conn, tmp_path, monkeypatch):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)

    def handler(request):
        return httpx.Response(500, json={"error": {"code": "server_error"}})

    import content_intake.pipeline.worker as worker_mod
    monkeypatch.setattr(worker_mod, "BACKOFF_BASE", 0.001)
    monkeypatch.setattr(worker_mod, "BACKOFF_CAP", 0.002)

    claimed = claim_item(db_conn, "worker-0", lease_seconds=30)
    process_item(db_conn, connect,
                 claimed, tmp_path, httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "annotation_failed"
    attempts = db_conn.execute("SELECT COUNT(*) AS n FROM item_attempts WHERE item_id = %s", (item_id,)).fetchone()
    assert attempts["n"] == 5


def test_start_attempt_race_is_handled_without_crashing(db_conn, tmp_path, monkeypatch):
    # Simulates the D-09 backstop: a stale worker (lease already stolen) reaches
    # start_attempt() concurrently with the new owner and loses the UNIQUE(item_id,
    # attempt_no) race. process_item must not crash or leave the slot held — it should
    # roll back and abandon the item, since the current owner is already handling it.
    import psycopg

    import content_intake.pipeline.worker as worker_mod

    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)
    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)

    def fake_start_attempt(conn, item_id, tenant, worker_id, slot_id):
        raise psycopg.errors.UniqueViolation("simulated race: attempt_no already taken")

    monkeypatch.setattr(worker_mod, "start_attempt", fake_start_attempt)

    def handler(request):
        raise AssertionError("stub must not be called when start_attempt loses the race")

    process_item(db_conn, connect, claimed, tmp_path,
                 httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")

    slots = db_conn.execute("SELECT held_by FROM stub_call_slots ORDER BY slot_id").fetchall()
    assert all(s["held_by"] is None for s in slots)
