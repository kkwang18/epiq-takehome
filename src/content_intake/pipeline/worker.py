# src/content_intake/pipeline/worker.py
import json
import random
import sys
import threading
import time
import traceback
import uuid
from pathlib import Path

import httpx
import psycopg

from content_intake.common import config
from content_intake.pipeline.db import connect
from content_intake.pipeline.extraction import detect_edge_case, extract_text
from content_intake.pipeline.stub_client import call_annotate

BACKOFF_BASE = config.BACKOFF_BASE
BACKOFF_CAP = config.BACKOFF_CAP
BACKOFF_JITTER = config.BACKOFF_JITTER
MAX_ATTEMPTS = config.MAX_ATTEMPTS
HTTP_TIMEOUT_SECONDS = config.HTTP_TIMEOUT_SECONDS
RETRYABLE_STATUSES = {500, 429}


def claim_item(conn, worker_id: str, lease_seconds: int) -> dict | None:
    row = conn.execute(
        """
        UPDATE items
        SET state = 'in_progress', leased_by = %(worker_id)s,
            leased_until = now() + %(lease_seconds)s * interval '1 second'
        WHERE item_id = (
            SELECT item_id FROM items
            WHERE (state = 'pending' AND (next_attempt_after IS NULL OR next_attempt_after <= now()))
               OR (state = 'in_progress' AND leased_until < now())
            -- order_index first (D-15): every item in a run shares one created_at (submit_run's
            -- single transaction), so created_at-first would let one run's whole backlog drain
            -- before a concurrently-submitted second run is ever touched. order_index starts at
            -- 0 independently per run, so sorting by it first interleaves runs fairly ("position
            -- 0 from whichever run has one, then position 1, ..."), while created_at still
            -- breaks ties and order_index still keeps originals ahead of their duplicates within
            -- one run (D-02) exactly as before. next_attempt_after is D-05's backoff timer: a
            -- pending item that just failed a retryable attempt isn't reclaimable until it passes.
            ORDER BY order_index, created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        """,
        {"worker_id": worker_id, "lease_seconds": lease_seconds},
    ).fetchone()
    conn.commit()
    if row is None:
        return None
    # psycopg3 loads uuid columns as uuid.UUID objects; normalize to str so
    # callers (and equality checks against str ids) see the same id type
    # that's used everywhere else in this codebase.
    return {k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in row.items()}


class LeaseRenewer:
    def __init__(self, connect_fn, item_id: str, worker_id: str, lease_seconds: int, interval: int):
        self._connect_fn = connect_fn
        self._item_id = item_id
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._interval = interval
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval + 2)

    def lost(self) -> bool:
        return self._lost.is_set()

    def _run(self) -> None:
        conn = self._connect_fn()
        try:
            while not self._stop.wait(self._interval):
                cur = conn.execute(
                    """
                    UPDATE items SET leased_until = now() + %(s)s * interval '1 second'
                    WHERE item_id = %(item_id)s AND leased_by = %(worker_id)s AND state = 'in_progress'
                    """,
                    {"s": self._lease_seconds, "item_id": self._item_id, "worker_id": self._worker_id},
                )
                conn.commit()
                if cur.rowcount == 0:
                    self._lost.set()
                    return
        finally:
            conn.close()


def claim_slot(conn, worker_id: str, lease_seconds: int) -> int | None:
    row = conn.execute(
        """
        UPDATE stub_call_slots
        SET held_by = %(worker_id)s, lease_until = now() + %(s)s * interval '1 second'
        WHERE slot_id = (
            SELECT slot_id FROM stub_call_slots
            WHERE held_by IS NULL OR lease_until < now()
            ORDER BY slot_id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING slot_id
        """,
        {"worker_id": worker_id, "s": lease_seconds},
    ).fetchone()
    conn.commit()
    return row["slot_id"] if row else None


def release_slot(conn, slot_id: int, worker_id: str) -> None:
    conn.execute(
        "UPDATE stub_call_slots SET held_by = NULL, lease_until = NULL WHERE slot_id = %s AND held_by = %s",
        (slot_id, worker_id),
    )
    conn.commit()


def start_attempt(conn, item_id: str, tenant: str, worker_id: str, slot_id: int) -> tuple[str, int]:
    row = conn.execute(
        "SELECT COALESCE(MAX(attempt_no), 0) + 1 AS n FROM item_attempts WHERE item_id = %s", (item_id,)
    ).fetchone()
    attempt_no = row["n"]
    attempt_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO item_attempts (attempt_id, item_id, tenant, attempt_no, started_at, worker_id, slot_id)
        VALUES (%s, %s, %s, %s, now(), %s, %s)
        """,
        (attempt_id, item_id, tenant, attempt_no, worker_id, slot_id),
    )
    conn.commit()
    return attempt_id, attempt_no


def complete_attempt(conn, attempt_id: str, http_status: int | None, outcome: str) -> None:
    conn.execute(
        "UPDATE item_attempts SET completed_at = now(), http_status = %s, outcome = %s WHERE attempt_id = %s",
        (http_status, outcome, attempt_id),
    )
    conn.commit()


def find_completed_success(conn, item_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM item_attempts WHERE item_id = %s AND outcome = 'success' AND completed_at IS NOT NULL "
        "ORDER BY attempt_no DESC LIMIT 1",
        (item_id,),
    ).fetchone()
    if row is not None:
        # psycopg3 loads uuid columns as uuid.UUID objects; normalize to str so
        # callers (and equality checks against str ids) see the same id type
        # that's used everywhere else in this codebase.
        row = {k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in row.items()}
    return row


def _backoff_delay(attempt: int) -> float:
    base = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_CAP)
    jitter = base * BACKOFF_JITTER
    return max(0.0, base + random.uniform(-jitter, jitter))


def _classify_outcome(result) -> tuple[int | None, str]:
    if result.error is not None:
        return None, result.error
    status = result.status_code
    if status == 200:
        return status, "success"
    if status == 400:
        return status, "invalid_request"
    if status in RETRYABLE_STATUSES:
        return status, "server_error" if status == 500 else "over_capacity"
    return status, "unexpected_status"


def _finalize(conn, item_id: str, worker_id: str, state: str, annotation=None, extracted_text=None, reason=None) -> None:
    conn.execute(
        """
        UPDATE items
        SET state = %(state)s, annotation = %(annotation)s, extracted_text = %(extracted_text)s,
            reason = %(reason)s, updated_at = now()
        WHERE item_id = %(item_id)s AND leased_by = %(worker_id)s
        """,
        {
            "state": state,
            "annotation": json.dumps(annotation) if annotation is not None else None,
            "extracted_text": extracted_text,
            "reason": json.dumps(reason) if reason is not None else None,
            "item_id": item_id, "worker_id": worker_id,
        },
    )
    conn.commit()


def process_item(conn, connect_fn, item: dict, corpus_files_dir: Path, stub_client, stub_base_url: str, worker_id: str) -> None:
    item_id = item["item_id"]
    tenant = item["tenant"]  # D-10: tenant comes only from the claimed row, never anywhere else

    # Read the item's bytes up front. This is a local filesystem read, not a stub call, so
    # doing it before the prior-success check does not violate the D-09 rule that a reclaimed
    # item must be checked for an already-completed successful attempt before anything is sent
    # to the stub. Having the bytes here lets the recovery path below recompute extracted_text
    # instead of finalizing a recovered item with a NULL one.
    data = (corpus_files_dir / item["source_path"]).read_bytes()

    prior_success = find_completed_success(conn, item_id)
    if prior_success:
        # item_attempts doesn't store the annotation payload itself — re-fetch it from
        # annotations_cache by content hash, since a successful attempt always writes there
        # (and commits that write *before* marking the attempt complete, see below).
        cached = conn.execute(
            "SELECT annotation FROM annotations_cache WHERE tenant = %s AND sha256 = %s",
            (tenant, item["sha256"]),
        ).fetchone()
        if cached is not None:
            _finalize(conn, item_id, worker_id, "succeeded", annotation=cached["annotation"],
                      extracted_text=extract_text(item["extension"], data))
            return
        # No cache row despite a completed successful attempt. The commit ordering below makes
        # this unreachable, but if it ever happens the safe move is to reprocess from scratch —
        # finalizing "succeeded" with an empty annotation would report silently wrong data as a
        # success. Fall through into normal processing instead of returning.

    edge_case = detect_edge_case(item["extension"], data)
    if edge_case is not None:
        _finalize(conn, item_id, worker_id, edge_case, reason={"code": edge_case})
        return

    extracted = extract_text(item["extension"], data)

    cached = conn.execute(
        "SELECT annotation FROM annotations_cache WHERE tenant = %s AND sha256 = %s",
        (tenant, item["sha256"]),
    ).fetchone()
    if cached is not None:
        _finalize(conn, item_id, worker_id, "succeeded", annotation=cached["annotation"], extracted_text=extracted)
        return

    cap_count = conn.execute(
        "SELECT COUNT(*) AS n FROM item_attempts WHERE item_id = %s", (item_id,)
    ).fetchone()["n"]
    if cap_count >= MAX_ATTEMPTS:
        _finalize(conn, item_id, worker_id, "annotation_failed", extracted_text=extracted,
                  reason={"code": "annotation_failed", "attempts": cap_count, "note": "cap_reached_at_claim"})
        return

    renewer = LeaseRenewer(connect_fn, item_id, worker_id, lease_seconds=config.LEASE_SECONDS, interval=config.LEASE_RENEW_INTERVAL)
    renewer.start()
    try:
        if renewer.lost():
            return
        slot_id = None
        while slot_id is None:
            slot_id = claim_slot(conn, worker_id, lease_seconds=config.LEASE_SECONDS)
            if slot_id is None:
                time.sleep(0.05 + random.uniform(0, 0.05))

        outcome = None
        annotation = None
        attempt_no = None
        last_status = None
        try:
            try:
                attempt_id, attempt_no = start_attempt(conn, item_id, tenant, worker_id, slot_id)
            except psycopg.errors.UniqueViolation:
                # UNIQUE(item_id, attempt_no) backstop (D-09): this worker's lease was
                # already stolen and the new owner recorded this attempt_no first.
                # Abandon the item — the current owner is already handling it.
                conn.rollback()
                return
            result = call_annotate(stub_client, stub_base_url, data, timeout=HTTP_TIMEOUT_SECONDS)
            last_status, outcome = _classify_outcome(result)
            if outcome == "success":
                annotation = result.body
                # Commit the cache row BEFORE the attempt row is marked complete. These
                # two commits are strictly sequential on one connection, so a later commit
                # cannot be durable unless the earlier one is: if a crash here leaves a
                # completed 'success' attempt behind, its cache row is guaranteed to exist,
                # and the recovery path at the top of this function can finalize from it.
                # The reverse order left a window where an attempt said "success" with no
                # annotation anywhere — reported as success with an empty annotation.
                # This still satisfies D-09: the attempt UPDATE commits after the HTTP call
                # returns, just before rather than after the cache write.
                conn.execute(
                    "INSERT INTO annotations_cache (tenant, sha256, annotation) VALUES (%s, %s, %s) "
                    "ON CONFLICT (tenant, sha256) DO NOTHING",
                    (tenant, item["sha256"], json.dumps(annotation)),
                )
                conn.commit()
            complete_attempt(conn, attempt_id, last_status, outcome)
        finally:
            release_slot(conn, slot_id, worker_id)

        if outcome == "success":
            _finalize(conn, item_id, worker_id, "succeeded", annotation=annotation, extracted_text=extracted)
        elif outcome == "invalid_request":
            _finalize(conn, item_id, worker_id, "annotation_invalid_request",
                      extracted_text=extracted, reason={"code": "invalid_request", "attempt": attempt_no})
        elif attempt_no >= MAX_ATTEMPTS:
            _finalize(conn, item_id, worker_id, "annotation_failed", extracted_text=extracted,
                      reason={"code": "annotation_failed", "attempts": attempt_no, "last_status": last_status})
        else:
            # Retryable (server_error, over_capacity, timeout, connection_error): release the
            # item rather than retrying in place (D-05) — any worker's next claim can pick it
            # up once next_attempt_after passes, and this worker goes straight back to claiming
            # other work instead of blocking on a sleep.
            delay = _backoff_delay(attempt_no)
            conn.execute(
                "UPDATE items SET state = 'pending', leased_by = NULL, leased_until = NULL, "
                "next_attempt_after = now() + %(delay)s * interval '1 second' "
                "WHERE item_id = %(item_id)s AND leased_by = %(worker_id)s",
                {"delay": delay, "item_id": item_id, "worker_id": worker_id},
            )
            conn.commit()
    finally:
        renewer.stop()


def run_worker(index: int) -> None:
    worker_id = f"worker-{index}"
    stub_client = httpx.Client()
    print(f"[{worker_id}] starting", flush=True)
    while True:
        conn = None
        try:
            conn = connect()
            item = claim_item(conn, worker_id, lease_seconds=config.LEASE_SECONDS)
            if item is None:
                time.sleep(0.5)
                continue
            run = conn.execute("SELECT corpus_dir FROM runs WHERE run_id = %s", (item["run_id"],)).fetchone()
            corpus_files_dir = Path(run["corpus_dir"]) / "files"
            process_item(conn, connect, item, corpus_files_dir, stub_client, config.STUB_BASE_URL, worker_id)
        except Exception:
            # One item must never be able to take the worker process down. Without this, an
            # item that fails deterministically (unreadable file, malformed row) is a poison
            # pill: it kills this worker, its lease expires, the next worker reclaims it and
            # dies the same way, and so on until all four workers are gone and the item never
            # reaches a terminal state. Logging and moving on leaves the item in_progress to be
            # reclaimed and retried after its lease expires — the right outcome for a transient
            # failure, and merely non-ideal (not incorrect) for a permanent one. Deliberately
            # NOT finalized to some new terminal state: D-04's vocabulary is fixed, and adding
            # to it is a design decision, not an error-handling detail.
            print(f"[{worker_id}] error while processing an item; continuing", flush=True)
            traceback.print_exc()
            sys.stderr.flush()
            # Brief pause so a poison item that is immediately reclaimable can't hot-loop.
            time.sleep(0.5)
        finally:
            if conn is not None:
                conn.close()


if __name__ == "__main__":
    run_worker(int(sys.argv[1]) if len(sys.argv) > 1 else 0)
