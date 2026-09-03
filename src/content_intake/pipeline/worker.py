# src/content_intake/pipeline/worker.py
import threading
import uuid


def claim_item(conn, worker_id: str, lease_seconds: int) -> dict | None:
    row = conn.execute(
        """
        UPDATE items
        SET state = 'in_progress', leased_by = %(worker_id)s,
            leased_until = now() + %(lease_seconds)s * interval '1 second'
        WHERE item_id = (
            SELECT item_id FROM items
            WHERE state = 'pending' OR (state = 'in_progress' AND leased_until < now())
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        """,
        {"worker_id": worker_id, "lease_seconds": lease_seconds},
    ).fetchone()
    conn.commit()
    if row is not None:
        # psycopg3 loads uuid columns as uuid.UUID objects; normalize to str so
        # callers (and equality checks against str ids) see the same id type
        # that's used everywhere else in this codebase.
        row = {k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in row.items()}
    return row


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
