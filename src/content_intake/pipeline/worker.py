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
