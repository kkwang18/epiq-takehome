# src/content_intake/scenario/fault.py
import time


def find_leasing_worker(conn) -> dict | None:
    row = conn.execute(
        "SELECT leased_by, item_id FROM items WHERE state = 'in_progress' AND leased_by IS NOT NULL LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    return {"leased_by": row["leased_by"], "item_id": str(row["item_id"])}


def worker_index_from_id(worker_id: str) -> int:
    return int(worker_id.rsplit("-", 1)[-1])


def wait_for_item_terminal(conn, item_id: str, timeout: float = 10.0, interval: float = 0.2) -> float | None:
    start = time.time()
    while time.time() - start < timeout:
        row = conn.execute("SELECT state FROM items WHERE item_id = %s", (item_id,)).fetchone()
        if row is not None and row["state"] not in ("pending", "in_progress"):
            return time.time()
        time.sleep(interval)
    return None
