# src/content_intake/scenario/runner.py
import time
from pathlib import Path

import httpx

from content_intake.common import config
from content_intake.pipeline.db import connect
from content_intake.scenario.fault import find_leasing_worker, worker_index_from_id, wait_for_item_terminal


def _submit(corpus_dir: Path, tenant: str) -> str:
    resp = httpx.post(f"{config.API_BASE_URL}/v1/runs", json={"corpus_dir": str(Path(corpus_dir).resolve()), "tenant": tenant})
    resp.raise_for_status()
    return resp.json()["run_id"]


def _status(run_id: str) -> dict:
    resp = httpx.get(f"{config.API_BASE_URL}/v1/runs/{run_id}/status")
    resp.raise_for_status()
    return resp.json()


def _terminal_split(status: dict) -> dict:
    states = status["states"]
    nonterminal = states.get("pending", 0) + states.get("in_progress", 0)
    total = sum(states.values())
    return {"terminal": total - nonterminal, "nonterminal": nonterminal}


def _fetch_item_attempts(conn, run_ids: list[str]) -> list[dict]:
    rows = conn.execute(
        "SELECT ia.completed_at, ia.http_status, ia.outcome, ia.attempt_no "
        "FROM item_attempts ia JOIN items i ON i.item_id = ia.item_id "
        "WHERE i.run_id = ANY(%s)",
        (run_ids,),
    ).fetchall()
    return [dict(r) for r in rows]


def _inject_fault(conn, run_cli_fn) -> dict:
    leasing = find_leasing_worker(conn)
    if leasing is None:
        raise RuntimeError("no worker currently holds a lease on a nonterminal item")
    index = worker_index_from_id(leasing["leased_by"])
    kill_time = time.time()
    run_cli_fn("kill-worker", "--index", str(index))
    recovered_time = wait_for_item_terminal(conn, leasing["item_id"], timeout=10.0, interval=0.2)
    return {
        "worker_id": leasing["leased_by"], "index": index, "item_id": leasing["item_id"],
        "kill_time": kill_time, "recovered_time": recovered_time,
    }


def run_execution(corpus_a_dir: Path, tenant_a: str, corpus_b_dir: Path, tenant_b: str,
                   kill: bool, poll_interval: float, run_cli_fn) -> dict:
    run_id_a = _submit(corpus_a_dir, tenant_a)
    run_id_b = _submit(corpus_b_dir, tenant_b)
    submitted_at = time.time()

    timeseries = []
    kill_event = None
    conn = connect()
    try:
        while True:
            status_a = _status(run_id_a)
            status_b = _status(run_id_b)
            split_a = _terminal_split(status_a)
            split_b = _terminal_split(status_b)
            timeseries.append({"t": time.time(), "run_a": split_a, "run_b": split_b})

            if (kill and kill_event is None
                    and split_a["terminal"] > 0 and split_a["nonterminal"] > 0
                    and split_b["terminal"] > 0 and split_b["nonterminal"] > 0):
                kill_event = _inject_fault(conn, run_cli_fn)

            if split_a["nonterminal"] == 0 and split_b["nonterminal"] == 0:
                break
            time.sleep(poll_interval)

        terminal_at = time.time()
        stub_stats = httpx.get(f"{config.STUB_BASE_URL}/v1/stats").json()
        item_attempts = _fetch_item_attempts(conn, [run_id_a, run_id_b])
    finally:
        conn.close()

    final_a = _status(run_id_a)
    final_b = _status(run_id_b)
    return {
        "run_a": {"run_id": run_id_a, "tenant": tenant_a, "submitted_at": submitted_at,
                  "terminal_at": terminal_at, "states": final_a["states"]},
        "run_b": {"run_id": run_id_b, "tenant": tenant_b, "submitted_at": submitted_at,
                  "terminal_at": terminal_at, "states": final_b["states"]},
        "timeseries": timeseries,
        "kill_event": kill_event,
        "stub_stats": stub_stats,
        "item_attempts": item_attempts,
    }
