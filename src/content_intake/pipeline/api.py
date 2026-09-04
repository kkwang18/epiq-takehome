# src/content_intake/pipeline/api.py
import hashlib
import json
import uuid
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from content_intake.pipeline.db import connect


def _item_to_json(row: dict) -> dict:
    out = dict(row)
    for key in ("item_id", "run_id"):
        out[key] = str(out[key])
    for key in ("created_at", "updated_at", "leased_until"):
        if out.get(key) is not None:
            out[key] = out[key].isoformat()
    return out


def submit_run(conn, corpus_dir: Path, tenant: str) -> str:
    corpus_dir = Path(corpus_dir).resolve()
    manifest = json.loads((corpus_dir / "manifest.json").read_text())
    run_id = str(uuid.uuid4())
    with conn.transaction():
        conn.execute(
            "INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, %s, %s, %s)",
            (run_id, manifest["corpus_id"], tenant, str(corpus_dir)),
        )
        for entry in manifest["files"]:
            file_path = corpus_dir / "files" / entry["path"]
            data = file_path.read_bytes()
            actual_sha = hashlib.sha256(data).hexdigest()
            if actual_sha != entry["sha256"]:
                raise ValueError(
                    f"sha256 mismatch for {entry['path']}: manifest={entry['sha256']} disk={actual_sha}"
                )
            conn.execute(
                """
                INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                                    role, order_index, duplicate_of, edge_case, expects_annotation, state)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                """,
                (
                    str(uuid.uuid4()), run_id, tenant, entry["path"], entry["extension"],
                    entry["bytes"], actual_sha, entry["role"], entry["order"],
                    entry.get("duplicate_of"), entry.get("edge_case"), entry["expects_annotation"],
                ),
            )
    return run_id


class SubmitRequest(BaseModel):
    corpus_dir: str
    tenant: str


def create_api_app() -> FastAPI:
    app = FastAPI()

    @app.post("/v1/runs")
    def submit(req: SubmitRequest):
        conn = connect()
        try:
            run_id = submit_run(conn, Path(req.corpus_dir), req.tenant)
            return {"run_id": run_id}
        finally:
            conn.close()

    @app.get("/v1/runs/{run_id}/status")
    def status(run_id: str):
        conn = connect()
        try:
            run = conn.execute("SELECT tenant FROM runs WHERE run_id = %s", (run_id,)).fetchone()
            if run is None:
                return JSONResponse(status_code=404, content={"error": "not_found"})
            rows = conn.execute(
                "SELECT state, COUNT(*) AS n FROM items WHERE run_id = %s GROUP BY state", (run_id,)
            ).fetchall()
            states = {r["state"]: r["n"] for r in rows}
            total = sum(states.values())
            nonterminal = states.get("pending", 0) + states.get("in_progress", 0)
            return {
                "run_id": run_id, "tenant": run["tenant"], "states": states,
                "terminal": total > 0 and nonterminal == 0,
            }
        finally:
            conn.close()

    @app.get("/v1/items/{item_id}")
    def get_item(item_id: str, tenant: str):
        conn = connect()
        try:
            row = conn.execute(
                "SELECT * FROM items WHERE item_id = %s AND tenant = %s", (item_id, tenant)
            ).fetchone()
            if row is None:
                return JSONResponse(status_code=404, content={"error": "not_found"})
            return _item_to_json(row)
        finally:
            conn.close()

    @app.get("/v1/runs/{run_id}/items")
    def list_items(run_id: str, tenant: str, state: str | None = None):
        conn = connect()
        try:
            run = conn.execute("SELECT tenant FROM runs WHERE run_id = %s", (run_id,)).fetchone()
            if run is None or run["tenant"] != tenant:
                return JSONResponse(status_code=404, content={"error": "not_found"})
            # created_at is identical for every item in a run (one transaction, one now()),
            # so order_index -- the manifest's own order -- is what actually orders this list.
            if state:
                rows = conn.execute(
                    "SELECT * FROM items WHERE run_id = %s AND tenant = %s AND state = %s "
                    "ORDER BY created_at, order_index",
                    (run_id, tenant, state),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM items WHERE run_id = %s AND tenant = %s ORDER BY created_at, order_index",
                    (run_id, tenant),
                ).fetchall()
            return [_item_to_json(r) for r in rows]
        finally:
            conn.close()

    return app
