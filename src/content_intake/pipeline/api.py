# src/content_intake/pipeline/api.py
import hashlib
import json
import uuid
from pathlib import Path

from fastapi import FastAPI
from pydantic import BaseModel

from content_intake.pipeline.db import connect


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
                                    role, duplicate_of, edge_case, expects_annotation, state)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                """,
                (
                    str(uuid.uuid4()), run_id, tenant, entry["path"], entry["extension"],
                    entry["bytes"], actual_sha, entry["role"], entry.get("duplicate_of"),
                    entry.get("edge_case"), entry["expects_annotation"],
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

    return app
