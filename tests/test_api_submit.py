# tests/test_api_submit.py
import json
from pathlib import Path

from fastapi.testclient import TestClient

from content_intake.generator.generate import generate_corpus
from content_intake.pipeline.api import create_api_app, submit_run


def test_submit_run_inserts_all_items(db_conn, tmp_path):
    out = tmp_path / "corpus"
    manifest = generate_corpus(seed=1, size=55, tenant="tenant-a", out_dir=out)
    run_id = submit_run(db_conn, out, "tenant-a")
    count = db_conn.execute("SELECT COUNT(*) AS n FROM items WHERE run_id = %s", (run_id,)).fetchone()["n"]
    assert count == manifest["totals"]["items"]


def test_submit_run_rejects_sha256_mismatch(db_conn, tmp_path):
    out = tmp_path / "corpus"
    manifest = generate_corpus(seed=1, size=55, tenant="tenant-a", out_dir=out)
    tampered_path = out / "files" / manifest["files"][0]["path"]
    tampered_path.write_bytes(tampered_path.read_bytes() + b"x")
    import pytest
    with pytest.raises(ValueError):
        submit_run(db_conn, out, "tenant-a")
    # nothing partially committed
    count = db_conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"]
    assert count == 0


def test_submit_endpoint_returns_run_id(tmp_path):
    out = tmp_path / "corpus"
    generate_corpus(seed=2, size=50, tenant="tenant-a", out_dir=out)
    client = TestClient(create_api_app())
    resp = client.post("/v1/runs", json={"corpus_dir": str(out), "tenant": "tenant-a"})
    assert resp.status_code == 200
    assert "run_id" in resp.json()
