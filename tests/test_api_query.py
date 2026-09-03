# tests/test_api_query.py
from fastapi.testclient import TestClient

from content_intake.generator.generate import generate_corpus
from content_intake.pipeline.api import create_api_app, submit_run


def _submit(db_conn, tmp_path, tenant="tenant-a", seed=1, size=50):
    out = tmp_path / f"corpus-{tenant}-{seed}"
    generate_corpus(seed=seed, size=size, tenant=tenant, out_dir=out)
    return submit_run(db_conn, out, tenant)


def test_status_reports_states_and_terminal_flag(db_conn, tmp_path):
    run_id = _submit(db_conn, tmp_path)
    client = TestClient(create_api_app())
    resp = client.get(f"/v1/runs/{run_id}/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == run_id
    assert body["tenant"] == "tenant-a"
    assert body["states"]["pending"] == 50
    assert body["terminal"] is False


def test_status_unknown_run_is_404(db_conn, tmp_path):
    client = TestClient(create_api_app())
    resp = client.get("/v1/runs/00000000-0000-0000-0000-000000000000/status")
    assert resp.status_code == 404


def test_item_returns_by_id_and_tenant(db_conn, tmp_path):
    run_id = _submit(db_conn, tmp_path)
    item_id = db_conn.execute("SELECT item_id FROM items WHERE run_id = %s LIMIT 1", (run_id,)).fetchone()["item_id"]
    client = TestClient(create_api_app())
    resp = client.get(f"/v1/items/{item_id}", params={"tenant": "tenant-a"})
    assert resp.status_code == 200
    assert resp.json()["item_id"] == str(item_id)


def test_item_wrong_tenant_is_404(db_conn, tmp_path):
    run_id = _submit(db_conn, tmp_path)
    item_id = db_conn.execute("SELECT item_id FROM items WHERE run_id = %s LIMIT 1", (run_id,)).fetchone()["item_id"]
    client = TestClient(create_api_app())
    resp = client.get(f"/v1/items/{item_id}", params={"tenant": "tenant-b"})
    assert resp.status_code == 404


def test_items_filtered_by_state(db_conn, tmp_path):
    run_id = _submit(db_conn, tmp_path)
    client = TestClient(create_api_app())
    resp = client.get(f"/v1/runs/{run_id}/items", params={"tenant": "tenant-a", "state": "pending"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 50
    assert all(item["state"] == "pending" for item in body)


def test_items_wrong_tenant_is_404(db_conn, tmp_path):
    run_id = _submit(db_conn, tmp_path)
    client = TestClient(create_api_app())
    resp = client.get(f"/v1/runs/{run_id}/items", params={"tenant": "tenant-b"})
    assert resp.status_code == 404
