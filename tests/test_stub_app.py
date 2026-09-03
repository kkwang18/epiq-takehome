# tests/test_stub_app.py
import base64
import time

from fastapi.testclient import TestClient

from content_intake.stub.app import create_app
from content_intake.stub.state import StubState


def make_client(**overrides):
    defaults = dict(
        latency_mode="fixed", latency_ms=0, latency_jitter_min_ms=50, latency_jitter_max_ms=250,
        latency_seed=1, failure_every_n=0, failure_status=500, in_flight_capacity=2,
    )
    defaults.update(overrides)
    state = StubState(**defaults)
    return TestClient(create_app(state)), state


def test_healthz():
    client, _ = make_client()
    resp = client.get("/healthz")
    assert resp.status_code == 200


def test_annotate_success():
    client, _ = make_client()
    body = {"content_b64": base64.b64encode(b"hello").decode()}
    resp = client.post("/v1/annotate", json=body)
    assert resp.status_code == 200
    data = resp.json()
    assert "sha256" in data


def test_annotate_rejects_unknown_field():
    client, _ = make_client()
    body = {"content_b64": base64.b64encode(b"hello").decode(), "tenant": "sneaky"}
    resp = client.post("/v1/annotate", json=body)
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_request"


def test_annotate_rejects_missing_field():
    client, _ = make_client()
    resp = client.post("/v1/annotate", json={})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_request"


def test_billing_increments_on_success():
    client, state = make_client()
    body = {"content_b64": base64.b64encode(b"hello").decode()}
    client.post("/v1/annotate", json=body)
    assert state.stats()["billed_calls"] == 1


def test_failure_schedule_returns_server_error_and_bills():
    client, state = make_client(failure_every_n=2)
    body = {"content_b64": base64.b64encode(b"hello").decode()}
    r1 = client.post("/v1/annotate", json=body)
    r2 = client.post("/v1/annotate", json=body)
    assert r1.status_code == 200
    assert r2.status_code == 500
    assert r2.json()["error"]["code"] == "server_error"
    assert state.stats()["billed_calls"] == 2
    assert state.stats()["server_error_calls"] == 1


def test_over_capacity_returns_429_and_bills(monkeypatch):
    client, state = make_client(in_flight_capacity=0)
    body = {"content_b64": base64.b64encode(b"hello").decode()}
    resp = client.post("/v1/annotate", json=body)
    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "over_capacity"
    assert state.stats()["billed_calls"] == 1
    assert state.stats()["over_capacity_calls"] == 1


def test_stats_endpoint():
    client, _ = make_client()
    resp = client.get("/v1/stats")
    assert resp.status_code == 200
    for key in ("billed_calls", "current_in_flight", "max_in_flight", "server_error_calls", "over_capacity_calls"):
        assert key in resp.json()


def test_reset_endpoint_clears_and_patches():
    client, state = make_client(failure_every_n=7)
    body = {"content_b64": base64.b64encode(b"hello").decode()}
    client.post("/v1/annotate", json=body)
    resp = client.post("/v1/reset", json={"failure_every_n": 1})
    assert resp.status_code == 200
    assert state.stats()["billed_calls"] == 0
    r = client.post("/v1/annotate", json=body)
    assert r.status_code == 500
