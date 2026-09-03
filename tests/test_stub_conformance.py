# tests/test_stub_conformance.py
"""
Black-box conformance suite for the annotation stub.
MUST NOT import any pipeline code — plain HTTP only, against a running stub.
Start the stub first: `./intake stub --port 8080` (or rely on STUB_URL for a
stub running elsewhere).
"""
import base64
import hashlib
import os
import time

import httpx
import pytest

STUB_URL = os.environ.get("STUB_URL", "http://localhost:8080")


@pytest.fixture(autouse=True)
def reset_stub():
    httpx.post(f"{STUB_URL}/v1/reset", json={
        "latency_mode": "fixed", "latency_ms": 0, "failure_every_n": 0, "in_flight_capacity": 2,
    })
    yield


def annotate(content: bytes) -> httpx.Response:
    return httpx.post(f"{STUB_URL}/v1/annotate", json={"content_b64": base64.b64encode(content).decode()})


def test_healthz():
    assert httpx.get(f"{STUB_URL}/healthz").status_code == 200


def test_annotate_success_and_stable_answer():
    r1 = annotate(b"conformance-content")
    r2 = annotate(b"conformance-content")
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["sha256"] == hashlib.sha256(b"conformance-content").hexdigest()
    assert r1.json() == r2.json()


def test_rejects_unexpected_field():
    r = httpx.post(f"{STUB_URL}/v1/annotate", json={
        "content_b64": base64.b64encode(b"x").decode(), "tenant": "sneaky"
    })
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"


def test_rejects_missing_field():
    r = httpx.post(f"{STUB_URL}/v1/annotate", json={})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"


def test_billed_calls_increments_on_success():
    before = httpx.get(f"{STUB_URL}/v1/stats").json()["billed_calls"]
    annotate(b"count-me")
    after = httpx.get(f"{STUB_URL}/v1/stats").json()["billed_calls"]
    assert after == before + 1


def test_failure_schedule_every_nth_call():
    httpx.post(f"{STUB_URL}/v1/reset", json={"failure_every_n": 3, "latency_ms": 0})
    statuses = [annotate(f"item-{i}".encode()).status_code for i in range(6)]
    assert statuses == [200, 200, 500, 200, 200, 500]


def test_over_capacity_returns_429_never_admitted():
    httpx.post(f"{STUB_URL}/v1/reset", json={"in_flight_capacity": 1, "latency_ms": 300})
    results = []

    def call(i):
        results.append(annotate(f"cap-{i}".encode()).status_code)

    import threading
    threads = [threading.Thread(target=call, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert 429 in results


def test_max_in_flight_reports_observed_peak():
    httpx.post(f"{STUB_URL}/v1/reset", json={"in_flight_capacity": 5, "latency_ms": 200})
    import threading
    threads = [threading.Thread(target=annotate, args=(f"peak-{i}".encode(),)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stats = httpx.get(f"{STUB_URL}/v1/stats").json()
    assert stats["max_in_flight"] >= 2


def test_latency_is_repeatable_with_same_seed():
    httpx.post(f"{STUB_URL}/v1/reset", json={"latency_mode": "jitter", "latency_seed": 999, "latency_jitter_min_ms": 10, "latency_jitter_max_ms": 20})
    start = time.monotonic()
    annotate(b"timing-a")
    d1 = time.monotonic() - start

    httpx.post(f"{STUB_URL}/v1/reset", json={"latency_mode": "jitter", "latency_seed": 999, "latency_jitter_min_ms": 10, "latency_jitter_max_ms": 20})
    start = time.monotonic()
    annotate(b"timing-b")
    d2 = time.monotonic() - start

    assert abs(d1 - d2) < 0.05


def test_reset_clears_counters():
    annotate(b"before-reset")
    httpx.post(f"{STUB_URL}/v1/reset", json={})
    stats = httpx.get(f"{STUB_URL}/v1/stats").json()
    assert stats["billed_calls"] == 0
