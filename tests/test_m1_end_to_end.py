import json
import subprocess
import time
from collections import defaultdict
from pathlib import Path

import httpx
import pytest

from content_intake.common import config
from content_intake.pipeline.db import connect

REPO_ROOT = Path(__file__).resolve().parents[1]

# The manifest records what each file is *supposed* to end up as; the pipeline records what it
# actually ended up as. Every name matches except "success", which D-04 spells "succeeded".
_EXPECTED_OUTCOME_TO_STATE = {"success": "succeeded"}


def run_cli(*args) -> dict:
    result = subprocess.run(
        [str(REPO_ROOT / "intake"), *args], cwd=REPO_ROOT, capture_output=True, text=True
    )
    assert result.returncode == 0, f"{args} failed: {result.stderr}"
    return json.loads(result.stdout)


def assert_items_match_manifest(corpus_dir: Path, items: list[dict]) -> None:
    """Cross-check every item's final state against its manifest entry's expected_outcome."""
    manifest = json.loads((corpus_dir / "manifest.json").read_text())
    expected = {entry["path"]: entry["expected_outcome"] for entry in manifest["files"]}
    actual = {item["source_path"]: item["state"] for item in items}

    assert set(actual) == set(expected), (
        f"item set differs from manifest: "
        f"missing={sorted(set(expected) - set(actual))} extra={sorted(set(actual) - set(expected))}"
    )
    mismatches = {
        path: (actual[path], outcome)
        for path, outcome in expected.items()
        if actual[path] != _EXPECTED_OUTCOME_TO_STATE.get(outcome, outcome)
    }
    assert not mismatches, f"state != expected_outcome for: {mismatches}"


@pytest.mark.slow
def test_m1_full_flow(tmp_path):
    subprocess.run([str(REPO_ROOT / "intake"), "down"], cwd=REPO_ROOT, capture_output=True)

    up_result = run_cli("up", "--workers", "4")
    assert up_result["workers"] == 4
    try:
        # Same seed and size for both tenants, deliberately: the generator's file bytes depend
        # only on (seed, size) and never on tenant (tests/test_generator.py asserts this), so
        # these two corpora are byte-identical file for file. That is what makes the
        # cross-tenant cache assertion at the end of this test meaningful (D-10) — both
        # tenants' items hash to the same sha256 values and are processed by the same pool of
        # four workers with interleaved claims.
        corpus_a = tmp_path / "corpus-a"
        run_cli("corpus", "--seed", "1", "--size", "60", "--tenant", "tenant-a", "--out", str(corpus_a))

        corpus_b = tmp_path / "corpus-b"
        run_cli("corpus", "--seed", "1", "--size", "60", "--tenant", "tenant-b", "--out", str(corpus_b))

        submit_a = run_cli("submit", "--corpus", str(corpus_a), "--tenant", "tenant-a")
        submit_b = run_cli("submit", "--corpus", str(corpus_b), "--tenant", "tenant-b")
        run_id_a, run_id_b = submit_a["run_id"], submit_b["run_id"]

        deadline = time.monotonic() + 90
        status_a = status_b = None
        while time.monotonic() < deadline:
            status_a = run_cli("status", "--run", run_id_a)
            status_b = run_cli("status", "--run", run_id_b)
            if status_a["terminal"] and status_b["terminal"]:
                break
            time.sleep(2)
        assert status_a["terminal"], f"run A did not terminate: {status_a}"
        assert status_b["terminal"], f"run B did not terminate: {status_b}"

        items_a = run_cli("items", "--tenant", "tenant-a", "--run", run_id_a)
        assert len(items_a) == 60
        assert all(item["state"] != "pending" and item["state"] != "in_progress" for item in items_a)

        empty_items = run_cli("items", "--tenant", "tenant-a", "--run", run_id_a, "--state", "empty_content")
        assert len(empty_items) == 1
        assert empty_items[0]["reason"]["code"] == "empty_content"

        decode_failed_items = run_cli("items", "--tenant", "tenant-a", "--run", run_id_a, "--state", "decode_failed")
        assert len(decode_failed_items) == 1

        succeeded_items = run_cli("items", "--tenant", "tenant-a", "--run", run_id_a, "--state", "succeeded")
        for item in succeeded_items:
            assert item["annotation"] is not None

        # Every item, both runs, ended in exactly the state its manifest entry predicted.
        items_b = run_cli("items", "--tenant", "tenant-b", "--run", run_id_b)
        assert len(items_b) == 60
        assert_items_match_manifest(corpus_a, items_a)
        assert_items_match_manifest(corpus_b, items_b)

        # The stub's own counters are the authority on whether the D-08 capacity gate actually
        # held under real concurrent load from four worker processes. over_capacity_calls is
        # the single number that proves it: the stub returns 429 and increments this only when
        # a call arrives while in_flight is already at capacity.
        stats = httpx.get(f"{config.STUB_BASE_URL}/v1/stats", timeout=5.0).json()
        assert stats["over_capacity_calls"] == 0, f"capacity gate leaked: {stats}"
        assert stats["max_in_flight"] <= config.IN_FLIGHT_CAPACITY, f"in-flight peak too high: {stats}"

        # D-10 tenant invariant, end to end. The two corpora are byte-identical, so every
        # annotated sha256 appears under both tenants — and must appear as two *separate*
        # annotations_cache rows, never one shared row. A single shared row (or one tenant's
        # set being a subset of the other's) would mean one tenant read or wrote through the
        # other's cache entry.
        conn = connect()
        try:
            cache_rows = conn.execute("SELECT tenant, sha256 FROM annotations_cache").fetchall()
            item_tenants = conn.execute(
                "SELECT run_id, array_agg(DISTINCT tenant) AS tenants FROM items GROUP BY run_id"
            ).fetchall()
        finally:
            conn.close()

        shas_by_tenant = defaultdict(set)
        for row in cache_rows:
            shas_by_tenant[row["tenant"]].add(row["sha256"])
        assert set(shas_by_tenant) == {"tenant-a", "tenant-b"}, f"unexpected cache tenants: {list(shas_by_tenant)}"
        assert shas_by_tenant["tenant-a"] == shas_by_tenant["tenant-b"], (
            "byte-identical corpora should produce the same set of content hashes per tenant"
        )
        assert len(shas_by_tenant["tenant-a"]) > 0
        # One row per (tenant, sha256) — i.e. exactly two rows for every shared hash, no sharing.
        assert len(cache_rows) == 2 * len(shas_by_tenant["tenant-a"]), (
            f"expected two cache rows per shared hash, got {len(cache_rows)} rows for "
            f"{len(shas_by_tenant['tenant-a'])} hashes"
        )
        # And no run's items were ever written with a tenant other than the one that submitted it.
        for row in item_tenants:
            assert len(row["tenants"]) == 1, f"run {row['run_id']} has mixed tenants: {row['tenants']}"

        first_item_id = items_a[0]["item_id"]
        cross_tenant = subprocess.run(
            [str(REPO_ROOT / "intake"), "item", "--tenant", "tenant-b", "--id", first_item_id],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        assert cross_tenant.returncode != 0

        wrong_run_items = subprocess.run(
            [str(REPO_ROOT / "intake"), "items", "--tenant", "tenant-b", "--run", run_id_a],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        assert wrong_run_items.returncode != 0
    finally:
        subprocess.run([str(REPO_ROOT / "intake"), "down"], cwd=REPO_ROOT, capture_output=True)
