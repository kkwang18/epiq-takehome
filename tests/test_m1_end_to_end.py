import json
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def run_cli(*args) -> dict:
    result = subprocess.run(
        [str(REPO_ROOT / "intake"), *args], cwd=REPO_ROOT, capture_output=True, text=True
    )
    assert result.returncode == 0, f"{args} failed: {result.stderr}"
    return json.loads(result.stdout)


@pytest.mark.slow
def test_m1_full_flow(tmp_path):
    subprocess.run([str(REPO_ROOT / "intake"), "down"], cwd=REPO_ROOT, capture_output=True)

    up_result = run_cli("up", "--workers", "4")
    assert up_result["workers"] == 4
    try:
        corpus_a = tmp_path / "corpus-a"
        run_cli("corpus", "--seed", "1", "--size", "60", "--tenant", "tenant-a", "--out", str(corpus_a))

        corpus_b = tmp_path / "corpus-b"
        run_cli("corpus", "--seed", "2", "--size", "55", "--tenant", "tenant-b", "--out", str(corpus_b))

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
