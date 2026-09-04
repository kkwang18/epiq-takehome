# tests/test_scenario_end_to_end.py
import subprocess
from pathlib import Path

import pytest

from content_intake.generator.generate import generate_corpus
from content_intake.scenario.assertions import check_retry_amplification, check_reconciliation, check_overlap
from content_intake.scenario.runner import run_execution

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_cli(*args):
    return subprocess.run(
        [str(REPO_ROOT / "intake"), *args], cwd=REPO_ROOT, capture_output=True, text=True
    )


def _originals(size: int) -> int:
    return size - round(size / 10) - 2


@pytest.mark.slow
def test_scaled_down_scenario_produces_sensible_assertions(tmp_path):
    corpus_a = tmp_path / "corpus-a"
    corpus_b = tmp_path / "corpus-b"
    generate_corpus(seed=20, size=60, tenant="tenant-a", out_dir=corpus_a)
    generate_corpus(seed=21, size=55, tenant="tenant-b", out_dir=corpus_b)

    assert _run_cli("down").returncode in (0, 1)
    assert _run_cli("up", "--workers", "4").returncode == 0
    try:
        control = run_execution(corpus_a, "tenant-a", corpus_b, "tenant-b", kill=False, poll_interval=0.3, run_cli_fn=_run_cli)
    finally:
        _run_cli("down")

    assert _run_cli("up", "--workers", "4").returncode == 0
    try:
        fault = run_execution(corpus_a, "tenant-a", corpus_b, "tenant-b", kill=True, poll_interval=0.3, run_cli_fn=_run_cli)
    finally:
        _run_cli("down")

    x_items = _originals(60) + _originals(55)
    control_retry = check_retry_amplification(len(control["item_attempts"]), x_items, 7, 5, is_fault=False)
    assert control_retry["passed"], control_retry
    fault_retry = check_retry_amplification(len(fault["item_attempts"]), x_items, 7, 5, is_fault=True)
    assert fault_retry["passed"], fault_retry

    control_recon = check_reconciliation(control["item_attempts"], control["stub_stats"]["billed_calls"], is_fault=False)
    assert control_recon["passed"], control_recon
    fault_recon = check_reconciliation(fault["item_attempts"], fault["stub_stats"]["billed_calls"], is_fault=True)
    assert fault_recon["passed"], fault_recon

    overlap = check_overlap(fault["timeseries"])
    assert overlap["passed"], overlap

    assert fault["stub_stats"]["max_in_flight"] <= 2
    assert fault["stub_stats"]["over_capacity_calls"] == 0

    assert fault["kill_event"] is not None
    assert fault["kill_event"]["recovered_time"] is not None
    assert fault["kill_event"]["recovered_time"] - fault["kill_event"]["kill_time"] <= 10.0
