# tests/test_scenario_runner.py
import subprocess
from pathlib import Path

import pytest

from content_intake.generator.generate import generate_corpus
from content_intake.scenario.runner import run_execution

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_cli(*args):
    return subprocess.run(
        [str(REPO_ROOT / "intake"), *args], cwd=REPO_ROOT, capture_output=True, text=True
    )


@pytest.mark.slow
def test_run_execution_no_kill_completes_both_runs(tmp_path):
    corpus_a = tmp_path / "corpus-a"
    corpus_b = tmp_path / "corpus-b"
    generate_corpus(seed=10, size=50, tenant="tenant-a", out_dir=corpus_a)
    generate_corpus(seed=11, size=50, tenant="tenant-b", out_dir=corpus_b)

    assert _run_cli("down").returncode in (0, 1)
    assert _run_cli("up", "--workers", "4").returncode == 0
    try:
        execution = run_execution(
            corpus_a_dir=corpus_a, tenant_a="tenant-a",
            corpus_b_dir=corpus_b, tenant_b="tenant-b",
            kill=False, poll_interval=0.3, run_cli_fn=_run_cli,
        )
        assert execution["run_a"]["states"]
        assert execution["run_b"]["states"]
        assert execution["run_a"]["seed"] == 10
        assert execution["run_a"]["size"] == 50
        assert execution["run_b"]["seed"] == 11
        assert execution["run_b"]["size"] == 50
        assert execution["kill_event"] is None
        assert "billed_calls" in execution["stub_stats"]
        assert len(execution["timeseries"]) >= 1
    finally:
        _run_cli("down")


@pytest.mark.slow
def test_run_execution_with_kill_records_kill_event(tmp_path):
    corpus_a = tmp_path / "corpus-a"
    corpus_b = tmp_path / "corpus-b"
    generate_corpus(seed=12, size=80, tenant="tenant-a", out_dir=corpus_a)
    generate_corpus(seed=13, size=80, tenant="tenant-b", out_dir=corpus_b)

    assert _run_cli("down").returncode in (0, 1)
    assert _run_cli("up", "--workers", "4").returncode == 0
    try:
        execution = run_execution(
            corpus_a_dir=corpus_a, tenant_a="tenant-a",
            corpus_b_dir=corpus_b, tenant_b="tenant-b",
            kill=True, poll_interval=0.3, run_cli_fn=_run_cli,
        )
        assert execution["kill_event"] is not None
        assert execution["kill_event"]["recovered_time"] is not None
        assert execution["kill_event"]["recovered_time"] - execution["kill_event"]["kill_time"] < 10.0
    finally:
        _run_cli("down")
