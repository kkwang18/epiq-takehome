# tests/test_scenario_evidence.py
import json

from content_intake.scenario.evidence import write_raw_evidence, write_evaluation_md


def _sample_execution():
    return {
        "run_a": {"run_id": "ra", "tenant": "tenant-a", "seed": 1, "size": 500,
                   "submitted_at": "2026-01-01T00:00:00", "terminal_at": "2026-01-01T00:01:00",
                   "states": {"succeeded": 498, "empty_content": 1, "decode_failed": 1}},
        "run_b": {"run_id": "rb", "tenant": "tenant-b", "seed": 2, "size": 400,
                   "submitted_at": "2026-01-01T00:00:01", "terminal_at": "2026-01-01T00:01:05",
                   "states": {"succeeded": 398, "empty_content": 1, "decode_failed": 1}},
        "timeseries": [{"t": 0, "run_a": {"terminal": 0, "nonterminal": 500}, "run_b": {"terminal": 0, "nonterminal": 0}}],
        "kill_event": None,
        "stub_stats": {"billed_calls": 900, "current_in_flight": 0, "max_in_flight": 2,
                        "server_error_calls": 128, "over_capacity_calls": 0},
        "item_attempts": [{"completed_at": "t"}] * 900,
    }


def _sample_assertions():
    return [
        {"id": "A1", "description": "killed worker owned a nonterminal item", "mandatory": True, "passed": True, "detail": "worker-2 held item x"},
        {"id": "A6", "description": "genuine overlap", "mandatory": True, "passed": True, "detail": "both directions observed"},
    ]


def test_write_raw_evidence_creates_expected_files(tmp_path):
    control = _sample_execution()
    fault = _sample_execution()
    assertion_results = _sample_assertions()
    write_raw_evidence(tmp_path, control, fault, assertion_results)

    for name in [
        "control-run-a.json", "control-run-b.json", "control-stub-stats.json", "control-timeseries.json",
        "fault-run-a.json", "fault-run-b.json", "fault-stub-stats.json", "fault-timeseries.json",
        "assertions.json",
    ]:
        assert (tmp_path / name).exists(), f"missing {name}"

    run_a = json.loads((tmp_path / "control-run-a.json").read_text())
    assert run_a["run_id"] == "ra"
    assertions = json.loads((tmp_path / "assertions.json").read_text())
    assert assertions[0]["id"] == "A1"


def test_write_raw_evidence_writes_kill_event_only_when_present(tmp_path):
    control = _sample_execution()
    fault = _sample_execution()
    fault["kill_event"] = {"worker_id": "worker-2", "index": 2, "item_id": "x", "kill_time": 1.0, "recovered_time": 2.5}
    write_raw_evidence(tmp_path, control, fault, _sample_assertions())
    assert (tmp_path / "fault-kill-event.json").exists()
    assert not (tmp_path / "control-kill-event.json").exists()


def test_write_evaluation_md_includes_assertions_and_verdict(tmp_path):
    control = _sample_execution()
    fault = _sample_execution()
    write_evaluation_md(tmp_path, control, fault, _sample_assertions())
    content = (tmp_path / "EVALUATION.md").read_text()
    assert "A1" in content
    assert "A6" in content
    assert "PASS" in content
