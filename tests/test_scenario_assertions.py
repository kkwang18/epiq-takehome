# tests/test_scenario_assertions.py
from content_intake.scenario.assertions import (
    retry_amplification_bound,
    check_retry_amplification,
    check_reconciliation,
    check_overlap,
)


def test_retry_amplification_bound_control():
    # X=100, p=1/7, max_attempts=5 -> E ~= 116.66, *1.5 ~= 175.0 -> ceil = 175
    bound = retry_amplification_bound(x_items=100, failure_every_n=7, max_attempts=5, is_fault=False)
    assert bound == 175


def test_retry_amplification_bound_fault_adds_five():
    bound_control = retry_amplification_bound(x_items=100, failure_every_n=7, max_attempts=5, is_fault=False)
    bound_fault = retry_amplification_bound(x_items=100, failure_every_n=7, max_attempts=5, is_fault=True)
    assert bound_fault == bound_control + 5


def test_check_retry_amplification_passes_within_bound():
    result = check_retry_amplification(total_attempts=120, x_items=100, failure_every_n=7, max_attempts=5, is_fault=False)
    assert result["passed"] is True
    assert result["total_attempts"] == 120


def test_check_retry_amplification_fails_over_bound():
    result = check_retry_amplification(total_attempts=500, x_items=100, failure_every_n=7, max_attempts=5, is_fault=False)
    assert result["passed"] is False


def test_check_reconciliation_control_requires_zero_dangling():
    rows = [{"completed_at": "t"}, {"completed_at": "t"}, {"completed_at": None}]
    result = check_reconciliation(rows, billed_calls=2, is_fault=False)
    assert result["passed"] is False
    assert result["dangling"] == 1


def test_check_reconciliation_control_passes_when_clean():
    rows = [{"completed_at": "t"}, {"completed_at": "t"}]
    result = check_reconciliation(rows, billed_calls=2, is_fault=False)
    assert result["passed"] is True


def test_check_reconciliation_fault_allows_one_dangling():
    rows = [{"completed_at": "t"}, {"completed_at": None}]
    result = check_reconciliation(rows, billed_calls=2, is_fault=True)
    assert result["passed"] is True


def test_check_reconciliation_fault_rejects_two_dangling():
    rows = [{"completed_at": None}, {"completed_at": None}]
    result = check_reconciliation(rows, billed_calls=2, is_fault=True)
    assert result["passed"] is False


def test_check_overlap_passes_when_both_directions_present():
    timeseries = [
        {"t": 0, "run_a": {"terminal": 0, "nonterminal": 500}, "run_b": {"terminal": 0, "nonterminal": 0}},
        {"t": 1, "run_a": {"terminal": 2, "nonterminal": 498}, "run_b": {"terminal": 0, "nonterminal": 400}},
        {"t": 2, "run_a": {"terminal": 450, "nonterminal": 50}, "run_b": {"terminal": 1, "nonterminal": 399}},
        {"t": 3, "run_a": {"terminal": 500, "nonterminal": 0}, "run_b": {"terminal": 400, "nonterminal": 0}},
    ]
    result = check_overlap(timeseries)
    assert result["passed"] is True
    assert result["a_terminal_b_nonterminal"] is True
    assert result["b_terminal_a_nonterminal"] is True


def test_check_overlap_fails_when_sequential():
    # Run B never gets a terminal item while A still has nonterminal ones.
    timeseries = [
        {"t": 0, "run_a": {"terminal": 0, "nonterminal": 500}, "run_b": {"terminal": 0, "nonterminal": 0}},
        {"t": 1, "run_a": {"terminal": 500, "nonterminal": 0}, "run_b": {"terminal": 0, "nonterminal": 400}},
        {"t": 2, "run_a": {"terminal": 500, "nonterminal": 0}, "run_b": {"terminal": 400, "nonterminal": 0}},
    ]
    result = check_overlap(timeseries)
    assert result["passed"] is False
    assert result["b_terminal_a_nonterminal"] is False
