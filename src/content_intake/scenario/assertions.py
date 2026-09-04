# src/content_intake/scenario/assertions.py
import math


def retry_amplification_bound(x_items: int, failure_every_n: int, max_attempts: int, is_fault: bool) -> int:
    p = 1.0 / failure_every_n
    expected = x_items * (1 - p ** max_attempts) / (1 - p)
    bound = math.ceil(expected * 1.5)
    if is_fault:
        bound += 5
    return bound


def check_retry_amplification(total_attempts: int, x_items: int, failure_every_n: int, max_attempts: int, is_fault: bool) -> dict:
    bound = retry_amplification_bound(x_items, failure_every_n, max_attempts, is_fault)
    return {"passed": total_attempts <= bound, "total_attempts": total_attempts, "bound": bound}


def check_reconciliation(item_attempts_rows: list[dict], billed_calls: int, is_fault: bool) -> dict:
    completed = sum(1 for r in item_attempts_rows if r["completed_at"] is not None)
    dangling = sum(1 for r in item_attempts_rows if r["completed_at"] is None)
    if is_fault:
        passed = dangling <= 1 and billed_calls == completed + dangling
    else:
        passed = dangling == 0 and billed_calls == completed
    return {"passed": passed, "completed": completed, "dangling": dangling, "billed_calls": billed_calls}


def check_overlap(timeseries: list[dict]) -> dict:
    a_terminal_b_nonterminal = any(
        s["run_a"]["terminal"] > 0 and s["run_b"]["nonterminal"] > 0 for s in timeseries
    )
    b_terminal_a_nonterminal = any(
        s["run_b"]["terminal"] > 0 and s["run_a"]["nonterminal"] > 0 for s in timeseries
    )
    return {
        "passed": a_terminal_b_nonterminal and b_terminal_a_nonterminal,
        "a_terminal_b_nonterminal": a_terminal_b_nonterminal,
        "b_terminal_a_nonterminal": b_terminal_a_nonterminal,
    }
