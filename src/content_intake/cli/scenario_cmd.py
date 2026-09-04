# src/content_intake/cli/scenario_cmd.py
import subprocess
import sys
from pathlib import Path

from content_intake.generator.generate import generate_corpus
from content_intake.scenario.assertions import check_retry_amplification, check_reconciliation, check_overlap
from content_intake.scenario.evidence import write_raw_evidence, write_evaluation_md
from content_intake.scenario.profile import DEFAULT_PROFILE
from content_intake.scenario.runner import run_execution

REPO_ROOT = Path(__file__).resolve().parents[3]


def add_scenario_parser(subparsers) -> None:
    subparsers.add_parser("scenario")


def _run_cli(*args) -> subprocess.CompletedProcess:
    return subprocess.run([str(REPO_ROOT / "intake"), *args], cwd=REPO_ROOT, capture_output=True, text=True)


def _x_items(execution: dict, profile) -> int:
    # Originals only: size - duplicates - edge_cases, summed across both runs.
    def originals(size: int) -> int:
        return size - round(size / 10) - 2
    return originals(profile.size_a) + originals(profile.size_b)


def _evaluate(control: dict, fault: dict, profile) -> list[dict]:
    results = []

    leasing = fault["kill_event"]
    results.append({"id": "A1", "description": "killed worker owned a nonterminal item",
                     "mandatory": True, "passed": leasing is not None,
                     "detail": str(leasing) if leasing else "no kill event recorded"})

    a2_passed = (
        leasing is not None and leasing["recovered_time"] is not None
        and (leasing["recovered_time"] - leasing["kill_time"]) <= 10.0
        and fault["run_a"]["states"].get("pending", 0) == 0
        and fault["run_a"]["states"].get("in_progress", 0) == 0
        and fault["run_b"]["states"].get("pending", 0) == 0
        and fault["run_b"]["states"].get("in_progress", 0) == 0
    )
    recovery_seconds = (leasing["recovered_time"] - leasing["kill_time"]) if (leasing and leasing["recovered_time"]) else None
    results.append({"id": "A2", "description": "recovery within 10s, every item terminates",
                     "mandatory": True, "passed": a2_passed, "detail": f"recovery_seconds={recovery_seconds}"})

    stats = fault["stub_stats"]
    a3_passed = stats["max_in_flight"] <= 2 and stats["over_capacity_calls"] == 0
    results.append({"id": "A3", "description": "max_in_flight<=2 and over_capacity_calls==0",
                     "mandatory": True, "passed": a3_passed, "detail": str(stats)})

    x_items = _x_items(control, profile)
    control_retry = check_retry_amplification(len(control["item_attempts"]), x_items, profile.failure_every_n, profile.max_attempts, is_fault=False)
    fault_retry = check_retry_amplification(len(fault["item_attempts"]), x_items, profile.failure_every_n, profile.max_attempts, is_fault=True)
    results.append({"id": "A4", "description": "retry amplification within D-06 bound",
                     "mandatory": True, "passed": control_retry["passed"] and fault_retry["passed"],
                     "detail": f"control={control_retry} fault={fault_retry}"})

    control_recon = check_reconciliation(control["item_attempts"], control["stub_stats"]["billed_calls"], is_fault=False)
    fault_recon = check_reconciliation(fault["item_attempts"], fault["stub_stats"]["billed_calls"], is_fault=True)
    results.append({"id": "A5", "description": "counters reconcile with item outcomes",
                     "mandatory": True, "passed": control_recon["passed"] and fault_recon["passed"],
                     "detail": f"control={control_recon} fault={fault_recon}"})

    overlap = check_overlap(fault["timeseries"])
    results.append({"id": "A6", "description": "genuine overlap, both directions",
                     "mandatory": True, "passed": overlap["passed"], "detail": str(overlap)})

    # Fetch one real item from run A to use for both the positive attribution check (A7) and
    # the negative cross-tenant check (A8) — a nonexistent ID would only prove "not found",
    # not that a REAL item belonging to tenant-a is actually hidden from tenant-b.
    import json as _json
    items_a = _run_cli("items", "--tenant", fault["run_a"]["tenant"], "--run", fault["run_a"]["run_id"])
    sample_item_id = None
    a7_passed = False
    parsed = None
    if items_a.returncode == 0:
        parsed = _json.loads(items_a.stdout)
        if parsed:
            sample_item_id = parsed[0]["item_id"]
            a7_passed = all(item["tenant"] == fault["run_a"]["tenant"] for item in parsed)
    results.append({"id": "A7", "description": "every item attributed to the correct run/tenant",
                     "mandatory": True, "passed": a7_passed,
                     "detail": f"checked {len(parsed) if parsed else 0} items from run_a all carry tenant={fault['run_a']['tenant']}"})

    if sample_item_id is not None:
        cross = _run_cli("item", "--tenant", fault["run_b"]["tenant"], "--id", sample_item_id)
        a8_passed = cross.returncode != 0
        a8_detail = f"real tenant-a item looked up as {fault['run_b']['tenant']}: exit_code={cross.returncode}"
    else:
        a8_passed = False
        a8_detail = "could not fetch a real item_id from run_a to test against"
    results.append({"id": "A8", "description": "cross-tenant lookup of a real item returns not found",
                     "mandatory": True, "passed": a8_passed, "detail": a8_detail})

    return results


def run_scenario(args) -> int:
    profile = DEFAULT_PROFILE
    generate_corpus(seed=profile.seed_a, size=profile.size_a, tenant=profile.tenant_a, out_dir=profile.corpus_a_dir, force=True)
    generate_corpus(seed=profile.seed_b, size=profile.size_b, tenant=profile.tenant_b, out_dir=profile.corpus_b_dir, force=True)

    if _run_cli("reset", "--workers", str(profile.workers)).returncode != 0:
        print("reset failed before control execution", file=sys.stderr)
        return 1
    control = run_execution(profile.corpus_a_dir, profile.tenant_a, profile.corpus_b_dir, profile.tenant_b,
                             kill=False, poll_interval=profile.poll_interval, run_cli_fn=_run_cli)

    if _run_cli("reset", "--workers", str(profile.workers)).returncode != 0:
        print("reset failed before fault execution", file=sys.stderr)
        return 1
    fault = run_execution(profile.corpus_a_dir, profile.tenant_a, profile.corpus_b_dir, profile.tenant_b,
                           kill=True, poll_interval=profile.poll_interval, run_cli_fn=_run_cli)

    assertion_results = _evaluate(control, fault, profile)

    _run_cli("down")

    raw_dir = REPO_ROOT / "evaluation" / "raw"
    write_raw_evidence(raw_dir, control, fault, assertion_results)
    write_evaluation_md(REPO_ROOT, control, fault, assertion_results)

    all_passed = True
    for a in assertion_results:
        status = "PASS" if a["passed"] else "FAIL"
        print(f"[{status}] {a['id']}: {a['description']} — {a['detail']}")
        if a["mandatory"] and not a["passed"]:
            all_passed = False

    return 0 if all_passed else 1
