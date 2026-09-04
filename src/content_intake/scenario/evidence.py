# src/content_intake/scenario/evidence.py
import json
from pathlib import Path


def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2, default=str))


def write_raw_evidence(out_dir: Path, control: dict, fault: dict, assertion_results: list[dict]) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for label, execution in (("control", control), ("fault", fault)):
        _write_json(out_dir / f"{label}-run-a.json", execution["run_a"])
        _write_json(out_dir / f"{label}-run-b.json", execution["run_b"])
        _write_json(out_dir / f"{label}-stub-stats.json", execution["stub_stats"])
        _write_json(out_dir / f"{label}-timeseries.json", execution["timeseries"])
        if execution.get("kill_event") is not None:
            _write_json(out_dir / f"{label}-kill-event.json", execution["kill_event"])
    _write_json(out_dir / "assertions.json", assertion_results)


def write_evaluation_md(out_dir: Path, control: dict, fault: dict, assertion_results: list[dict]) -> None:
    out_dir = Path(out_dir)
    lines = ["# EVALUATION\n"]
    lines.append("## Assertions\n")
    lines.append("| ID | Description | Mandatory | Verdict | Detail |")
    lines.append("|---|---|---|---|---|")
    for a in assertion_results:
        verdict = "PASS" if a["passed"] else "FAIL"
        lines.append(f"| {a['id']} | {a['description']} | {a['mandatory']} | {verdict} | {a['detail']} |")
    lines.append("")
    for label, execution in (("Control", control), ("Fault", fault)):
        lines.append(f"## {label} execution\n")
        for run_key in ("run_a", "run_b"):
            run = execution[run_key]
            lines.append(f"- **{run['tenant']}** (run `{run['run_id']}`, seed {run['seed']}, size {run['size']}): "
                         f"submitted {run['submitted_at']}, terminal {run['terminal_at']}, states {run['states']}")
        lines.append(f"- Stub stats: {execution['stub_stats']}")
        if execution.get("kill_event") is not None:
            lines.append(f"- Kill event: {execution['kill_event']}")
        lines.append("")
    out_path = out_dir / "EVALUATION.md"
    out_path.write_text("\n".join(lines))
