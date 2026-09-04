# M2 Scenario Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rework the M1 retry mechanism per the revised D-05 (release-based backoff, durable claim-time attempt-cap enforcement), then build `./intake scenario` — a paired control/fault execution that proves the M2 robustness requirements (REQ-2.1–2.4) and produces `EVALUATION.md` + `evaluation/raw/` evidence.

**Architecture:** Phase 1 reworks `claim_item`/`process_item` in the existing `src/content_intake/pipeline/worker.py` so an item is processed one attempt per claim (not an in-process retry loop), with backoff expressed as a release-and-requeue rather than a sleep. Phase 2 adds a new `src/content_intake/scenario/` package (pure-function assertion/evidence logic, kept separate from the orchestration that drives live processes) plus a `./intake scenario` CLI command that runs the real `./intake` binary as a subprocess for lifecycle control (`reset`, `kill-worker`) exactly as `tests/test_m1_end_to_end.py` already does.

**Tech Stack:** Same as M1 — Python 3.11+, psycopg 3, httpx, the existing Postgres schema and Pipeline API.

**Spec:** [`DECISIONS.md`](../../../DECISIONS.md) (D-05 revision, D-06, D-07, D-15), [`ARCHITECTURE.md`](../../../ARCHITECTURE.md), [`docs/superpowers/specs/2026-09-04-m2-scenario-harness-design.md`](../specs/2026-09-04-m2-scenario-harness-design.md)

## Global Constraints

- Fixed evaluation profile: tenant-a 500 items seed 1, tenant-b 400 items seed 2, 4 workers, stub latency fixed 150ms / `failure_every_n=7` / `in_flight_capacity=2` in both executions, kill is the only injected difference.
- `./intake scenario` takes no flags, runs the fixed profile, prints each assertion pass/fail with reasons, exits 0 only if every mandatory assertion passes, writes evidence under `evaluation/raw/`.
- Execution isolation: each of the two executions (control, fault) starts from an empty pipeline state via `./intake reset` — no content or annotation records reused between them. Corpus *files* are generated once and reused (deterministic, content-addressed by seed+size — this is not part of "pipeline state").
- Mandatory assertions (exit-code-gating): REQ-2.2(a) killed worker owned a nonterminal item and stays dead; REQ-2.2(b) recovery within 10s and every item terminates; REQ-2.2(c) `max_in_flight<=2` and `over_capacity_calls==0`; REQ-2.2(d) retry amplification within the D-06 bound and counters reconcile; REQ-2.3 genuine overlap (both directions) plus correct attribution and the cross-tenant negative test.
- D-06 retry-amplification bound: `total_attempts <= ceil(X * 1.1666 * 1.5)` for control, `+5` for fault, where `X` = items across both runs that reach the stub (originals only) and `1.1666 = (1-(1/7)^5)/(1-1/7)`.
- Wall-clock time, items/sec, and the billed-call delta are reported as measurements only — no invented pass/fail threshold.
- Every mutating write in the worker stays conditioned on `leased_by = $worker_id` (unchanged invariant from M1).
- Postgres is already running via `docker compose up -d postgres` for all task verification; `./intake up`/`down`/`reset` manage the stub/API/worker processes on top of it.

---

## File Structure

```
epiq/
├── src/content_intake/
│   ├── pipeline/
│   │   ├── schema.sql                # + next_attempt_after column
│   │   └── worker.py                 # claim query (D-15 reorder, backoff clause), process_item restructure
│   ├── scenario/
│   │   ├── __init__.py
│   │   ├── profile.py                # ScenarioProfile dataclass, DEFAULT_PROFILE
│   │   ├── assertions.py             # pure functions: retry-amplification bound, reconciliation, overlap check
│   │   ├── fault.py                  # find_leasing_worker, inject_fault, wait_for_item_terminal
│   │   ├── runner.py                 # run_execution: submit both corpora, poll loop, timeseries, fault hook
│   │   └── evidence.py               # write_raw_evidence, write_evaluation_md
│   └── cli/
│       └── scenario_cmd.py           # ./intake scenario: orchestrates reset/generate/two executions/evaluate/report
├── tests/
│   ├── test_worker_claim.py          # + next_attempt_after / D-15 ordering tests
│   ├── test_worker_process_item.py   # rewritten retry-related tests for single-attempt/release shape
│   ├── test_scenario_assertions.py
│   ├── test_scenario_fault.py
│   └── test_scenario_end_to_end.py   # slow, scaled-down profile, proves the whole flow
├── evaluation/raw/                   # produced by running the real scenario (Task 9)
├── EVALUATION.md                     # produced by Task 9
└── .gitignore                        # + .scenario_corpora/
```

---

### Task 1: Schema and claim query — `next_attempt_after` and D-15 ordering

**Files:**
- Modify: `src/content_intake/pipeline/schema.sql`
- Modify: `src/content_intake/pipeline/worker.py` (the `claim_item` function)
- Test: `tests/test_worker_claim.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `claim_item` keeps its existing signature `claim_item(conn, worker_id: str, lease_seconds: int) -> dict | None` — only its SQL and the schema change. Task 2 relies on the claim query correctly excluding items whose `next_attempt_after` is in the future.

- [ ] **Step 1: Add the column to the schema**

```sql
-- add to src/content_intake/pipeline/schema.sql, inside the items table definition,
-- right after leased_until
    next_attempt_after TIMESTAMPTZ,
```

Also add an idempotent migration line right after the `CREATE TABLE` block for `items` (so a
database created before this change picks up the column without a manual drop, matching the
`order_index` migration pattern already in the file):

```sql
ALTER TABLE items ADD COLUMN IF NOT EXISTS next_attempt_after TIMESTAMPTZ;
```

- [ ] **Step 2: Write the failing tests**

```python
# add to tests/test_worker_claim.py
def test_pending_item_with_future_next_attempt_after_is_not_claimed(db_conn):
    run_id, item_id = _insert_run_and_item(db_conn)
    db_conn.execute(
        "UPDATE items SET next_attempt_after = now() + interval '30 seconds' WHERE item_id = %s",
        (item_id,),
    )
    db_conn.commit()
    assert claim_item(db_conn, "worker-0", lease_seconds=5) is None


def test_pending_item_with_past_next_attempt_after_is_claimed(db_conn):
    run_id, item_id = _insert_run_and_item(db_conn)
    db_conn.execute(
        "UPDATE items SET next_attempt_after = now() - interval '1 second' WHERE item_id = %s",
        (item_id,),
    )
    db_conn.commit()
    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    assert claimed["item_id"] == item_id


def test_pending_item_with_null_next_attempt_after_is_claimed(db_conn):
    run_id, item_id = _insert_run_and_item(db_conn)
    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    assert claimed["item_id"] == item_id


def test_claim_order_is_order_index_first_across_runs(db_conn, tmp_path):
    # Two runs, each with two items. Run A submitted first (earlier created_at) but its
    # order_index=1 item should NOT jump ahead of run B's order_index=0 item.
    run_a = str(uuid.uuid4())
    run_b = str(uuid.uuid4())
    db_conn.execute(
        "INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, 'c-a', 'tenant-a', %s)",
        (run_a, str(tmp_path)),
    )
    db_conn.commit()
    import time as _time
    _time.sleep(0.01)  # ensure run_b's created_at is strictly later
    db_conn.execute(
        "INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, 'c-b', 'tenant-b', %s)",
        (run_b, str(tmp_path)),
    )
    db_conn.commit()

    def _insert(run_id, tenant, order_index):
        item_id = str(uuid.uuid4())
        db_conn.execute(
            """
            INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                                role, order_index, expects_annotation, state)
            VALUES (%s, %s, %s, 'p', 'txt', 5, %s, 'original', %s, true, 'pending')
            """,
            (item_id, run_id, tenant, item_id, order_index),
        )
        db_conn.commit()
        return item_id

    a0 = _insert(run_a, "tenant-a", 0)
    a1 = _insert(run_a, "tenant-a", 1)
    b0 = _insert(run_b, "tenant-b", 0)

    first = claim_item(db_conn, "worker-0", lease_seconds=5)
    second = claim_item(db_conn, "worker-1", lease_seconds=5)
    third = claim_item(db_conn, "worker-2", lease_seconds=5)

    # order_index 0 from both runs claimed before either run's order_index 1 —
    # run A's created_at is earlier, so ties at order_index=0 break toward A.
    assert first["item_id"] == a0
    assert second["item_id"] == b0
    assert third["item_id"] == a1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_worker_claim.py -v`
Expected: FAIL — schema doesn't have `next_attempt_after` yet, and the claim order doesn't match

- [ ] **Step 3: Update the claim query**

This also replaces the function's existing inline comment (the current one explains the old
`created_at, order_index` rationale, which D-15 reverses — leaving it in place would describe
the wrong ordering):

```python
# replace claim_item (including its inline comment) in src/content_intake/pipeline/worker.py
def claim_item(conn, worker_id: str, lease_seconds: int) -> dict | None:
    row = conn.execute(
        """
        UPDATE items
        SET state = 'in_progress', leased_by = %(worker_id)s,
            leased_until = now() + %(lease_seconds)s * interval '1 second'
        WHERE item_id = (
            SELECT item_id FROM items
            WHERE (state = 'pending' AND (next_attempt_after IS NULL OR next_attempt_after <= now()))
               OR (state = 'in_progress' AND leased_until < now())
            -- order_index first (D-15): every item in a run shares one created_at (submit_run's
            -- single transaction), so created_at-first would let one run's whole backlog drain
            -- before a concurrently-submitted second run is ever touched. order_index starts at
            -- 0 independently per run, so sorting by it first interleaves runs fairly ("position
            -- 0 from whichever run has one, then position 1, ..."), while created_at still
            -- breaks ties and order_index still keeps originals ahead of their duplicates within
            -- one run (D-02) exactly as before. next_attempt_after is D-05's backoff timer: a
            -- pending item that just failed a retryable attempt isn't reclaimable until it passes.
            ORDER BY order_index, created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        """,
        {"worker_id": worker_id, "lease_seconds": lease_seconds},
    ).fetchone()
    conn.commit()
    if row is None:
        return None
    return {k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in row.items()}
```

Apply the updated schema against the live Postgres before running tests (the migration line
handles a pre-existing database, but you still need to actually run it once):

```bash
python3 -c "
from content_intake.pipeline.db import connect, apply_schema
conn = connect()
apply_schema(conn)
conn.close()
print('schema updated')
"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_worker_claim.py -v`
Expected: all pass (10 total: 6 pre-existing + 4 new)

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/pipeline/schema.sql src/content_intake/pipeline/worker.py tests/test_worker_claim.py
git commit -m "Add next_attempt_after column; reorder claim query to order_index-first (D-05, D-15)"
```

---

### Task 2: `process_item` restructure — single attempt, claim-time cap, release-with-backoff

**Files:**
- Modify: `src/content_intake/pipeline/worker.py` (`process_item`, plus a new `_classify_outcome` helper)
- Test: `tests/test_worker_process_item.py` (retry-related tests rewritten; edge-case/cache/tenant-isolation/prior-success tests are unaffected and stay as-is)

**Interfaces:**
- Consumes: `claim_slot`, `release_slot`, `start_attempt`, `complete_attempt`, `find_completed_success` (unchanged from M1), `call_annotate` (unchanged), `detect_edge_case`/`extract_text` (unchanged).
- Produces: `process_item(conn, connect_fn, item, corpus_files_dir, stub_client, stub_base_url, worker_id) -> None` — same signature as M1, but now resolves **at most one attempt** per call instead of looping internally. A claimed item that needs another attempt after this call returns will be `state='pending'` again (not `in_progress`), picked up by a future claim (this worker's or another's).

This task's tests exercise the *whole* multi-attempt lifecycle of one item by calling
`process_item` repeatedly (each call simulating one worker-cycle claim), not by expecting one
call to resolve everything — that shape change is the point of this task.

- [ ] **Step 1: Write the failing tests** (replace the existing retry-related tests in
  `tests/test_worker_process_item.py` — leave every other test in that file untouched)

```python
# replace test_400_terminates_immediately_as_invalid_request and
# test_exhausted_retries_lands_in_annotation_failed and
# test_start_attempt_race_is_handled_without_crashing in tests/test_worker_process_item.py
# with the following (keep the file's existing imports and _insert_item helper):

def test_400_terminates_immediately_as_invalid_request(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(400, json={"error": {"code": "invalid_request"}})

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect, claimed, tmp_path,
                 httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state, reason FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "annotation_invalid_request"
    assert len(calls) == 1


def test_retryable_failure_releases_item_instead_of_looping(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(500, json={"error": {"code": "server_error"}})

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect, claimed, tmp_path,
                 httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    assert len(calls) == 1  # exactly one attempt this call, not an internal loop
    row = db_conn.execute(
        "SELECT state, leased_by, leased_until, next_attempt_after FROM items WHERE item_id = %s",
        (item_id,),
    ).fetchone()
    assert row["state"] == "pending"
    assert row["leased_by"] is None
    assert row["leased_until"] is None
    assert row["next_attempt_after"] is not None
    attempts = db_conn.execute(
        "SELECT COUNT(*) AS n FROM item_attempts WHERE item_id = %s", (item_id,)
    ).fetchone()
    assert attempts["n"] == 1


def test_five_claims_of_a_permanently_failing_item_lands_in_annotation_failed(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)

    def handler(request):
        return httpx.Response(500, json={"error": {"code": "server_error"}})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    for i in range(5):
        # simulate the item's next_attempt_after having already passed by clearing it,
        # since this test doesn't want to wait on real backoff timers
        db_conn.execute(
            "UPDATE items SET next_attempt_after = NULL WHERE item_id = %s", (item_id,)
        )
        db_conn.commit()
        claimed = claim_item(db_conn, f"worker-{i}", lease_seconds=5)
        assert claimed is not None, f"claim {i} returned nothing"
        process_item(db_conn, connect, claimed, tmp_path, client, "http://stub", f"worker-{i}")

    row = db_conn.execute("SELECT state, reason FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "annotation_failed"
    assert row["reason"]["attempts"] == 5
    attempts = db_conn.execute(
        "SELECT COUNT(*) AS n FROM item_attempts WHERE item_id = %s", (item_id,)
    ).fetchone()
    assert attempts["n"] == 5


def test_claim_time_cap_enforced_even_after_a_worker_death_resets_nothing(db_conn, tmp_path):
    # Simulates 5 attempts already durably recorded (as if by workers that then died before
    # finalizing), and asserts a FRESH claim of this item does not make a 6th HTTP call —
    # it must finalize as annotation_failed purely from the durable count.
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)
    for i in range(5):
        attempt_id = str(uuid.uuid4())
        db_conn.execute(
            "INSERT INTO item_attempts (attempt_id, item_id, tenant, attempt_no, started_at, "
            "worker_id, slot_id, completed_at, http_status, outcome) "
            "VALUES (%s, %s, 'tenant-a', %s, now(), 'worker-dead', 1, now(), 500, 'server_error')",
            (attempt_id, item_id, i + 1),
        )
    db_conn.commit()

    def handler(request):
        raise AssertionError("must not call the stub once the durable cap is already reached")

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect, claimed, tmp_path,
                 httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "annotation_failed"


def test_start_attempt_race_is_handled_without_crashing(db_conn, tmp_path, monkeypatch):
    import psycopg
    import content_intake.pipeline.worker as worker_mod

    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)
    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)

    def fake_start_attempt(conn, item_id, tenant, worker_id, slot_id):
        raise psycopg.errors.UniqueViolation("simulated race: attempt_no already taken")

    monkeypatch.setattr(worker_mod, "start_attempt", fake_start_attempt)

    def handler(request):
        raise AssertionError("stub must not be called when start_attempt loses the race")

    process_item(db_conn, connect, claimed, tmp_path,
                 httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")

    slots = db_conn.execute("SELECT held_by FROM stub_call_slots ORDER BY slot_id").fetchall()
    assert all(s["held_by"] is None for s in slots)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_worker_process_item.py -v`
Expected: FAIL — `test_retryable_failure_releases_item_instead_of_looping` and the 5-claim/cap
tests fail because the current implementation still loops internally and never releases

- [ ] **Step 3: Restructure `process_item`**

Replace the entire retry-loop body of `process_item` in `src/content_intake/pipeline/worker.py`
(everything from the `renewer = LeaseRenewer(...)` line through the end of the function) with:

```python
def _classify_outcome(result) -> tuple[int | None, str]:
    if result.error is not None:
        return None, result.error
    status = result.status_code
    if status == 200:
        return status, "success"
    if status == 400:
        return status, "invalid_request"
    if status in RETRYABLE_STATUSES:
        return status, "server_error" if status == 500 else "over_capacity"
    return status, "unexpected_status"


# Replace from "renewer = LeaseRenewer(..." to the end of process_item with:
    cap_count = conn.execute(
        "SELECT COUNT(*) AS n FROM item_attempts WHERE item_id = %s", (item_id,)
    ).fetchone()["n"]
    if cap_count >= MAX_ATTEMPTS:
        _finalize(conn, item_id, worker_id, "annotation_failed", extracted_text=extracted,
                  reason={"code": "annotation_failed", "attempts": cap_count, "note": "cap_reached_at_claim"})
        return

    renewer = LeaseRenewer(connect_fn, item_id, worker_id, lease_seconds=config.LEASE_SECONDS, interval=config.LEASE_RENEW_INTERVAL)
    renewer.start()
    try:
        if renewer.lost():
            return
        slot_id = None
        while slot_id is None:
            slot_id = claim_slot(conn, worker_id, lease_seconds=config.LEASE_SECONDS)
            if slot_id is None:
                time.sleep(0.05 + random.uniform(0, 0.05))

        outcome = None
        annotation = None
        attempt_no = None
        last_status = None
        try:
            try:
                attempt_id, attempt_no = start_attempt(conn, item_id, tenant, worker_id, slot_id)
            except psycopg.errors.UniqueViolation:
                conn.rollback()
                return
            result = call_annotate(stub_client, stub_base_url, data, timeout=HTTP_TIMEOUT_SECONDS)
            last_status, outcome = _classify_outcome(result)
            if outcome == "success":
                annotation = result.body
                conn.execute(
                    "INSERT INTO annotations_cache (tenant, sha256, annotation) VALUES (%s, %s, %s) "
                    "ON CONFLICT (tenant, sha256) DO NOTHING",
                    (tenant, item["sha256"], json.dumps(annotation)),
                )
                conn.commit()
            complete_attempt(conn, attempt_id, last_status, outcome)
        finally:
            release_slot(conn, slot_id, worker_id)

        if outcome == "success":
            _finalize(conn, item_id, worker_id, "succeeded", annotation=annotation, extracted_text=extracted)
        elif outcome == "invalid_request":
            _finalize(conn, item_id, worker_id, "annotation_invalid_request",
                      extracted_text=extracted, reason={"code": "invalid_request", "attempt": attempt_no})
        elif attempt_no >= MAX_ATTEMPTS:
            _finalize(conn, item_id, worker_id, "annotation_failed", extracted_text=extracted,
                      reason={"code": "annotation_failed", "attempts": attempt_no, "last_status": last_status})
        else:
            delay = _backoff_delay(attempt_no)
            conn.execute(
                "UPDATE items SET state = 'pending', leased_by = NULL, leased_until = NULL, "
                "next_attempt_after = now() + %(delay)s * interval '1 second' "
                "WHERE item_id = %(item_id)s AND leased_by = %(worker_id)s",
                {"delay": delay, "item_id": item_id, "worker_id": worker_id},
            )
            conn.commit()
    finally:
        renewer.stop()
```

Keep everything ABOVE the `renewer = LeaseRenewer(...)` line unchanged (the prior-success check,
edge-case detection, cache lookup) — only the retry mechanics below it change.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_worker_process_item.py -v`
Expected: all pass

- [ ] **Step 5: Run the full non-slow suite to confirm no regressions elsewhere**

Run: `pytest -v` (with Postgres up; stub not required for this subset)
Expected: all pass except the 10 pre-existing `test_stub_conformance.py` errors (needs a live stub — unrelated to this task)

- [ ] **Step 6: Commit**

```bash
git add src/content_intake/pipeline/worker.py tests/test_worker_process_item.py
git commit -m "Restructure process_item: single attempt per claim, durable cap check, release-with-backoff (D-05)"
```

---

### Task 3: Scenario profile and pure assertion functions

**Files:**
- Create: `src/content_intake/scenario/__init__.py` (empty)
- Create: `src/content_intake/scenario/profile.py`
- Create: `src/content_intake/scenario/assertions.py`
- Test: `tests/test_scenario_assertions.py`

**Interfaces:**
- Produces: `ScenarioProfile` dataclass, `DEFAULT_PROFILE`; `retry_amplification_bound(x_items, failure_every_n, max_attempts, is_fault) -> int`, `check_retry_amplification(total_attempts, x_items, failure_every_n, max_attempts, is_fault) -> dict`, `check_reconciliation(item_attempts_rows, billed_calls, is_fault) -> dict`, `check_overlap(timeseries) -> dict`. Task 6 (`runner.py`) and Task 7 (`cli/scenario_cmd.py`) consume all of these.

- [ ] **Step 1: Write `profile.py`** (no test — plain data)

```python
# src/content_intake/scenario/profile.py
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass
class ScenarioProfile:
    tenant_a: str
    seed_a: int
    size_a: int
    tenant_b: str
    seed_b: int
    size_b: int
    corpus_a_dir: Path
    corpus_b_dir: Path
    workers: int = 4
    poll_interval: float = 0.5
    failure_every_n: int = 7
    max_attempts: int = 5


DEFAULT_PROFILE = ScenarioProfile(
    tenant_a="tenant-a", seed_a=1, size_a=500,
    tenant_b="tenant-b", seed_b=2, size_b=400,
    corpus_a_dir=REPO_ROOT / ".scenario_corpora" / "tenant-a",
    corpus_b_dir=REPO_ROOT / ".scenario_corpora" / "tenant-b",
)
```

- [ ] **Step 2: Write the failing tests**

```python
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
        {"t": 2, "run_a": {"terminal": 500, "nonterminal": 0}, "run_b": {"terminal": 1, "nonterminal": 399}},
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_scenario_assertions.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `assertions.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_scenario_assertions.py -v`
Expected: 10 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/scenario/__init__.py src/content_intake/scenario/profile.py \
        src/content_intake/scenario/assertions.py tests/test_scenario_assertions.py
git commit -m "Add scenario profile and pure D-06/reconciliation/overlap assertion functions"
```

---

### Task 4: Fault injection

**Files:**
- Create: `src/content_intake/scenario/fault.py`
- Test: `tests/test_scenario_fault.py`

**Interfaces:**
- Consumes: `connect()` (Task 3 of M1's plan, `content_intake.pipeline.db`).
- Produces: `find_leasing_worker(conn) -> dict | None` (returns `{"leased_by": str, "item_id": str}` or `None`), `worker_index_from_id(worker_id: str) -> int`, `wait_for_item_terminal(conn, item_id: str, timeout: float = 10.0, interval: float = 0.2) -> float | None` (returns a `time.time()` value or `None` on timeout). Task 6 (`runner.py`) composes these with an actual `kill-worker` CLI call.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_scenario_fault.py
import time
import uuid

from content_intake.scenario.fault import (
    find_leasing_worker,
    worker_index_from_id,
    wait_for_item_terminal,
)


def _insert_run_and_item(conn, tmp_path, state="pending", leased_by=None):
    run_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, 'c1', 'tenant-a', %s)",
        (run_id, str(tmp_path)),
    )
    conn.execute(
        """
        INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                            role, order_index, expects_annotation, state, leased_by)
        VALUES (%s, %s, 'tenant-a', 'p', 'txt', 5, 'abc', 'original', 0, true, %s, %s)
        """,
        (item_id, run_id, state, leased_by),
    )
    conn.commit()
    return run_id, item_id


def test_find_leasing_worker_returns_none_when_nothing_in_progress(db_conn, tmp_path):
    _insert_run_and_item(db_conn, tmp_path, state="pending")
    assert find_leasing_worker(db_conn) is None


def test_find_leasing_worker_returns_a_worker_and_item(db_conn, tmp_path):
    run_id, item_id = _insert_run_and_item(db_conn, tmp_path, state="in_progress", leased_by="worker-2")
    result = find_leasing_worker(db_conn)
    assert result is not None
    assert result["leased_by"] == "worker-2"
    assert str(result["item_id"]) == item_id


def test_worker_index_from_id():
    assert worker_index_from_id("worker-0") == 0
    assert worker_index_from_id("worker-3") == 3


def test_wait_for_item_terminal_returns_time_once_terminal(db_conn, tmp_path):
    run_id, item_id = _insert_run_and_item(db_conn, tmp_path, state="in_progress", leased_by="worker-0")

    import threading

    def finalize_after_delay():
        time.sleep(0.3)
        db_conn2 = None
        from content_intake.pipeline.db import connect
        c = connect()
        c.execute("UPDATE items SET state = 'succeeded' WHERE item_id = %s", (item_id,))
        c.commit()
        c.close()

    t = threading.Thread(target=finalize_after_delay)
    t.start()
    recovered_at = wait_for_item_terminal(db_conn, item_id, timeout=2.0, interval=0.1)
    t.join()
    assert recovered_at is not None


def test_wait_for_item_terminal_returns_none_on_timeout(db_conn, tmp_path):
    run_id, item_id = _insert_run_and_item(db_conn, tmp_path, state="in_progress", leased_by="worker-0")
    result = wait_for_item_terminal(db_conn, item_id, timeout=0.3, interval=0.1)
    assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_scenario_fault.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `fault.py`**

```python
# src/content_intake/scenario/fault.py
import time


def find_leasing_worker(conn) -> dict | None:
    row = conn.execute(
        "SELECT leased_by, item_id FROM items WHERE state = 'in_progress' AND leased_by IS NOT NULL LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    return {"leased_by": row["leased_by"], "item_id": str(row["item_id"])}


def worker_index_from_id(worker_id: str) -> int:
    return int(worker_id.rsplit("-", 1)[-1])


def wait_for_item_terminal(conn, item_id: str, timeout: float = 10.0, interval: float = 0.2) -> float | None:
    start = time.time()
    while time.time() - start < timeout:
        row = conn.execute("SELECT state FROM items WHERE item_id = %s", (item_id,)).fetchone()
        if row is not None and row["state"] not in ("pending", "in_progress"):
            return time.time()
        time.sleep(interval)
    return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_scenario_fault.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/scenario/fault.py tests/test_scenario_fault.py
git commit -m "Add fault-injection primitives: leasing-worker lookup, index parsing, recovery wait"
```

---

### Task 5: Evidence writers

**Files:**
- Create: `src/content_intake/scenario/evidence.py`
- Test: `tests/test_scenario_evidence.py`

**Interfaces:**
- Consumes: nothing from earlier tasks directly — takes plain dicts/lists as arguments so it can be tested without a live execution.
- Produces: `write_raw_evidence(out_dir, control, fault, assertion_results) -> None`, `write_evaluation_md(out_dir, control, fault, assertion_results) -> None`. Task 7 (`cli/scenario_cmd.py`) calls both.

- [ ] **Step 1: Write the failing tests**

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_scenario_evidence.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `evidence.py`**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_scenario_evidence.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/scenario/evidence.py tests/test_scenario_evidence.py
git commit -m "Add raw evidence and EVALUATION.md writers"
```

---

### Task 6: Execution runner

**Files:**
- Create: `src/content_intake/scenario/runner.py`
- Test: `tests/test_scenario_runner.py`

**Interfaces:**
- Consumes: `find_leasing_worker`, `worker_index_from_id`, `wait_for_item_terminal` (Task 4); `connect()` (M1); the Pipeline API's `POST /v1/runs` and `GET /v1/runs/{run_id}/status` (M1, via `httpx`); `config.API_BASE_URL`/`config.STUB_BASE_URL` (M1).
- Produces: `run_execution(corpus_a_dir, tenant_a, corpus_b_dir, tenant_b, kill: bool, poll_interval: float, run_cli_fn) -> dict` — the exact shape consumed by Task 5's `write_raw_evidence`/`write_evaluation_md` and Task 3's `check_overlap`/`check_reconciliation` (`run_a`, `run_b`, `timeseries`, `kill_event`, `stub_stats`, `item_attempts`, each shaped as in Task 5's `_sample_execution()`).

This task is integration-level — it needs a live Pipeline API, stub, and at least one live worker
to test meaningfully. Test it against a small real corpus rather than mocking the HTTP calls, the
same way `tests/test_m1_end_to_end.py` tests the CLI.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_scenario_runner.py -v -m slow`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `runner.py`**

```python
# src/content_intake/scenario/runner.py
import time
from pathlib import Path

import httpx

from content_intake.common import config
from content_intake.pipeline.db import connect
from content_intake.scenario.fault import find_leasing_worker, worker_index_from_id, wait_for_item_terminal


def _submit(corpus_dir: Path, tenant: str) -> str:
    resp = httpx.post(f"{config.API_BASE_URL}/v1/runs", json={"corpus_dir": str(Path(corpus_dir).resolve()), "tenant": tenant})
    resp.raise_for_status()
    return resp.json()["run_id"]


def _status(run_id: str) -> dict:
    resp = httpx.get(f"{config.API_BASE_URL}/v1/runs/{run_id}/status")
    resp.raise_for_status()
    return resp.json()


def _terminal_split(status: dict) -> dict:
    states = status["states"]
    nonterminal = states.get("pending", 0) + states.get("in_progress", 0)
    total = sum(states.values())
    return {"terminal": total - nonterminal, "nonterminal": nonterminal}


def _fetch_item_attempts(conn, run_ids: list[str]) -> list[dict]:
    rows = conn.execute(
        "SELECT ia.completed_at, ia.http_status, ia.outcome, ia.attempt_no "
        "FROM item_attempts ia JOIN items i ON i.item_id = ia.item_id "
        "WHERE i.run_id = ANY(%s)",
        (run_ids,),
    ).fetchall()
    return [dict(r) for r in rows]


def _inject_fault(conn, run_cli_fn) -> dict:
    leasing = find_leasing_worker(conn)
    if leasing is None:
        raise RuntimeError("no worker currently holds a lease on a nonterminal item")
    index = worker_index_from_id(leasing["leased_by"])
    kill_time = time.time()
    run_cli_fn("kill-worker", "--index", str(index))
    recovered_time = wait_for_item_terminal(conn, leasing["item_id"], timeout=10.0, interval=0.2)
    return {
        "worker_id": leasing["leased_by"], "index": index, "item_id": leasing["item_id"],
        "kill_time": kill_time, "recovered_time": recovered_time,
    }


def run_execution(corpus_a_dir: Path, tenant_a: str, corpus_b_dir: Path, tenant_b: str,
                   kill: bool, poll_interval: float, run_cli_fn) -> dict:
    run_id_a = _submit(corpus_a_dir, tenant_a)
    run_id_b = _submit(corpus_b_dir, tenant_b)
    submitted_at = time.time()

    timeseries = []
    kill_event = None
    conn = connect()
    try:
        while True:
            status_a = _status(run_id_a)
            status_b = _status(run_id_b)
            split_a = _terminal_split(status_a)
            split_b = _terminal_split(status_b)
            timeseries.append({"t": time.time(), "run_a": split_a, "run_b": split_b})

            if (kill and kill_event is None
                    and split_a["terminal"] > 0 and split_a["nonterminal"] > 0
                    and split_b["terminal"] > 0 and split_b["nonterminal"] > 0):
                kill_event = _inject_fault(conn, run_cli_fn)

            if split_a["nonterminal"] == 0 and split_b["nonterminal"] == 0:
                break
            time.sleep(poll_interval)

        terminal_at = time.time()
        stub_stats = httpx.get(f"{config.STUB_BASE_URL}/v1/stats").json()
        item_attempts = _fetch_item_attempts(conn, [run_id_a, run_id_b])
    finally:
        conn.close()

    final_a = _status(run_id_a)
    final_b = _status(run_id_b)
    return {
        "run_a": {"run_id": run_id_a, "tenant": tenant_a, "submitted_at": submitted_at,
                  "terminal_at": terminal_at, "states": final_a["states"]},
        "run_b": {"run_id": run_id_b, "tenant": tenant_b, "submitted_at": submitted_at,
                  "terminal_at": terminal_at, "states": final_b["states"]},
        "timeseries": timeseries,
        "kill_event": kill_event,
        "stub_stats": stub_stats,
        "item_attempts": item_attempts,
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_scenario_runner.py -v -m slow`
Expected: 2 passed (allow a couple of minutes — each test brings up the full environment)

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/scenario/runner.py tests/test_scenario_runner.py
git commit -m "Add scenario execution runner: submit both corpora, poll with timeseries, fault hook"
```

---

### Task 7: `./intake scenario` CLI wiring

**Files:**
- Create: `src/content_intake/cli/scenario_cmd.py`
- Modify: `src/content_intake/cli/main.py`

**Interfaces:**
- Consumes: `DEFAULT_PROFILE` (Task 3), `run_execution` (Task 6), `check_retry_amplification`/`check_reconciliation`/`check_overlap` (Task 3), `write_raw_evidence`/`write_evaluation_md` (Task 5), `generate_corpus` (M1).
- Produces: `run_scenario(args) -> int` — the full orchestration; `main.py` dispatches `scenario` to it.

- [ ] **Step 1: Write `scenario_cmd.py`**

```python
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
    a1_passed = leasing is not None and leasing["recovered_time"] is not None
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
    if items_a.returncode == 0:
        parsed = _json.loads(items_a.stdout)
        if parsed:
            sample_item_id = parsed[0]["item_id"]
            a7_passed = all(item["tenant"] == fault["run_a"]["tenant"] for item in parsed)
    results.append({"id": "A7", "description": "every item attributed to the correct run/tenant",
                     "mandatory": True, "passed": a7_passed,
                     "detail": f"checked {len(parsed) if items_a.returncode == 0 and parsed else 0} items from run_a all carry tenant={fault['run_a']['tenant']}"})

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

    _run_cli("down")

    assertion_results = _evaluate(control, fault, profile)

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
```

- [ ] **Step 2: Wire into `main.py`**

```python
# add to src/content_intake/cli/main.py
from content_intake.cli.scenario_cmd import add_scenario_parser, run_scenario
# register: add_scenario_parser(subparsers)
# dispatch: elif args.command == "scenario": return run_scenario(args)
```

- [ ] **Step 3: Add `.scenario_corpora/` to `.gitignore`**

```
# append to .gitignore
.scenario_corpora/
```

- [ ] **Step 4: Verify the module imports cleanly and the CLI dispatches**

Run: `source .venv/bin/activate && ./intake --help` (or run any existing subcommand, e.g.
`./intake version`) to confirm adding the `scenario` import didn't break argument parsing.
Expected: works exactly as before, `scenario` now listed as a subcommand.

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/cli/scenario_cmd.py src/content_intake/cli/main.py .gitignore
git commit -m "Wire ./intake scenario: full paired control/fault orchestration and reporting"
```

---

### Task 8: End-to-end proof with a scaled-down profile

**Files:**
- Test: `tests/test_scenario_end_to_end.py`

**Interfaces:**
- Consumes: `run_scenario` is not called directly (it hardcodes `DEFAULT_PROFILE`) — this test drives `./intake scenario` won't work for a scaled-down profile, so instead it calls the same building blocks `run_scenario` uses (`run_execution`, the `_evaluate`-equivalent assertion checks, `write_raw_evidence`) directly, with a small profile, to prove the whole assertion pipeline computes sensible results end to end before Task 9 spends the time on the real 900-item profile.

- [ ] **Step 1: Write the test**

```python
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
    fault_retry = check_retry_amplification(len(fault["item_attempts"]), x_items, 7, 5, is_fault=True)
    assert control_retry["passed"], control_retry
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
```

- [ ] **Step 2: Run it**

Run: `pytest tests/test_scenario_end_to_end.py -v -m slow`
Expected: passes. If `A4`/`A5`-equivalent assertions fail, that's a real signal to investigate
(don't loosen the bound — re-check the retry mechanism from Task 2, since these thresholds are
derived, not arbitrary).

- [ ] **Step 3: Commit**

```bash
git add tests/test_scenario_end_to_end.py
git commit -m "Add scaled-down scenario end-to-end proof (retry amplification, reconciliation, overlap, recovery)"
```

---

### Task 9: Run the real profile, produce the graded evidence, update the README

**Files:**
- Modify: `README.md`
- Create (by running the CLI, not hand-written): `EVALUATION.md`, `evaluation/raw/*`

**Interfaces:** none — this task's deliverable is the actual evidence, produced by actually
running `./intake scenario` against the real, fixed profile.

- [ ] **Step 1: Run the real scenario**

```bash
source .venv/bin/activate
./intake scenario
```

This takes several minutes (900 items across two executions, plus the ~1-in-7 failure rate's
retries). Let it run to completion.

- [ ] **Step 2: Read the printed assertion output and `EVALUATION.md`**

If every mandatory assertion passed, proceed to Step 3. If any failed, do not edit the assertion
logic to make it pass — investigate whether it's a real defect (fix it, following the normal
TDD workflow, then re-run the full scenario from Step 1) or a genuine, explainable limitation
(document it plainly in `EVALUATION.md` as the standing instructions require: state whether it's
an expected limitation, a scope cut, or a defect, and what you'd change).

- [ ] **Step 3: Verify `evaluation/raw/` contains everything the profile requires**

```bash
ls evaluation/raw/
```

Confirm run IDs, start/finish times, per-state totals, attempt/retry totals, kill-to-recovery
timing, and the stub's `/v1/stats` fields are all present across the written files (cross-check
against the "Evidence output" section of `docs/superpowers/specs/2026-09-04-m2-scenario-harness-design.md`).

- [ ] **Step 4: Update `README.md`**

Add a `## Scenario (M2)` section documenting `./intake scenario` (no flags, runs the fixed
profile, prints pass/fail per assertion, writes `EVALUATION.md` + `evaluation/raw/`), and record
the exact seeds/sizes/tenants used (1/500/tenant-a, 2/400/tenant-b) per the assignment's
requirement that every reported run's parameters be recorded.

- [ ] **Step 5: Commit**

```bash
git add README.md EVALUATION.md evaluation/raw/
git commit -m "Run the real M2 scenario profile; add EVALUATION.md and evaluation/raw/ evidence"
```

---

## Self-Review Notes

- **Spec coverage:** D-05 rework → Tasks 1–2. D-06 → Task 3 (`retry_amplification_bound`) consumed by Task 7's A4. D-15 → Task 1. REQ-2.1 (OS-process workers) was already satisfied in M1 (D-12), unchanged here. REQ-2.2(a–d) → assertions A1–A5 in Task 7. REQ-2.3 → A6–A8. REQ-2.4 (paired control, billed-call delta + explanation) → Task 7's two `run_execution` calls plus the reconciliation detail strings; the delta itself is derivable from `fault["stub_stats"]["billed_calls"] - control["stub_stats"]["billed_calls"]`, reported in `EVALUATION.md` via Task 5's writer. Evidence/report requirements → Task 5, exercised end-to-end by Tasks 6/8, produced for real by Task 9.
- **Placeholder scan:** no TBD/TODO; every code block is complete, runnable code.
- **Type/signature consistency:** `run_execution`'s return shape (keys `run_a`, `run_b`, `timeseries`, `kill_event`, `stub_stats`, `item_attempts`) is defined in Task 6 and consumed identically by Task 5's tests, Task 7's `_evaluate`/`_x_items`, and Task 8's assertions — same key names throughout. `ScenarioProfile`'s fields (`tenant_a`, `seed_a`, `size_a`, `corpus_a_dir`, ..., `failure_every_n`, `max_attempts`) are defined in Task 3 and used identically in Task 7's `_x_items`/`run_scenario`.
- **Not covered here, by design:** `traces/README.md` and exporting agent session transcripts (Section 9 of the assignment) — that's a documentation/export step outside implementation, not part of this plan.
