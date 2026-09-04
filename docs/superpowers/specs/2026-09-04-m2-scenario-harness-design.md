# M2 Scenario Harness — Design

Companion to `ARCHITECTURE.md` and `DECISIONS.md` (read those first — this doc only covers what
doesn't fit their structure: the scenario runner's algorithm, fault-injection mechanics, the exact
assertion list, and the evidence/report format). Implements REQ-2.1 through REQ-2.4 and D-06
against the assignment's fixed evaluation profile.

## Prerequisite

This harness is built against the **revised** D-05 retry mechanism (release-based backoff,
claim-time attempt-cap enforcement) and D-15 (claim ordering `order_index, created_at`) — both
already reflected in `ARCHITECTURE.md`/`DECISIONS.md`. The implementation plan does that rework
first, as its own set of tasks, before building anything described here.

## Fixed profile (from the assignment, not a design choice)

| Setting | Value |
|---|---|
| First corpus | tenant `tenant-a`, 500 items, seed 1 |
| Second corpus | tenant `tenant-b`, 400 items, seed 2 |
| Workers | 4 OS processes |
| Submission timing | Submit corpus B while ≥25% of corpus A's items remain nonterminal |
| Stub config (both executions) | latency fixed 150ms, `failure_every_n=7`, `in_flight_capacity=2` |
| Injected difference | Fault execution kills one worker; control execution does not |
| Execution isolation | Each execution starts from empty pipeline state (`./intake reset`), stub counters/sequences reset, no content/annotation reuse between executions |

## Scenario runner algorithm (`./intake scenario`)

Corpus A and B are generated once on disk before either execution (deterministic and
content-addressed by `(seed, size)` — regenerating would produce byte-identical files anyway) and
resubmitted fresh into each execution's empty Postgres. "Execution isolation" governs *pipeline*
state (Postgres, stub counters) — it says nothing about the source files, which aren't part of
that state.

```
generate(corpus_a, seed=1, size=500, tenant=tenant-a)
generate(corpus_b, seed=2, size=400, tenant=tenant-b)
reset()                                    # fresh Postgres volume, stub reset, 4 workers up
control_evidence = run_execution(kill=False)
reset()                                    # execution isolation — no state carried over
fault_evidence   = run_execution(kill=True)
assertions = evaluate(control_evidence, fault_evidence)
write_evaluation_md(control_evidence, fault_evidence, assertions)
write_raw_evidence(control_evidence, fault_evidence, assertions)
print assertions, one line each, pass/fail + reason
exit 0 iff every mandatory assertion passed
```

`run_execution(kill)`:

```
run_a = submit(corpus_a, tenant-a)
run_b = submit(corpus_b, tenant-b)
timeseries = []
killed = None
loop:
    snapshot = (now, status(run_a), status(run_b))
    timeseries.append(snapshot)
    if kill and killed is None and both runs have >=1 terminal and >=1 nonterminal item:
        killed = inject_fault(run_a, run_b)          # see below
    if both runs fully terminal:
        break
    sleep(poll_interval)          # 0.5s default
stub_stats = GET /v1/stats
return { run_a, run_b, timeseries, killed, stub_stats, item_attempts(run_a ∪ run_b) }
```

Both corpora are submitted back to back with no deliberate wait — at that point ~100% of run A's
items are nonterminal, clearing the ≥25% floor with maximum margin and maximizing the window
where both runs are actually being processed concurrently (see D-15 — without the claim-ordering
fix this would starve run B; with it, both runs interleave from shortly after submission).

`inject_fault(run_a, run_b)`:

```
leasing = SELECT DISTINCT leased_by FROM items WHERE state = 'in_progress'
worker_id = pick any one from leasing (arbitrary — REQ-2.2(a) only needs *a* nonterminal owner, not a specific one)
item_id = the item currently leased by worker_id
index = parse index from worker_id (e.g. "worker-2" -> 2)
kill_time = now()
./intake kill-worker --index index
poll item_id's own state until terminal (bounded by the 10s assertion, not by the run's own poll loop)
recovered_time = now()
return { worker_id, index, item_id, kill_time, recovered_time }
```

Picking a worker from `SELECT DISTINCT leased_by FROM items WHERE state='in_progress'` — rather
than relying on `kill-worker`'s own default "first alive" selection — is what makes REQ-2.2(a)
(*the killed worker owned a nonterminal item*) provable rather than merely likely: with 900 items
and 4 workers, an idle worker is possible but improbable, and provable beats probable.

## Assertions

Mandatory (gate the exit code):

| ID | Requirement | Check |
|---|---|---|
| A1 | REQ-2.2(a): killed worker owned a nonterminal item, stays dead | `killed.item_id` had `state='in_progress', leased_by=killed.worker_id` at kill time (captured, not inferred); after the run, `.intake_state.json`'s worker entry shows `alive: false` and the PID is not running |
| A2 | REQ-2.2(b): recovery within 10s, every item terminates | `killed.recovered_time - killed.kill_time <= 10s`; both runs' final status show `terminal: true` |
| A3 | REQ-2.2(c): shared capacity limit holds | Fault execution's `stub_stats.max_in_flight <= 2` and `stub_stats.over_capacity_calls == 0` |
| A4 | REQ-2.2(d), part 1: retries follow documented policy | D-06's retry-amplification bound (below), applied to both executions |
| A5 | REQ-2.2(d), part 2: counters reconcile with outcomes | See Reconciliation below |
| A6 | REQ-2.3: genuine overlap, both directions | The fault execution's `timeseries` contains a snapshot where run A has ≥1 terminal item and run B has ≥1 nonterminal item, **and** a (possibly different) snapshot where run B has ≥1 terminal item and run A has ≥1 nonterminal item |
| A7 | REQ-2.3: correct attribution | Every item in run A's item list has `tenant == 'tenant-a'`; every item in run B's has `tenant == 'tenant-b'` |
| A8 | REQ-2.3: cross-tenant negative test | `./intake item --tenant tenant-b --id <a run-A item_id>` exits non-zero; `./intake items --tenant tenant-a --run <run_b>` exits non-zero |

Informational (reported, never gate the exit code): wall-clock time per execution, items/sec,
billed-call delta between control and fault (with explanation — see REQ-2.4 below).

**D-06 retry-amplification bound (A4).** For each execution: `X` = count of items across both
runs that are neither an edge case nor a cache hit (originals only, computed from each corpus's
manifest — duplicates always hit the cache given D-15's ordering, so they contribute 0 attempts
in the expected case). `E = X × 1.1666` (the geometric expectation from `p=1/7`, `max_attempts=5`
— derivation in `DECISIONS.md` D-06). Assert `total_attempts <= ceil(E × 1.5)` for the control
execution, `ceil(E × 1.5) + 5` for the fault execution (the `+5` covers the killed item's
worst-case full restart).

**Reconciliation (A5).** Let `completed` = count of `item_attempts` rows with `completed_at IS
NOT NULL`, `dangling` = count with `completed_at IS NULL`. For the control execution, assert
`dangling == 0` and `billed_calls == completed` (no kill, so every attempt should resolve
cleanly). For the fault execution, assert `dangling <= 1` (only the killed item's in-flight call,
if any, can be interrupted mid-flight) and `billed_calls == completed + dangling` (the stub's own
counter is definitive about whether an interrupted call actually reached it — this is exactly
what D-09 built `item_attempts` to make provable rather than guessed).

**REQ-2.4 billed-call delta.** `billed_calls_fault - billed_calls_control`, reported with its
explanation: expected to equal the fault execution's `dangling` count (0 or 1) plus the number of
*additional* attempts the killed item needed after recovery (its own `item_attempts` count minus
what it would have needed if never interrupted — visible directly from that item's own attempt
rows). No pass/fail bound — this is the "report billed calls for both, the delta, and its
explanation" the profile asks for, not a threshold.

## Evidence output

`evaluation/raw/`:
- `control-run-a.json`, `control-run-b.json`, `fault-run-a.json`, `fault-run-b.json` — per run:
  `run_id`, `tenant`, corpus `seed`/`size`, `submitted_at`, `terminal_at`, per-state item counts,
  every `item_attempts` row for that run's items.
- `control-stub-stats.json`, `fault-stub-stats.json` — the stub's `/v1/stats` snapshot at the end
  of that execution (all of EXT-REQ-6's fields).
- `control-timeseries.json`, `fault-timeseries.json` — the polling time-series backing A6.
- `fault-kill-event.json` — `worker_id`, `index`, `pid`, `item_id`, `kill_time`, `recovered_time`,
  recovery duration.
- `assertions.json` — every assertion above, with its verdict and the values that produced it.

`EVALUATION.md`: recorded configuration (retry limits/timeouts/backoff/lease timing, copied from
`DECISIONS.md` D-05/D-01/D-08 — not re-derived, just cited), one paragraph per execution (wall
clock, items/sec, terminal-state counts, stub stats), the assertion table with pass/fail and
reasons, the billed-call delta and its explanation, and — for any assertion that fails — whether
it's an expected limitation, a scope cut, or a defect, and what would change. An explained failure
is still recorded as a failure, never softened into a pass.

## Testing strategy for the harness itself

The harness is exercised by actually running it (`./intake scenario` against the real profile) —
this is the same posture as M1's `test_m1_end_to_end.py`, which drives the real CLI rather than
mocking pipeline internals. Smaller pieces get unit tests where they're isolated enough to be
worth it: the D-06 threshold formula (pure function of `X`/`failure_every_n`/`max_attempts`), the
reconciliation math (pure function of counts), and the worker-selection query (`SELECT DISTINCT
leased_by ...`, testable against a live Postgres the same way Tasks 14/15 tested claim/slot
queries in M1).

## Explicitly out of scope

- Exactly-once billing across every crash boundary (unchanged from M1 — D-09 measures and
  explains duplicate billing, never claims to prevent it).
- A restart supervisor for crashed/killed workers (`kill-worker`'s "stays dead" is a hard
  requirement, not a gap to fix).
- Testing the unhandled-exception-crashes-a-worker gap noted in `ARCHITECTURE.md`'s crash-window
  table — the M2 profile doesn't inject file corruption or any fault other than one `SIGKILL`.
