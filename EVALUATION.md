# EVALUATION

## Assertions

| ID | Description | Mandatory | Verdict | Detail |
|---|---|---|---|---|
| A1 | killed worker owned a nonterminal item | True | PASS | {'worker_id': 'worker-3', 'index': 3, 'item_id': 'bed194e2-7ad1-4d73-9d49-3dae03e7b7be', 'kill_time': 1788551352.090951, 'recovered_time': 1788551357.191527} |
| A2 | recovery within 10s, every item terminates | True | PASS | recovery_seconds=5.100575923919678 |
| A3 | max_in_flight<=2 and over_capacity_calls==0 | True | PASS | {'billed_calls': 941, 'current_in_flight': 0, 'max_in_flight': 2, 'server_error_calls': 134, 'over_capacity_calls': 0} |
| A4 | retry amplification within D-06 bound | True | PASS | control={'passed': True, 'total_attempts': 940, 'bound': 1411} fault={'passed': True, 'total_attempts': 941, 'bound': 1416} |
| A5 | counters reconcile with item outcomes | True | PASS | control={'passed': True, 'completed': 940, 'dangling': 0, 'billed_calls': 940} fault={'passed': True, 'completed': 941, 'dangling': 0, 'billed_calls': 941} |
| A6 | genuine overlap, both directions | True | PASS | {'passed': True, 'a_terminal_b_nonterminal': True, 'b_terminal_a_nonterminal': True} |
| A7 | every item attributed to the correct run/tenant | True | PASS | checked 500 items from run_a all carry tenant=tenant-a |
| A8 | cross-tenant lookup of a real item returns not found | True | PASS | real tenant-a item looked up as tenant-b: exit_code=1 |

## Control execution (no fault injected)

| Tenant | Run ID | Corpus | Duration | Outcome |
|---|---|---|---|---|
| tenant-a | `1db78607-868c-4e43-9bd2-ca4240616662` | seed 1, 500 items | 86.5s | 498 succeeded, 1 decode_failed, 1 empty_content |
| tenant-b | `07e1c104-7a6c-4915-bda1-ab444ed1e0f4` | seed 2, 400 items | 86.5s | 398 succeeded, 1 decode_failed, 1 empty_content |

Both runs submitted together at 15:47:40, both reached terminal at 15:49:06 (2026-09-04).

**Stub load:** 940 billed calls, 134 server errors (the injected 1-in-7 failure rate), 0 over-capacity (429) responses, peak concurrency held exactly at the configured cap of 2.

## Fault execution (one worker killed mid-run)

| Tenant | Run ID | Corpus | Duration | Outcome |
|---|---|---|---|---|
| tenant-a | `824e5f39-eb6f-4269-914b-ba353aaaf572` | seed 1, 500 items | 90.3s | 498 succeeded, 1 decode_failed, 1 empty_content |
| tenant-b | `1d269db4-fad1-48ca-a022-06d90f7d71cc` | seed 2, 400 items | 90.3s | 398 succeeded, 1 decode_failed, 1 empty_content |

Both runs submitted at 15:49:11, both reached terminal at 15:50:42 (2026-09-04).

**Kill event:** `worker-3` was SIGKILLed at 15:49:12.09, mid-processing of item `bed194e2-7ad1-4d73-9d49-3dae03e7b7be`. That item's work resumed on another worker and reached a terminal state by 15:49:17.19 — **5.1 seconds of recovery**, well inside the 10s bound.

**Stub load:** 941 billed calls, 134 server errors, 0 over-capacity (429) responses, peak concurrency again held at 2.

### Reading it together

Same corpora, same failure schedule, same capacity limit in both runs — the only difference is the kill. Final item outcomes are identical between control and fault (498/1/1 for tenant-a, 398/1/1 for tenant-b), the fault run took ~4s longer (consistent with one item's extra retry cycle after the kill), and the killed item recovered in 5.1s. That is the point of the paired design: the fault changes how long one interrupted item takes to land, not what gets processed.
