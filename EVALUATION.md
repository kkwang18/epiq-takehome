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

## Control execution

- **tenant-a** (run `1db78607-868c-4e43-9bd2-ca4240616662`, seed 1, size 500): submitted 1788551260.01021, terminal 1788551346.474328, states {'succeeded': 498, 'decode_failed': 1, 'empty_content': 1}
- **tenant-b** (run `07e1c104-7a6c-4915-bda1-ab444ed1e0f4`, seed 2, size 400): submitted 1788551260.01021, terminal 1788551346.474328, states {'succeeded': 398, 'decode_failed': 1, 'empty_content': 1}
- Stub stats: {'billed_calls': 940, 'current_in_flight': 0, 'max_in_flight': 2, 'server_error_calls': 134, 'over_capacity_calls': 0}

## Fault execution

- **tenant-a** (run `824e5f39-eb6f-4269-914b-ba353aaaf572`, seed 1, size 500): submitted 1788551351.487669, terminal 1788551441.7403111, states {'succeeded': 498, 'decode_failed': 1, 'empty_content': 1}
- **tenant-b** (run `1d269db4-fad1-48ca-a022-06d90f7d71cc`, seed 2, size 400): submitted 1788551351.487669, terminal 1788551441.7403111, states {'succeeded': 398, 'decode_failed': 1, 'empty_content': 1}
- Stub stats: {'billed_calls': 941, 'current_in_flight': 0, 'max_in_flight': 2, 'server_error_calls': 134, 'over_capacity_calls': 0}
- Kill event: {'worker_id': 'worker-3', 'index': 3, 'item_id': 'bed194e2-7ad1-4d73-9d49-3dae03e7b7be', 'kill_time': 1788551352.090951, 'recovered_time': 1788551357.191527}
