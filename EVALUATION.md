# EVALUATION

## Assertions

| ID | Description | Mandatory | Verdict | Detail |
|---|---|---|---|---|
| A1 | killed worker owned a nonterminal item | True | PASS | {'worker_id': 'worker-2', 'index': 2, 'item_id': 'e960ee00-b331-468c-9f3c-e42d2c1865dd', 'kill_time': 1788532662.792564, 'recovered_time': 1788532667.646222} |
| A2 | recovery within 10s, every item terminates | True | PASS | recovery_seconds=4.853658199310303 |
| A3 | max_in_flight<=2 and over_capacity_calls==0 | True | PASS | {'billed_calls': 941, 'current_in_flight': 0, 'max_in_flight': 2, 'server_error_calls': 134, 'over_capacity_calls': 0} |
| A4 | retry amplification within D-06 bound | True | PASS | control={'passed': True, 'total_attempts': 940, 'bound': 1411} fault={'passed': True, 'total_attempts': 941, 'bound': 1416} |
| A5 | counters reconcile with item outcomes | True | PASS | control={'passed': True, 'completed': 940, 'dangling': 0, 'billed_calls': 940} fault={'passed': True, 'completed': 941, 'dangling': 0, 'billed_calls': 941} |
| A6 | genuine overlap, both directions | True | PASS | {'passed': True, 'a_terminal_b_nonterminal': True, 'b_terminal_a_nonterminal': True} |
| A7 | every item attributed to the correct run/tenant | True | PASS | checked 500 items from run_a all carry tenant=tenant-a |
| A8 | cross-tenant lookup of a real item returns not found | True | PASS | real tenant-a item looked up as tenant-b: exit_code=1 |

## Control execution

- **tenant-a** (run `a6221b2d-7c64-46f2-8dde-79aadd3bc0a7`, seed 1, size 500): submitted 1788532569.186212, terminal 1788532657.2999501, states {'decode_failed': 1, 'empty_content': 1, 'succeeded': 498}
- **tenant-b** (run `60d19525-9e64-4ff6-b0f9-039772ac1f93`, seed 2, size 400): submitted 1788532569.186212, terminal 1788532657.2999501, states {'decode_failed': 1, 'empty_content': 1, 'succeeded': 398}
- Stub stats: {'billed_calls': 940, 'current_in_flight': 0, 'max_in_flight': 2, 'server_error_calls': 134, 'over_capacity_calls': 0}

## Fault execution

- **tenant-a** (run `6b6ed076-621e-4f18-a327-947ad3a5786a`, seed 1, size 500): submitted 1788532662.1792278, terminal 1788532752.0855522, states {'succeeded': 498, 'decode_failed': 1, 'empty_content': 1}
- **tenant-b** (run `4caa5c76-fd06-46c9-b0d2-128c25bd9972`, seed 2, size 400): submitted 1788532662.1792278, terminal 1788532752.0855522, states {'succeeded': 398, 'decode_failed': 1, 'empty_content': 1}
- Stub stats: {'billed_calls': 941, 'current_in_flight': 0, 'max_in_flight': 2, 'server_error_calls': 134, 'over_capacity_calls': 0}
- Kill event: {'worker_id': 'worker-2', 'index': 2, 'item_id': 'e960ee00-b331-468c-9f3c-e42d2c1865dd', 'kill_time': 1788532662.792564, 'recovered_time': 1788532667.646222}
