# Content Intake Pipeline

## Prerequisites

- Docker (for Postgres)
- Python 3.11+

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
chmod +x intake
```

## Quickstart

```bash
./intake up --workers 4
./intake corpus --seed 1 --size 100 --tenant tenant-a --out /tmp/corpus-a
./intake submit --corpus /tmp/corpus-a --tenant tenant-a
# => {"run_id": "ec2aa267-e78b-4529-ad00-568c529ccddf"}
./intake status --run ec2aa267-e78b-4529-ad00-568c529ccddf
# => {"run_id": "ec2aa267-e78b-4529-ad00-568c529ccddf", "tenant": "tenant-a", "states": {"decode_failed": 1, "empty_content": 1, "succeeded": 98}, "terminal": true}
./intake items --tenant tenant-a --run ec2aa267-e78b-4529-ad00-568c529ccddf --state succeeded
# => [{"item_id": "8777952b-391a-4aa3-ace8-9f5ee797cfd5", "run_id": "ec2aa267-e78b-4529-ad00-568c529ccddf", "tenant": "tenant-a", "source_path": "orig_0.txt", ...}
# items are listed in manifest order (order_index), so the first record is always orig_0.txt
./intake item --tenant tenant-a --id 8777952b-391a-4aa3-ace8-9f5ee797cfd5
# => {"item_id": "8777952b-391a-4aa3-ace8-9f5ee797cfd5", "run_id": "ec2aa267-e78b-4529-ad00-568c529ccddf", "tenant": "tenant-a", "source_path": "orig_0.txt", "extension": "txt", "bytes": 295, "role": "original", "order_index": 0, "state": "succeeded", ...}
./intake down
# => {"status": "down"}
```

## Other subcommands

```bash
./intake corpus --verify --out /tmp/corpus-a   # re-derive the corpus from its own manifest.json and diff it (D-13)
./intake reset --workers 4                     # down + up in one call (fresh Postgres volume, fresh processes)
./intake kill-worker [--index N]               # SIGKILL one live worker and leave it dead (no restart)
./intake stub --port 8080                      # run the annotation stub in the foreground
```

If `./intake up` fails, the stub, API, and worker processes each write to their own file under
`.intake_logs/` (`stub.log`, `api.log`, `worker-0.log` … `worker-N.log`) — those are where a
startup failure or a worker-side exception shows up, since the detached processes never write to
the terminal that launched them.

## Testing

Postgres must be up for the pipeline tests — use `docker compose up -d postgres` for this, not
`./intake up`. The latter also starts 4 real worker processes, which will race the tests for any
`pending` item they insert (e.g. `tests/test_worker_loop.py`), causing spurious failures or hangs.

```bash
source .venv/bin/activate

# Full suite. Excludes the slow acceptance test by default (see pytest.ini).
# The stub conformance suite needs a stub listening, so start one first in another terminal:
#   ./intake stub --port 8080
pytest -v

# Black-box stub conformance suite on its own. Requires a running stub; it speaks plain
# HTTP only and imports no pipeline code, so it can be pointed at any conforming stub.
STUB_URL=http://localhost:8080 pytest tests/test_stub_conformance.py -v

# Slow end-to-end acceptance test: brings the full environment up (Postgres, stub, API,
# 4 workers), submits two tenants' corpora, waits for both runs to reach terminal, and
# tears everything back down itself.
pytest -m slow -v
```

## Scenario (M2)

```bash
./intake scenario
```

Takes no flags — runs the fixed evaluation profile (tenant-a, 500 items, seed 1; tenant-b, 400
items, seed 2; 4 workers; stub latency fixed 150ms, `failure_every_n=7`, `in_flight_capacity=2`
in both executions). Brings the environment up from empty state, runs a paired **control**
execution (no fault) and a **fault** execution (kills one worker mid-run) — each from a fresh
`./intake reset`, so no content or annotation records are reused between them — then prints every
mandatory assertion (REQ-2.2/2.3) as `[PASS]`/`[FAIL]` with its supporting detail, writes
`EVALUATION.md` at the repo root and machine-readable evidence under `evaluation/raw/`, and exits
0 only if every mandatory assertion passed. Takes several minutes (900 items × 2 executions).

## Reported run (for grading)

Seed `1`, size `100`, tenant `tenant-a` — generated at `/tmp/corpus-a` (above), submitted as run ID `ec2aa267-e78b-4529-ad00-568c529ccddf`.

**Final state:** 98 succeeded, 1 decode_failed, 1 empty_content (100 items total).

All example commands in the Quickstart section above were executed against this real run and produce output as shown.

The M2 scenario's reported run: tenant-a seed `1` size `500`, tenant-b seed `2` size `400` (the
fixed evaluation profile — see `docs/superpowers/specs/2026-09-04-m2-scenario-harness-design.md`).
All 8 mandatory assertions passed; see `EVALUATION.md` and `evaluation/raw/` for the full evidence.

## Architecture and decisions

See [`ARCHITECTURE.md`](ARCHITECTURE.md) and [`DECISIONS.md`](DECISIONS.md).

## Known scope cuts

- An unreadable source file is caught and finalized immediately as the terminal state `processing_error` (D-04) — no retry, no worker crash. Any *other* genuinely unhandled exception during processing (e.g. a transient Postgres error) is still logged and left `in_progress` (the worker survives and moves on; the item's lease expires and it is reclaimed and retried), and an item failing this way *deterministically* is still retried indefinitely rather than reaching a terminal state — this residual case is deliberately out of scope, since giving every conceivable exception its own terminal state would blur "permanent, detected condition" with "transient infrastructure hiccup." Not exercised by the M2 profile, which doesn't inject infrastructure faults or any fault other than one `SIGKILL`.
- Exactly-once billing across every crash boundary is out of scope by design (D-09) — duplicate billing is measured and explained (see the billed-call delta in `EVALUATION.md`), never claimed to be prevented.
