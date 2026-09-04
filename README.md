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
# => {"run_id": "a91dd1d6-e1a6-4525-9302-45bbf90a7708"}
./intake status --run a91dd1d6-e1a6-4525-9302-45bbf90a7708
# => {"run_id": "a91dd1d6-e1a6-4525-9302-45bbf90a7708", "tenant": "tenant-a", "states": {"succeeded": 98, "decode_failed": 1, "empty_content": 1}, "terminal": true}
./intake items --tenant tenant-a --run a91dd1d6-e1a6-4525-9302-45bbf90a7708 --state succeeded | head -1
# => [{"item_id": "04f79a2b-3966-44c6-a9fa-c9c4ed77ff41", "run_id": "a91dd1d6-e1a6-4525-9302-45bbf90a7708", "tenant": "tenant-a", ...}
./intake item --tenant tenant-a --id 04f79a2b-3966-44c6-a9fa-c9c4ed77ff41
# => {"item_id": "04f79a2b-3966-44c6-a9fa-c9c4ed77ff41", "run_id": "a91dd1d6-e1a6-4525-9302-45bbf90a7708", "tenant": "tenant-a", "source_path": "orig_13.json", "extension": "json", "state": "succeeded", ...}
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

Postgres must be up for the pipeline tests (`./intake up` or `docker compose up -d postgres`).

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

## Reported run (for grading)

Seed `1`, size `100`, tenant `tenant-a` — generated at `/tmp/corpus-a` (above), submitted as run ID `a91dd1d6-e1a6-4525-9302-45bbf90a7708`.

**Final state:** 98 succeeded, 1 decode_failed, 1 empty_content (100 items total).

All example commands in the Quickstart section above were executed against this real run and produce output as shown.

## Architecture and decisions

See [`ARCHITECTURE.md`](ARCHITECTURE.md) and [`DECISIONS.md`](DECISIONS.md).

## Known M1 scope cuts

- `D-06` (secondary performance thresholds) and `D-07` (local-environment fidelity) are deferred to the M2 build, where they can be set against real observed numbers (see `DECISIONS.md`).
- `kill-worker` is implemented and exercised manually, but the M2 scenario harness (`./intake scenario`) that asserts recovery timing does not exist yet.
- An item whose processing raises an unhandled exception is logged and left `in_progress` (the worker survives and moves on; the item's lease expires and it is reclaimed and retried). An item that fails this way *deterministically* will therefore be retried indefinitely rather than reaching a terminal state — giving it one requires a terminal state that D-04's fixed vocabulary does not yet have, which is an M2 decision.
