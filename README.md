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

## Reported run (for grading)

Seed `1`, size `100`, tenant `tenant-a` — generated at `/tmp/corpus-a` (above), submitted as run ID `a91dd1d6-e1a6-4525-9302-45bbf90a7708`.

**Final state:** 98 succeeded, 1 decode_failed, 1 empty_content (100 items total).

All example commands in the Quickstart section above were executed against this real run and produce output as shown.

## Architecture and decisions

See [`ARCHITECTURE.md`](ARCHITECTURE.md) and [`DECISIONS.md`](DECISIONS.md).

## Known M1 scope cuts

- `D-06` (secondary performance thresholds) and `D-07` (local-environment fidelity) are deferred to the M2 build, where they can be set against real observed numbers (see `DECISIONS.md`).
- `kill-worker` is implemented and exercised manually, but the M2 scenario harness (`./intake scenario`) that asserts recovery timing does not exist yet.
- The `run_worker` function in `src/content_intake/pipeline/worker.py` calls `process_item` in a loop with no exception handling — if `process_item` encounters an unhandled exception (e.g. reading an unreadable corpus file), the entire worker process crashes rather than recovering to claim the next item. This gap was not exercised during M1 testing but is a known, unaddressed limitation flagged during code review.
