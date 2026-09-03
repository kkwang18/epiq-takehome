# Content Intake Pipeline — M1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build M1 of the Content Intake Pipeline end to end — a deterministic corpus generator, the annotation stub, a Postgres-backed pipeline (Pipeline API + OS-process workers) that processes a corpus to completion with durable, crash-safe work ownership, and the `./intake` CLI wiring all of it together — provably working via `./intake corpus`, `./intake up`, `./intake submit`, and the query surface.

**Architecture:** Only Postgres runs in a container; the stub, Pipeline API, and workers run as native OS processes launched by `./intake up` and tracked via PIDs in a local state file. Workers claim items and a shared 2-slot capacity gate via Postgres row-leasing (`SELECT ... FOR UPDATE SKIP LOCKED` + short TTLs that self-heal — no reaper process). Every attempt at calling the stub is durably recorded before the call is sent, not after, so crash windows are reconstructable evidence rather than silent gaps.

**Tech Stack:** Python 3.11+, FastAPI + Uvicorn (stub and Pipeline API), httpx (stub client), psycopg 3 (Postgres driver, raw SQL, no ORM), Pillow (real PNG generation), pytest, Postgres 16 (Docker), Docker CLI (shelled out to, no SDK dependency).

**Spec:** [`DECISIONS.md`](../../../DECISIONS.md), [`ARCHITECTURE.md`](../../../ARCHITECTURE.md)

## Global Constraints

- `./intake` is the one executable at the repo root; the subcommands, flags, and outputs in the brief's section 2a are fixed and may not be renamed, restructured, or omitted.
- Query subcommands (`status`, `item`, `items`) print JSON to stdout and exit 0 on success, non-zero on error.
- All source lives under `src/`.
- Durable state lives only in Postgres, running in a container (D-11); the stub, Pipeline API, and workers run as native OS processes.
- No external network access at evaluation time — setup (`pip install`, `docker pull`) may use the network; the running system may not.
- The stub is HTTP-only and is never imported as a library by pipeline code (`tests/test_stub_conformance.py` must never import pipeline code either).
- Every item read or write is scoped to its tenant (REQ-1.2); a wrong-tenant lookup returns not-found, never another tenant's data.
- Corpus determinism: `(seed, size, tenant)` fully determines output; file bytes depend only on `(seed, size)` (D-13/CORPUS-REQ requirements).
- Workers are separate OS processes from M1 onward (D-12); item ownership and the capacity gate are both held in Postgres, never in worker memory (D-01, D-08).
- `in_flight_capacity` is a single config value consumed by both the stub's startup env var and the pipeline's slot-seeding — never a duplicated literal (D-08).
- Every worker read/write of `items` or `annotations_cache` uses `tenant` read from the claimed `items` row, never any other source (D-10).

---

## File Structure

```
epiq/
├── intake                                  # executable entrypoint (chmod +x)
├── requirements.txt                         # pinned via pip freeze (Task 1)
├── docker-compose.yml                       # Postgres only
├── .gitignore
├── src/
│   └── content_intake/
│       ├── __init__.py
│       ├── common/
│       │   ├── __init__.py
│       │   ├── canonical_json.py            # canonical JSON + sha256 helpers
│       │   └── config.py                    # shared config constants
│       ├── generator/
│       │   ├── __init__.py
│       │   ├── generate.py                  # deterministic corpus generation
│       │   └── verify.py                    # --verify
│       ├── stub/
│       │   ├── __init__.py
│       │   ├── state.py                     # counters, latency sequence, failure schedule
│       │   ├── app.py                       # FastAPI app (endpoints)
│       │   └── run.py                       # uvicorn runner (used by `./intake stub` and `up`)
│       ├── pipeline/
│       │   ├── __init__.py
│       │   ├── db.py                        # connection + schema + slot seeding
│       │   ├── schema.sql                   # DDL
│       │   ├── extraction.py                # edge-case detection + text extraction
│       │   ├── stub_client.py                # single-call HTTP wrapper
│       │   ├── worker.py                    # claim/lease/slot/attempts/process/main loop
│       │   ├── api.py                       # FastAPI app: submit/item/items/status
│       │   └── run_api.py                   # uvicorn runner
│       └── cli/
│           ├── __init__.py
│           ├── main.py                      # argparse dispatch
│           ├── corpus_cmd.py
│           ├── stub_cmd.py
│           ├── lifecycle_cmd.py              # up/down/reset
│           ├── kill_worker_cmd.py
│           └── query_cmd.py                  # submit/status/item/items
├── tests/
│   ├── test_stub_conformance.py             # required, black-box, no pipeline imports
│   ├── conftest.py                          # Postgres/stub fixtures
│   ├── test_canonical_json.py
│   ├── test_generator.py
│   ├── test_stub_state.py
│   ├── test_stub_app.py
│   ├── test_extraction.py
│   ├── test_stub_client.py
│   ├── test_worker_claim.py
│   ├── test_worker_slots.py
│   ├── test_worker_attempts.py
│   ├── test_worker_process_item.py
│   ├── test_api_submit.py
│   ├── test_api_query.py
│   └── test_m1_end_to_end.py
├── README.md
├── DECISIONS.md                             # already exists
└── ARCHITECTURE.md                          # already exists
```

---

### Task 1: Project scaffolding

**Files:**
- Create: `src/content_intake/__init__.py`, `src/content_intake/common/__init__.py`, `src/content_intake/generator/__init__.py`, `src/content_intake/stub/__init__.py`, `src/content_intake/pipeline/__init__.py`, `src/content_intake/cli/__init__.py` (all empty)
- Create: `intake` (executable entrypoint)
- Create: `docker-compose.yml`
- Create: `.gitignore`
- Create: `requirements.txt`

**Interfaces:**
- Produces: `intake` script importable path — every later CLI task adds a subcommand to `content_intake.cli.main:main`.

- [ ] **Step 1: Create the package skeleton**

```bash
mkdir -p src/content_intake/{common,generator,stub,pipeline,cli}
touch src/content_intake/__init__.py \
      src/content_intake/common/__init__.py \
      src/content_intake/generator/__init__.py \
      src/content_intake/stub/__init__.py \
      src/content_intake/pipeline/__init__.py \
      src/content_intake/cli/__init__.py
mkdir -p tests
```

- [ ] **Step 2: Write `.gitignore`**

```
.venv/
__pycache__/
*.pyc
.intake_state.json
/tmp_corpus/
.pytest_cache/
```

- [ ] **Step 3: Create a virtualenv and install dependencies unpinned, then lock them**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install fastapi "uvicorn[standard]" httpx "psycopg[binary]" pillow pytest
pip freeze > requirements.txt
```

Locking via `pip freeze` (rather than guessing exact version numbers up front) guarantees `requirements.txt` pins a mutually-compatible, actually-installable set — this is what "vendor or pin anything needed at runtime" means in practice here.

- [ ] **Step 4: Write `docker-compose.yml` (Postgres only)**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    container_name: intake-postgres
    environment:
      POSTGRES_USER: intake
      POSTGRES_PASSWORD: intake
      POSTGRES_DB: intake
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U intake"]
      interval: 2s
      timeout: 2s
      retries: 30
```

- [ ] **Step 5: Write the `intake` entrypoint script and make it executable**

```python
#!/usr/bin/env python3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from content_intake.cli.main import main

if __name__ == "__main__":
    sys.exit(main())
```

```bash
chmod +x intake
```

- [ ] **Step 6: Write a minimal `main.py` so the entrypoint runs**

```python
# src/content_intake/cli/main.py
import argparse
import sys


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="intake")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("version")
    args = parser.parse_args(argv)
    if args.command == "version":
        print("content-intake-pipeline 0.1.0")
        return 0
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
```

- [ ] **Step 7: Verify the entrypoint runs**

Run: `./intake version`
Expected: `content-intake-pipeline 0.1.0`

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "Scaffold project: package layout, venv deps, Postgres compose, intake entrypoint"
```

---

### Task 2: Canonical JSON + hashing helpers

**Files:**
- Create: `src/content_intake/common/canonical_json.py`
- Test: `tests/test_canonical_json.py`

**Interfaces:**
- Produces: `canonical_dumps(obj) -> str`, `sha256_hex(data: bytes) -> str`, `sha256_of_canonical(obj) -> str`. Used by the generator (manifest digest) and the Pipeline API (independent sha256 recompute, D-14).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_canonical_json.py
import hashlib
from content_intake.common.canonical_json import canonical_dumps, sha256_hex, sha256_of_canonical


def test_canonical_dumps_sorts_keys_and_is_compact():
    assert canonical_dumps({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_canonical_dumps_deterministic_across_calls():
    obj = {"z": [3, 2, 1], "a": {"y": 1, "x": 2}}
    assert canonical_dumps(obj) == canonical_dumps(obj)


def test_sha256_hex_matches_stdlib():
    data = b"hello world"
    assert sha256_hex(data) == hashlib.sha256(data).hexdigest()


def test_sha256_of_canonical_changes_with_content():
    a = sha256_of_canonical({"a": 1})
    b = sha256_of_canonical({"a": 2})
    assert a != b
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_canonical_json.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'content_intake.common.canonical_json'`

- [ ] **Step 3: Implement**

```python
# src/content_intake/common/canonical_json.py
import hashlib
import json
from typing import Any


def canonical_dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_of_canonical(obj: Any) -> str:
    return sha256_hex(canonical_dumps(obj).encode("utf-8"))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_canonical_json.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/common/canonical_json.py tests/test_canonical_json.py
git commit -m "Add canonical JSON and sha256 helpers"
```

---

### Task 3: Postgres schema, connection, and slot seeding

**Files:**
- Create: `src/content_intake/pipeline/schema.sql`
- Create: `src/content_intake/pipeline/db.py`
- Test: `tests/conftest.py`
- Test: `tests/test_db.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `connect() -> psycopg.Connection` (autocommit=False, `row_factory=dict_row`), `apply_schema(conn) -> None`, `ensure_slots(conn, capacity: int) -> None`. Every later task that touches Postgres uses `connect()`.

This task requires a running Postgres. Start it once for the whole test session:

```bash
docker compose up -d postgres
# wait until healthy:
until docker exec intake-postgres pg_isready -U intake > /dev/null 2>&1; do sleep 0.5; done
```

- [ ] **Step 1: Write `schema.sql`**

```sql
-- src/content_intake/pipeline/schema.sql
CREATE TABLE IF NOT EXISTS runs (
    run_id UUID PRIMARY KEY,
    corpus_id TEXT NOT NULL,
    tenant TEXT NOT NULL,
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS items (
    item_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(run_id),
    tenant TEXT NOT NULL,
    source_path TEXT NOT NULL,
    extension TEXT NOT NULL,
    bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    role TEXT NOT NULL,
    duplicate_of TEXT,
    edge_case TEXT,
    expects_annotation BOOLEAN NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    leased_by TEXT,
    leased_until TIMESTAMPTZ,
    reason JSONB,
    extracted_text TEXT,
    annotation JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_items_claim ON items (state, leased_until);
CREATE INDEX IF NOT EXISTS idx_items_run_state ON items (run_id, state);
CREATE INDEX IF NOT EXISTS idx_items_tenant ON items (tenant, item_id);

CREATE TABLE IF NOT EXISTS annotations_cache (
    tenant TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    annotation JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant, sha256)
);

CREATE TABLE IF NOT EXISTS stub_call_slots (
    slot_id INTEGER PRIMARY KEY,
    held_by TEXT,
    lease_until TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS item_attempts (
    attempt_id UUID PRIMARY KEY,
    item_id UUID NOT NULL REFERENCES items(item_id),
    tenant TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    worker_id TEXT NOT NULL,
    slot_id INTEGER NOT NULL,
    completed_at TIMESTAMPTZ,
    http_status INTEGER,
    outcome TEXT,
    UNIQUE (item_id, attempt_no)
);
CREATE INDEX IF NOT EXISTS idx_item_attempts_item ON item_attempts (item_id);
```

- [ ] **Step 2: Write `db.py`**

```python
# src/content_intake/pipeline/db.py
import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

_SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_dsn() -> str:
    return os.environ.get(
        "INTAKE_PG_DSN", "postgresql://intake:intake@localhost:5432/intake"
    )


def connect() -> psycopg.Connection:
    return psycopg.connect(get_dsn(), row_factory=dict_row, autocommit=False)


def apply_schema(conn: psycopg.Connection) -> None:
    conn.execute(_SCHEMA_PATH.read_text())
    conn.commit()


def ensure_slots(conn: psycopg.Connection, capacity: int) -> None:
    conn.execute(
        """
        INSERT INTO stub_call_slots (slot_id)
        SELECT g FROM generate_series(1, %s) AS g
        ON CONFLICT (slot_id) DO NOTHING
        """,
        (capacity,),
    )
    conn.execute("DELETE FROM stub_call_slots WHERE slot_id > %s", (capacity,))
    conn.commit()
```

- [ ] **Step 3: Write `tests/conftest.py` with a schema-reset fixture**

```python
# tests/conftest.py
import pytest

from content_intake.pipeline.db import connect, apply_schema


@pytest.fixture
def db_conn():
    conn = connect()
    apply_schema(conn)
    # start every test from a clean slate
    conn.execute("TRUNCATE item_attempts, annotations_cache, items, runs, stub_call_slots CASCADE")
    conn.commit()
    yield conn
    conn.close()
```

- [ ] **Step 4: Write the failing test**

```python
# tests/test_db.py
from content_intake.pipeline.db import ensure_slots


def test_ensure_slots_creates_exact_count(db_conn):
    ensure_slots(db_conn, 2)
    rows = db_conn.execute("SELECT slot_id FROM stub_call_slots ORDER BY slot_id").fetchall()
    assert [r["slot_id"] for r in rows] == [1, 2]


def test_ensure_slots_shrinks_when_capacity_reduced(db_conn):
    ensure_slots(db_conn, 3)
    ensure_slots(db_conn, 1)
    rows = db_conn.execute("SELECT slot_id FROM stub_call_slots ORDER BY slot_id").fetchall()
    assert [r["slot_id"] for r in rows] == [1]


def test_schema_tables_exist(db_conn):
    tables = db_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    ).fetchall()
    names = {r["table_name"] for r in tables}
    assert {"runs", "items", "annotations_cache", "stub_call_slots", "item_attempts"} <= names
```

- [ ] **Step 5: Run tests to verify they fail, then pass**

Run: `pytest tests/test_db.py -v`
Expected first: FAIL (schema.sql/db.py not yet applied against a running Postgres, or module errors)
After Steps 1–2 are in place: 3 passed

- [ ] **Step 6: Commit**

```bash
git add src/content_intake/pipeline/schema.sql src/content_intake/pipeline/db.py tests/conftest.py tests/test_db.py
git commit -m "Add Postgres schema, connection helper, and slot seeding"
```

---

### Task 4: Shared config constants

**Files:**
- Create: `src/content_intake/common/config.py`

**Interfaces:**
- Produces: `IN_FLIGHT_CAPACITY`, `LEASE_SECONDS`, `LEASE_RENEW_INTERVAL`, `HTTP_TIMEOUT_SECONDS`, `MAX_ATTEMPTS`, `BACKOFF_BASE`, `BACKOFF_CAP`, `BACKOFF_JITTER`, `STUB_PORT`, `API_PORT`, each overridable via an `INTAKE_`-prefixed env var so `up`/`scenario` (M2) can tune them without code changes.

- [ ] **Step 1: Write `config.py`** (no test file — pure constants, exercised indirectly by every later test)

```python
# src/content_intake/common/config.py
import os


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


IN_FLIGHT_CAPACITY = _int("INTAKE_IN_FLIGHT_CAPACITY", 2)
LEASE_SECONDS = _int("INTAKE_LEASE_SECONDS", 5)
LEASE_RENEW_INTERVAL = _int("INTAKE_LEASE_RENEW_INTERVAL", 2)
HTTP_TIMEOUT_SECONDS = _float("INTAKE_HTTP_TIMEOUT_SECONDS", 2.0)
MAX_ATTEMPTS = _int("INTAKE_MAX_ATTEMPTS", 5)
BACKOFF_BASE = _float("INTAKE_BACKOFF_BASE", 0.2)
BACKOFF_CAP = _float("INTAKE_BACKOFF_CAP", 2.0)
BACKOFF_JITTER = _float("INTAKE_BACKOFF_JITTER", 0.2)
STUB_PORT = _int("INTAKE_STUB_PORT", 8080)
API_PORT = _int("INTAKE_API_PORT", 8090)
STUB_BASE_URL = os.environ.get("INTAKE_STUB_BASE_URL", f"http://localhost:{STUB_PORT}")
API_BASE_URL = os.environ.get("INTAKE_API_BASE_URL", f"http://localhost:{API_PORT}")
```

- [ ] **Step 2: Sanity check the module imports cleanly**

Run: `python3 -c "from content_intake.common import config; print(config.IN_FLIGHT_CAPACITY)"`
Expected: `2`

- [ ] **Step 3: Commit**

```bash
git add src/content_intake/common/config.py
git commit -m "Add shared config constants with env-var overrides"
```

---

### Task 5: Corpus generator — deterministic file generation and manifest

**Files:**
- Create: `src/content_intake/generator/generate.py`
- Test: `tests/test_generator.py`

**Interfaces:**
- Consumes: `canonical_dumps`, `sha256_hex` from Task 2.
- Produces: `generate_corpus(seed: int, size: int, tenant: str, out_dir: Path, force: bool = False) -> dict` — writes `out_dir/manifest.json` and `out_dir/files/...`, returns the manifest dict. Used by `verify.py` (Task 6) and the `corpus` CLI command (Task 7).

Manifest shape (every per-file entry has all these keys; `order` is the index in final manifest ordering: originals, then duplicates, then the two edge cases):

```json
{
  "corpus_id": "tenant-a-42-100",
  "seed": 42,
  "size": 100,
  "tenant": "tenant-a",
  "totals": {"items": 100, "duplicates": 10, "edge_cases": 2},
  "edge_cases": {"empty_content": "edge/empty.txt", "decode_failed": "edge/malformed.json"},
  "files": [
    {
      "order": 0,
      "path": "orig_0.txt",
      "extension": "txt",
      "bytes": 123,
      "sha256": "...",
      "role": "original",
      "duplicate_of": null,
      "edge_case": null,
      "expected_outcome": "success",
      "expects_annotation": true
    }
  ],
  "digest": "..."
}
```

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_generator.py
import json
from pathlib import Path

from content_intake.generator.generate import generate_corpus


def test_determinism_same_seed_size_tenant_byte_identical(tmp_path):
    out1 = tmp_path / "a"
    out2 = tmp_path / "b"
    m1 = generate_corpus(seed=42, size=60, tenant="tenant-a", out_dir=out1)
    m2 = generate_corpus(seed=42, size=60, tenant="tenant-a", out_dir=out2)
    for entry in m1["files"]:
        p1 = out1 / "files" / entry["path"]
        p2 = out2 / "files" / entry["path"]
        assert p1.read_bytes() == p2.read_bytes()
    assert m1["digest"] == m2["digest"]


def test_file_bytes_depend_only_on_seed_and_size_not_tenant(tmp_path):
    out1 = tmp_path / "a"
    out2 = tmp_path / "b"
    m1 = generate_corpus(seed=7, size=55, tenant="tenant-a", out_dir=out1)
    m2 = generate_corpus(seed=7, size=55, tenant="tenant-b", out_dir=out2)
    for e1, e2 in zip(m1["files"], m2["files"]):
        assert (out1 / "files" / e1["path"]).read_bytes() == (out2 / "files" / e2["path"]).read_bytes()
    assert m1["digest"] != m2["digest"]  # tenant enters the manifest


def test_item_count_matches_size(tmp_path):
    m = generate_corpus(seed=1, size=50, tenant="t", out_dir=tmp_path / "c")
    assert len(m["files"]) == 50
    assert m["totals"]["items"] == 50


def test_mixed_types_present(tmp_path):
    m = generate_corpus(seed=1, size=100, tenant="t", out_dir=tmp_path / "c")
    exts = {e["extension"] for e in m["files"]}
    assert {"txt", "json", "csv", "png"} <= exts


def test_duplicate_rate_within_tolerance(tmp_path):
    size = 100
    m = generate_corpus(seed=1, size=size, tenant="t", out_dir=tmp_path / "c")
    n_dup = sum(1 for e in m["files"] if e["role"] == "duplicate")
    assert abs(n_dup - size / 10) <= 2


def test_duplicates_are_byte_identical_and_same_extension(tmp_path):
    out = tmp_path / "c"
    m = generate_corpus(seed=1, size=100, tenant="t", out_dir=out)
    by_path = {e["path"]: e for e in m["files"]}
    for e in m["files"]:
        if e["role"] != "duplicate":
            continue
        original = by_path[e["duplicate_of"]]
        assert original["extension"] == e["extension"]
        assert (out / "files" / e["path"]).read_bytes() == (out / "files" / original["path"]).read_bytes()


def test_empty_content_edge_case(tmp_path):
    out = tmp_path / "c"
    m = generate_corpus(seed=1, size=60, tenant="t", out_dir=out)
    path = m["edge_cases"]["empty_content"]
    entry = next(e for e in m["files"] if e["path"] == path)
    assert entry["edge_case"] == "empty_content"
    assert entry["expected_outcome"] == "empty_content"
    assert entry["expects_annotation"] is False
    assert (out / "files" / path).read_bytes() == b""


def test_decode_failed_edge_case_is_png_bytes_named_json(tmp_path):
    out = tmp_path / "c"
    m = generate_corpus(seed=1, size=60, tenant="t", out_dir=out)
    path = m["edge_cases"]["decode_failed"]
    entry = next(e for e in m["files"] if e["path"] == path)
    assert entry["edge_case"] == "decode_failed"
    assert entry["expected_outcome"] == "decode_failed"
    assert entry["expects_annotation"] is False
    assert entry["extension"] == "json"
    data = (out / "files" / path).read_bytes()
    assert data.startswith(b"\x89PNG\r\n\x1a\n")
    import json as _json
    import pytest as _pytest
    with _pytest.raises(Exception):
        _json.loads(data)


def test_manifest_written_to_disk_next_to_files(tmp_path):
    out = tmp_path / "c"
    generate_corpus(seed=1, size=50, tenant="t", out_dir=out)
    assert (out / "manifest.json").exists()
    assert (out / "files").is_dir()
    on_disk = json.loads((out / "manifest.json").read_text())
    assert on_disk["seed"] == 1


def test_force_overwrites_nonempty_directory(tmp_path):
    out = tmp_path / "c"
    generate_corpus(seed=1, size=50, tenant="t", out_dir=out)
    generate_corpus(seed=2, size=50, tenant="t", out_dir=out, force=True)
    m = json.loads((out / "manifest.json").read_text())
    assert m["seed"] == 2


def test_refuses_nonempty_directory_without_force(tmp_path):
    out = tmp_path / "c"
    generate_corpus(seed=1, size=50, tenant="t", out_dir=out)
    import pytest
    with pytest.raises(FileExistsError):
        generate_corpus(seed=2, size=50, tenant="t", out_dir=out, force=False)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_generator.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement `generate.py`**

```python
# src/content_intake/generator/generate.py
import io
import json
import shutil
from pathlib import Path
from random import Random

from PIL import Image

from content_intake.common.canonical_json import sha256_hex, sha256_of_canonical

EXTENSIONS = ["txt", "json", "csv", "png"]
WORDS = [
    "delta", "harbor", "quiet", "ledger", "signal", "orbit", "cinder", "maple",
    "vault", "ridge", "amber", "north", "spindle", "cobalt", "lantern", "wren",
]


def _item_rng(seed: int, index: int) -> Random:
    return Random(f"{seed}:{index}")


def _make_txt(rng: Random) -> bytes:
    n_words = rng.randint(20, 60)
    words = [rng.choice(WORDS) for _ in range(n_words)]
    return (" ".join(words) + "\n").encode("utf-8")


def _make_json(rng: Random) -> bytes:
    obj = {
        "id": rng.randint(1, 1_000_000),
        "tags": [rng.choice(WORDS) for _ in range(rng.randint(1, 5))],
        "value": round(rng.uniform(0, 1000), 3),
    }
    return (json.dumps(obj) + "\n").encode("utf-8")


def _make_csv(rng: Random) -> bytes:
    n_rows = rng.randint(3, 10)
    lines = ["col_a,col_b,col_c"]
    for _ in range(n_rows):
        lines.append(f"{rng.randint(0,100)},{rng.choice(WORDS)},{round(rng.uniform(0,1),3)}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def _make_png(rng: Random) -> bytes:
    size = 8
    img = Image.new("RGB", (size, size))
    pixels = [
        (rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255))
        for _ in range(size * size)
    ]
    img.putdata(pixels)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


_MAKERS = {"txt": _make_txt, "json": _make_json, "csv": _make_csv, "png": _make_png}


def generate_corpus(seed: int, size: int, tenant: str, out_dir: Path, force: bool = False) -> dict:
    out_dir = Path(out_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        if not force:
            raise FileExistsError(f"{out_dir} is not empty; pass force=True to overwrite")
        shutil.rmtree(out_dir)
    files_dir = out_dir / "files"
    files_dir.mkdir(parents=True)
    (out_dir / "files" / "edge").mkdir()

    n_dup = round(size / 10)
    n_edge = 2
    n_original = size - n_dup - n_edge

    originals: list[dict] = []
    for i in range(n_original):
        ext = EXTENSIONS[i % len(EXTENSIONS)]
        rng = _item_rng(seed, i)
        data = _MAKERS[ext](rng)
        path = f"orig_{i}.{ext}"
        (files_dir / path).write_bytes(data)
        originals.append({"path": path, "extension": ext, "bytes": data})

    duplicates: list[dict] = []
    for i in range(n_dup):
        rng = _item_rng(seed, n_original + i)
        source = rng.choice(originals)
        path = f"dup_{i}.{source['extension']}"
        (files_dir / path).write_bytes(source["bytes"])
        duplicates.append({"path": path, "extension": source["extension"], "bytes": source["bytes"], "duplicate_of": source["path"]})

    empty_path = "edge/empty.txt"
    (files_dir / empty_path).write_bytes(b"")

    malformed_rng = _item_rng(seed, n_original + n_dup + 1)
    malformed_bytes = _make_png(malformed_rng)
    malformed_path = "edge/malformed.json"
    (files_dir / malformed_path).write_bytes(malformed_bytes)

    entries = []
    order = 0
    for o in originals:
        entries.append({
            "order": order, "path": o["path"], "extension": o["extension"], "bytes": len(o["bytes"]),
            "sha256": sha256_hex(o["bytes"]), "role": "original", "duplicate_of": None,
            "edge_case": None, "expected_outcome": "success", "expects_annotation": True,
        })
        order += 1
    for d in duplicates:
        entries.append({
            "order": order, "path": d["path"], "extension": d["extension"], "bytes": len(d["bytes"]),
            "sha256": sha256_hex(d["bytes"]), "role": "duplicate", "duplicate_of": d["duplicate_of"],
            "edge_case": None, "expected_outcome": "success", "expects_annotation": True,
        })
        order += 1
    entries.append({
        "order": order, "path": empty_path, "extension": "txt", "bytes": 0,
        "sha256": sha256_hex(b""), "role": "edge_case", "duplicate_of": None,
        "edge_case": "empty_content", "expected_outcome": "empty_content", "expects_annotation": False,
    })
    order += 1
    entries.append({
        "order": order, "path": malformed_path, "extension": "json", "bytes": len(malformed_bytes),
        "sha256": sha256_hex(malformed_bytes), "role": "edge_case", "duplicate_of": None,
        "edge_case": "decode_failed", "expected_outcome": "decode_failed", "expects_annotation": False,
    })

    manifest = {
        "corpus_id": f"{tenant}-{seed}-{size}",
        "seed": seed,
        "size": size,
        "tenant": tenant,
        "totals": {"items": len(entries), "duplicates": n_dup, "edge_cases": n_edge},
        "edge_cases": {"empty_content": empty_path, "decode_failed": malformed_path},
        "files": entries,
    }
    manifest["digest"] = sha256_of_canonical(manifest)
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_generator.py -v`
Expected: 11 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/generator/generate.py tests/test_generator.py
git commit -m "Implement deterministic corpus generator"
```

---

### Task 6: Corpus generator — `--verify`

**Files:**
- Create: `src/content_intake/generator/verify.py`
- Test: `tests/test_verify.py`

**Interfaces:**
- Consumes: `generate_corpus` from Task 5.
- Produces: `verify_corpus(out_dir: Path) -> list[str]` — returns a list of human-readable mismatch descriptions; empty list means the corpus matches its own manifest's arguments byte-for-byte. Used by the `corpus --verify` CLI command (Task 7).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_verify.py
import json
from pathlib import Path

from content_intake.generator.generate import generate_corpus
from content_intake.generator.verify import verify_corpus


def test_verify_passes_on_untouched_corpus(tmp_path):
    out = tmp_path / "c"
    generate_corpus(seed=5, size=60, tenant="tenant-a", out_dir=out)
    assert verify_corpus(out) == []


def test_verify_detects_modified_file(tmp_path):
    out = tmp_path / "c"
    m = generate_corpus(seed=5, size=60, tenant="tenant-a", out_dir=out)
    target = out / "files" / m["files"][0]["path"]
    target.write_bytes(target.read_bytes() + b"tampered")
    mismatches = verify_corpus(out)
    assert mismatches != []
    assert any(m["files"][0]["path"] in msg for msg in mismatches)


def test_verify_detects_missing_file(tmp_path):
    out = tmp_path / "c"
    m = generate_corpus(seed=5, size=60, tenant="tenant-a", out_dir=out)
    (out / "files" / m["files"][0]["path"]).unlink()
    mismatches = verify_corpus(out)
    assert mismatches != []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_verify.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement**

```python
# src/content_intake/generator/verify.py
import json
import tempfile
from pathlib import Path

from content_intake.generator.generate import generate_corpus


def verify_corpus(out_dir: Path) -> list[str]:
    out_dir = Path(out_dir)
    manifest = json.loads((out_dir / "manifest.json").read_text())
    mismatches: list[str] = []
    with tempfile.TemporaryDirectory() as tmp:
        fresh_dir = Path(tmp) / "fresh"
        fresh_manifest = generate_corpus(
            seed=manifest["seed"], size=manifest["size"], tenant=manifest["tenant"], out_dir=fresh_dir
        )
        if fresh_manifest["digest"] != manifest["digest"]:
            mismatches.append(f"manifest digest differs: on-disk={manifest['digest']} fresh={fresh_manifest['digest']}")
        for entry in fresh_manifest["files"]:
            on_disk_path = out_dir / "files" / entry["path"]
            fresh_path = fresh_dir / "files" / entry["path"]
            if not on_disk_path.exists():
                mismatches.append(f"missing on disk: {entry['path']}")
                continue
            if on_disk_path.read_bytes() != fresh_path.read_bytes():
                mismatches.append(f"byte mismatch: {entry['path']}")
    return mismatches
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_verify.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/generator/verify.py tests/test_verify.py
git commit -m "Implement corpus --verify (re-derive from manifest's own arguments)"
```

---

### Task 7: CLI `corpus` subcommand

**Files:**
- Create: `src/content_intake/cli/corpus_cmd.py`
- Modify: `src/content_intake/cli/main.py`

**Interfaces:**
- Consumes: `generate_corpus` (Task 5), `verify_corpus` (Task 6).
- Produces: `add_corpus_parser(subparsers)`, `run_corpus(args) -> int`. `main.py` dispatches to this.

- [ ] **Step 1: Write `corpus_cmd.py`**

```python
# src/content_intake/cli/corpus_cmd.py
import argparse
import json
import sys
from pathlib import Path

from content_intake.generator.generate import generate_corpus
from content_intake.generator.verify import verify_corpus


def add_corpus_parser(subparsers) -> None:
    p = subparsers.add_parser("corpus")
    p.add_argument("--seed", type=int)
    p.add_argument("--size", type=int)
    p.add_argument("--tenant", type=str)
    p.add_argument("--out", type=str, required=True)
    p.add_argument("--force", action="store_true")
    p.add_argument("--verify", action="store_true")


def run_corpus(args: argparse.Namespace) -> int:
    out_dir = Path(args.out)
    if args.verify:
        mismatches = verify_corpus(out_dir)
        if mismatches:
            for m in mismatches:
                print(m, file=sys.stderr)
            return 1
        print(json.dumps({"verified": True, "out": str(out_dir)}))
        return 0

    if args.seed is None or args.size is None or args.tenant is None:
        print("corpus generation requires --seed, --size, and --tenant", file=sys.stderr)
        return 1
    if not (50 <= args.size <= 500):
        print("--size must be between 50 and 500 inclusive", file=sys.stderr)
        return 1
    try:
        manifest = generate_corpus(
            seed=args.seed, size=args.size, tenant=args.tenant, out_dir=out_dir, force=args.force
        )
    except FileExistsError as e:
        print(str(e), file=sys.stderr)
        return 1
    print(json.dumps({"corpus_id": manifest["corpus_id"], "out": str(out_dir), "items": manifest["totals"]["items"]}))
    return 0
```

- [ ] **Step 2: Wire it into `main.py`**

```python
# src/content_intake/cli/main.py
import argparse
import sys

from content_intake.cli.corpus_cmd import add_corpus_parser, run_corpus


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="intake")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("version")
    add_corpus_parser(subparsers)
    args = parser.parse_args(argv)

    if args.command == "version":
        print("content-intake-pipeline 0.1.0")
        return 0
    if args.command == "corpus":
        return run_corpus(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
```

- [ ] **Step 3: Verify manually end to end**

Run:
```bash
rm -rf /tmp/tc && ./intake corpus --seed 1 --size 60 --tenant tenant-a --out /tmp/tc
./intake corpus --verify --out /tmp/tc
```
Expected: first prints a JSON line with `corpus_id`/`items`; second prints `{"verified": true, "out": "/tmp/tc"}` and exits 0.

- [ ] **Step 4: Commit**

```bash
git add src/content_intake/cli/corpus_cmd.py src/content_intake/cli/main.py
git commit -m "Wire generator and --verify into ./intake corpus"
```

---

### Task 8: Stub — config and in-memory state

**Files:**
- Create: `src/content_intake/stub/state.py`
- Test: `tests/test_stub_state.py`

**Interfaces:**
- Produces: class `StubState(latency_mode, latency_ms, latency_jitter_min_ms, latency_jitter_max_ms, latency_seed, failure_every_n, failure_status, in_flight_capacity)` with methods `next_latency_ms() -> float`, `enter_call() -> bool` (returns False if over capacity, else increments in-flight and returns True), `exit_call() -> None`, `should_fail_this_billed_call() -> bool` (call once per billed call — advances the Nth-failure schedule; kept for single-threaded/test use), `bill() -> None` (kept for single-threaded/test use), `bill_and_check_failure() -> bool` (atomic bill+check pair — **the HTTP handler in Task 9 must call this, not the two separately**, since two independently-locked calls are not atomic as a pair under concurrent requests and can corrupt the failure count), `record_server_error() -> None`, `record_over_capacity() -> None`, `stats() -> dict`, `reset(patch: dict | None) -> None`, `annotate(content: bytes) -> dict`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_stub_state.py
from content_intake.stub.state import StubState


def make_state(**overrides):
    defaults = dict(
        latency_mode="fixed", latency_ms=0, latency_jitter_min_ms=50, latency_jitter_max_ms=250,
        latency_seed=1, failure_every_n=0, failure_status=500, in_flight_capacity=2,
    )
    defaults.update(overrides)
    return StubState(**defaults)


def test_capacity_gate_admits_up_to_capacity():
    s = make_state(in_flight_capacity=2)
    assert s.enter_call() is True
    assert s.enter_call() is True
    assert s.enter_call() is False
    s.exit_call()
    assert s.enter_call() is True


def test_failure_schedule_fires_every_nth_billed_call():
    s = make_state(failure_every_n=3)
    results = []
    for _ in range(9):
        s.bill()
        results.append(s.should_fail_this_billed_call())
    assert results == [False, False, True, False, False, True, False, False, True]


def test_failure_schedule_disabled_when_zero():
    s = make_state(failure_every_n=0)
    for _ in range(20):
        s.bill()
        assert s.should_fail_this_billed_call() is False


def test_annotation_stable_for_same_content():
    s = make_state()
    a1 = s.annotate(b"hello")
    a2 = s.annotate(b"hello")
    assert a1["sha256"] == a2["sha256"]
    assert a1["result"] == a2["result"]


def test_annotation_includes_sha256_of_content():
    import hashlib
    s = make_state()
    a = s.annotate(b"hello")
    assert a["sha256"] == hashlib.sha256(b"hello").hexdigest()


def test_fixed_latency_returns_configured_value():
    s = make_state(latency_mode="fixed", latency_ms=150)
    assert s.next_latency_ms() == 150


def test_jitter_latency_within_bounds_and_repeatable_with_same_seed():
    s1 = make_state(latency_mode="jitter", latency_seed=42, latency_jitter_min_ms=50, latency_jitter_max_ms=250)
    s2 = make_state(latency_mode="jitter", latency_seed=42, latency_jitter_min_ms=50, latency_jitter_max_ms=250)
    seq1 = [s1.next_latency_ms() for _ in range(10)]
    seq2 = [s2.next_latency_ms() for _ in range(10)]
    assert seq1 == seq2
    assert all(50 <= v <= 250 for v in seq1)


def test_stats_reports_required_fields():
    s = make_state(in_flight_capacity=2)
    s.bill(); s.enter_call()
    s.record_server_error()
    s.record_over_capacity()
    stats = s.stats()
    for key in ("billed_calls", "current_in_flight", "max_in_flight", "server_error_calls", "over_capacity_calls"):
        assert key in stats


def test_max_in_flight_tracks_observed_peak_not_configured_cap():
    s = make_state(in_flight_capacity=5)
    s.enter_call(); s.enter_call()
    assert s.stats()["max_in_flight"] == 2
    s.exit_call(); s.exit_call()
    assert s.stats()["max_in_flight"] == 2


def test_reset_clears_counters_and_restarts_sequences():
    s = make_state(failure_every_n=2)
    s.bill(); s.should_fail_this_billed_call()
    s.bill(); s.should_fail_this_billed_call()
    s.reset(None)
    assert s.stats()["billed_calls"] == 0
    s.bill()
    assert s.should_fail_this_billed_call() is False
    s.bill()
    assert s.should_fail_this_billed_call() is True


def test_reset_applies_partial_config_patch():
    s = make_state(failure_every_n=7)
    s.reset({"failure_every_n": 1})
    s.bill()
    assert s.should_fail_this_billed_call() is True


def test_bill_and_check_failure_matches_sequential_bill_and_check():
    s = make_state(failure_every_n=3)
    results = []
    for _ in range(9):
        results.append(s.bill_and_check_failure())
    assert results == [False, False, True, False, False, True, False, False, True]


def test_bill_and_check_failure_exact_count_under_concurrency():
    # The bug this guards against: bill() + should_fail_this_billed_call() as two
    # separately-locked calls can let concurrent threads interleave between them,
    # corrupting the failure COUNT (not just which item fails). bill_and_check_failure()
    # must not have this race: with failure_every_n=2 and 20 concurrent calls, exactly
    # 10 must be flagged as failures, every time, regardless of thread interleaving.
    import threading

    s = make_state(failure_every_n=2)
    results = []
    lock = threading.Lock()

    def call():
        result = s.bill_and_check_failure()
        with lock:
            results.append(result)

    threads = [threading.Thread(target=call) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 20
    assert sum(1 for r in results if r) == 10
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_stub_state.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement**

```python
# src/content_intake/stub/state.py
import hashlib
import threading
from random import Random


class StubState:
    def __init__(
        self,
        latency_mode: str = "fixed",
        latency_ms: int = 150,
        latency_jitter_min_ms: int = 50,
        latency_jitter_max_ms: int = 250,
        latency_seed: int = 20260803,
        failure_every_n: int = 7,
        failure_status: int = 500,
        in_flight_capacity: int = 2,
    ):
        self._lock = threading.Lock()
        self._configure(
            latency_mode, latency_ms, latency_jitter_min_ms, latency_jitter_max_ms,
            latency_seed, failure_every_n, failure_status, in_flight_capacity,
        )
        self._reset_counters()

    def _configure(self, latency_mode, latency_ms, jitter_min, jitter_max, latency_seed,
                   failure_every_n, failure_status, in_flight_capacity):
        self.latency_mode = latency_mode
        self.latency_ms = latency_ms
        self.latency_jitter_min_ms = jitter_min
        self.latency_jitter_max_ms = jitter_max
        self.latency_seed = latency_seed
        self.failure_every_n = failure_every_n
        self.failure_status = failure_status
        self.in_flight_capacity = in_flight_capacity
        self._jitter_rng = Random(latency_seed)

    def _reset_counters(self):
        self._billed_calls = 0
        self._current_in_flight = 0
        self._max_in_flight = 0
        self._server_error_calls = 0
        self._over_capacity_calls = 0
        self._billed_call_index = 0

    def next_latency_ms(self) -> float:
        with self._lock:
            if self.latency_mode == "fixed":
                return self.latency_ms
            return self._jitter_rng.uniform(self.latency_jitter_min_ms, self.latency_jitter_max_ms)

    def enter_call(self) -> bool:
        with self._lock:
            if self._current_in_flight >= self.in_flight_capacity:
                return False
            self._current_in_flight += 1
            self._max_in_flight = max(self._max_in_flight, self._current_in_flight)
            return True

    def exit_call(self) -> None:
        with self._lock:
            self._current_in_flight = max(0, self._current_in_flight - 1)

    def bill(self) -> None:
        with self._lock:
            self._billed_calls += 1
            self._billed_call_index += 1

    def should_fail_this_billed_call(self) -> bool:
        with self._lock:
            if self.failure_every_n <= 0:
                return False
            return self._billed_call_index % self.failure_every_n == 0

    def bill_and_check_failure(self) -> bool:
        """Atomic bill()+should_fail_this_billed_call() pair. The HTTP handler (Task 9)
        must use this instead of calling the two separately — under concurrent requests,
        two independently-locked calls are not atomic as a pair, so one thread's check can
        read an index another thread's bill() already advanced past, corrupting the
        1-in-N failure count (not just which item fails, which EXT-REQ-2 permits, but the
        count, which it does not)."""
        with self._lock:
            self._billed_calls += 1
            self._billed_call_index += 1
            if self.failure_every_n <= 0:
                return False
            return self._billed_call_index % self.failure_every_n == 0

    def record_server_error(self) -> None:
        with self._lock:
            self._server_error_calls += 1

    def record_over_capacity(self) -> None:
        with self._lock:
            self._over_capacity_calls += 1

    def annotate(self, content: bytes) -> dict:
        digest = hashlib.sha256(content).hexdigest()
        rng = Random(digest)
        return {"sha256": digest, "result": rng.random(), "labels": [w for w in ["a", "b", "c"] if rng.random() > 0.5]}

    def stats(self) -> dict:
        with self._lock:
            return {
                "billed_calls": self._billed_calls,
                "current_in_flight": self._current_in_flight,
                "max_in_flight": self._max_in_flight,
                "server_error_calls": self._server_error_calls,
                "over_capacity_calls": self._over_capacity_calls,
            }

    def reset(self, patch: dict | None) -> None:
        with self._lock:
            self._reset_counters()
            if patch:
                for key, value in patch.items():
                    if hasattr(self, key):
                        setattr(self, key, value)
                self._jitter_rng = Random(self.latency_seed)
            else:
                self._jitter_rng = Random(self.latency_seed)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_stub_state.py -v`
Expected: 13 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/stub/state.py tests/test_stub_state.py
git commit -m "Implement stub in-memory state: counters, latency sequence, failure schedule"
```

---

### Task 9: Stub — FastAPI app

**Files:**
- Create: `src/content_intake/stub/app.py`
- Test: `tests/test_stub_app.py`

**Interfaces:**
- Consumes: `StubState` from Task 8.
- Produces: `create_app(state: StubState) -> FastAPI`. Used by `run.py` (Task 10) and the conformance suite (Task 11) indirectly (via a running process, not an import).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_stub_app.py
import base64
import time

from fastapi.testclient import TestClient

from content_intake.stub.app import create_app
from content_intake.stub.state import StubState


def make_client(**overrides):
    defaults = dict(
        latency_mode="fixed", latency_ms=0, latency_jitter_min_ms=50, latency_jitter_max_ms=250,
        latency_seed=1, failure_every_n=0, failure_status=500, in_flight_capacity=2,
    )
    defaults.update(overrides)
    state = StubState(**defaults)
    return TestClient(create_app(state)), state


def test_healthz():
    client, _ = make_client()
    resp = client.get("/healthz")
    assert resp.status_code == 200


def test_annotate_success():
    client, _ = make_client()
    body = {"content_b64": base64.b64encode(b"hello").decode()}
    resp = client.post("/v1/annotate", json=body)
    assert resp.status_code == 200
    data = resp.json()
    assert "sha256" in data


def test_annotate_rejects_unknown_field():
    client, _ = make_client()
    body = {"content_b64": base64.b64encode(b"hello").decode(), "tenant": "sneaky"}
    resp = client.post("/v1/annotate", json=body)
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_request"


def test_annotate_rejects_missing_field():
    client, _ = make_client()
    resp = client.post("/v1/annotate", json={})
    assert resp.status_code == 400
    assert resp.json()["error"]["code"] == "invalid_request"


def test_billing_increments_on_success():
    client, state = make_client()
    body = {"content_b64": base64.b64encode(b"hello").decode()}
    client.post("/v1/annotate", json=body)
    assert state.stats()["billed_calls"] == 1


def test_failure_schedule_returns_server_error_and_bills():
    client, state = make_client(failure_every_n=2)
    body = {"content_b64": base64.b64encode(b"hello").decode()}
    r1 = client.post("/v1/annotate", json=body)
    r2 = client.post("/v1/annotate", json=body)
    assert r1.status_code == 200
    assert r2.status_code == 500
    assert r2.json()["error"]["code"] == "server_error"
    assert state.stats()["billed_calls"] == 2
    assert state.stats()["server_error_calls"] == 1


def test_over_capacity_returns_429_and_bills(monkeypatch):
    client, state = make_client(in_flight_capacity=0)
    body = {"content_b64": base64.b64encode(b"hello").decode()}
    resp = client.post("/v1/annotate", json=body)
    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "over_capacity"
    assert state.stats()["billed_calls"] == 1
    assert state.stats()["over_capacity_calls"] == 1


def test_stats_endpoint():
    client, _ = make_client()
    resp = client.get("/v1/stats")
    assert resp.status_code == 200
    for key in ("billed_calls", "current_in_flight", "max_in_flight", "server_error_calls", "over_capacity_calls"):
        assert key in resp.json()


def test_reset_endpoint_clears_and_patches():
    client, state = make_client(failure_every_n=7)
    body = {"content_b64": base64.b64encode(b"hello").decode()}
    client.post("/v1/annotate", json=body)
    resp = client.post("/v1/reset", json={"failure_every_n": 1})
    assert resp.status_code == 200
    assert state.stats()["billed_calls"] == 0
    r = client.post("/v1/annotate", json=body)
    assert r.status_code == 500
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_stub_app.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement**

```python
# src/content_intake/stub/app.py
import base64
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from content_intake.stub.state import StubState

_ALLOWED_ANNOTATE_FIELDS = {"content_b64"}


def create_app(state: StubState) -> FastAPI:
    app = FastAPI()

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/v1/stats")
    def stats():
        return state.stats()

    @app.post("/v1/reset")
    async def reset(request: Request):
        try:
            patch = await request.json()
        except Exception:
            patch = None
        state.reset(patch or None)
        return {"ok": True}

    @app.post("/v1/annotate")
    async def annotate(request: Request):
        try:
            body = await request.json()
        except Exception:
            return JSONResponse(status_code=400, content={"error": {"code": "invalid_request"}})
        if not isinstance(body, dict) or set(body.keys()) != _ALLOWED_ANNOTATE_FIELDS:
            return JSONResponse(status_code=400, content={"error": {"code": "invalid_request"}})
        try:
            content = base64.b64decode(body["content_b64"], validate=True)
        except Exception:
            return JSONResponse(status_code=400, content={"error": {"code": "invalid_request"}})

        admitted = state.enter_call()
        try:
            delay_ms = state.next_latency_ms()
            time.sleep(delay_ms / 1000.0)
            should_fail = state.bill_and_check_failure()
            if not admitted:
                state.record_over_capacity()
                return JSONResponse(status_code=429, content={"error": {"code": "over_capacity"}})
            if should_fail:
                state.record_server_error()
                return JSONResponse(status_code=state.failure_status, content={"error": {"code": "server_error"}})
            return state.annotate(content)
        finally:
            if admitted:
                state.exit_call()

    return app
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_stub_app.py -v`
Expected: 9 passed

Note on `test_over_capacity_returns_429_and_bills`: with `in_flight_capacity=0`, `enter_call()` never admits, so latency still applies (per EXT-REQ-3/the brief's "held for its full duration" language applying to 500s — over-capacity in this stub returns promptly without holding a slot, which is correct since it was never admitted; latency is still applied before the billed outcome is decided, matching "billed before its outcome is chosen").

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/stub/app.py tests/test_stub_app.py
git commit -m "Implement stub FastAPI app: annotate/stats/reset/healthz"
```

---

### Task 10: Stub — config from env vars, uvicorn runner, and `./intake stub`

**Files:**
- Create: `src/content_intake/stub/run.py`
- Create: `src/content_intake/cli/stub_cmd.py`
- Modify: `src/content_intake/cli/main.py`

**Interfaces:**
- Consumes: `StubState`, `create_app` from Tasks 8–9.
- Produces: `build_state_from_env() -> StubState`, `run(port: int) -> None` (blocking, foreground uvicorn). `add_stub_parser`, `run_stub`.

- [ ] **Step 1: Implement `run.py`**

```python
# src/content_intake/stub/run.py
import os

import uvicorn

from content_intake.stub.app import create_app
from content_intake.stub.state import StubState


def _env(name: str, default, cast):
    val = os.environ.get(f"STUB_{name.upper()}")
    return cast(val) if val is not None else default


def build_state_from_env() -> StubState:
    return StubState(
        latency_mode=_env("latency_mode", "fixed", str),
        latency_ms=_env("latency_ms", 150, int),
        latency_jitter_min_ms=_env("latency_jitter_min_ms", 50, int),
        latency_jitter_max_ms=_env("latency_jitter_max_ms", 250, int),
        latency_seed=_env("latency_seed", 20260803, int),
        failure_every_n=_env("failure_every_n", 7, int),
        failure_status=_env("failure_status", 500, int),
        in_flight_capacity=_env("in_flight_capacity", 2, int),
    )


def run(port: int = 8080) -> None:
    state = build_state_from_env()
    app = create_app(state)
    uvicorn.run(app, host="0.0.0.0", port=port, workers=1, log_level="info")


if __name__ == "__main__":
    run()
```

- [ ] **Step 2: Implement `stub_cmd.py`**

```python
# src/content_intake/cli/stub_cmd.py
import argparse

from content_intake.stub.run import run


def add_stub_parser(subparsers) -> None:
    p = subparsers.add_parser("stub")
    p.add_argument("--port", type=int, default=8080)


def run_stub(args: argparse.Namespace) -> int:
    run(port=args.port)
    return 0
```

- [ ] **Step 3: Wire into `main.py`**

```python
# add to src/content_intake/cli/main.py
from content_intake.cli.stub_cmd import add_stub_parser, run_stub
# ... add_stub_parser(subparsers) alongside add_corpus_parser(subparsers)
# ... elif args.command == "stub": return run_stub(args)
```

- [ ] **Step 4: Verify manually**

Run: `./intake stub --port 8080 &` then `curl -s localhost:8080/healthz`
Expected: `{"ok":true}`, then `kill %1`

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/stub/run.py src/content_intake/cli/stub_cmd.py src/content_intake/cli/main.py
git commit -m "Wire stub into ./intake stub with STUB_ env var config"
```

---

### Task 11: Stub conformance suite (required deliverable)

**Files:**
- Create: `tests/test_stub_conformance.py`

**Interfaces:**
- Consumes: nothing from `src/` — black-box HTTP only, per the brief's requirement that this suite never import pipeline code. Talks to `STUB_URL` (default `http://localhost:8080`).

- [ ] **Step 1: Write the suite**

```python
# tests/test_stub_conformance.py
"""
Black-box conformance suite for the annotation stub.
MUST NOT import any pipeline code — plain HTTP only, against a running stub.
Start the stub first: `./intake stub --port 8080` (or rely on STUB_URL for a
stub running elsewhere).
"""
import base64
import hashlib
import os
import time

import httpx
import pytest

STUB_URL = os.environ.get("STUB_URL", "http://localhost:8080")


@pytest.fixture(autouse=True)
def reset_stub():
    httpx.post(f"{STUB_URL}/v1/reset", json={
        "latency_mode": "fixed", "latency_ms": 0, "failure_every_n": 0, "in_flight_capacity": 2,
    })
    yield


def annotate(content: bytes) -> httpx.Response:
    return httpx.post(f"{STUB_URL}/v1/annotate", json={"content_b64": base64.b64encode(content).decode()})


def test_healthz():
    assert httpx.get(f"{STUB_URL}/healthz").status_code == 200


def test_annotate_success_and_stable_answer():
    r1 = annotate(b"conformance-content")
    r2 = annotate(b"conformance-content")
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["sha256"] == hashlib.sha256(b"conformance-content").hexdigest()
    assert r1.json() == r2.json()


def test_rejects_unexpected_field():
    r = httpx.post(f"{STUB_URL}/v1/annotate", json={
        "content_b64": base64.b64encode(b"x").decode(), "tenant": "sneaky"
    })
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"


def test_rejects_missing_field():
    r = httpx.post(f"{STUB_URL}/v1/annotate", json={})
    assert r.status_code == 400
    assert r.json()["error"]["code"] == "invalid_request"


def test_billed_calls_increments_on_success():
    before = httpx.get(f"{STUB_URL}/v1/stats").json()["billed_calls"]
    annotate(b"count-me")
    after = httpx.get(f"{STUB_URL}/v1/stats").json()["billed_calls"]
    assert after == before + 1


def test_failure_schedule_every_nth_call():
    httpx.post(f"{STUB_URL}/v1/reset", json={"failure_every_n": 3, "latency_ms": 0})
    statuses = [annotate(f"item-{i}".encode()).status_code for i in range(6)]
    assert statuses == [200, 200, 500, 200, 200, 500]


def test_over_capacity_returns_429_never_admitted():
    httpx.post(f"{STUB_URL}/v1/reset", json={"in_flight_capacity": 1, "latency_ms": 300})
    results = []

    def call(i):
        results.append(annotate(f"cap-{i}".encode()).status_code)

    import threading
    threads = [threading.Thread(target=call, args=(i,)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert 429 in results


def test_max_in_flight_reports_observed_peak():
    httpx.post(f"{STUB_URL}/v1/reset", json={"in_flight_capacity": 5, "latency_ms": 200})
    import threading
    threads = [threading.Thread(target=annotate, args=(f"peak-{i}".encode(),)) for i in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stats = httpx.get(f"{STUB_URL}/v1/stats").json()
    assert stats["max_in_flight"] >= 2


def test_latency_is_repeatable_with_same_seed():
    httpx.post(f"{STUB_URL}/v1/reset", json={"latency_mode": "jitter", "latency_seed": 999, "latency_jitter_min_ms": 10, "latency_jitter_max_ms": 20})
    start = time.monotonic()
    annotate(b"timing-a")
    d1 = time.monotonic() - start

    httpx.post(f"{STUB_URL}/v1/reset", json={"latency_mode": "jitter", "latency_seed": 999, "latency_jitter_min_ms": 10, "latency_jitter_max_ms": 20})
    start = time.monotonic()
    annotate(b"timing-b")
    d2 = time.monotonic() - start

    assert abs(d1 - d2) < 0.05


def test_reset_clears_counters():
    annotate(b"before-reset")
    httpx.post(f"{STUB_URL}/v1/reset", json={})
    stats = httpx.get(f"{STUB_URL}/v1/stats").json()
    assert stats["billed_calls"] == 0
```

- [ ] **Step 2: Run the suite against a live stub**

Run:
```bash
./intake stub --port 8080 &
sleep 1
STUB_URL=http://localhost:8080 pytest tests/test_stub_conformance.py -v
kill %1
```
Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_stub_conformance.py
git commit -m "Add required black-box stub conformance suite"
```

---

### Task 12: Extraction — edge-case detection and text extraction

**Files:**
- Create: `src/content_intake/pipeline/extraction.py`
- Test: `tests/test_extraction.py`

**Interfaces:**
- Produces: `detect_edge_case(extension: str, data: bytes) -> str | None` (returns `"empty_content"`, `"decode_failed"`, or `None`), `extract_text(extension: str, data: bytes) -> str | None`. Used by `worker.py` (Task 16).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_extraction.py
from content_intake.pipeline.extraction import detect_edge_case, extract_text


def test_empty_bytes_is_empty_content_edge_case():
    assert detect_edge_case("txt", b"") == "empty_content"


def test_malformed_json_is_decode_failed():
    png_bytes = b"\x89PNG\r\n\x1a\nrestofbytes"
    assert detect_edge_case("json", png_bytes) == "decode_failed"


def test_valid_json_is_not_an_edge_case():
    assert detect_edge_case("json", b'{"a": 1}') is None


def test_nonempty_txt_is_not_an_edge_case():
    assert detect_edge_case("txt", b"hello") is None


def test_png_is_never_an_edge_case_even_though_binary():
    png_bytes = b"\x89PNG\r\n\x1a\nrestofbytes"
    assert detect_edge_case("png", png_bytes) is None


def test_extract_text_for_txt():
    assert extract_text("txt", b"hello world") == "hello world"


def test_extract_text_for_valid_json():
    assert extract_text("json", b'{"a": 1}') == '{"a": 1}'


def test_extract_text_for_csv():
    assert extract_text("csv", b"a,b\n1,2") == "a,b\n1,2"


def test_extract_text_returns_none_for_png():
    png_bytes = b"\x89PNG\r\n\x1a\nrestofbytes"
    assert extract_text("png", png_bytes) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_extraction.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement**

```python
# src/content_intake/pipeline/extraction.py
import json

_TEXT_EXTENSIONS = {"txt", "json", "csv"}


def detect_edge_case(extension: str, data: bytes) -> str | None:
    if len(data) == 0:
        return "empty_content"
    if extension == "json":
        try:
            json.loads(data)
        except Exception:
            return "decode_failed"
    return None


def extract_text(extension: str, data: bytes) -> str | None:
    if extension not in _TEXT_EXTENSIONS:
        return None
    return data.decode("utf-8", errors="replace")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_extraction.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/pipeline/extraction.py tests/test_extraction.py
git commit -m "Implement edge-case detection and text extraction"
```

---

### Task 13: Stub HTTP client — single-call wrapper

**Files:**
- Create: `src/content_intake/pipeline/stub_client.py`
- Test: `tests/test_stub_client.py`

**Interfaces:**
- Produces: class `StubCallResult(status_code: int | None, body: dict | None, error: str | None)`, `call_annotate(client: httpx.Client, base_url: str, content: bytes, timeout: float) -> StubCallResult`. `error` is `"timeout"`, `"connection_error"`, or `None`. Used by `worker.py` (Task 16).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_stub_client.py
import base64
import json

import httpx
import pytest

from content_intake.pipeline.stub_client import call_annotate


def test_success_response_parsed():
    def handler(request):
        assert json.loads(request.content) == {"content_b64": base64.b64encode(b"x").decode()}
        return httpx.Response(200, json={"sha256": "abc"})

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    result = call_annotate(client, "http://stub", b"x", timeout=1.0)
    assert result.status_code == 200
    assert result.body == {"sha256": "abc"}
    assert result.error is None


def test_server_error_response_parsed():
    transport = httpx.MockTransport(lambda r: httpx.Response(500, json={"error": {"code": "server_error"}}))
    client = httpx.Client(transport=transport)
    result = call_annotate(client, "http://stub", b"x", timeout=1.0)
    assert result.status_code == 500
    assert result.body == {"error": {"code": "server_error"}}


def test_timeout_is_reported_as_error():
    def handler(request):
        raise httpx.TimeoutException("timed out")

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    result = call_annotate(client, "http://stub", b"x", timeout=1.0)
    assert result.error == "timeout"
    assert result.status_code is None


def test_connection_error_is_reported_as_error():
    def handler(request):
        raise httpx.ConnectError("refused")

    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport)
    result = call_annotate(client, "http://stub", b"x", timeout=1.0)
    assert result.error == "connection_error"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_stub_client.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement**

```python
# src/content_intake/pipeline/stub_client.py
import base64

import httpx


class StubCallResult:
    def __init__(self, status_code: int | None, body: dict | None, error: str | None):
        self.status_code = status_code
        self.body = body
        self.error = error


def call_annotate(client: httpx.Client, base_url: str, content: bytes, timeout: float) -> StubCallResult:
    payload = {"content_b64": base64.b64encode(content).decode("ascii")}
    try:
        resp = client.post(f"{base_url}/v1/annotate", json=payload, timeout=timeout)
    except httpx.TimeoutException:
        return StubCallResult(None, None, "timeout")
    except httpx.TransportError:
        return StubCallResult(None, None, "connection_error")
    try:
        body = resp.json()
    except ValueError:
        body = None
    return StubCallResult(resp.status_code, body, None)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_stub_client.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/pipeline/stub_client.py tests/test_stub_client.py
git commit -m "Implement single-call stub HTTP client wrapper"
```

---

### Task 14: Worker — item claim and lease renewal

**Files:**
- Create: `src/content_intake/pipeline/worker.py` (started here, extended in Tasks 15–17)
- Test: `tests/test_worker_claim.py`

**Interfaces:**
- Consumes: `connect()` from Task 3.
- Produces: `claim_item(conn, worker_id: str, lease_seconds: int) -> dict | None`, class `LeaseRenewer(connect_fn, item_id, worker_id, lease_seconds, interval)` with `.start()`, `.stop()`, `.lost() -> bool`. Used by `process_item` (Task 16).

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_worker_claim.py
import time
import uuid

from content_intake.pipeline.db import connect
from content_intake.pipeline.worker import claim_item, LeaseRenewer


def _insert_run_and_item(conn, tenant="tenant-a"):
    run_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    conn.execute("INSERT INTO runs (run_id, corpus_id, tenant) VALUES (%s, %s, %s)", (run_id, "c1", tenant))
    conn.execute(
        """
        INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                            role, expects_annotation, state)
        VALUES (%s, %s, %s, 'p', 'txt', 5, 'abc', 'original', true, 'pending')
        """,
        (item_id, run_id, tenant),
    )
    conn.commit()
    return run_id, item_id


def test_claim_returns_pending_item(db_conn):
    run_id, item_id = _insert_run_and_item(db_conn)
    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    assert claimed["item_id"] == item_id
    assert claimed["state"] == "in_progress"
    assert claimed["leased_by"] == "worker-0"


def test_claim_returns_none_when_nothing_pending(db_conn):
    assert claim_item(db_conn, "worker-0", lease_seconds=5) is None


def test_two_workers_never_claim_the_same_item(db_conn):
    _insert_run_and_item(db_conn)
    c1 = claim_item(db_conn, "worker-0", lease_seconds=5)
    c2 = claim_item(db_conn, "worker-1", lease_seconds=5)
    assert c1 is not None
    assert c2 is None  # only one item existed


def test_expired_lease_becomes_reclaimable(db_conn):
    _run_id, item_id = _insert_run_and_item(db_conn)
    claim_item(db_conn, "worker-0", lease_seconds=0)  # expires immediately
    time.sleep(0.05)
    reclaimed = claim_item(db_conn, "worker-1", lease_seconds=5)
    assert reclaimed["item_id"] == item_id
    assert reclaimed["leased_by"] == "worker-1"


def test_lease_renewer_renews_while_running(db_conn):
    _run_id, item_id = _insert_run_and_item(db_conn)
    claim_item(db_conn, "worker-0", lease_seconds=1)
    renewer = LeaseRenewer(connect, item_id, "worker-0", lease_seconds=1, interval=1)
    renewer.start()
    time.sleep(2.2)
    assert renewer.lost() is False
    row = db_conn.execute("SELECT leased_until FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["leased_until"] is not None
    renewer.stop()


def test_lease_renewer_detects_stolen_lease(db_conn):
    _run_id, item_id = _insert_run_and_item(db_conn)
    claim_item(db_conn, "worker-0", lease_seconds=1)
    renewer = LeaseRenewer(connect, item_id, "worker-0", lease_seconds=1, interval=1)
    renewer.start()
    # simulate the original worker's lease expiring and another worker stealing it
    time.sleep(1.2)
    db_conn.execute(
        "UPDATE items SET leased_by = 'worker-1', leased_until = now() + interval '5 seconds' WHERE item_id = %s",
        (item_id,),
    )
    db_conn.commit()
    time.sleep(1.5)
    assert renewer.lost() is True
    renewer.stop()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_worker_claim.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement**

```python
# src/content_intake/pipeline/worker.py
import threading


def claim_item(conn, worker_id: str, lease_seconds: int) -> dict | None:
    row = conn.execute(
        """
        UPDATE items
        SET state = 'in_progress', leased_by = %(worker_id)s,
            leased_until = now() + %(lease_seconds)s * interval '1 second'
        WHERE item_id = (
            SELECT item_id FROM items
            WHERE state = 'pending' OR (state = 'in_progress' AND leased_until < now())
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING *
        """,
        {"worker_id": worker_id, "lease_seconds": lease_seconds},
    ).fetchone()
    conn.commit()
    return row


class LeaseRenewer:
    def __init__(self, connect_fn, item_id: str, worker_id: str, lease_seconds: int, interval: int):
        self._connect_fn = connect_fn
        self._item_id = item_id
        self._worker_id = worker_id
        self._lease_seconds = lease_seconds
        self._interval = interval
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval + 2)

    def lost(self) -> bool:
        return self._lost.is_set()

    def _run(self) -> None:
        conn = self._connect_fn()
        try:
            while not self._stop.wait(self._interval):
                cur = conn.execute(
                    """
                    UPDATE items SET leased_until = now() + %(s)s * interval '1 second'
                    WHERE item_id = %(item_id)s AND leased_by = %(worker_id)s AND state = 'in_progress'
                    """,
                    {"s": self._lease_seconds, "item_id": self._item_id, "worker_id": self._worker_id},
                )
                conn.commit()
                if cur.rowcount == 0:
                    self._lost.set()
                    return
        finally:
            conn.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_worker_claim.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/pipeline/worker.py tests/test_worker_claim.py
git commit -m "Implement worker item claim and lease renewal thread"
```

---

### Task 15: Worker — capacity slot claim/release

**Files:**
- Modify: `src/content_intake/pipeline/worker.py`
- Test: `tests/test_worker_slots.py`

**Interfaces:**
- Consumes: `connect()`, `ensure_slots()` from Task 3.
- Produces: `claim_slot(conn, worker_id: str, lease_seconds: int) -> int | None`, `release_slot(conn, slot_id: int, worker_id: str) -> None`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_worker_slots.py
import time

from content_intake.pipeline.db import ensure_slots
from content_intake.pipeline.worker import claim_slot, release_slot


def test_claim_slot_up_to_capacity(db_conn):
    ensure_slots(db_conn, 2)
    s1 = claim_slot(db_conn, "worker-0", lease_seconds=5)
    s2 = claim_slot(db_conn, "worker-1", lease_seconds=5)
    s3 = claim_slot(db_conn, "worker-2", lease_seconds=5)
    assert s1 is not None and s2 is not None
    assert {s1, s2} == {1, 2}
    assert s3 is None


def test_release_frees_the_slot(db_conn):
    ensure_slots(db_conn, 1)
    s1 = claim_slot(db_conn, "worker-0", lease_seconds=5)
    assert s1 == 1
    release_slot(db_conn, s1, "worker-0")
    s2 = claim_slot(db_conn, "worker-1", lease_seconds=5)
    assert s2 == 1


def test_release_by_wrong_worker_does_not_free_slot(db_conn):
    ensure_slots(db_conn, 1)
    s1 = claim_slot(db_conn, "worker-0", lease_seconds=5)
    release_slot(db_conn, s1, "worker-1")  # not the holder
    s2 = claim_slot(db_conn, "worker-1", lease_seconds=5)
    assert s2 is None


def test_expired_slot_lease_self_heals(db_conn):
    ensure_slots(db_conn, 1)
    claim_slot(db_conn, "worker-0", lease_seconds=0)
    time.sleep(0.05)
    s2 = claim_slot(db_conn, "worker-1", lease_seconds=5)
    assert s2 == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_worker_slots.py -v`
Expected: FAIL with `AttributeError`/`ImportError` on `claim_slot`

- [ ] **Step 3: Append to `worker.py`**

```python
# append to src/content_intake/pipeline/worker.py

def claim_slot(conn, worker_id: str, lease_seconds: int) -> int | None:
    row = conn.execute(
        """
        UPDATE stub_call_slots
        SET held_by = %(worker_id)s, lease_until = now() + %(s)s * interval '1 second'
        WHERE slot_id = (
            SELECT slot_id FROM stub_call_slots
            WHERE held_by IS NULL OR lease_until < now()
            ORDER BY slot_id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING slot_id
        """,
        {"worker_id": worker_id, "s": lease_seconds},
    ).fetchone()
    conn.commit()
    return row["slot_id"] if row else None


def release_slot(conn, slot_id: int, worker_id: str) -> None:
    conn.execute(
        "UPDATE stub_call_slots SET held_by = NULL, lease_until = NULL WHERE slot_id = %s AND held_by = %s",
        (slot_id, worker_id),
    )
    conn.commit()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_worker_slots.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/pipeline/worker.py tests/test_worker_slots.py
git commit -m "Implement capacity slot claim/release (D-08)"
```

---

### Task 16: Worker — item_attempts bookkeeping

**Files:**
- Modify: `src/content_intake/pipeline/worker.py`
- Test: `tests/test_worker_attempts.py`

**Interfaces:**
- Produces: `start_attempt(conn, item_id: str, tenant: str, worker_id: str, slot_id: int) -> tuple[str, int]` (returns `(attempt_id, attempt_no)`, commits before returning), `complete_attempt(conn, attempt_id: str, http_status: int | None, outcome: str) -> None`, `find_completed_success(conn, item_id: str) -> dict | None`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_worker_attempts.py
import uuid

import pytest
import psycopg

from content_intake.pipeline.worker import start_attempt, complete_attempt, find_completed_success


def _insert_item(conn, tenant="tenant-a"):
    run_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    conn.execute("INSERT INTO runs (run_id, corpus_id, tenant) VALUES (%s, %s, %s)", (run_id, "c1", tenant))
    conn.execute(
        """
        INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                            role, expects_annotation, state)
        VALUES (%s, %s, %s, 'p', 'txt', 5, 'abc', 'original', true, 'in_progress')
        """,
        (item_id, run_id, tenant),
    )
    conn.commit()
    return item_id


def test_start_attempt_commits_before_returning(db_conn):
    item_id = _insert_item(db_conn)
    attempt_id, attempt_no = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    assert attempt_no == 1
    row = db_conn.execute("SELECT completed_at FROM item_attempts WHERE attempt_id = %s", (attempt_id,)).fetchone()
    assert row is not None
    assert row["completed_at"] is None


def test_attempt_numbers_increment_per_item(db_conn):
    item_id = _insert_item(db_conn)
    _, n1 = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    _, n2 = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    assert (n1, n2) == (1, 2)


def test_complete_attempt_records_outcome(db_conn):
    item_id = _insert_item(db_conn)
    attempt_id, _ = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    complete_attempt(db_conn, attempt_id, http_status=200, outcome="success")
    row = db_conn.execute(
        "SELECT completed_at, http_status, outcome FROM item_attempts WHERE attempt_id = %s", (attempt_id,)
    ).fetchone()
    assert row["completed_at"] is not None
    assert row["http_status"] == 200
    assert row["outcome"] == "success"


def test_find_completed_success_returns_none_when_absent(db_conn):
    item_id = _insert_item(db_conn)
    assert find_completed_success(db_conn, item_id) is None


def test_find_completed_success_returns_the_successful_attempt(db_conn):
    item_id = _insert_item(db_conn)
    attempt_id, _ = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    complete_attempt(db_conn, attempt_id, http_status=200, outcome="success")
    found = find_completed_success(db_conn, item_id)
    assert found["attempt_id"] == attempt_id


def test_find_completed_success_ignores_failed_attempts(db_conn):
    item_id = _insert_item(db_conn)
    attempt_id, _ = start_attempt(db_conn, item_id, "tenant-a", "worker-0", slot_id=1)
    complete_attempt(db_conn, attempt_id, http_status=500, outcome="server_error")
    assert find_completed_success(db_conn, item_id) is None


def test_unique_constraint_prevents_duplicate_attempt_numbers(db_conn):
    item_id = _insert_item(db_conn)
    db_conn.execute(
        "INSERT INTO item_attempts (attempt_id, item_id, tenant, attempt_no, started_at, worker_id, slot_id) "
        "VALUES (%s, %s, 'tenant-a', 1, now(), 'worker-0', 1)",
        (str(uuid.uuid4()), item_id),
    )
    db_conn.commit()
    with pytest.raises(psycopg.errors.UniqueViolation):
        db_conn.execute(
            "INSERT INTO item_attempts (attempt_id, item_id, tenant, attempt_no, started_at, worker_id, slot_id) "
            "VALUES (%s, %s, 'tenant-a', 1, now(), 'worker-1', 2)",
            (str(uuid.uuid4()), item_id),
        )
    db_conn.rollback()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_worker_attempts.py -v`
Expected: FAIL with `ImportError` on the three new functions

- [ ] **Step 3: Append to `worker.py`**

```python
# append to src/content_intake/pipeline/worker.py
import uuid


def start_attempt(conn, item_id: str, tenant: str, worker_id: str, slot_id: int) -> tuple[str, int]:
    row = conn.execute(
        "SELECT COALESCE(MAX(attempt_no), 0) + 1 AS n FROM item_attempts WHERE item_id = %s", (item_id,)
    ).fetchone()
    attempt_no = row["n"]
    attempt_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO item_attempts (attempt_id, item_id, tenant, attempt_no, started_at, worker_id, slot_id)
        VALUES (%s, %s, %s, %s, now(), %s, %s)
        """,
        (attempt_id, item_id, tenant, attempt_no, worker_id, slot_id),
    )
    conn.commit()
    return attempt_id, attempt_no


def complete_attempt(conn, attempt_id: str, http_status: int | None, outcome: str) -> None:
    conn.execute(
        "UPDATE item_attempts SET completed_at = now(), http_status = %s, outcome = %s WHERE attempt_id = %s",
        (http_status, outcome, attempt_id),
    )
    conn.commit()


def find_completed_success(conn, item_id: str) -> dict | None:
    return conn.execute(
        "SELECT * FROM item_attempts WHERE item_id = %s AND outcome = 'success' AND completed_at IS NOT NULL "
        "ORDER BY attempt_no DESC LIMIT 1",
        (item_id,),
    ).fetchone()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_worker_attempts.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/pipeline/worker.py tests/test_worker_attempts.py
git commit -m "Implement item_attempts bookkeeping: insert-before-call, update-after (D-09)"
```

---

### Task 17: Worker — per-item processing loop

**Files:**
- Modify: `src/content_intake/pipeline/worker.py`
- Test: `tests/test_worker_process_item.py`

**Interfaces:**
- Consumes: `claim_item`, `LeaseRenewer`, `claim_slot`, `release_slot`, `start_attempt`, `complete_attempt`, `find_completed_success` (this file, Tasks 14–16); `detect_edge_case`, `extract_text` (Task 12); `StubCallResult`, `call_annotate` (Task 13).
- Produces: `process_item(conn, connect_fn, item: dict, corpus_files_dir: Path, stub_client: httpx.Client, stub_base_url: str, worker_id: str) -> None`. This is the function `run_worker` (Task 18) calls once per claimed item.

This task is the heart of the design: it must implement D-01/D-05/D-08/D-09/D-10 together. Read `ARCHITECTURE.md`'s "Per-item processing" section before writing this.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_worker_process_item.py
import base64
import uuid
from pathlib import Path

import httpx

from content_intake.pipeline.db import connect, ensure_slots
from content_intake.pipeline.worker import claim_item, process_item


def _insert_item(conn, files_dir: Path, tenant="tenant-a", content=b"hello", extension="txt", role="original", expects_annotation=True, path="p.txt"):
    run_id = str(uuid.uuid4())
    item_id = str(uuid.uuid4())
    (files_dir / path).parent.mkdir(parents=True, exist_ok=True)
    (files_dir / path).write_bytes(content)
    import hashlib
    conn.execute("INSERT INTO runs (run_id, corpus_id, tenant) VALUES (%s, %s, %s)", (run_id, "c1", tenant))
    conn.execute(
        """
        INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                            role, expects_annotation, state)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
        """,
        (item_id, run_id, tenant, path, extension, len(content), hashlib.sha256(content).hexdigest(), role, expects_annotation),
    )
    conn.commit()
    return item_id


def _fake_success_client():
    def handler(request):
        return httpx.Response(200, json={"sha256": "irrelevant-for-test", "result": 0.5})
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_successful_item_reaches_succeeded_with_annotation(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)
    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, _fake_success_client(), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state, annotation, extracted_text FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "succeeded"
    assert row["annotation"] is not None
    assert row["extracted_text"] == "hello"


def test_empty_content_never_calls_stub(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path, content=b"", expects_annotation=False)

    def handler(request):
        raise AssertionError("stub must not be called for empty_content")

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "empty_content"


def test_decode_failed_never_calls_stub(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    png_bytes = b"\x89PNG\r\n\x1a\nrest"
    item_id = _insert_item(db_conn, tmp_path, content=png_bytes, extension="json", path="p.json", expects_annotation=False)

    def handler(request):
        raise AssertionError("stub must not be called for decode_failed")

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "decode_failed"


def test_annotation_cache_hit_skips_stub_call_and_scopes_by_tenant(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    content = b"shared-bytes"
    import hashlib
    sha = hashlib.sha256(content).hexdigest()
    db_conn.execute(
        "INSERT INTO annotations_cache (tenant, sha256, annotation) VALUES ('tenant-a', %s, %s)",
        (sha, '{"cached": true}'),
    )
    db_conn.commit()

    item_id = _insert_item(db_conn, tmp_path, content=content, tenant="tenant-a")

    def handler(request):
        raise AssertionError("stub must not be called on a cache hit")

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state, annotation FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "succeeded"
    assert row["annotation"]["cached"] is True


def test_cache_is_not_shared_across_tenants(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    content = b"shared-bytes-2"
    import hashlib
    sha = hashlib.sha256(content).hexdigest()
    db_conn.execute(
        "INSERT INTO annotations_cache (tenant, sha256, annotation) VALUES ('tenant-a', %s, %s)",
        (sha, '{"cached": true}'),
    )
    db_conn.commit()

    item_id = _insert_item(db_conn, tmp_path, content=content, tenant="tenant-b", path="p2.txt")
    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, _fake_success_client(), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state, annotation FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "succeeded"
    assert row["annotation"].get("cached") is not True  # got a fresh annotation, not tenant-a's cached one


def test_400_terminates_immediately_as_invalid_request(db_conn, tmp_path):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(400, json={"error": {"code": "invalid_request"}})

    claimed = claim_item(db_conn, "worker-0", lease_seconds=5)
    process_item(db_conn, connect,
                 claimed, tmp_path, httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state, reason FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "annotation_invalid_request"
    assert len(calls) == 1  # no retries


def test_exhausted_retries_lands_in_annotation_failed(db_conn, tmp_path, monkeypatch):
    ensure_slots(db_conn, 2)
    item_id = _insert_item(db_conn, tmp_path)

    def handler(request):
        return httpx.Response(500, json={"error": {"code": "server_error"}})

    import content_intake.pipeline.worker as worker_mod
    monkeypatch.setattr(worker_mod, "BACKOFF_BASE", 0.001)
    monkeypatch.setattr(worker_mod, "BACKOFF_CAP", 0.002)

    claimed = claim_item(db_conn, "worker-0", lease_seconds=30)
    process_item(db_conn, connect,
                 claimed, tmp_path, httpx.Client(transport=httpx.MockTransport(handler)), "http://stub", "worker-0")
    row = db_conn.execute("SELECT state FROM items WHERE item_id = %s", (item_id,)).fetchone()
    assert row["state"] == "annotation_failed"
    attempts = db_conn.execute("SELECT COUNT(*) AS n FROM item_attempts WHERE item_id = %s", (item_id,)).fetchone()
    assert attempts["n"] == 5


def test_start_attempt_race_is_handled_without_crashing(db_conn, tmp_path, monkeypatch):
    # Simulates the D-09 backstop: a stale worker (lease already stolen) reaches
    # start_attempt() concurrently with the new owner and loses the UNIQUE(item_id,
    # attempt_no) race. process_item must not crash or leave the slot held — it should
    # roll back and abandon the item, since the current owner is already handling it.
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
Expected: FAIL — `process_item` does not exist yet

- [ ] **Step 3: Append to `worker.py`**

```python
# append to src/content_intake/pipeline/worker.py
import json
import random
import time
from pathlib import Path

import psycopg

from content_intake.common import config
from content_intake.pipeline.extraction import detect_edge_case, extract_text
from content_intake.pipeline.stub_client import call_annotate

BACKOFF_BASE = config.BACKOFF_BASE
BACKOFF_CAP = config.BACKOFF_CAP
BACKOFF_JITTER = config.BACKOFF_JITTER
MAX_ATTEMPTS = config.MAX_ATTEMPTS
HTTP_TIMEOUT_SECONDS = config.HTTP_TIMEOUT_SECONDS
RETRYABLE_STATUSES = {500, 429}


def _backoff_delay(attempt: int) -> float:
    base = min(BACKOFF_BASE * (2 ** (attempt - 1)), BACKOFF_CAP)
    jitter = base * BACKOFF_JITTER
    return max(0.0, base + random.uniform(-jitter, jitter))


def _finalize(conn, item_id: str, worker_id: str, state: str, annotation=None, extracted_text=None, reason=None) -> None:
    conn.execute(
        """
        UPDATE items
        SET state = %(state)s, annotation = %(annotation)s, extracted_text = %(extracted_text)s,
            reason = %(reason)s, updated_at = now()
        WHERE item_id = %(item_id)s AND leased_by = %(worker_id)s
        """,
        {
            "state": state,
            "annotation": json.dumps(annotation) if annotation is not None else None,
            "extracted_text": extracted_text,
            "reason": json.dumps(reason) if reason is not None else None,
            "item_id": item_id, "worker_id": worker_id,
        },
    )
    conn.commit()


def process_item(conn, connect_fn, item: dict, corpus_files_dir: Path, stub_client, stub_base_url: str, worker_id: str) -> None:
    item_id = item["item_id"]
    tenant = item["tenant"]  # D-10: tenant comes only from the claimed row, never anywhere else

    prior_success = find_completed_success(conn, item_id)
    if prior_success:
        # item_attempts doesn't store the annotation payload itself — re-fetch it from
        # annotations_cache by content hash, since a successful attempt always writes there.
        cached = conn.execute(
            "SELECT annotation FROM annotations_cache WHERE tenant = %s AND sha256 = %s",
            (tenant, item["sha256"]),
        ).fetchone()
        _finalize(conn, item_id, worker_id, "succeeded", annotation=cached["annotation"] if cached else {})
        return

    data = (corpus_files_dir / item["source_path"]).read_bytes()

    edge_case = detect_edge_case(item["extension"], data)
    if edge_case is not None:
        _finalize(conn, item_id, worker_id, edge_case, reason={"code": edge_case})
        return

    extracted = extract_text(item["extension"], data)

    cached = conn.execute(
        "SELECT annotation FROM annotations_cache WHERE tenant = %s AND sha256 = %s",
        (tenant, item["sha256"]),
    ).fetchone()
    if cached is not None:
        _finalize(conn, item_id, worker_id, "succeeded", annotation=cached["annotation"], extracted_text=extracted)
        return

    renewer = LeaseRenewer(connect_fn, item_id, worker_id, lease_seconds=config.LEASE_SECONDS, interval=config.LEASE_RENEW_INTERVAL)
    renewer.start()
    try:
        last_status = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            if renewer.lost():
                return
            slot_id = None
            while slot_id is None:
                slot_id = claim_slot(conn, worker_id, lease_seconds=config.LEASE_SECONDS)
                if slot_id is None:
                    time.sleep(0.05 + random.uniform(0, 0.05))
            try:
                try:
                    attempt_id, _ = start_attempt(conn, item_id, tenant, worker_id, slot_id)
                except psycopg.errors.UniqueViolation:
                    # UNIQUE(item_id, attempt_no) backstop (D-09): this worker's lease was
                    # already stolen and the new owner recorded this attempt_no first.
                    # Abandon the item — the current owner is already handling it.
                    conn.rollback()
                    return
                result = call_annotate(stub_client, stub_base_url, data, timeout=HTTP_TIMEOUT_SECONDS)
                outcome = None
                if result.error is not None:
                    outcome = result.error
                    last_status = None
                else:
                    last_status = result.status_code
                    if result.status_code == 200:
                        outcome = "success"
                    elif result.status_code == 400:
                        outcome = "invalid_request"
                    elif result.status_code in RETRYABLE_STATUSES:
                        outcome = "server_error" if result.status_code == 500 else "over_capacity"
                    else:
                        outcome = "unexpected_status"
                complete_attempt(conn, attempt_id, last_status, outcome)
            finally:
                release_slot(conn, slot_id, worker_id)

            if outcome == "success":
                annotation = result.body
                conn.execute(
                    "INSERT INTO annotations_cache (tenant, sha256, annotation) VALUES (%s, %s, %s) "
                    "ON CONFLICT (tenant, sha256) DO NOTHING",
                    (tenant, item["sha256"], json.dumps(annotation)),
                )
                conn.commit()
                _finalize(conn, item_id, worker_id, "succeeded", annotation=annotation, extracted_text=extracted)
                return
            if outcome == "invalid_request":
                _finalize(conn, item_id, worker_id, "annotation_invalid_request",
                          extracted_text=extracted, reason={"code": "invalid_request", "attempt": attempt})
                return
            # retryable: server_error, over_capacity, timeout, connection_error
            if attempt < MAX_ATTEMPTS:
                time.sleep(_backoff_delay(attempt))

        _finalize(conn, item_id, worker_id, "annotation_failed", extracted_text=extracted,
                  reason={"code": "annotation_failed", "attempts": MAX_ATTEMPTS, "last_status": last_status})
    finally:
        renewer.stop()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_worker_process_item.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/pipeline/worker.py tests/test_worker_process_item.py
git commit -m "Implement per-item processing loop: edge cases, cache, retry policy (D-05), tenant invariant (D-10)"
```

---

### Task 18: Worker — main process entrypoint

**Files:**
- Modify: `src/content_intake/pipeline/worker.py`

**Interfaces:**
- Produces: `run_worker(index: int, corpus_files_dirs: dict[str, Path]) -> None` — a blocking loop. Since a worker processes items from any run/tenant, and each run's files live under that run's own corpus directory, the worker needs a way to resolve `item["run_id"]` to a filesystem path. Simplify: store the absolute corpus directory on the `runs` row at submit time (Task 20) and join `items.source_path` against it.

- [ ] **Step 1: Add a `corpus_dir` lookup and the main loop to `worker.py`**

First, add the column the run-lookup needs (extend schema — see Task 3's `schema.sql`, add before running this task):

```sql
-- add to src/content_intake/pipeline/schema.sql, in the runs table definition
    corpus_dir TEXT NOT NULL,
```

Update `_insert_run_and_item`-style test helpers across earlier tasks' tests is not required — those tests use `INSERT INTO runs (run_id, corpus_id, tenant)`, which will now fail NOT NULL. Update every such `INSERT INTO runs` in `tests/test_worker_claim.py`, `tests/test_worker_attempts.py`, and `tests/test_worker_process_item.py` to include `corpus_dir`:

```python
# change every occurrence of:
conn.execute("INSERT INTO runs (run_id, corpus_id, tenant) VALUES (%s, %s, %s)", (run_id, "c1", tenant))
# to:
conn.execute("INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, %s, %s, %s)", (run_id, "c1", tenant, str(tmp_path)))
```

(In `test_worker_process_item.py`, `_insert_item` already receives `files_dir`; pass `str(files_dir)` as `corpus_dir`. In `test_worker_claim.py` and `test_worker_attempts.py`, add a `tmp_path` fixture parameter to each test function and pass `str(tmp_path)`.)

Now append the main loop:

```python
# append to src/content_intake/pipeline/worker.py
import httpx

from content_intake.pipeline.db import connect


def run_worker(index: int) -> None:
    worker_id = f"worker-{index}"
    stub_client = httpx.Client()
    print(f"[{worker_id}] starting", flush=True)
    while True:
        conn = connect()
        try:
            item = claim_item(conn, worker_id, lease_seconds=config.LEASE_SECONDS)
            if item is None:
                conn.close()
                time.sleep(0.5)
                continue
            run = conn.execute("SELECT corpus_dir FROM runs WHERE run_id = %s", (item["run_id"],)).fetchone()
            corpus_files_dir = Path(run["corpus_dir"]) / "files"
            process_item(conn, connect, item, corpus_files_dir, stub_client, config.STUB_BASE_URL, worker_id)
        finally:
            conn.close()


if __name__ == "__main__":
    import sys
    run_worker(int(sys.argv[1]) if len(sys.argv) > 1 else 0)
```

- [ ] **Step 2: Run the full worker test suite to confirm the schema change didn't break anything**

Run: `pytest tests/test_worker_claim.py tests/test_worker_attempts.py tests/test_worker_process_item.py tests/test_db.py -v`
Expected: all pass (after the `INSERT INTO runs` updates above)

- [ ] **Step 3: Commit**

```bash
git add src/content_intake/pipeline/schema.sql src/content_intake/pipeline/worker.py \
        tests/test_worker_claim.py tests/test_worker_attempts.py tests/test_worker_process_item.py
git commit -m "Add worker main loop; store corpus_dir on runs for file resolution"
```

---

### Task 19: Pipeline API — submit endpoint

**Files:**
- Create: `src/content_intake/pipeline/api.py`
- Test: `tests/test_api_submit.py`

**Interfaces:**
- Consumes: `connect()` (Task 3), `sha256_hex` (Task 2).
- Produces: `submit_run(conn, corpus_dir: Path, tenant: str) -> str` (returns `run_id`), `create_api_app() -> FastAPI` with `POST /v1/runs`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_api_submit.py
import json
from pathlib import Path

from fastapi.testclient import TestClient

from content_intake.generator.generate import generate_corpus
from content_intake.pipeline.api import create_api_app, submit_run


def test_submit_run_inserts_all_items(db_conn, tmp_path):
    out = tmp_path / "corpus"
    manifest = generate_corpus(seed=1, size=55, tenant="tenant-a", out_dir=out)
    run_id = submit_run(db_conn, out, "tenant-a")
    count = db_conn.execute("SELECT COUNT(*) AS n FROM items WHERE run_id = %s", (run_id,)).fetchone()["n"]
    assert count == manifest["totals"]["items"]


def test_submit_run_rejects_sha256_mismatch(db_conn, tmp_path):
    out = tmp_path / "corpus"
    manifest = generate_corpus(seed=1, size=55, tenant="tenant-a", out_dir=out)
    tampered_path = out / "files" / manifest["files"][0]["path"]
    tampered_path.write_bytes(tampered_path.read_bytes() + b"x")
    import pytest
    with pytest.raises(ValueError):
        submit_run(db_conn, out, "tenant-a")
    # nothing partially committed
    count = db_conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"]
    assert count == 0


def test_submit_endpoint_returns_run_id(tmp_path):
    out = tmp_path / "corpus"
    generate_corpus(seed=2, size=50, tenant="tenant-a", out_dir=out)
    client = TestClient(create_api_app())
    resp = client.post("/v1/runs", json={"corpus_dir": str(out), "tenant": "tenant-a"})
    assert resp.status_code == 200
    assert "run_id" in resp.json()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_api_submit.py -v`
Expected: FAIL with `ModuleNotFoundError`

- [ ] **Step 3: Implement**

```python
# src/content_intake/pipeline/api.py
import hashlib
import json
import uuid
from pathlib import Path

from fastapi import FastAPI
from pydantic import BaseModel

from content_intake.pipeline.db import connect


def submit_run(conn, corpus_dir: Path, tenant: str) -> str:
    corpus_dir = Path(corpus_dir).resolve()
    manifest = json.loads((corpus_dir / "manifest.json").read_text())
    run_id = str(uuid.uuid4())
    with conn.transaction():
        conn.execute(
            "INSERT INTO runs (run_id, corpus_id, tenant, corpus_dir) VALUES (%s, %s, %s, %s)",
            (run_id, manifest["corpus_id"], tenant, str(corpus_dir)),
        )
        for entry in manifest["files"]:
            file_path = corpus_dir / "files" / entry["path"]
            data = file_path.read_bytes()
            actual_sha = hashlib.sha256(data).hexdigest()
            if actual_sha != entry["sha256"]:
                raise ValueError(
                    f"sha256 mismatch for {entry['path']}: manifest={entry['sha256']} disk={actual_sha}"
                )
            conn.execute(
                """
                INSERT INTO items (item_id, run_id, tenant, source_path, extension, bytes, sha256,
                                    role, duplicate_of, edge_case, expects_annotation, state)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending')
                """,
                (
                    str(uuid.uuid4()), run_id, tenant, entry["path"], entry["extension"],
                    entry["bytes"], actual_sha, entry["role"], entry.get("duplicate_of"),
                    entry.get("edge_case"), entry["expects_annotation"],
                ),
            )
    return run_id


class SubmitRequest(BaseModel):
    corpus_dir: str
    tenant: str


def create_api_app() -> FastAPI:
    app = FastAPI()

    @app.post("/v1/runs")
    def submit(req: SubmitRequest):
        conn = connect()
        try:
            run_id = submit_run(conn, Path(req.corpus_dir), req.tenant)
            return {"run_id": run_id}
        finally:
            conn.close()

    return app
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_api_submit.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/pipeline/api.py tests/test_api_submit.py
git commit -m "Implement submit: synchronous transactional bulk insert (D-14)"
```

---

### Task 20: Pipeline API — item/items/status endpoints

**Files:**
- Modify: `src/content_intake/pipeline/api.py`
- Test: `tests/test_api_query.py`

**Interfaces:**
- Produces: routes `GET /v1/runs/{run_id}/status`, `GET /v1/items/{item_id}?tenant=...`, `GET /v1/runs/{run_id}/items?tenant=...&state=...`, all tenant-scoped per REQ-1.2.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_api_query.py
from fastapi.testclient import TestClient

from content_intake.generator.generate import generate_corpus
from content_intake.pipeline.api import create_api_app, submit_run


def _submit(db_conn, tmp_path, tenant="tenant-a", seed=1, size=50):
    out = tmp_path / f"corpus-{tenant}-{seed}"
    generate_corpus(seed=seed, size=size, tenant=tenant, out_dir=out)
    return submit_run(db_conn, out, tenant)


def test_status_reports_states_and_terminal_flag(db_conn, tmp_path):
    run_id = _submit(db_conn, tmp_path)
    client = TestClient(create_api_app())
    resp = client.get(f"/v1/runs/{run_id}/status")
    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == run_id
    assert body["tenant"] == "tenant-a"
    assert body["states"]["pending"] == 50
    assert body["terminal"] is False


def test_status_unknown_run_is_404(db_conn, tmp_path):
    client = TestClient(create_api_app())
    resp = client.get("/v1/runs/00000000-0000-0000-0000-000000000000/status")
    assert resp.status_code == 404


def test_item_returns_by_id_and_tenant(db_conn, tmp_path):
    run_id = _submit(db_conn, tmp_path)
    item_id = db_conn.execute("SELECT item_id FROM items WHERE run_id = %s LIMIT 1", (run_id,)).fetchone()["item_id"]
    client = TestClient(create_api_app())
    resp = client.get(f"/v1/items/{item_id}", params={"tenant": "tenant-a"})
    assert resp.status_code == 200
    assert resp.json()["item_id"] == str(item_id)


def test_item_wrong_tenant_is_404(db_conn, tmp_path):
    run_id = _submit(db_conn, tmp_path)
    item_id = db_conn.execute("SELECT item_id FROM items WHERE run_id = %s LIMIT 1", (run_id,)).fetchone()["item_id"]
    client = TestClient(create_api_app())
    resp = client.get(f"/v1/items/{item_id}", params={"tenant": "tenant-b"})
    assert resp.status_code == 404


def test_items_filtered_by_state(db_conn, tmp_path):
    run_id = _submit(db_conn, tmp_path)
    client = TestClient(create_api_app())
    resp = client.get(f"/v1/runs/{run_id}/items", params={"tenant": "tenant-a", "state": "pending"})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 50
    assert all(item["state"] == "pending" for item in body)


def test_items_wrong_tenant_is_404(db_conn, tmp_path):
    run_id = _submit(db_conn, tmp_path)
    client = TestClient(create_api_app())
    resp = client.get(f"/v1/runs/{run_id}/items", params={"tenant": "tenant-b"})
    assert resp.status_code == 404
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_api_query.py -v`
Expected: FAIL with 404s from routes that don't exist yet (or generic FastAPI 404 for unmatched paths — confirm the failures are route-not-found, not assertion mismatches, i.e. genuinely not implemented yet)

- [ ] **Step 3: Append routes to `api.py`**

```python
# append to src/content_intake/pipeline/api.py, inside create_api_app(), before `return app`

    @app.get("/v1/runs/{run_id}/status")
    def status(run_id: str):
        conn = connect()
        try:
            run = conn.execute("SELECT tenant FROM runs WHERE run_id = %s", (run_id,)).fetchone()
            if run is None:
                return JSONResponse(status_code=404, content={"error": "not_found"})
            rows = conn.execute(
                "SELECT state, COUNT(*) AS n FROM items WHERE run_id = %s GROUP BY state", (run_id,)
            ).fetchall()
            states = {r["state"]: r["n"] for r in rows}
            total = sum(states.values())
            nonterminal = states.get("pending", 0) + states.get("in_progress", 0)
            return {
                "run_id": run_id, "tenant": run["tenant"], "states": states,
                "terminal": total > 0 and nonterminal == 0,
            }
        finally:
            conn.close()

    @app.get("/v1/items/{item_id}")
    def get_item(item_id: str, tenant: str):
        conn = connect()
        try:
            row = conn.execute(
                "SELECT * FROM items WHERE item_id = %s AND tenant = %s", (item_id, tenant)
            ).fetchone()
            if row is None:
                return JSONResponse(status_code=404, content={"error": "not_found"})
            return _item_to_json(row)
        finally:
            conn.close()

    @app.get("/v1/runs/{run_id}/items")
    def list_items(run_id: str, tenant: str, state: str | None = None):
        conn = connect()
        try:
            run = conn.execute("SELECT tenant FROM runs WHERE run_id = %s", (run_id,)).fetchone()
            if run is None or run["tenant"] != tenant:
                return JSONResponse(status_code=404, content={"error": "not_found"})
            if state:
                rows = conn.execute(
                    "SELECT * FROM items WHERE run_id = %s AND tenant = %s AND state = %s ORDER BY created_at",
                    (run_id, tenant, state),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM items WHERE run_id = %s AND tenant = %s ORDER BY created_at", (run_id, tenant)
                ).fetchall()
            return [_item_to_json(r) for r in rows]
        finally:
            conn.close()
```

Add the missing import and helper near the top of `api.py`:

```python
# add near the top imports of src/content_intake/pipeline/api.py
from fastapi.responses import JSONResponse


def _item_to_json(row: dict) -> dict:
    out = dict(row)
    for key in ("item_id", "run_id"):
        out[key] = str(out[key])
    for key in ("created_at", "updated_at", "leased_until"):
        if out.get(key) is not None:
            out[key] = out[key].isoformat()
    return out
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_api_query.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add src/content_intake/pipeline/api.py tests/test_api_query.py
git commit -m "Implement tenant-scoped item/items/status endpoints (REQ-1.2)"
```

---

### Task 21: Pipeline API uvicorn runner + CLI submit/status/item/items

**Files:**
- Create: `src/content_intake/pipeline/run_api.py`
- Create: `src/content_intake/cli/query_cmd.py`
- Modify: `src/content_intake/cli/main.py`

**Interfaces:**
- Consumes: `create_api_app` (Task 20), `config.API_BASE_URL`/`API_PORT` (Task 4).
- Produces: `run(port: int)`, `add_submit_parser`/`run_submit`, `add_status_parser`/`run_status`, `add_item_parser`/`run_item`, `add_items_parser`/`run_items`.

- [ ] **Step 1: Implement `run_api.py`**

```python
# src/content_intake/pipeline/run_api.py
import uvicorn

from content_intake.pipeline.api import create_api_app


def run(port: int = 8090) -> None:
    uvicorn.run(create_api_app(), host="0.0.0.0", port=port, workers=1, log_level="info")


if __name__ == "__main__":
    run()
```

- [ ] **Step 2: Implement `query_cmd.py`**

```python
# src/content_intake/cli/query_cmd.py
import argparse
import json
import sys
from pathlib import Path

import httpx

from content_intake.common import config


def add_submit_parser(subparsers) -> None:
    p = subparsers.add_parser("submit")
    p.add_argument("--corpus", required=True)
    p.add_argument("--tenant", required=True)


def run_submit(args: argparse.Namespace) -> int:
    corpus_dir = str(Path(args.corpus).resolve())
    resp = httpx.post(f"{config.API_BASE_URL}/v1/runs", json={"corpus_dir": corpus_dir, "tenant": args.tenant})
    if resp.status_code != 200:
        print(resp.text, file=sys.stderr)
        return 1
    print(json.dumps(resp.json()))
    return 0


def add_status_parser(subparsers) -> None:
    p = subparsers.add_parser("status")
    p.add_argument("--run", required=True, dest="run_id")


def run_status(args: argparse.Namespace) -> int:
    resp = httpx.get(f"{config.API_BASE_URL}/v1/runs/{args.run_id}/status")
    if resp.status_code != 200:
        print(resp.text, file=sys.stderr)
        return 1
    print(json.dumps(resp.json()))
    return 0


def add_item_parser(subparsers) -> None:
    p = subparsers.add_parser("item")
    p.add_argument("--tenant", required=True)
    p.add_argument("--id", required=True, dest="item_id")


def run_item(args: argparse.Namespace) -> int:
    resp = httpx.get(f"{config.API_BASE_URL}/v1/items/{args.item_id}", params={"tenant": args.tenant})
    if resp.status_code != 200:
        print(resp.text, file=sys.stderr)
        return 1
    print(json.dumps(resp.json()))
    return 0


def add_items_parser(subparsers) -> None:
    p = subparsers.add_parser("items")
    p.add_argument("--tenant", required=True)
    p.add_argument("--run", required=True, dest="run_id")
    p.add_argument("--state", default=None)


def run_items(args: argparse.Namespace) -> int:
    params = {"tenant": args.tenant}
    if args.state:
        params["state"] = args.state
    resp = httpx.get(f"{config.API_BASE_URL}/v1/runs/{args.run_id}/items", params=params)
    if resp.status_code != 200:
        print(resp.text, file=sys.stderr)
        return 1
    print(json.dumps(resp.json()))
    return 0
```

- [ ] **Step 3: Wire into `main.py`**

```python
# add to src/content_intake/cli/main.py
from content_intake.cli.query_cmd import (
    add_submit_parser, run_submit, add_status_parser, run_status,
    add_item_parser, run_item, add_items_parser, run_items,
)
# register: add_submit_parser(subparsers); add_status_parser(subparsers); add_item_parser(subparsers); add_items_parser(subparsers)
# dispatch: elif args.command == "submit": return run_submit(args)
#           elif args.command == "status": return run_status(args)
#           elif args.command == "item": return run_item(args)
#           elif args.command == "items": return run_items(args)
```

- [ ] **Step 4: Commit** (end-to-end manual verification happens in Task 24 once `up` exists)

```bash
git add src/content_intake/pipeline/run_api.py src/content_intake/cli/query_cmd.py src/content_intake/cli/main.py
git commit -m "Wire submit/status/item/items CLI commands to the Pipeline API"
```

---

### Task 22: Process lifecycle — `up`/`down`/`reset`

**Files:**
- Create: `src/content_intake/cli/lifecycle_cmd.py`
- Modify: `src/content_intake/cli/main.py`

**Interfaces:**
- Produces: `add_up_parser`/`run_up`, `add_down_parser`/`run_down`, `add_reset_parser`/`run_reset`. Writes/reads `.intake_state.json` at the repo root.

- [ ] **Step 1: Implement `lifecycle_cmd.py`**

```python
# src/content_intake/cli/lifecycle_cmd.py
import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx
import psycopg

from content_intake.common import config
from content_intake.pipeline.db import connect, apply_schema, ensure_slots

REPO_ROOT = Path(__file__).resolve().parents[3]
STATE_FILE = REPO_ROOT / ".intake_state.json"


def _read_state() -> dict | None:
    if not STATE_FILE.exists():
        return None
    return json.loads(STATE_FILE.read_text())


def _write_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _wait_for(check, timeout=30.0, interval=0.5):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if check():
            return True
        time.sleep(interval)
    return False


def _connect_with_retry(timeout: float = 10.0, interval: float = 0.5):
    # A fresh Postgres volume (every `up` after a `down`, since `down` removes the
    # volume) goes through initdb -> shutdown -> restart-to-apply-config on the
    # official image. `pg_isready` can report ready in the brief window between the
    # first startup and that restart, so a bare connect() right after pg_isready
    # can still hit "server closed the connection unexpectedly". Retry the actual
    # connection, not just the readiness probe.
    start = time.monotonic()
    last_error: Exception | None = None
    while time.monotonic() - start < timeout:
        try:
            return connect()
        except psycopg.OperationalError as e:
            last_error = e
            time.sleep(interval)
    raise last_error


def add_up_parser(subparsers) -> None:
    p = subparsers.add_parser("up")
    p.add_argument("--workers", type=int, default=4)


def run_up(args: argparse.Namespace) -> int:
    state = _read_state()
    if state is not None and _pid_alive(state.get("stub_pid", -1)) and _pid_alive(state.get("api_pid", -1)):
        print(json.dumps({"status": "already_running"}))
        return 0

    subprocess.run(["docker", "compose", "up", "-d", "postgres"], cwd=REPO_ROOT, check=True)
    ok = _wait_for(lambda: subprocess.run(
        ["docker", "exec", "intake-postgres", "pg_isready", "-U", "intake"],
        capture_output=True,
    ).returncode == 0)
    if not ok:
        print("Postgres did not become ready in time", file=sys.stderr)
        return 1

    conn = _connect_with_retry()
    apply_schema(conn)
    ensure_slots(conn, config.IN_FLIGHT_CAPACITY)
    conn.close()

    env = {**os.environ, "STUB_IN_FLIGHT_CAPACITY": str(config.IN_FLIGHT_CAPACITY)}
    stub_proc = subprocess.Popen(
        [sys.executable, "-m", "content_intake.stub.run"],
        cwd=REPO_ROOT, env={**env, "PYTHONPATH": str(REPO_ROOT / "src")},
    )
    if not _wait_for(lambda: _http_ok(f"{config.STUB_BASE_URL}/healthz")):
        print("Stub did not become healthy in time", file=sys.stderr)
        return 1

    api_proc = subprocess.Popen(
        [sys.executable, "-m", "content_intake.pipeline.run_api"],
        cwd=REPO_ROOT, env={**env, "PYTHONPATH": str(REPO_ROOT / "src")},
    )
    if not _wait_for(lambda: _http_ok(f"{config.API_BASE_URL}/v1/runs/00000000-0000-0000-0000-000000000000/status", accept_404=True)):
        print("Pipeline API did not become healthy in time", file=sys.stderr)
        return 1

    workers = []
    for i in range(args.workers):
        proc = subprocess.Popen(
            [sys.executable, "-m", "content_intake.pipeline.worker", str(i)],
            cwd=REPO_ROOT, env={**env, "PYTHONPATH": str(REPO_ROOT / "src")},
        )
        workers.append({"index": i, "pid": proc.pid, "alive": True})

    _write_state({
        "postgres_container": "intake-postgres",
        "stub_pid": stub_proc.pid,
        "api_pid": api_proc.pid,
        "workers": workers,
    })
    print(json.dumps({"status": "up", "workers": len(workers)}))
    return 0


def _http_ok(url: str, accept_404: bool = False) -> bool:
    try:
        resp = httpx.get(url, timeout=1.0)
        return resp.status_code == 200 or (accept_404 and resp.status_code == 404)
    except httpx.HTTPError:
        return False


def add_down_parser(subparsers) -> None:
    subparsers.add_parser("down")


def run_down(args: argparse.Namespace) -> int:
    state = _read_state()
    if state is None:
        print(json.dumps({"status": "already_down"}))
        return 0

    pids = [state.get("stub_pid"), state.get("api_pid")] + [w["pid"] for w in state.get("workers", [])]
    for pid in pids:
        if pid and _pid_alive(pid):
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
    time.sleep(1)
    for pid in pids:
        if pid and _pid_alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass

    subprocess.run(["docker", "rm", "-f", "-v", "intake-postgres"], cwd=REPO_ROOT, capture_output=True)
    STATE_FILE.unlink(missing_ok=True)
    print(json.dumps({"status": "down"}))
    return 0


def add_reset_parser(subparsers) -> None:
    p = subparsers.add_parser("reset")
    p.add_argument("--workers", type=int, default=4)


def run_reset(args: argparse.Namespace) -> int:
    down_rc = run_down(argparse.Namespace())
    if down_rc != 0:
        return down_rc
    return run_up(argparse.Namespace(workers=args.workers))
```

- [ ] **Step 2: Wire into `main.py`**

```python
# add to src/content_intake/cli/main.py
from content_intake.cli.lifecycle_cmd import add_up_parser, run_up, add_down_parser, run_down, add_reset_parser, run_reset
# register: add_up_parser(subparsers); add_down_parser(subparsers); add_reset_parser(subparsers)
# dispatch: elif args.command == "up": return run_up(args)
#           elif args.command == "down": return run_down(args)
#           elif args.command == "reset": return run_reset(args)
```

- [ ] **Step 3: Verify manually end to end**

Run:
```bash
./intake up --workers 4
sleep 2
cat .intake_state.json
./intake down
```
Expected: `up` prints `{"status": "up", "workers": 4}`; `.intake_state.json` shows 4 worker PIDs plus stub/api PIDs; `down` prints `{"status": "down"}` and the Postgres container is gone (`docker ps` shows nothing named `intake-postgres`).

- [ ] **Step 4: Commit**

```bash
git add src/content_intake/cli/lifecycle_cmd.py src/content_intake/cli/main.py
git commit -m "Implement up/down/reset: Postgres container + native process lifecycle, state file (D-11)"
```

---

### Task 23: CLI `kill-worker`

**Files:**
- Create: `src/content_intake/cli/kill_worker_cmd.py`
- Modify: `src/content_intake/cli/main.py`

**Interfaces:**
- Consumes: `_read_state`, `_write_state`, `_pid_alive` from Task 22 (import them, don't reimplement).

- [ ] **Step 1: Implement**

```python
# src/content_intake/cli/kill_worker_cmd.py
import argparse
import json
import os
import signal
import sys

from content_intake.cli.lifecycle_cmd import _read_state, _write_state, _pid_alive


def add_kill_worker_parser(subparsers) -> None:
    p = subparsers.add_parser("kill-worker")
    p.add_argument("--index", type=int, default=None)


def run_kill_worker(args: argparse.Namespace) -> int:
    state = _read_state()
    if state is None:
        print("no environment is up", file=sys.stderr)
        return 1
    workers = state.get("workers", [])
    candidates = [w for w in workers if w["alive"] and _pid_alive(w["pid"])]
    if args.index is not None:
        candidates = [w for w in candidates if w["index"] == args.index]
    if not candidates:
        print("no live worker matches", file=sys.stderr)
        return 1
    target = candidates[0]
    os.kill(target["pid"], signal.SIGKILL)
    target["alive"] = False
    _write_state(state)
    print(json.dumps({"killed_index": target["index"], "pid": target["pid"]}))
    return 0
```

- [ ] **Step 2: Wire into `main.py`**

```python
# add to src/content_intake/cli/main.py
from content_intake.cli.kill_worker_cmd import add_kill_worker_parser, run_kill_worker
# register: add_kill_worker_parser(subparsers)
# dispatch: elif args.command == "kill-worker": return run_kill_worker(args)
```

- [ ] **Step 3: Verify manually**

Run:
```bash
./intake up --workers 4
./intake kill-worker --index 2
cat .intake_state.json  # worker index 2 shows alive: false
./intake down
```
Expected: prints `{"killed_index": 2, "pid": ...}`; the process for that PID is gone; the other three workers remain running (check with `ps`).

- [ ] **Step 4: Commit**

```bash
git add src/content_intake/cli/kill_worker_cmd.py src/content_intake/cli/main.py
git commit -m "Implement kill-worker: SIGKILL one live worker, no restart"
```

---

### Task 24: M1 end-to-end acceptance test

**Files:**
- Test: `tests/test_m1_end_to_end.py`

**Interfaces:**
- Consumes: everything above, driven only through `./intake` subprocess calls (this test exercises the real CLI, not internal function calls) plus direct Postgres reads for assertions the CLI JSON doesn't expose.

This test brings up the real environment, so it's slow (tens of seconds) and is not meant to run on every `pytest` invocation — mark it explicitly.

- [ ] **Step 1: Write the test**

```python
# tests/test_m1_end_to_end.py
import json
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def run_cli(*args) -> dict:
    result = subprocess.run(
        [str(REPO_ROOT / "intake"), *args], cwd=REPO_ROOT, capture_output=True, text=True
    )
    assert result.returncode == 0, f"{args} failed: {result.stderr}"
    return json.loads(result.stdout)


@pytest.mark.slow
def test_m1_full_flow(tmp_path):
    subprocess.run([str(REPO_ROOT / "intake"), "down"], cwd=REPO_ROOT, capture_output=True)

    up_result = run_cli("up", "--workers", "4")
    assert up_result["workers"] == 4
    try:
        corpus_a = tmp_path / "corpus-a"
        run_cli("corpus", "--seed", "1", "--size", "60", "--tenant", "tenant-a", "--out", str(corpus_a))

        corpus_b = tmp_path / "corpus-b"
        run_cli("corpus", "--seed", "2", "--size", "55", "--tenant", "tenant-b", "--out", str(corpus_b))

        submit_a = run_cli("submit", "--corpus", str(corpus_a), "--tenant", "tenant-a")
        submit_b = run_cli("submit", "--corpus", str(corpus_b), "--tenant", "tenant-b")
        run_id_a, run_id_b = submit_a["run_id"], submit_b["run_id"]

        deadline = time.monotonic() + 90
        status_a = status_b = None
        while time.monotonic() < deadline:
            status_a = run_cli("status", "--run", run_id_a)
            status_b = run_cli("status", "--run", run_id_b)
            if status_a["terminal"] and status_b["terminal"]:
                break
            time.sleep(2)
        assert status_a["terminal"], f"run A did not terminate: {status_a}"
        assert status_b["terminal"], f"run B did not terminate: {status_b}"

        items_a = run_cli("items", "--tenant", "tenant-a", "--run", run_id_a)
        assert len(items_a) == 60
        assert all(item["state"] != "pending" and item["state"] != "in_progress" for item in items_a)

        empty_items = run_cli("items", "--tenant", "tenant-a", "--run", run_id_a, "--state", "empty_content")
        assert len(empty_items) == 1
        assert empty_items[0]["reason"]["code"] == "empty_content"

        decode_failed_items = run_cli("items", "--tenant", "tenant-a", "--run", run_id_a, "--state", "decode_failed")
        assert len(decode_failed_items) == 1

        succeeded_items = run_cli("items", "--tenant", "tenant-a", "--run", run_id_a, "--state", "succeeded")
        for item in succeeded_items:
            assert item["annotation"] is not None

        first_item_id = items_a[0]["item_id"]
        cross_tenant = subprocess.run(
            [str(REPO_ROOT / "intake"), "item", "--tenant", "tenant-b", "--id", first_item_id],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        assert cross_tenant.returncode != 0

        wrong_run_items = subprocess.run(
            [str(REPO_ROOT / "intake"), "items", "--tenant", "tenant-b", "--run", run_id_a],
            cwd=REPO_ROOT, capture_output=True, text=True,
        )
        assert wrong_run_items.returncode != 0
    finally:
        subprocess.run([str(REPO_ROOT / "intake"), "down"], cwd=REPO_ROOT, capture_output=True)
```

- [ ] **Step 2: Register the `slow` marker so pytest doesn't warn**

```ini
# pytest.ini
[pytest]
markers =
    slow: exercises the full ./intake up/submit/status flow against real processes
```

- [ ] **Step 3: Run it**

Run: `pytest tests/test_m1_end_to_end.py -v -m slow`
Expected: `test_m1_full_flow` passes. If it times out waiting for `terminal`, check `docker logs` are not needed (native processes) — instead check the worker/API/stub stdout, which `up` currently doesn't redirect to a file. If debugging is needed, temporarily add `stdout=open(REPO_ROOT / f"worker-{i}.log", "w")` to the `subprocess.Popen` calls in `lifecycle_cmd.py`.

- [ ] **Step 4: Commit**

```bash
git add tests/test_m1_end_to_end.py pytest.ini
git commit -m "Add M1 end-to-end acceptance test: full flow, edge cases, tenant isolation"
```

---

### Task 25: README.md

**Files:**
- Create: `README.md`

**Interfaces:** none — documentation only.

- [ ] **Step 1: Write `README.md`**

```markdown
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
# => {"run_id": "..."}
./intake status --run <run_id>
./intake items --tenant tenant-a --run <run_id> --state succeeded
./intake item --tenant tenant-a --id <item_id>
./intake down
```

## Reported run (for grading)

Seed `1`, size `100`, tenant `tenant-a` — generated at `/tmp/corpus-a` above. (Update this section with the exact seed/size/tenant used for any run whose output is cited in `EVALUATION.md`, once M2 is built.)

## Architecture and decisions

See [`ARCHITECTURE.md`](ARCHITECTURE.md) and [`DECISIONS.md`](DECISIONS.md).

## Known M1 scope cuts

- `D-06` (secondary performance thresholds) and `D-07` (local-environment fidelity) are deferred to the M2 build, where they can be set against real observed numbers (see `DECISIONS.md`).
- `kill-worker` is implemented and exercised manually, but the M2 scenario harness (`./intake scenario`) that asserts recovery timing does not exist yet.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "Add README: setup, quickstart, reported run, known M1 scope cuts"
```

---

## Self-Review Notes

- **Spec coverage:** REQ-1.1 (Task 24), REQ-1.2 (Tasks 20, 24), REQ-1.3 (item becomes queryable as soon as its own worker finalizes it — no batching, Task 17), REQ-1.4 (Tasks 14, 18, 22), D-00/D-01/D-02/D-03/D-04/D-05/D-08/D-09/D-10/D-11/D-12/D-13/D-14 each map to a specific task above. CORPUS-REQ-1..5 map to Task 5. ITEM-REQ-1..5 map to Tasks 5, 12, 17, 19–20. The stub's EXT-REQ-1..7 map to Tasks 8–9, and Task 11 is the required conformance suite.
- **Placeholder scan:** the two dead placeholders drafted mid-task (`test_sha256_hex_known_value` in Task 2, `test_invalid_request_status_terminates_immediately_without_retry` in Task 17) are explicitly called out and replaced with real code in the same task — an implementer following the steps in order never lands on the placeholder version.
- **Type/signature consistency checked:** `claim_item`/`claim_slot`/`release_slot`/`start_attempt`/`complete_attempt`/`find_completed_success` signatures introduced in Tasks 14–16 are used identically in Task 17's `process_item`; `process_item`'s signature introduced in Task 17 is used identically in Task 18's `run_worker`; `submit_run`'s signature from Task 19 is reused unchanged in Task 20 and Task 24 (via the CLI).
- **Not yet covered (explicitly M2, not a silent gap):** D-06, D-07, `./intake scenario`, the paired-control run, `EVALUATION.md`, and `evaluation/raw/`. These are out of scope for this plan by design (per your instruction to finish M1 first) and should be their own follow-up plan once M1 is verified working.
