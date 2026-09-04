# Architecture

Current as of M1 design. Kept up to date through M2 (scenario runner, fault injection, and the
still-open D-06/D-07 will extend this doc, not replace it).

## Components

| Component | What it is | Runs as |
|---|---|---|
| Postgres | The only durable store: run/item state, extracted text, annotations, the work queue, the capacity semaphore | Container (the one piece required to be containerized) |
| Stub | Simulated annotation dependency (section 5 of the brief) | Native process, single Uvicorn worker, foreground under `./intake stub` |
| Pipeline API | FastAPI service: `submit`, `item`, `items`, `status`. The only writer of new runs/items; enforces tenant scoping on every read and write | Native process, launched by `./intake up` |
| Worker (×N) | Standalone script; claims one item at a time from Postgres, processes it, writes the result | Native OS process, one per worker, launched by `./intake up --workers <n>` |
| CLI (`./intake`) | Dispatches subcommands: some run local logic directly (`corpus`, `stub`), some manage container/process lifecycle (`up`/`down`/`reset`/`kill-worker`), some are thin HTTP clients to the Pipeline API (`submit`/`status`/`item`/`items`) | Entry point |

Only Postgres is containerized (D-11). The corpus directory (`--out <dir>`) can be anywhere on
the host filesystem, and containerizing the stub/API/workers would mean volume-mounting
arbitrary host paths for no benefit — native processes also keep localhost networking to the
stub trivial. Workers are separate OS processes from M1 onward, not upgraded later (D-12):
`./intake up` tracks each worker's PID (and the stub/API PIDs, and the Postgres container ID) in
a local state file so `down` and `kill-worker` know what to stop.

## Data flow

```mermaid
flowchart TD
    CLI["./intake CLI"]
    API["Pipeline API<br/>(FastAPI)"]
    PG[("Postgres<br/>runs / items / annotations_cache / stub_call_slots")]
    Stub["Annotation Stub<br/>(FastAPI, single worker)"]
    W1["Worker 1"]
    W2["Worker 2"]
    W3["Worker 3"]
    W4["Worker 4"]

    CLI -- "corpus --seed --size --tenant" --> FS[("files/ + manifest.json<br/>on local disk")]
    CLI -- "submit --corpus --tenant" --> API
    API -- "hash every file,<br/>bulk-insert items (1 txn)" --> PG
    API -- "run_id" --> CLI

    CLI -- "status / item / items" --> API
    API -- "tenant-scoped SELECT" --> PG
    API -- "JSON" --> CLI

    W1 & W2 & W3 & W4 -- "claim item<br/>(SELECT...FOR UPDATE SKIP LOCKED)" --> PG
    W1 & W2 & W3 & W4 -- "claim stub_call_slots row" --> PG
    W1 & W2 & W3 & W4 -- "POST /v1/annotate" --> Stub
    W1 & W2 & W3 & W4 -- "write result + release slot + release lease" --> PG

    CLI -- "kill-worker" --> W3
```

## Work handoff (the two hardest structural problems)

**Item ownership (D-01).** A worker claims exactly one item at a time with:

```sql
UPDATE items
SET state = 'in_progress', leased_by = $worker_id, leased_until = now() + $lease_ttl
WHERE id = (
  SELECT id FROM items
  WHERE state = 'pending' OR (state = 'in_progress' AND leased_until < now())
  ORDER BY created_at, order_index
  LIMIT 1
  FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

`created_at` alone is not a sufficient ordering: `submit` inserts every item of a run inside one
transaction (D-14) and Postgres's `now()` is the *transaction* timestamp, so all of a run's items
share one `created_at` and the claim order within a run would be arbitrary heap order.
`order_index` — the manifest's own `order` field, which lists originals first, then duplicates,
then edge cases — is the tiebreaker. It matters beyond tidiness: D-02's `annotations_cache` only
converts duplicate content into *avoided* cost if an original is generally processed before its
duplicates, and arbitrary ordering breaks that. `created_at` stays first in the sort so runs are
still served FIFO by submission time relative to each other.

If the owning worker is SIGKILLed, no one releases the lease explicitly — the item simply
becomes claimable again once `leased_until` passes, purely because every future claim attempt's
own `WHERE` clause already covers expired leases. No reaper process, no supervisor, no shared
memory between the four workers: Postgres is the only thing they share, and it already
serializes the claim correctly under `SKIP LOCKED`.

Item lease TTL: 5 seconds. Processing one item can span several stub attempts with backoff
(D-05) and easily exceed one lease window, so the worker runs a background thread that renews
the lease every 2 seconds while work continues:

```sql
UPDATE items SET leased_until = now() + interval '5 seconds'
WHERE item_id = $1 AND leased_by = $2 AND state = 'in_progress';
```

A renewal that affects 0 rows means this worker's lease was already stolen — the thread flips an
in-process flag the main loop checks between steps, so the worker abandons this item and moves
on rather than finishing work it no longer owns. That flag is a latency optimization, not the
correctness guarantee: the guarantee is that renewal *and* the eventual terminal write both
carry `WHERE leased_by = $worker_id`, so a stale worker's writes simply match zero rows once
someone else holds the lease — it can never resurrect a stolen item or clobber the new owner's
result, flag or no flag.

**Capacity gate (D-08).** The stub allows only `in_flight_capacity` (default 2) concurrent calls,
enforced across all four workers with `over_capacity_calls` required to stay at exactly zero.
This is a *different* resource than item ownership — a worker can hold an item's lease without
being mid-HTTP-call. A `stub_call_slots` table, seeded with `in_flight_capacity` rows at startup
(config-driven, same value passed to the stub via `STUB_IN_FLIGHT_CAPACITY` — never a duplicated
literal), is claimed with the identical lease-and-self-heal pattern immediately before a stub
call and released in a `finally` immediately after. A killed worker's held slot expires and
becomes claimable the same way an abandoned item does.

## Per-item processing

1. Claim item (above). Read the file's bytes from disk (a local read, not a stub call). Then,
   before anything is sent to the stub, check `item_attempts` for this item for an
   already-completed *successful* attempt (possible if a previous holder got a 200 back but died
   before writing `items.state`) — if found, re-read the annotation from `annotations_cache` by
   content hash (`item_attempts` stores the outcome, not the payload) and finalize from it,
   re-deriving `extracted_text` from the bytes read above. Step 5's commit ordering guarantees
   that cache row exists whenever a completed successful attempt does; in the impossible case
   that it doesn't, the worker falls through and reprocesses the item normally rather than
   finalizing `succeeded` with an empty annotation.
2. Empty bytes → terminal `empty_content`. `.json` failing `json.loads` → terminal
   `decode_failed`. Both detected before anything touches the stub or `stub_call_slots`.
3. Extract text for `.txt`/`.json`/`.csv`. `.png` gets no extracted text but still proceeds to
   annotation — extractability is not grounds to skip the call.
4. Look up `annotations_cache` for `(tenant, sha256)` where `tenant` is read from the claimed
   `items` row (D-10 — never from anywhere else, since workers bypass the API's tenant scoping
   entirely). Hit → reuse, terminal `succeeded`, no HTTP call.
5. Miss → retry loop, up to 5 attempts (D-05), 2s HTTP timeout, backoff `200ms × 2^(attempt-1)`
   capped at 2s with ±20% jitter. Each attempt:
   - Claim a `stub_call_slots` row (D-08).
   - **Commit** an `item_attempts` insert (`started_at`, `worker_id`, `slot_id`, `attempt_no`) —
     this transaction lands *before* the HTTP call is sent.
   - `POST /v1/annotate`.
   - On `200` only: **commit** the `annotations_cache` row for `(tenant, sha256)`. This lands
     *before* the attempt is marked complete, deliberately — see below.
   - **Commit** an `item_attempts` update (`completed_at`, `http_status`, `outcome`) — this
     transaction lands after the call returns (or the timeout fires).
   - Release the slot in a `finally`.
   - `200` → write the annotation and extracted text to `items`, terminal `succeeded`.
     `400` → terminal `annotation_invalid_request` immediately, no further attempts.
     `500`/`429`/timeout → retryable; loop again if attempts remain, otherwise terminal
     `annotation_failed`.

The cache commit precedes the attempt-completion commit because step 1's recovery path reads the
cache to reconstruct an annotation it can't get from `item_attempts`. Both commits are sequential
on one connection, so a later one cannot be durable unless the earlier one is: a completed
`outcome='success'` attempt therefore *implies* its cache row survived. The opposite order left a
crash window where an attempt row claimed success with the annotation nowhere on disk, and the
next claimant reported the item `succeeded` with an empty annotation — a silent data error
dressed as a success. This does not weaken D-09: the attempt insert still commits before the HTTP
call and the update still commits after it returns.

**Crash windows against this sequence** (all writes above are conditioned on
`leased_by = $worker_id`, so a stale worker can never complete a stolen item regardless of which
window it dies in):

| Worker dies... | State left behind | What happens |
|---|---|---|
| before the `item_attempts` insert commits | No attempt row for this try | Lease expires, reclaimed, retried fresh. No billing occurred, and nothing suggests otherwise. |
| after the insert commits, whether before the request was even sent, mid-flight, or after a response arrived but before the update commits | Row with `completed_at IS NULL` | Durable, queryable evidence of a *possibly*-billed, unrecorded attempt (D-09). Reconciliation reads this as "cannot rule out billing," never as confirmation — the row can't distinguish "died before sending" from "died waiting on the response," and the conservative reading is the safe one. This is the case M2 measures and explains, not one it can prevent. |
| after a `200`'s cache row commits, before the attempt update commits (a sub-case of the row above) | Cache row present; attempt still `completed_at IS NULL` | The next claimant finds no *completed* success, so it reprocesses from step 1 — but its step-4 cache lookup now hits, so it finalizes `succeeded` from the cache without re-calling the stub. The attempt row is still correctly readable as "cannot rule out billing." |
| after the update commits, before the slot is released | Attempt row complete; `stub_call_slots` row still marked held | The slot's own short lease expires and self-heals (D-08) — no special handling needed. |
| after a successful outcome is recorded, before `items.state` is written | Attempt row shows `outcome='success'`; item still `in_progress`; cache row guaranteed present (it committed first) | Next claimant finalizes from the completed `item_attempts` row plus the cache (step 1 above) instead of re-calling the stub. |
| anywhere, with an unhandled exception rather than a kill | Item left `in_progress` | The worker logs a traceback to `.intake_logs/worker-N.log` and claims the next item rather than dying — one bad item must not take down a worker, and then each worker that reclaims it in turn. The item's lease expires and it is reclaimed and retried like any abandoned item. An item that fails this way deterministically is retried indefinitely; giving it a terminal state needs a state D-04's fixed vocabulary does not have, so that is left as an M2 decision rather than invented here. |

## Data model (Postgres)

- `runs(run_id PK, corpus_id, tenant, submitted_at)`
- `items(item_id PK, run_id FK, tenant, source_path, extension, bytes, sha256, role, order_index, duplicate_of, edge_case, expects_annotation, state, leased_by, leased_until, reason JSONB, extracted_text TEXT NULL, annotation JSONB NULL, created_at, updated_at)` — `order_index` is the manifest's `order` field (renamed off the reserved keyword) and is the claim/listing tiebreaker within a run, where `created_at` is identical for every row
- `annotations_cache(tenant, sha256, annotation JSONB, created_at, PRIMARY KEY(tenant, sha256))`
- `stub_call_slots(slot_id PK, held_by TEXT NULL, lease_until TIMESTAMPTZ NULL)` — row count = `in_flight_capacity`
- `item_attempts(attempt_id PK, item_id FK, tenant, attempt_no, started_at, worker_id, slot_id, completed_at NULL, http_status NULL, outcome NULL, UNIQUE(item_id, attempt_no))` — insert commits before the stub call, update commits after (D-09)

Query surface (`item`/`items`/`status`) reads these tables directly through the Pipeline API —
no separate read path, no cache layer beyond `annotations_cache` itself (D-03).

## Deferred (M2)

Secondary performance thresholds (D-06) and environment-fidelity discussion (D-07) are recorded
as open in `DECISIONS.md` and will be resolved and reflected here once the M2 scenario harness is
being built. (D-05, the stub-boundary failure policy, is decided above — only its validation
under real M2 load is deferred.)
