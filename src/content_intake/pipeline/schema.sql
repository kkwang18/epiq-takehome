-- src/content_intake/pipeline/schema.sql
CREATE TABLE IF NOT EXISTS runs (
    run_id UUID PRIMARY KEY,
    corpus_id TEXT NOT NULL,
    tenant TEXT NOT NULL,
    corpus_dir TEXT NOT NULL,
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
    -- Position of this item in its corpus manifest ("order" in manifest.json, renamed to
    -- avoid the reserved SQL keyword). Every item in a run shares one created_at, because
    -- submit_run inserts them all inside a single transaction and now() is the transaction
    -- timestamp -- so created_at alone is not a usable ordering within a run. order_index is
    -- the tiebreaker that makes "originals before their duplicates" real, which is what lets
    -- annotations_cache actually avoid billed calls for duplicate content (D-02).
    order_index INTEGER NOT NULL,
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

-- Migration for databases created before order_index existed. On a fresh database both
-- statements are no-ops (the column is already in the CREATE TABLE above); on an existing
-- one the ADD backfills existing rows with 0 and the DROP DEFAULT restores the
-- "callers must supply it" contract the CREATE TABLE declares.
ALTER TABLE items ADD COLUMN IF NOT EXISTS order_index INTEGER NOT NULL DEFAULT 0;
ALTER TABLE items ALTER COLUMN order_index DROP DEFAULT;

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
