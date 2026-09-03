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
