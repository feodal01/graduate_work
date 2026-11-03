-- Permanent baseline and completion markers survive outbox retention/cleanup.
CREATE TABLE IF NOT EXISTS notifications_source.catalog_bootstrap_loads (
    load_id text PRIMARY KEY CHECK (length(trim(load_id)) > 0),
    singleton boolean NOT NULL DEFAULT true UNIQUE CHECK (singleton),
    source_index text NOT NULL,
    source_uuid text NOT NULL,
    start_boundary timestamptz NOT NULL,
    restore_writes boolean NOT NULL,
    legacy_load_id uuid REFERENCES notifications_source.legacy_catalog_loads(load_id),
    fenced_at timestamptz,
    captured_at timestamptz,
    released_at timestamptz,
    completed_at timestamptz,
    CHECK (captured_at IS NULL OR fenced_at IS NOT NULL),
    CHECK (released_at IS NULL OR captured_at IS NOT NULL),
    CHECK (completed_at IS NULL OR released_at IS NOT NULL)
);
CREATE TABLE IF NOT EXISTS notifications_source.catalog_bootstrap_members (
    load_id text NOT NULL REFERENCES notifications_source.catalog_bootstrap_loads(load_id),
    film_id uuid NOT NULL,
    payload jsonb NOT NULL,
    enqueued_at timestamptz,
    PRIMARY KEY (load_id, film_id)
);
CREATE INDEX IF NOT EXISTS catalog_bootstrap_pending
    ON notifications_source.catalog_bootstrap_members(load_id,film_id)
    WHERE enqueued_at IS NULL;
