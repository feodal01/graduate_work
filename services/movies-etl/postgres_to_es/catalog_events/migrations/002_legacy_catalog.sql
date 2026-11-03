-- Immutable membership captured from the old WORKING index, never all PG rows.
-- Task9 can reuse load_id, started_at and this snapshot after capture completes.
CREATE TABLE IF NOT EXISTS notifications_source.legacy_catalog_loads (
    load_id uuid PRIMARY KEY,
    source_index text NOT NULL,
    target_index text NOT NULL UNIQUE,
    started_at timestamptz NOT NULL,
    captured_at timestamptz,
    completed_at timestamptz,
    CHECK (source_index <> target_index)
);
CREATE TABLE IF NOT EXISTS notifications_source.legacy_catalog_membership (
    load_id uuid NOT NULL REFERENCES notifications_source.legacy_catalog_loads(load_id),
    film_id uuid NOT NULL,
    payload jsonb NOT NULL,
    PRIMARY KEY (load_id, film_id)
);
