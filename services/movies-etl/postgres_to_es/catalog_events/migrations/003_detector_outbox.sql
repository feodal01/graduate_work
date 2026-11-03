CREATE TABLE IF NOT EXISTS notifications_source.known_films (
    film_id uuid PRIMARY KEY,
    film_version bigint NOT NULL CHECK (film_version > 0)
);
CREATE TABLE IF NOT EXISTS notifications_source.outbox (
    sequence bigserial PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    film_id uuid NOT NULL,
    film_version bigint NOT NULL CHECK (film_version > 0),
    payload jsonb NOT NULL,
    load_id text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_at timestamptz,
    UNIQUE (load_id, film_id)
);
CREATE INDEX IF NOT EXISTS movies_outbox_pending
    ON notifications_source.outbox (sequence) WHERE published_at IS NULL;
-- Task9 fills this readiness gate only after all historical known rows are durable.
-- baseline_at is the INITIAL snapshot boundary, never the bootstrap completion clock.
CREATE TABLE IF NOT EXISTS notifications_source.detector_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    bootstrap_load_id text,
    baseline_at timestamptz,
    bootstrap_completed_at timestamptz,
    watermark_at timestamptz,
    last_reconciled_at timestamptz,
    active_from timestamptz,
    active_until timestamptz,
    active_reconcile boolean NOT NULL DEFAULT false,
    last_sort jsonb,
    CHECK (bootstrap_completed_at IS NULL OR
           (baseline_at IS NOT NULL AND bootstrap_load_id IS NOT NULL))
);
INSERT INTO notifications_source.detector_state (singleton) VALUES (true)
ON CONFLICT DO NOTHING;
