CREATE SCHEMA IF NOT EXISTS notifications_source;

-- Payload and both hashes are committed before any Elasticsearch request.
-- No FK: publication identity survives content deletion/reimport and reindexing.
CREATE TABLE IF NOT EXISTS notifications_source.source_state (
    film_id uuid PRIMARY KEY,
    payload jsonb NOT NULL,
    semantic_hash text NOT NULL,
    document_hash text NOT NULL,
    film_version bigint NOT NULL CHECK (film_version > 0),
    index_revision bigint NOT NULL CHECK (index_revision > 0),
    catalog_published_at timestamptz NOT NULL,
    catalog_updated_at timestamptz NOT NULL,
    CHECK (catalog_updated_at >= catalog_published_at)
);
