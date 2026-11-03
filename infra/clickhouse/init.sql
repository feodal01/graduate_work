CREATE DATABASE IF NOT EXISTS ugc ON CLUSTER company_cluster;

CREATE TABLE IF NOT EXISTS ugc.events_local ON CLUSTER company_cluster
(
    event_id UUID,
    user_id UUID,
    event_type LowCardinality(String),
    occurred_at DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    request_id Nullable(String),
    film_id Nullable(UUID),
    page LowCardinality(Nullable(String)),
    element_type LowCardinality(Nullable(String)),
    element_id Nullable(String),
    position_seconds Nullable(Float64),
    duration_seconds Nullable(Float64),
    completion_ratio Nullable(Float32),
    from_quality LowCardinality(Nullable(String)),
    to_quality LowCardinality(Nullable(String)),
    filters String,
    payload String
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/ugc/events', '{replica}', ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (event_type, user_id, event_id);

CREATE TABLE IF NOT EXISTS ugc.events ON CLUSTER company_cluster
AS ugc.events_local
ENGINE = Distributed('company_cluster', 'ugc', 'events_local', cityHash64(toString(user_id)));

CREATE TABLE IF NOT EXISTS ugc.film_watches_local ON CLUSTER company_cluster
(
    event_id UUID,
    user_id UUID,
    film_id UUID,
    event_type LowCardinality(String),
    position_seconds Float64,
    duration_seconds Float64,
    completion_ratio Float32,
    from_quality LowCardinality(String),
    to_quality LowCardinality(String),
    occurred_at DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/ugc/film_watches', '{replica}', ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (film_id, user_id, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS ugc.film_watches_mv ON CLUSTER company_cluster
TO ugc.film_watches_local AS
SELECT
    event_id,
    user_id,
    film_id,
    event_type,
    ifNull(position_seconds, 0) AS position_seconds,
    ifNull(duration_seconds, 0) AS duration_seconds,
    ifNull(completion_ratio, 0) AS completion_ratio,
    ifNull(from_quality, '') AS from_quality,
    ifNull(to_quality, '') AS to_quality,
    occurred_at,
    received_at,
    ingested_at
FROM ugc.events_local
WHERE event_type IN ('watch_progress', 'watch_completed', 'quality_changed')
  AND film_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS ugc.film_watches ON CLUSTER company_cluster
AS ugc.film_watches_local
ENGINE = Distributed('company_cluster', 'ugc', 'film_watches_local', cityHash64(toString(user_id)));

CREATE TABLE IF NOT EXISTS ugc.page_events_local ON CLUSTER company_cluster
(
    event_id UUID,
    user_id UUID,
    page LowCardinality(String),
    event_type LowCardinality(String),
    element_type LowCardinality(String),
    element_id String,
    duration_seconds Float64,
    occurred_at DateTime64(3, 'UTC'),
    received_at DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC')
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/ugc/page_events', '{replica}', ingested_at)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (page, user_id, event_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS ugc.page_events_mv ON CLUSTER company_cluster
TO ugc.page_events_local AS
SELECT
    event_id,
    user_id,
    page,
    event_type,
    ifNull(element_type, '') AS element_type,
    ifNull(element_id, '') AS element_id,
    ifNull(duration_seconds, 0) AS duration_seconds,
    occurred_at,
    received_at,
    ingested_at
FROM ugc.events_local
WHERE event_type IN ('click', 'page_view')
  AND page IS NOT NULL;

CREATE TABLE IF NOT EXISTS ugc.page_events ON CLUSTER company_cluster
AS ugc.page_events_local
ENGINE = Distributed('company_cluster', 'ugc', 'page_events_local', cityHash64(toString(user_id)));
