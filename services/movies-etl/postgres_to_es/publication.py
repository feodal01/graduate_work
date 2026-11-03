"""Durable publication snapshots. PostgreSQL commits before Elasticsearch I/O."""

import hashlib
import json
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import psycopg2
from psycopg2.extras import Json, RealDictCursor

from postgres_to_es.db import get_postgres_dsl
from postgres_to_es.pipelines.extractor.queries import fetch_film_work_rows
from postgres_to_es.pipelines.transform.pipeline import transform_rows
from postgres_to_es.schemas import MovieDocument
from postgres_to_es.settings import postgres_backoff_settings

_METADATA = {"film_version", "index_revision", "catalog_published_at", "catalog_updated_at"}
_SEMANTIC = {"id", "title", "description", "creation_date", "imdb_rating", "genres"}


@postgres_backoff_settings.for_postgres()
def migrate_publication_state() -> None:
    """Apply additive source migrations, serialized across starting ETL workers."""
    directory = Path(__file__).parent / "catalog_events/migrations"
    with closing(psycopg2.connect(**get_postgres_dsl())) as conn, conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                ("movies-source-migrations",),
            )
            for path in sorted(directory.glob("*.sql")):
                cursor.execute(path.read_text())


def _hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


def _canonical(document: MovieDocument) -> MovieDocument:
    data = document.model_dump(mode="json", exclude=_METADATA)
    for field in ("genres", "actors", "directors", "writers"):
        data[field] = sorted(data[field], key=lambda entry: (entry["id"], entry["name"]))
    for field in ("actors_names", "directors_names", "writers_names"):
        data[field] = sorted(data[field])
    return MovieDocument.model_validate(data)


def _lock_film(cursor: Any, film_id: UUID) -> None:
    cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (str(film_id),))


def _persist_snapshot(cursor: Any, document: MovieDocument, now: datetime) -> MovieDocument:
    """Caller holds the per-film lock; full snapshot and versions change together."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("Publication time must be timezone-aware")
    now = now.astimezone(timezone.utc)
    document = _canonical(document)
    payload = document.model_dump(mode="json", exclude=_METADATA)
    semantic_hash = _hash({key: payload[key] for key in _SEMANTIC})
    document_hash = _hash(payload)
    cursor.execute(
        "SELECT * FROM notifications_source.source_state WHERE film_id=%s", (document.id,)
    )
    previous = cursor.fetchone()
    if previous and previous["document_hash"] == document_hash:
        return MovieDocument.model_validate(previous["payload"])
    semantic_changed = previous is None or previous["semantic_hash"] != semantic_hash
    document.film_version = previous["film_version"] + int(semantic_changed) if previous else 1
    document.index_revision = previous["index_revision"] + 1 if previous else 1
    if previous:
        document.catalog_published_at = previous["catalog_published_at"].astimezone(timezone.utc)
        updated_at = previous["catalog_updated_at"].astimezone(timezone.utc)
        if semantic_changed:
            updated_at = max(now, updated_at + timedelta(microseconds=1))
        document.catalog_updated_at = updated_at
    else:
        document.catalog_published_at = document.catalog_updated_at = now
    cursor.execute(
        """
        INSERT INTO notifications_source.source_state
            (film_id, payload, semantic_hash, document_hash, film_version, index_revision,
             catalog_published_at, catalog_updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (film_id) DO UPDATE SET
            payload=EXCLUDED.payload, semantic_hash=EXCLUDED.semantic_hash,
            document_hash=EXCLUDED.document_hash, film_version=EXCLUDED.film_version,
            index_revision=EXCLUDED.index_revision, catalog_updated_at=EXCLUDED.catalog_updated_at
        """,
        (
            document.id,
            Json(document.model_dump(mode="json")),
            semantic_hash,
            document_hash,
            document.film_version,
            document.index_revision,
            document.catalog_published_at,
            document.catalog_updated_at,
        ),
    )
    return document


@postgres_backoff_settings.for_postgres()
def prepare_publication(film_id: UUID, now: datetime) -> MovieDocument:
    """Re-read current source under the film lock, persist, then release before ES.

    `now` is the first publication attempt time, not an assertion of ES visibility.
    The detector must read the working index, never this ledger.
    """
    with closing(psycopg2.connect(**get_postgres_dsl())) as conn, conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            _lock_film(cursor, film_id)
            rows = fetch_film_work_rows(conn, [str(film_id)])
            if not rows:
                raise LookupError(f"Film {film_id} no longer exists in the source")
            return _persist_snapshot(cursor, transform_rows(rows)[0], now)
