"""Database transactions for the ES detector. No network I/O inside page commits."""

from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import datetime
from typing import Any
from uuid import uuid4

import psycopg2
from psycopg2.extensions import connection
from psycopg2.extras import Json, RealDictCursor

from postgres_to_es.catalog_events.contracts import FilmSnapshot, MovieDTO
from postgres_to_es.db import get_postgres_dsl

DETECTOR_LOCK = "movies-catalog-detector"


@contextmanager
def process_lock(name: str) -> Iterator[connection]:
    """Session lock also used by bootstrap; releases automatically on process death."""
    with closing(psycopg2.connect(**get_postgres_dsl())) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (name,))
            locked = cur.fetchone()
            if locked is None or not locked[0]:
                raise RuntimeError(f"Another process holds {name}")
        yield conn


def readiness() -> dict[str, Any]:
    with closing(psycopg2.connect(**get_postgres_dsl())) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM notifications_source.detector_state WHERE singleton")
            result = cur.fetchone()
            if result is None:
                raise RuntimeError("Detector schema not initialized; run migrate")
            return dict(result)


def add_event(cur: Any, film: FilmSnapshot, *, load_id: str | None = None) -> bool:
    """Caller owns detector lock and transaction. Bootstrap may force updated events."""
    cur.execute(
        "SELECT film_version FROM notifications_source.known_films WHERE film_id=%s",
        (str(film.film_id),),
    )
    previous = cur.fetchone()
    if load_id is None and previous and previous["film_version"] >= film.film_version:
        return False
    event = MovieDTO(
        **film.model_dump(),
        event_id=uuid4(),
        event_type="film.updated" if previous or load_id else "film.published",
    )
    cur.execute(
        """INSERT INTO notifications_source.outbox
        (event_id,film_id,film_version,payload,load_id) VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (load_id,film_id) DO NOTHING RETURNING event_id""",
        (
            str(event.event_id),
            str(film.film_id),
            film.film_version,
            Json(event.model_dump(mode="json")),
            load_id,
        ),
    )
    added = cur.fetchone() is not None
    cur.execute(
        """INSERT INTO notifications_source.known_films (film_id,film_version)
        VALUES (%s,%s) ON CONFLICT (film_id) DO UPDATE
        SET film_version=GREATEST(known_films.film_version,EXCLUDED.film_version)""",
        (str(film.film_id), film.film_version),
    )
    return added


def save_progress(cur: Any, last_sort: list[Any]) -> None:
    cur.execute(
        "UPDATE notifications_source.detector_state SET last_sort=%s WHERE singleton",
        (Json(last_sort),),
    )


def save_page(conn: connection, films: list[FilmSnapshot], last_sort: list[Any]) -> int:
    with conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            count = sum(add_event(cur, film) for film in films)
            save_progress(cur, last_sort)
            return count


def begin_window(
    conn: connection, lower: datetime | None, upper: datetime, reconcile: bool
) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE notifications_source.detector_state
            SET active_from=%s,active_until=%s,active_reconcile=%s WHERE singleton""",
            (lower, upper, reconcile),
        )


def finish_window(conn: connection, upper: datetime, reconcile: bool) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE notifications_source.detector_state SET
            watermark_at=GREATEST(watermark_at,%s),
            last_reconciled_at=CASE WHEN %s THEN %s ELSE last_reconciled_at END,
            active_from=NULL,active_until=NULL,active_reconcile=false WHERE singleton""",
            (upper, reconcile, upper),
        )
