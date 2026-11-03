import datetime
from collections.abc import Mapping
from typing import Any, TypedDict, cast

import psycopg2.extensions
from psycopg2 import sql
from psycopg2.extras import RealDictCursor


class ExtractedFilmWorkRow(TypedDict):
    """Одна строка результата _MERGE_FILM_WORKS: fw × person-role × genre.

    person_*/role и genre_* — None, когда у фильма нет ни одной привязанной
    персоны/жанра соответственно (из-за LEFT JOIN).
    """

    fw_id: str
    title: str
    description: str | None
    imdb_rating: float | None
    creation_date: datetime.date | None
    type: str
    created: datetime.datetime
    modified: datetime.datetime
    role: str | None
    person_id: str | None
    person_full_name: str | None
    genre_id: str | None
    genre_name: str | None


# source_table → (таблица сущности, M2M-таблица связи с film_work).
# film_work обрабатывается напрямую и в этом отображении не участвует.
_RELATED_VIA: dict[str, tuple[str, str]] = {
    "person": ("person", "person_film_work"),
    "genre": ("genre", "genre_film_work"),
}

_FILM_WORK_IDS_IN_RANGE = sql.SQL("""
    SELECT fw.id
    FROM content.film_work fw
    WHERE (fw.modified, fw.id) >= (%s, %s::uuid)
      AND (fw.modified, fw.id) <= (%s, %s::uuid)
    ORDER BY fw.modified, fw.id
""")

_RELATED_FILM_WORK_IDS_IN_RANGE = sql.SQL("""
    SELECT DISTINCT m2m.film_work_id
    FROM content.{entity} entity
    JOIN content.{m2m} m2m ON m2m.{entity_fk} = entity.id
    WHERE (entity.modified, entity.id) >= (%s, %s::uuid)
      AND (entity.modified, entity.id) <= (%s, %s::uuid)
""")

_MERGE_FILM_WORKS = sql.SQL("""
    SELECT
        fw.id AS fw_id,
        fw.title,
        fw.description,
        fw.rating AS imdb_rating,
        fw.creation_date,
        fw.type,
        fw.created,
        fw.modified,
        pfw.role,
        p.id AS person_id,
        p.full_name AS person_full_name,
        g.id AS genre_id,
        g.name AS genre_name
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
    LEFT JOIN content.person p ON p.id = pfw.person_id
    LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
    LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE fw.id = ANY(%s::uuid[])
    ORDER BY fw.modified, fw.id
""")


def affected_film_work_ids(
    conn: psycopg2.extensions.connection,
    job: Mapping[str, Any],
) -> list[str]:
    """id фильмов, затронутых job-ом, в зависимости от его source_table."""
    source_table = job["source_table"]
    params = (
        job["first_modified"],
        job["first_id"],
        job["last_modified"],
        job["last_id"],
    )

    if source_table == "film_work":
        query: sql.Composable = _FILM_WORK_IDS_IN_RANGE
    elif source_table in _RELATED_VIA:
        entity, m2m = _RELATED_VIA[source_table]
        query = _RELATED_FILM_WORK_IDS_IN_RANGE.format(
            entity=sql.Identifier(entity),
            m2m=sql.Identifier(m2m),
            entity_fk=sql.Identifier(f"{entity}_id"),
        )
    else:
        raise ValueError(f"Unsupported source_table: {source_table!r}")

    with conn.cursor() as cursor:
        cursor.execute(query, params)
        return [str(row[0]) for row in cursor.fetchall()]


def fetch_film_work_rows(
    conn: psycopg2.extensions.connection,
    film_work_ids: list[str],
) -> list[ExtractedFilmWorkRow]:
    """Сджойненные строки фильмов (fw × person-role × genre) для набора id."""
    if not film_work_ids:
        return []

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(_MERGE_FILM_WORKS, (film_work_ids,))
        return [cast(ExtractedFilmWorkRow, dict(row)) for row in cursor.fetchall()]
