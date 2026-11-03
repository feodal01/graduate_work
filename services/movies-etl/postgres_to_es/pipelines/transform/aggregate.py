"""Группировка плоских строк extractor-а (fw × person-role × genre) в payload фильма."""

from typing import Any

from postgres_to_es.pipelines.extractor.queries import ExtractedFilmWorkRow

# 'actor' | 'director' | 'writer' (см. postgres.ddl: person_film_work.role)
# Ключ допускает None: у строк без персоны role тоже None (LEFT JOIN).
_ROLE_TO_FIELD: dict[str | None, str] = {
    "actor": "actors",
    "director": "directors",
    "writer": "writers",
}


def group_rows_by_film_work(
    rows: list[ExtractedFilmWorkRow],
) -> dict[str, list[ExtractedFilmWorkRow]]:
    """Группирует плоские строки по fw_id, сохраняя исходный порядок."""
    groups: dict[str, list[ExtractedFilmWorkRow]] = {}
    for row in rows:
        groups.setdefault(row["fw_id"], []).append(row)
    return groups


def build_movie_payload(fw_id: str, rows: list[ExtractedFilmWorkRow]) -> dict[str, Any]:
    """Собирает payload для MovieDocument.model_validate из строк одного fw_id.

    LEFT JOIN person_film_work × genre_film_work в _MERGE_FILM_WORKS даёт
    кросс-произведение: каждая персона повторяется по разу на жанр (и
    наоборот), поэтому persons дедуплицируются по (person_id, role),
    genres — по genre_id.
    """
    first = rows[0]
    genres: dict[str, dict[str, str | None]] = {}
    people: dict[str, dict[str, dict[str, str | None]]] = {
        field: {} for field in _ROLE_TO_FIELD.values()
    }

    for row in rows:
        if row["genre_id"] is not None:
            genres.setdefault(
                row["genre_id"],
                {"id": row["genre_id"], "name": row["genre_name"]},
            )

        if row["person_id"] is not None:
            field = _ROLE_TO_FIELD.get(row["role"])
            if field is None:
                raise ValueError(f"Unknown person role {row['role']!r} for film_work {fw_id!r}")
            people[field].setdefault(
                row["person_id"],
                {"id": row["person_id"], "name": row["person_full_name"]},
            )

    return {
        "id": fw_id,
        "title": first["title"],
        "description": first["description"],
        "imdb_rating": first["imdb_rating"],
        "creation_date": first["creation_date"],
        "genres": list(genres.values()),
        "directors": list(people["directors"].values()),
        "actors": list(people["actors"].values()),
        "writers": list(people["writers"].values()),
        "directors_names": [person["name"] for person in people["directors"].values()],
        "actors_names": [person["name"] for person in people["actors"].values()],
        "writers_names": [person["name"] for person in people["writers"].values()],
    }
