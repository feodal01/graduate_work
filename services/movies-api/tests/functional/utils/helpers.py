import uuid
from datetime import date
from typing import Any


def person(name: str, person_id: str | None = None) -> dict[str, Any]:
    return {'id': person_id or str(uuid.uuid4()), 'name': name}


def genre(name: str, genre_id: str | None = None) -> dict[str, Any]:
    return {'id': genre_id or str(uuid.uuid4()), 'name': name}


def film_document(
    title: str,
    film_id: str | None = None,
    imdb_rating: float | None = 7.0,
    description: str | None = 'Test film',
    genres: list[dict[str, Any]] | None = None,
    actors: list[dict[str, Any]] | None = None,
    writers: list[dict[str, Any]] | None = None,
    directors: list[dict[str, Any]] | None = None,
    creation_date: date | None = date(2020, 1, 1),
) -> dict[str, Any]:
    """Документ фильма целиком: приложение требует все эти поля, иначе 500."""
    genres = genres if genres is not None else [genre('Action')]
    actors = actors if actors is not None else [person('Ann Actor')]
    writers = writers if writers is not None else [person('Will Writer')]
    directors = directors if directors is not None else [person('Dan Director')]

    return {
        'id': film_id or str(uuid.uuid4()),
        'creation_date': (creation_date.isoformat() if creation_date is not None else None),
        'title': title,
        'imdb_rating': imdb_rating,
        'description': description,
        'genres': genres,
        'actors': actors,
        'writers': writers,
        'directors': directors,
        'actors_names': [item['name'] for item in actors],
        'writers_names': [item['name'] for item in writers],
        'directors_names': [item['name'] for item in directors],
    }
