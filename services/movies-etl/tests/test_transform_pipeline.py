import datetime
from datetime import date

import pytest

from postgres_to_es.pipelines.extractor.queries import ExtractedFilmWorkRow
from postgres_to_es.pipelines.transform.aggregate import build_movie_payload
from postgres_to_es.pipelines.transform.pipeline import transform_rows

_CREATED = datetime.datetime(2021, 1, 1)
_MODIFIED = datetime.datetime(2021, 1, 2)


def _row(
    *,
    fw_id: str,
    title: str = "Star Wars",
    description: str | None = "A long time ago",
    imdb_rating: float | None = 8.6,
    creation_date: date | None = date(2025, 8, 15),
    role: str | None = None,
    person_id: str | None = None,
    person_full_name: str | None = None,
    genre_id: str | None = None,
    genre_name: str | None = None,
) -> ExtractedFilmWorkRow:
    return {
        "fw_id": fw_id,
        "title": title,
        "description": description,
        "imdb_rating": imdb_rating,
        "creation_date": creation_date,
        "type": "movie",
        "created": _CREATED,
        "modified": _MODIFIED,
        "role": role,
        "person_id": person_id,
        "person_full_name": person_full_name,
        "genre_id": genre_id,
        "genre_name": genre_name,
    }


def test_build_movie_payload_dedups_cross_joined_persons_and_genres() -> None:
    # Arrange: LEFT JOIN person_film_work x genre_film_work даёт 2 persons x 2 genres = 4 строки
    rows = [
        _row(
            fw_id="film-1",
            role="actor",
            person_id="p1",
            person_full_name="Mark Hamill",
            genre_id="g1",
            genre_name="Sci-Fi",
        ),
        _row(
            fw_id="film-1",
            role="actor",
            person_id="p1",
            person_full_name="Mark Hamill",
            genre_id="g2",
            genre_name="Adventure",
        ),
        _row(
            fw_id="film-1",
            role="actor",
            person_id="p2",
            person_full_name="Harrison Ford",
            genre_id="g1",
            genre_name="Sci-Fi",
        ),
        _row(
            fw_id="film-1",
            role="actor",
            person_id="p2",
            person_full_name="Harrison Ford",
            genre_id="g2",
            genre_name="Adventure",
        ),
    ]

    # Act
    payload = build_movie_payload("film-1", rows)

    # Assert: дубли по person_id и genre_id убраны, несмотря на 4 исходные строки
    assert payload["genres"] == [
        {"id": "g1", "name": "Sci-Fi"},
        {"id": "g2", "name": "Adventure"},
    ]
    assert [actor["id"] for actor in payload["actors"]] == ["p1", "p2"]
    assert payload["actors_names"] == ["Mark Hamill", "Harrison Ford"]
    assert payload["directors"] == []
    assert payload["writers"] == []


def test_transform_rows_produces_valid_documents_for_each_film_work() -> None:
    # Arrange: две разные фильмы в одном батче строк
    rows = [
        _row(fw_id="film-1", role="director", person_id="p1", person_full_name="George Lucas"),
        _row(
            fw_id="film-2",
            title="Indiana Jones",
            imdb_rating=7.9,
            genre_id="g3",
            genre_name="Action",
        ),
    ]

    # Act
    documents = transform_rows(rows)

    # Assert
    assert {doc.id for doc in documents} == {"film-1", "film-2"}
    film_1 = next(doc for doc in documents if doc.id == "film-1")
    assert film_1.directors_names == ["George Lucas"]
    assert film_1.creation_date == date(2025, 8, 15)
    assert film_1.to_elasticsearch()["creation_date"] == "2025-08-15"
    film_2 = next(doc for doc in documents if doc.id == "film-2")
    assert film_2.title == "Indiana Jones"
    assert film_2.to_elasticsearch()["genres"] == [{"id": "g3", "name": "Action"}]


def test_transform_omits_unknown_creation_date_from_elasticsearch() -> None:
    document = transform_rows([_row(fw_id="film-1", creation_date=None)])[0]

    assert document.creation_date is None
    assert "creation_date" not in document.to_elasticsearch()


def test_transform_rows_raises_on_unknown_role_instead_of_silently_dropping_it() -> None:
    # Arrange: БД не ограничивает person_film_work.role CHECK-констрейнтом,
    # поэтому появление новой роли — это giveup-случай (рассинхрон кода и
    # данных), а не транзиентная проблема одной записи: всплывает наружу
    # и валит весь батч, а не складывается в тихо игнорируемый список ошибок.
    rows = [
        _row(fw_id="film-1", role="producer", person_id="p1", person_full_name="Unknown Role"),
        _row(fw_id="film-2"),
    ]

    # Act / Assert
    with pytest.raises(ValueError, match="producer"):
        transform_rows(rows)
