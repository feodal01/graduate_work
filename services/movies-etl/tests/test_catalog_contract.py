import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from postgres_to_es.catalog_events.contracts import FilmSnapshot, MovieDTO


def snapshot() -> dict[str, Any]:
    return dict(
        film_id=str(uuid4()),
        film_version=1,
        title="Film",
        description=None,
        creation_date=None,
        imdb_rating=None,
        genres=[],
        catalog_published_at="2026-01-01T03:00:00+03:00",
        catalog_updated_at="2026-01-01T03:00:00+03:00",
    )


def test_exact_movies_contract_fields_and_utc() -> None:
    contract = json.loads(
        (
            Path(__file__).resolve().parents[3] / "contracts/notifications/movies.schema.json"
        ).read_text()
    )
    event = MovieDTO(**snapshot(), event_id=uuid4(), event_type="film.published")
    wire = event.model_dump(mode="json")
    assert set(wire) == set(contract["properties"]) == set(contract["required"])
    assert wire["catalog_published_at"] == "2026-01-01T00:00:00Z"
    assert wire["catalog_updated_at"] == "2026-01-01T00:00:00Z"
    with pytest.raises(ValidationError):
        FilmSnapshot.model_validate({**snapshot(), "film_version": True})
    with pytest.raises(ValidationError):
        FilmSnapshot.model_validate({**snapshot(), "catalog_updated_at": "2026-01-01T00:00:00"})


def test_sparse_etl_source_restores_nullable_fields_but_requires_metadata() -> None:
    from postgres_to_es.schemas import MovieDocument

    source = MovieDocument.model_validate(
        {
            "id": str(uuid4()),
            "title": "Sparse film",
            "film_version": 1,
            "catalog_published_at": "2026-01-01T00:00:00Z",
            "catalog_updated_at": "2026-01-01T00:00:00Z",
        }
    ).to_elasticsearch()
    assert "description" not in source
    film = FilmSnapshot.from_source(source)
    wire = MovieDTO(**film.model_dump(), event_id=uuid4(), event_type="film.updated").model_dump()
    assert wire["description"] is None
    assert wire["creation_date"] is None
    assert wire["imdb_rating"] is None
    del source["film_version"]
    with pytest.raises((KeyError, ValidationError)):
        FilmSnapshot.from_source(source)
