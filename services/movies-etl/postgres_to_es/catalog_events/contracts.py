"""Notification projection of the working ES document; wire fields match MovieDTO."""

from datetime import date, datetime, timezone
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class Genre(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: UUID
    name: str


class FilmSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    film_id: UUID
    film_version: int = Field(ge=1, strict=True)
    catalog_published_at: datetime
    catalog_updated_at: datetime
    title: str
    description: str | None
    creation_date: date | None
    imdb_rating: float | None
    genres: list[Genre]

    @field_validator("catalog_published_at", "catalog_updated_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Catalog dates must be timezone-aware")
        return value.astimezone(timezone.utc)

    @classmethod
    def from_source(cls, source: dict[str, Any]) -> "FilmSnapshot":
        # ETL omits nullable fields from ES _source; the wire contract requires explicit nulls.
        source = {"description": None, "creation_date": None, "imdb_rating": None, **source}
        return cls.model_validate(
            {
                field: source["id"] if field == "film_id" else source[field]
                for field in cls.model_fields
            }
        )


class MovieDTO(FilmSnapshot):
    event_id: UUID
    event_type: Literal["film.published", "film.updated"]
