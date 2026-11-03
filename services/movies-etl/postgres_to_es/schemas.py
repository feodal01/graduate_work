"""Pydantic-модели документа индекса movies в Elasticsearch."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, TypedDict
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PersonRole(BaseModel):
    """Вложенный объект для actors / directors / writers (ES type: nested)."""

    model_config = ConfigDict(extra="forbid")

    id: str
    name: str

    @field_validator("id", mode="before")
    @classmethod
    def coerce_id(cls, value: UUID | str) -> str:
        return str(value)


class Genre(BaseModel):
    """Вложенный объект жанра (ES type: nested)."""

    model_config = ConfigDict(extra="forbid")

    id: str
    name: str

    @field_validator("id", mode="before")
    @classmethod
    def coerce_id(cls, value: UUID | str) -> str:
        return str(value)


class MovieDocument(BaseModel):
    """Документ фильма для индекса movies (см. etl/es_schema.txt)."""

    model_config = ConfigDict(extra="forbid")

    id: str
    film_version: int | None = Field(default=None, gt=0)
    catalog_published_at: datetime | None = None
    catalog_updated_at: datetime | None = None
    index_revision: int | None = Field(default=None, gt=0)
    imdb_rating: float | None = None
    creation_date: date | None = None
    genres: list[Genre] = Field(default_factory=list[Genre])
    title: str
    description: str | None = None
    directors_names: list[str] = Field(default_factory=list[str])
    actors_names: list[str] = Field(default_factory=list[str])
    writers_names: list[str] = Field(default_factory=list[str])
    directors: list[PersonRole] = Field(default_factory=list[PersonRole])
    actors: list[PersonRole] = Field(default_factory=list[PersonRole])
    writers: list[PersonRole] = Field(default_factory=list[PersonRole])

    @field_validator("id", mode="before")
    @classmethod
    def coerce_id(cls, value: UUID | str) -> str:
        return str(value)

    @field_validator(
        "directors_names",
        "actors_names",
        "writers_names",
        mode="before",
    )
    @classmethod
    def none_to_empty_list(cls, value: list[str] | None) -> list[str]:
        return value or []

    @field_validator("genres", mode="before")
    @classmethod
    def none_to_empty_genres(cls, value: list[Genre] | None) -> list[Genre]:
        return value or []

    @field_validator("directors", "actors", "writers", mode="before")
    @classmethod
    def none_to_empty_roles(cls, value: list[PersonRole] | None) -> list[PersonRole]:
        return value or []

    def to_elasticsearch(self) -> dict[str, Any]:
        """Сериализация для _bulk API: без null и без лишних полей."""
        return self.model_dump(mode="json", exclude_none=True, exclude={"index_revision"})

    @classmethod
    def from_postgres_row(cls, row: dict[str, Any]) -> MovieDocument:
        """
        Сборка документа из строки агрегирующего SQL-запроса.

        Ожидаемые ключи row:
            id, title, description, imdb_rating, creation_date,
            genres, directors, actors, writers,
            directors_names, actors_names, writers_names
        """
        return cls.model_validate(row)


class PostgresDSL(TypedDict):
    dbname: str
    user: str
    password: str
    host: str
    port: int
    options: str
