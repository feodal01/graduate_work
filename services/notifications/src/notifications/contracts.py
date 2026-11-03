"""Local DTOs for the shared notifications wire schemas; no broker side effects."""

import re
from datetime import date, datetime, timedelta
from typing import Annotated, Any, Literal, Self, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
)

NonnegativeInt = Annotated[int, Field(strict=True, ge=0)]
PositiveInt = Annotated[int, Field(strict=True, ge=1)]
ReviewId = Annotated[str, Field(pattern=r"^[0-9a-fA-F]{24}$")]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AuthEvent(Contract):
    event_id: UUID
    event_type: Literal["user.registered", "user.profile_updated", "user.deleted"]
    user_id: UUID
    user_version: PositiveInt
    tz: str | None = None

    @model_validator(mode="before")
    @classmethod
    def discard_legacy_contacts(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: item
                for key, item in cast(dict[str, Any], value).items()
                if key not in {"email", "first_name", "last_name", "locale"}
            }
        return value

    @model_validator(mode="after")
    def profile_fields(self) -> Self:
        if self.event_type == "user.deleted":
            if "tz" in self.model_fields_set:
                raise ValueError("user.deleted must not carry timezone")
        elif "tz" not in self.model_fields_set:
            raise ValueError("profile events require nullable timezone")
        return self

    @model_serializer(mode="wrap")
    def serialize(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        result: dict[str, Any] = handler(self)
        if self.event_type == "user.deleted":
            result.pop("tz", None)
        return result


class LikesEvent(Contract):
    event_id: UUID
    event_type: Literal["review.likes_updated"]
    review_id: ReviewId
    user_id: UUID
    likes_count: NonnegativeInt
    likes_version: NonnegativeInt
    load_id: str | None = None


class Genre(Contract):
    id: UUID
    name: str


class FilmEvent(Contract):
    event_id: UUID
    event_type: Literal["film.published", "film.updated"]
    film_id: UUID
    film_version: PositiveInt
    catalog_published_at: datetime
    catalog_updated_at: datetime
    title: str
    description: str | None
    creation_date: date | None
    imdb_rating: Annotated[float, Field(strict=True, allow_inf_nan=False)] | None
    genres: list[Genre]

    @field_validator("catalog_published_at", "catalog_updated_at", mode="before")
    @classmethod
    def utc_wire_time(cls, value: Any) -> Any:
        if isinstance(value, str):
            if not re.search(r"[Tt].*(Z|\+00:00)$", value):
                raise ValueError("catalog timestamps must be UTC RFC3339")
        elif not isinstance(value, datetime):
            raise ValueError("catalog timestamps must be UTC datetimes")
        return value

    @field_validator("catalog_published_at", "catalog_updated_at")
    @classmethod
    def utc_datetime(cls, value: datetime) -> datetime:
        if value.utcoffset() != timedelta(0):
            raise ValueError("catalog timestamps must have UTC offset")
        return value

    @field_validator("creation_date", mode="before")
    @classmethod
    def date_only(cls, value: Any) -> Any:
        if value is None or type(value) is date:
            return value
        if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError("creation_date must be a date, not a timestamp")
        return value


class SourcePosition(Contract):
    topic: str
    partition: NonnegativeInt
    next_offset: NonnegativeInt


class DeliverySnapshot(Contract):
    delivery_id: UUID
    user_id: UUID
    kind: str
    payload: dict[str, Any]
