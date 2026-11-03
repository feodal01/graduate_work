"""Документы MongoDB: оценки фильмов, закладки, рецензии и голоса за рецензии."""

from datetime import datetime, timezone
from enum import Enum
from uuid import UUID

from beanie import Document, PydanticObjectId
from pydantic import Field
from pymongo import ASCENDING, DESCENDING, IndexModel

MIN_SCORE = 0
MAX_SCORE = 10
LIKE_SCORE = MAX_SCORE
DISLIKE_SCORE = MIN_SCORE
MAX_REVIEW_TEXT_LENGTH = 5000


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class FilmRating(Document):
    """Оценка фильма пользователем: 10 — лайк, 0 — дизлайк, остальное — оценка."""

    user_id: UUID
    film_id: UUID
    score: int = Field(ge=MIN_SCORE, le=MAX_SCORE)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    class Settings:
        name = 'film_ratings'
        indexes = [
            IndexModel([('user_id', ASCENDING), ('film_id', ASCENDING)], unique=True),
            IndexModel([('film_id', ASCENDING), ('score', ASCENDING)]),
            IndexModel([('user_id', ASCENDING), ('score', ASCENDING), ('updated_at', DESCENDING)]),
        ]


class Bookmark(Document):
    """Фильм, отложенный пользователем на потом."""

    user_id: UUID
    film_id: UUID
    created_at: datetime = Field(default_factory=utc_now)

    class Settings:
        name = 'bookmarks'
        indexes = [
            IndexModel([('user_id', ASCENDING), ('film_id', ASCENDING)], unique=True),
            IndexModel([('user_id', ASCENDING), ('created_at', DESCENDING)]),
        ]


class Review(Document):
    """Рецензия. score, likes и dislikes денормализованы для сортировки списка."""

    user_id: UUID
    film_id: UUID
    text: str = Field(min_length=1, max_length=MAX_REVIEW_TEXT_LENGTH)
    score: int | None = Field(default=None, ge=MIN_SCORE, le=MAX_SCORE)
    likes: int = Field(default=0, ge=0)
    likes_version: int = Field(default=0, ge=0)
    dislikes: int = Field(default=0, ge=0)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    class Settings:
        name = 'reviews'
        indexes = [
            IndexModel([('film_id', ASCENDING), ('user_id', ASCENDING)], unique=True),
            IndexModel([('film_id', ASCENDING), ('created_at', DESCENDING)]),
            IndexModel([('film_id', ASCENDING), ('likes', DESCENDING)]),
            IndexModel([('film_id', ASCENDING), ('dislikes', DESCENDING)]),
            IndexModel([('film_id', ASCENDING), ('score', DESCENDING)]),
        ]


class VoteValue(str, Enum):
    LIKE = 'like'
    DISLIKE = 'dislike'


class ReviewVote(Document):
    """Лайк или дизлайк пользователя к рецензии."""

    review_id: PydanticObjectId
    user_id: UUID
    value: VoteValue
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    class Settings:
        name = 'review_votes'
        indexes = [
            IndexModel([('review_id', ASCENDING), ('user_id', ASCENDING)], unique=True),
        ]


class LikesOutbox(Document):
    """Неизменяемый снимок счётчика; relay отмечает только published_at."""

    event_id: UUID
    event_type: str = 'review.likes_updated'
    review_id: str
    user_id: UUID
    likes_count: int = Field(ge=0)
    likes_version: int = Field(ge=0)
    load_id: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    published_at: datetime | None = None

    class Settings:
        name = 'ugc_outbox'
        indexes = [
            IndexModel([('event_id', ASCENDING)], unique=True),
            IndexModel([('published_at', ASCENDING), ('created_at', ASCENDING)]),
            IndexModel(
                [('load_id', ASCENDING), ('review_id', ASCENDING)],
                unique=True,
                partialFilterExpression={'load_id': {'$type': 'string'}},
            ),
        ]


DOCUMENT_MODELS: list[type[Document]] = [FilmRating, Bookmark, Review, ReviewVote, LikesOutbox]
