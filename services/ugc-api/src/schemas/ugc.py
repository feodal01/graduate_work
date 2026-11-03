from datetime import datetime
from typing import Generic, TypeVar
from uuid import UUID

from beanie import PydanticObjectId
from pydantic import BaseModel, Field

from models.documents import MAX_REVIEW_TEXT_LENGTH, MAX_SCORE, MIN_SCORE, VoteValue

ItemT = TypeVar('ItemT')


class Page(BaseModel, Generic[ItemT]):
    page_number: int
    page_size: int
    items: list[ItemT]


class RatingIn(BaseModel):
    score: int = Field(
        ge=MIN_SCORE,
        le=MAX_SCORE,
        description='Оценка 0…10: 10 — лайк, 0 — дизлайк',
        examples=[10],
    )


class RatingOut(BaseModel):
    film_id: UUID
    score: int
    created_at: datetime
    updated_at: datetime


class FilmRatingSummary(BaseModel):
    film_id: UUID
    likes: int = Field(default=0, description='Количество оценок 10')
    dislikes: int = Field(default=0, description='Количество оценок 0')
    ratings_count: int = Field(default=0, description='Количество всех оценок')
    average_score: float | None = Field(
        default=None,
        description='Средняя пользовательская оценка; null, если оценок нет',
    )


class BookmarkOut(BaseModel):
    film_id: UUID
    created_at: datetime


class ReviewIn(BaseModel):
    text: str = Field(min_length=1, max_length=MAX_REVIEW_TEXT_LENGTH)


class ReviewOut(BaseModel):
    id: PydanticObjectId
    film_id: UUID
    user_id: UUID
    text: str
    score: int | None = Field(description='Оценка фильма автором рецензии')
    likes: int
    dislikes: int
    created_at: datetime
    updated_at: datetime


class VoteIn(BaseModel):
    value: VoteValue
