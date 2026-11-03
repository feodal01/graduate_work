from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class LikesSnapshot(BaseModel):
    """Итоговое число лайков и версия для получателя — автора рецензии."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    event_id: UUID
    event_type: Literal['review.likes_updated'] = 'review.likes_updated'
    review_id: str = Field(pattern=r'^[0-9a-fA-F]{24}$')
    user_id: UUID
    likes_count: int = Field(ge=0)
    likes_version: int = Field(ge=0)
    load_id: str | None = None
