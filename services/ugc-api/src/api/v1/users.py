from typing import Annotated

from fastapi import APIRouter, Query

from dependencies import PaginationDep
from models.documents import MAX_SCORE, MIN_SCORE
from schemas.ugc import BookmarkOut, Page, RatingOut
from security.jwt import AccessTokenDep
from services import bookmarks, ratings

router = APIRouter()


@router.get(
    '/ratings',
    response_model=Page[RatingOut],
    summary='Мои оценки',
    description='score=10 — список понравившихся фильмов, score=0 — дизлайки.',
)
async def list_ratings(
    token: AccessTokenDep,
    pagination: PaginationDep,
    score: Annotated[
        int | None,
        Query(ge=MIN_SCORE, le=MAX_SCORE, description='Только оценки с этим значением'),
    ] = None,
) -> Page[RatingOut]:
    items = await ratings.list_user_ratings(
        token.user_id,
        score=score,
        skip=pagination.skip,
        limit=pagination.page_size,
    )
    return Page[RatingOut](
        page_number=pagination.page_number,
        page_size=pagination.page_size,
        items=[RatingOut.model_validate(item, from_attributes=True) for item in items],
    )


@router.get(
    '/bookmarks',
    response_model=Page[BookmarkOut],
    summary='Мои закладки',
    description='Новые закладки первыми.',
)
async def list_bookmarks(token: AccessTokenDep, pagination: PaginationDep) -> Page[BookmarkOut]:
    items = await bookmarks.list_bookmarks(
        token.user_id,
        skip=pagination.skip,
        limit=pagination.page_size,
    )
    return Page[BookmarkOut](
        page_number=pagination.page_number,
        page_size=pagination.page_size,
        items=[BookmarkOut.model_validate(item, from_attributes=True) for item in items],
    )
