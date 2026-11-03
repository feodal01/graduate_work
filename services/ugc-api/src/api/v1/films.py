from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, status

from dependencies import PaginationDep
from schemas.ugc import (
    BookmarkOut,
    FilmRatingSummary,
    Page,
    RatingIn,
    RatingOut,
    ReviewIn,
    ReviewOut,
)
from security.jwt import AccessTokenDep
from services import bookmarks, ratings, reviews
from services.reviews import ReviewSort

router = APIRouter()


@router.put(
    '/{film_id}/rating',
    response_model=RatingOut,
    summary='Поставить или изменить оценку',
    description='Лайк — оценка 10, дизлайк — 0. Повторный вызов меняет оценку.',
)
async def set_rating(film_id: UUID, payload: RatingIn, token: AccessTokenDep) -> RatingOut:
    rating = await ratings.set_rating(token.user_id, film_id, payload.score)
    return RatingOut.model_validate(rating, from_attributes=True)


@router.get('/{film_id}/rating', response_model=RatingOut, summary='Моя оценка фильма')
async def get_rating(film_id: UUID, token: AccessTokenDep) -> RatingOut:
    rating = await ratings.get_rating(token.user_id, film_id)
    if rating is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Rating not found')
    return RatingOut.model_validate(rating, from_attributes=True)


@router.delete(
    '/{film_id}/rating',
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Удалить оценку',
)
async def delete_rating(film_id: UUID, token: AccessTokenDep) -> None:
    if not await ratings.delete_rating(token.user_id, film_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Rating not found')


@router.get(
    '/{film_id}/rating/summary',
    response_model=FilmRatingSummary,
    summary='Лайки, дизлайки и средняя оценка фильма',
)
async def get_summary(film_id: UUID) -> FilmRatingSummary:
    return await ratings.get_summary(film_id)


@router.put(
    '/{film_id}/bookmark',
    response_model=BookmarkOut,
    summary='Добавить фильм в закладки',
    description='Повторный вызов не создаёт дубль.',
)
async def add_bookmark(film_id: UUID, token: AccessTokenDep) -> BookmarkOut:
    bookmark = await bookmarks.add_bookmark(token.user_id, film_id)
    return BookmarkOut.model_validate(bookmark, from_attributes=True)


@router.delete(
    '/{film_id}/bookmark',
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Удалить фильм из закладок',
)
async def delete_bookmark(film_id: UUID, token: AccessTokenDep) -> None:
    if not await bookmarks.delete_bookmark(token.user_id, film_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Bookmark not found')


@router.post(
    '/{film_id}/reviews',
    response_model=ReviewOut,
    status_code=status.HTTP_201_CREATED,
    summary='Написать рецензию',
    description='Одна рецензия на фильм от пользователя. К ней привязывается его оценка фильма.',
    responses={409: {'description': 'Рецензия пользователя на этот фильм уже есть'}},
)
async def create_review(film_id: UUID, payload: ReviewIn, token: AccessTokenDep) -> ReviewOut:
    review = await reviews.create_review(token.user_id, film_id, payload.text)
    return ReviewOut.model_validate(review, from_attributes=True)


@router.get(
    '/{film_id}/reviews',
    response_model=Page[ReviewOut],
    summary='Рецензии к фильму',
    description='Сортировка по дате, лайкам, дизлайкам или оценке автора; «-» — по убыванию.',
)
async def list_reviews(
    film_id: UUID,
    pagination: PaginationDep,
    sort: Annotated[ReviewSort, Query(description='Поле сортировки')] = ReviewSort.NEWEST,
) -> Page[ReviewOut]:
    items = await reviews.list_reviews(
        film_id,
        sort=sort,
        skip=pagination.skip,
        limit=pagination.page_size,
    )
    return Page[ReviewOut](
        page_number=pagination.page_number,
        page_size=pagination.page_size,
        items=[ReviewOut.model_validate(item, from_attributes=True) for item in items],
    )
