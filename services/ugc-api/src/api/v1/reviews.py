from beanie import PydanticObjectId
from fastapi import APIRouter, status

from schemas.ugc import ReviewIn, ReviewOut, VoteIn
from security.jwt import AccessTokenDep
from services import reviews

router = APIRouter()


@router.get('/{review_id}', response_model=ReviewOut, summary='Рецензия')
async def get_review(review_id: PydanticObjectId) -> ReviewOut:
    review = await reviews.get_review(review_id)
    return ReviewOut.model_validate(review, from_attributes=True)


@router.patch('/{review_id}', response_model=ReviewOut, summary='Изменить текст своей рецензии')
async def update_review(
    review_id: PydanticObjectId,
    payload: ReviewIn,
    token: AccessTokenDep,
) -> ReviewOut:
    review = await reviews.update_review(review_id, token.user_id, payload.text)
    return ReviewOut.model_validate(review, from_attributes=True)


@router.delete(
    '/{review_id}',
    status_code=status.HTTP_204_NO_CONTENT,
    summary='Удалить свою рецензию',
)
async def delete_review(review_id: PydanticObjectId, token: AccessTokenDep) -> None:
    await reviews.delete_review(review_id, token.user_id)


@router.put(
    '/{review_id}/vote',
    response_model=ReviewOut,
    summary='Лайк или дизлайк рецензии',
    description='Повторный вызов с другим значением меняет голос.',
)
async def vote(review_id: PydanticObjectId, payload: VoteIn, token: AccessTokenDep) -> ReviewOut:
    review = await reviews.vote(review_id, token.user_id, payload.value)
    return ReviewOut.model_validate(review, from_attributes=True)


@router.delete('/{review_id}/vote', response_model=ReviewOut, summary='Отозвать голос')
async def delete_vote(review_id: PydanticObjectId, token: AccessTokenDep) -> ReviewOut:
    review = await reviews.delete_vote(review_id, token.user_id)
    return ReviewOut.model_validate(review, from_attributes=True)
