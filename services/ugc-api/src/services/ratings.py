from typing import Any
from uuid import UUID

from pymongo import ReturnDocument

from models.documents import DISLIKE_SCORE, LIKE_SCORE, FilmRating, Review, utc_now
from schemas.ugc import FilmRatingSummary


async def set_rating(user_id: UUID, film_id: UUID, score: int) -> FilmRating:
    """Создаёт или меняет оценку атомарно и переносит её в рецензию пользователя."""
    now = utc_now()
    document = await FilmRating.get_pymongo_collection().find_one_and_update(
        {'user_id': user_id, 'film_id': film_id},
        {'$set': {'score': score, 'updated_at': now}, '$setOnInsert': {'created_at': now}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    await _sync_review_score(user_id, film_id, score)
    rating: FilmRating = FilmRating.model_validate(document)
    return rating


async def get_rating(user_id: UUID, film_id: UUID) -> FilmRating | None:
    return await FilmRating.find_one(
        FilmRating.user_id == user_id,
        FilmRating.film_id == film_id,
    )


async def delete_rating(user_id: UUID, film_id: UUID) -> bool:
    result = await FilmRating.get_pymongo_collection().delete_one(
        {'user_id': user_id, 'film_id': film_id},
    )
    if result.deleted_count:
        await _sync_review_score(user_id, film_id, None)
    return bool(result.deleted_count)


async def get_summary(film_id: UUID) -> FilmRatingSummary:
    pipeline: list[dict[str, Any]] = [
        {
            '$group': {
                '_id': None,
                'ratings_count': {'$sum': 1},
                'average_score': {'$avg': '$score'},
                'likes': {'$sum': {'$cond': [{'$eq': ['$score', LIKE_SCORE]}, 1, 0]}},
                'dislikes': {'$sum': {'$cond': [{'$eq': ['$score', DISLIKE_SCORE]}, 1, 0]}},
            },
        },
    ]
    rows = await FilmRating.find(FilmRating.film_id == film_id).aggregate(pipeline).to_list()
    if not rows:
        return FilmRatingSummary(film_id=film_id)
    return FilmRatingSummary(film_id=film_id, **{k: v for k, v in rows[0].items() if k != '_id'})


async def list_user_ratings(
    user_id: UUID,
    *,
    score: int | None,
    skip: int,
    limit: int,
) -> list[FilmRating]:
    query = FilmRating.find(FilmRating.user_id == user_id)
    if score is not None:
        query = query.find(FilmRating.score == score)
    return await query.sort('-updated_at').skip(skip).limit(limit).to_list()


async def _sync_review_score(user_id: UUID, film_id: UUID, score: int | None) -> None:
    await Review.get_pymongo_collection().update_one(
        {'user_id': user_id, 'film_id': film_id},
        {'$set': {'score': score}},
    )
