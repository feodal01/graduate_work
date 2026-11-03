from collections.abc import Awaitable, Callable
from enum import Enum
from typing import TypeVar
from uuid import UUID, uuid4

from beanie import PydanticObjectId, SortDirection
from pymongo import ReturnDocument
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.errors import DuplicateKeyError
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from dependencies import get_mongo_client
from events.contracts import LikesSnapshot
from events.outbox import append_likes_event
from models.documents import FilmRating, Review, ReviewVote, VoteValue, utc_now
from services.errors import ConflictError, ForbiddenError, NotFoundError
from services.review_writes import acquire_review_write

T = TypeVar('T')


class ReviewSort(str, Enum):
    NEWEST = '-created_at'
    OLDEST = 'created_at'
    MOST_LIKED = '-likes'
    LEAST_LIKED = 'likes'
    MOST_DISLIKED = '-dislikes'
    LEAST_DISLIKED = 'dislikes'
    HIGHEST_SCORE = '-score'
    LOWEST_SCORE = 'score'


_VOTE_COUNTERS = {VoteValue.LIKE: 'likes', VoteValue.DISLIKE: 'dislikes'}


async def create_review(user_id: UUID, film_id: UUID, text: str) -> Review:
    """Создаёт рецензию и привязывает к ней текущую оценку фильма пользователем."""
    review_id = PydanticObjectId()

    async def create(session: AsyncClientSession) -> Review:
        rating = await FilmRating.get_pymongo_collection().find_one(
            {'user_id': user_id, 'film_id': film_id}, session=session
        )
        review = Review(
            id=review_id,
            user_id=user_id,
            film_id=film_id,
            text=text,
            score=rating['score'] if rating else None,
        )
        await Review.get_pymongo_collection().insert_one(
            review.model_dump(by_alias=True, exclude={'revision_id'}), session=session
        )
        return review

    try:
        return await _run_review_write(review_id, create)
    except DuplicateKeyError as exc:
        raise ConflictError from exc


async def get_review(review_id: PydanticObjectId) -> Review:
    review = await Review.get(review_id)
    if review is None:
        raise NotFoundError
    return review


async def update_review(review_id: PydanticObjectId, user_id: UUID, text: str) -> Review:
    async def update(session: AsyncClientSession) -> Review:
        await _get_own_review(review_id, user_id, session)
        document = await Review.get_pymongo_collection().find_one_and_update(
            {'_id': review_id},
            {'$set': {'text': text, 'updated_at': utc_now()}},
            return_document=ReturnDocument.AFTER,
            session=session,
        )
        review: Review = Review.model_validate(document)
        return review

    return await _run_review_write(review_id, update)


async def delete_review(review_id: PydanticObjectId, user_id: UUID) -> None:
    async def delete(session: AsyncClientSession) -> None:
        await _get_own_review(review_id, user_id, session)
        await Review.get_pymongo_collection().delete_one({'_id': review_id}, session=session)
        await ReviewVote.get_pymongo_collection().delete_many(
            {'review_id': review_id}, session=session
        )

    await _run_review_write(review_id, delete)


async def list_reviews(
    film_id: UUID,
    *,
    sort: ReviewSort,
    skip: int,
    limit: int,
) -> list[Review]:
    field = sort.value.lstrip('-')
    direction = SortDirection.DESCENDING if sort.value.startswith('-') else SortDirection.ASCENDING
    return (
        await Review.find(Review.film_id == film_id)
        .sort([(field, direction), ('_id', direction)])
        .skip(skip)
        .limit(limit)
        .to_list()
    )


async def vote(review_id: PydanticObjectId, user_id: UUID, value: VoteValue) -> Review:
    """Голос, счётчики, версия и событие сохраняются одной транзакцией."""

    async def change(session: AsyncClientSession) -> Review:
        review = await _get_review_in_session(review_id, session)
        collection = ReviewVote.get_pymongo_collection()
        key = {'review_id': review_id, 'user_id': user_id}
        previous = await collection.find_one(key, session=session)
        if previous is not None and previous['value'] == value.value:
            return review
        now = utc_now()
        await collection.update_one(
            key,
            {
                '$set': {'value': value.value, 'updated_at': now},
                '$setOnInsert': {'created_at': now},
            },
            upsert=True,
            session=session,
        )
        increments = {_VOTE_COUNTERS[value]: 1}
        if previous is not None:
            increments[_VOTE_COUNTERS[VoteValue(previous['value'])]] = -1
        return await _increment_counters(review_id, increments, session)

    return await _run_review_write(review_id, change)


async def delete_vote(review_id: PydanticObjectId, user_id: UUID) -> Review:
    async def delete(session: AsyncClientSession) -> Review:
        await _get_review_in_session(review_id, session)
        previous = await ReviewVote.get_pymongo_collection().find_one_and_delete(
            {'review_id': review_id, 'user_id': user_id}, session=session
        )
        if previous is None:
            raise NotFoundError
        return await _increment_counters(
            review_id, {_VOTE_COUNTERS[VoteValue(previous['value'])]: -1}, session
        )

    return await _run_review_write(review_id, delete)


async def _get_review_in_session(
    review_id: PydanticObjectId, session: AsyncClientSession
) -> Review:
    document = await Review.get_pymongo_collection().find_one({'_id': review_id}, session=session)
    if document is None:
        raise NotFoundError
    review: Review = Review.model_validate(document)
    return review


async def _get_own_review(
    review_id: PydanticObjectId, user_id: UUID, session: AsyncClientSession
) -> Review:
    review = await _get_review_in_session(review_id, session)
    if review.user_id != user_id:
        raise ForbiddenError
    return review


async def _increment_counters(
    review_id: PydanticObjectId, increments: dict[str, int], session: AsyncClientSession
) -> Review:
    likes_changed = bool(increments.get('likes'))
    if likes_changed:
        increments = {**increments, 'likes_version': 1}
    document = await Review.get_pymongo_collection().find_one_and_update(
        {'_id': review_id},
        {'$inc': increments},
        return_document=ReturnDocument.AFTER,
        session=session,
    )
    if document is None:
        raise NotFoundError
    review: Review = Review.model_validate(document)
    if likes_changed:
        await append_likes_event(
            session,
            LikesSnapshot(
                event_id=uuid4(),
                review_id=str(review_id),
                user_id=review.user_id,
                likes_count=review.likes,
                likes_version=review.likes_version,
            ),
        )
    return review


async def _run_review_write(
    review_id: PydanticObjectId, operation: Callable[[AsyncClientSession], Awaitable[T]]
) -> T:
    async def transaction(session: AsyncClientSession) -> T:
        await acquire_review_write(session, review_id)
        return await operation(session)

    async with get_mongo_client().start_session() as session:
        # with_transaction перечитывает голос и счётчики после WriteConflict и
        # отдельно повторяет commit при UnknownTransactionCommitResult.
        return await session.with_transaction(
            transaction,
            read_concern=ReadConcern('snapshot'),
            write_concern=WriteConcern('majority'),
        )
