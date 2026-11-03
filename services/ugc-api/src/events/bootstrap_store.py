"""Постоянный снимок и отметки загрузки, независимые от очистки outbox."""

import hashlib
from typing import Any
from uuid import uuid4

from pymongo import ReturnDocument
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from core.config import settings
from dependencies import get_mongo_client
from events.contracts import LikesSnapshot
from events.outbox import append_likes_event
from services.review_writes import (
    REVIEW_WRITE_BUCKETS,
    REVIEW_WRITE_GATE_COLLECTION,
    initialize_review_write_gate,
)


def database() -> AsyncDatabase[dict[str, Any]]:
    return get_mongo_client()[settings.mongo_db]


async def initialize_bootstrap_store() -> None:
    db = database()
    await initialize_review_write_gate()
    await db.likes_bootstrap_rows.create_index([('load_id', 1), ('review_id', 1)], unique=True)
    await db.likes_bootstrap_rows.create_index([('load_id', 1), ('enqueued', 1), ('review_id', 1)])


async def begin_capture(load_id: str) -> None:
    if not load_id:
        raise ValueError('load_id must not be empty')
    await initialize_bootstrap_store()
    db = database()

    async def freeze(session: AsyncClientSession) -> None:
        existing = await db.likes_bootstrap_loads.find_one({'_id': load_id}, session=session)
        if existing:
            if existing['state'] != 'capturing':
                raise ValueError('Cannot capture a completed snapshot')
            if (
                await db[REVIEW_WRITE_GATE_COLLECTION].count_documents(
                    {'frozen': True, 'load_id': load_id}, session=session
                )
                != REVIEW_WRITE_BUCKETS
            ):
                raise RuntimeError('Incomplete persisted write barrier')
            return
        if await db[REVIEW_WRITE_GATE_COLLECTION].count_documents(
            {'frozen': True}, session=session
        ):
            raise ValueError('Write barrier belongs to another capture')
        result = await db[REVIEW_WRITE_GATE_COLLECTION].update_many(
            {'_id': {'$gte': 0, '$lt': REVIEW_WRITE_BUCKETS}},
            {'$set': {'frozen': True, 'load_id': load_id}, '$inc': {'revision': 1}},
            session=session,
        )
        if result.matched_count != REVIEW_WRITE_BUCKETS:
            raise RuntimeError('Incomplete review write barrier')
        await db.likes_bootstrap_loads.insert_one(
            {
                '_id': load_id,
                'state': 'capturing',
                'cursor': None,
                'captured_count': 0,
                'enqueued_count': 0,
                'revision': 0,
            },
            session=session,
        )

    async with get_mongo_client().start_session() as session:
        await session.with_transaction(
            freeze, read_concern=ReadConcern('snapshot'), write_concern=WriteConcern('majority')
        )


async def capture_batch(load_id: str, limit: int) -> int:
    db = database()

    async def capture(session: AsyncClientSession) -> int:
        state = await db.likes_bootstrap_loads.find_one_and_update(
            {'_id': load_id, 'state': 'capturing'},
            {'$inc': {'revision': 1}},
            return_document=ReturnDocument.AFTER,
            session=session,
        )
        if not state:
            raise ValueError('Capture is not active')
        query = {'_id': {'$gt': state['cursor']}} if state['cursor'] is not None else {}
        reviews = (
            await db.reviews.find(query, session=session).sort('_id', 1).limit(limit).to_list()
        )
        if not reviews:
            return 0
        rows = []
        for review in reviews:
            payload = LikesSnapshot(
                event_id=uuid4(),
                review_id=str(review['_id']),
                user_id=review['user_id'],
                likes_count=review.get('likes', 0),
                likes_version=review.get('likes_version', 0),
                load_id=load_id,
            )
            rows.append(
                {
                    'load_id': load_id,
                    'review_id': payload.review_id,
                    'payload': payload.model_dump(),
                    'enqueued': False,
                }
            )
        await db.likes_bootstrap_rows.insert_many(rows, session=session)
        await db.likes_bootstrap_loads.update_one(
            {'_id': load_id},
            {'$set': {'cursor': reviews[-1]['_id']}, '$inc': {'captured_count': len(rows)}},
            session=session,
        )
        return len(rows)

    async with get_mongo_client().start_session() as session:
        return await session.with_transaction(capture, write_concern=WriteConcern('majority'))


async def capture_reviews(load_id: str) -> int:
    await begin_capture(load_id)
    while await capture_batch(load_id, settings.bootstrap_batch_size):
        pass
    db = database()
    # Writes remain frozen. Rows are immutable, and all reviews were captured before hashing.
    digest = hashlib.sha256()
    count = 0
    async for row in db.likes_bootstrap_rows.find({'load_id': load_id}).sort('review_id', 1):
        digest.update(f'{row["review_id"]},{row["payload"]["event_id"]}\n'.encode())
        count += 1
    manifest = {'load_id': load_id, 'expected_count': count, 'manifest_hash': digest.hexdigest()}

    async def finish(session: AsyncClientSession) -> None:
        updated = await db.likes_bootstrap_loads.update_one(
            {'_id': load_id, 'state': 'capturing', 'captured_count': count},
            {'$set': {'state': 'captured', 'manifest': manifest}},
            session=session,
        )
        if not updated.matched_count:
            raise ValueError('Capture is already completed or incomplete')
        released = await db[REVIEW_WRITE_GATE_COLLECTION].update_many(
            {'load_id': load_id, 'frozen': True},
            {'$set': {'frozen': False}, '$unset': {'load_id': ''}},
            session=session,
        )
        if released.matched_count != REVIEW_WRITE_BUCKETS:
            raise RuntimeError('Incomplete persisted write barrier')

    async with get_mongo_client().start_session() as session:
        await session.with_transaction(finish, write_concern=WriteConcern('majority'))
    return count


async def enqueue_reviews(load_id: str, limit: int) -> int:
    if limit < 1:
        raise ValueError('limit must be positive')
    db = database()

    async def enqueue(session: AsyncClientSession) -> int:
        state = await db.likes_bootstrap_loads.find_one_and_update(
            {'_id': load_id},
            {'$inc': {'revision': 1}},
            return_document=ReturnDocument.AFTER,
            session=session,
        )
        if not state or state['state'] == 'capturing':
            raise ValueError('Capture must complete before enqueue')
        if state['state'] == 'enqueued':
            return 0
        rows = (
            await db.likes_bootstrap_rows.find(
                {'load_id': load_id, 'enqueued': False}, session=session
            )
            .sort('review_id', 1)
            .limit(limit)
            .to_list()
        )
        for row in rows:
            await append_likes_event(session, LikesSnapshot.model_validate(row['payload']))
        if rows:
            await db.likes_bootstrap_rows.update_many(
                {'_id': {'$in': [row['_id'] for row in rows]}},
                {'$set': {'enqueued': True}},
                session=session,
            )
        new_count = state['enqueued_count'] + len(rows)
        await db.likes_bootstrap_loads.update_one(
            {'_id': load_id},
            {
                '$set': {
                    'enqueued_count': new_count,
                    'state': 'enqueued' if new_count == state['captured_count'] else 'captured',
                }
            },
            session=session,
        )
        return len(rows)

    async with get_mongo_client().start_session() as session:
        return await session.with_transaction(enqueue, write_concern=WriteConcern('majority'))


async def load_status(load_id: str) -> dict[str, Any]:
    state = await database().likes_bootstrap_loads.find_one({'_id': load_id})
    if not state:
        raise ValueError('Unknown load_id')
    if state['state'] == 'capturing':
        raise ValueError('Capture is incomplete; manifest is not available')
    return dict(state['manifest'])
