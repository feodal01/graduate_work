"""Пересылка outbox: один активный batch, ACK Kafka, затем commit MongoDB."""

import argparse
import asyncio
import logging
from typing import Any
from uuid import UUID

from aiokafka import AIOKafkaProducer
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.errors import DuplicateKeyError
from pymongo.write_concern import WriteConcern

from core.config import settings
from dependencies import get_mongo_client
from events.contracts import LikesSnapshot
from events.runtime import mongo_runtime
from models.documents import utc_now

logger = logging.getLogger(__name__)


async def initialize_relay_lock() -> None:
    locks = get_mongo_client()[settings.mongo_db].ugc_relay_lock
    if not await locks.find_one({'_id': 'likes'}):
        try:
            await locks.insert_one({'_id': 'likes', 'revision': 0})
        except DuplicateKeyError:
            pass  # Another process initialized the same permanent lock.


async def mark_published(session: AsyncClientSession, event_ids: list[UUID]) -> None:
    if event_ids:
        await get_mongo_client()[settings.mongo_db].ugc_outbox.update_many(
            {'event_id': {'$in': event_ids}, 'published_at': None},
            {'$set': {'published_at': utc_now()}},
            session=session,
        )


async def publish_batch(*, producer: Any = None) -> int:
    if producer is None:
        if not settings.kafka_bootstrap_servers:
            raise ValueError('KAFKA_BOOTSTRAP_SERVERS is required')
        await validate_transaction_lifetime()
        async with AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            enable_idempotence=True,
        ) as owned_producer:
            return await publish_batch(producer=owned_producer)
    await initialize_relay_lock()
    db = get_mongo_client()[settings.mongo_db]

    async def send(row: dict[str, Any]) -> Any:
        payload = LikesSnapshot.model_validate(
            {key: row[key] for key in LikesSnapshot.model_fields if key in row}
        )
        acknowledgement = await producer.send(
            settings.kafka_likes_topic,
            payload.model_dump_json().encode(),
            key=payload.review_id.encode(),
        )
        return await acknowledgement

    async with get_mongo_client().start_session() as session:
        # Deliberately no with_transaction: its callback retry would repeat external sends.
        async with await session.start_transaction(
            write_concern=WriteConcern('majority'),
            max_commit_time_ms=settings.relay_commit_timeout_ms,
        ):

            async def locked_batch() -> int:
                await db.ugc_relay_lock.update_one(
                    {'_id': 'likes'},
                    {'$inc': {'revision': 1}},
                    session=session,
                )
                rows = (
                    await db.ugc_outbox.find({'published_at': None}, session=session)
                    .sort([('created_at', 1), ('_id', 1)])
                    .limit(settings.relay_batch_size)
                    .to_list()
                )
                # Each send is enqueued independently; no per-message ACK serialization.
                results = await asyncio.gather(*(send(row) for row in rows), return_exceptions=True)
                confirmed = []
                for row, result in zip(rows, results, strict=True):
                    if isinstance(result, BaseException):
                        logger.error(
                            'Kafka did not acknowledge event %s: %s', row['event_id'], result
                        )
                    else:
                        confirmed.append(row['event_id'])
                await mark_published(session, confirmed)
                return len(confirmed)

            # Cancellation aborts the DB transaction and releases the lock. Buffered network
            # sends can still arrive later; replay always retains the same event_id/payload.
            return await asyncio.wait_for(locked_batch(), settings.relay_batch_timeout_seconds)


async def validate_transaction_lifetime() -> None:
    client = get_mongo_client()
    hello = await client.admin.command('hello')
    configured = settings.relay_mongo_transaction_lifetime_seconds
    if hello.get('msg') == 'isdbgrid':
        logger.warning(
            'mongos cannot expose shard transactionLifetimeLimitSeconds; deployment must verify '
            'each shard has a lifetime >= configured %ss',
            configured,
        )
        return
    actual = await client.admin.command({'getParameter': 1, 'transactionLifetimeLimitSeconds': 1})
    if actual['transactionLifetimeLimitSeconds'] < configured:
        raise ValueError('Configured Mongo transaction lifetime exceeds actual server lifetime')


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--once', action='store_true', help='Publish at most one batch and exit')
    args = parser.parse_args()
    if not settings.kafka_bootstrap_servers:
        parser.error('KAFKA_BOOTSTRAP_SERVERS is required')
    logging.basicConfig(level=logging.INFO)
    async with mongo_runtime():
        await validate_transaction_lifetime()
        producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            enable_idempotence=True,
            request_timeout_ms=int(settings.relay_batch_timeout_seconds * 1000),
        )
        await producer.start()
        try:
            while True:
                try:
                    count = await publish_batch(producer=producer)
                except Exception:
                    if args.once:
                        raise
                    logger.exception('Relay batch failed; unpublished events will be retried')
                    await asyncio.sleep(settings.relay_poll_interval_seconds)
                else:
                    if args.once:
                        return
                    if not count:
                        await asyncio.sleep(settings.relay_poll_interval_seconds)
        finally:
            await producer.stop()


if __name__ == '__main__':
    asyncio.run(main())
