"""Standalone Auth outbox relay; this process never starts the HTTP application."""

import asyncio
import json
import logging
import signal
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager
from typing import Protocol

from aiokafka import AIOKafkaProducer
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from core.config import AuthRelaySettings, KafkaSettings, PostgresSettings
from db.sql.postgres import PostgresDatabase
from models import NotificationOutbox

logger = logging.getLogger(__name__)
# Stable, source-specific lock shared by every process connected to this database.
_RELAY_LOCK = 0x4155544852454C


class Producer(Protocol):
    async def send(
        self, topic: str, value: bytes, *, key: bytes
    ) -> Awaitable[object]: ...


class BatchPublishError(RuntimeError):
    """Some records were not acknowledged; confirmed records have been committed."""


class AuthRelay:
    def __init__(
        self,
        engine: AsyncEngine,
        producer: Producer,
        *,
        config: AuthRelaySettings | None = None,
    ) -> None:
        self.engine = engine
        self.producer = producer
        self.config = config or AuthRelaySettings()

    async def _send(self, row: NotificationOutbox) -> object:
        async with asyncio.timeout(self.config.delivery_timeout_seconds):
            delivery = await self.producer.send(
                self.config.topic,
                json.dumps(
                    row.payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(',', ':'),
                ).encode(),
                key=str(row.user_id).encode(),
            )
            return await delivery

    async def publish_batch(self) -> int:
        """Deliver a bounded batch and commit only Kafka-confirmed event IDs."""
        failures = 0
        confirmed = []
        # Keep the lock on this connection until the confirmation transaction ends.
        # A transaction lock is released by PostgreSQL on rollback/crash, without
        # risking a session-level lock leaking into the connection pool.
        async with AsyncSession(self.engine) as session, session.begin():
            acquired = await session.scalar(
                text('SELECT pg_try_advisory_xact_lock(:key)'), {'key': _RELAY_LOCK}
            )
            if not acquired:
                return 0
            rows = (
                await session.scalars(
                    select(NotificationOutbox)
                    .where(NotificationOutbox.published_at.is_(None))
                    .order_by(
                        NotificationOutbox.created_at, NotificationOutbox.event_id
                    )
                    .limit(self.config.batch_size)
                )
            ).all()
            if not rows:
                return 0
            # All sends are scheduled before any delivery result is collected.
            # A bounded deadline also releases leadership when Kafka is unavailable.
            outcomes = await asyncio.gather(
                *(self._send(row) for row in rows), return_exceptions=True
            )
            for row, outcome in zip(rows, outcomes, strict=True):
                if isinstance(outcome, BaseException):
                    failures += 1
                else:
                    confirmed.append(row.event_id)
            if confirmed:
                await session.execute(
                    update(NotificationOutbox)
                    .where(NotificationOutbox.event_id.in_(confirmed))
                    .values(published_at=func.now())
                )
        if failures:
            raise BatchPublishError(
                f'{failures} events failed; {len(confirmed)} confirmations committed'
            )
        return len(confirmed)

    async def run(self) -> None:
        """Poll immediately after success; pause only for an empty batch or error."""
        while True:
            try:
                count = await self.publish_batch()
            except Exception:
                logger.exception(
                    'Auth outbox batch failed; unconfirmed events will retry'
                )
            else:
                if count:
                    continue
            await asyncio.sleep(self.config.poll_interval_seconds)


@asynccontextmanager
async def _runtime() -> AsyncIterator[AuthRelay]:
    config = AuthRelaySettings()
    postgres = PostgresSettings()
    database = PostgresDatabase(
        postgres.dsn,
        connect_timeout_seconds=postgres.connect_timeout_seconds,
        pool_timeout_seconds=postgres.pool_timeout_seconds,
        command_timeout_seconds=postgres.command_timeout_seconds,
        statement_timeout_ms=postgres.statement_timeout_ms,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KafkaSettings().bootstrap_servers,
        enable_idempotence=True,
    )
    try:
        await producer.start()
        yield AuthRelay(database.engine, producer, config=config)
    finally:
        try:
            try:
                await asyncio.wait_for(producer.stop(), config.shutdown_timeout_seconds)
            except TimeoutError:
                logger.warning(
                    'Kafka shutdown timed out; unpublished events will retry'
                )
        finally:
            await database.close()


async def publish_batch() -> int:
    """Publish a single batch using configured PostgreSQL and Kafka endpoints."""
    async with _runtime() as relay:
        return await relay.publish_batch()


async def main() -> None:
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, task.cancel)
    try:
        while True:
            try:
                async with _runtime() as relay:
                    await relay.run()
            except Exception:
                logger.exception('Auth relay startup failed; retrying')
                await asyncio.sleep(AuthRelaySettings().poll_interval_seconds)
    except asyncio.CancelledError:
        logger.info('Auth relay stopped')
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(signum)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
