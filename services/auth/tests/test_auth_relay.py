import asyncio
import json
import os
import signal
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import create_autospec
from uuid import uuid4

import pytest
from sqlalchemy import func, insert, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

import relay as relay_module
from core.config import AuthRelaySettings
from models import NotificationOutbox
from relay import AuthRelay, BatchPublishError, Producer
from tests import test_notification_migration

notification_database = test_notification_migration.notification_database
pytestmark = pytest.mark.integration


@pytest.fixture
async def kafka_topic() -> AsyncIterator[str]:
    """Provision and remove only this test's UUID topic on the dedicated broker."""
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    brokers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:19094')
    assert brokers == '127.0.0.1:19094', 'Use the dedicated integration Kafka'
    topic = f'notifications-test-auth-{uuid4().hex}'
    admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
    try:
        await admin.start()
        await admin.create_topics([NewTopic(topic, 3, 1)])
        yield topic
    finally:
        try:
            await admin.delete_topics([topic])
        finally:
            await admin.close()


async def seed(engine: AsyncEngine, count: int) -> list[dict]:
    """Insert durable, immutable snapshots into the migrated test database."""
    rows = []
    for index in range(count):
        event_id, user_id = uuid4(), uuid4()
        event_type = ('user.registered', 'user.profile_updated', 'user.deleted')[
            index % 3
        ]
        contacts = (
            {}
            if event_type == 'user.deleted'
            else dict(
                email='viewer@example.org',
                first_name=None,
                last_name=None,
                locale='ru-RU',
                tz=None,
            )
        )
        rows.append(
            dict(
                event_id=event_id,
                user_id=user_id,
                user_version=1,
                event_type=event_type,
                payload=dict(
                    event_id=str(event_id),
                    user_id=str(user_id),
                    user_version=1,
                    event_type=event_type,
                    **contacts,
                ),
            )
        )
    async with engine.begin() as connection:
        await connection.execute(insert(NotificationOutbox), rows)
    return rows


class ControlledProducer:
    """Expose enqueue versus delivery separately to test concurrent acknowledgements."""

    def __init__(self, count: int, failed: int = 0) -> None:
        self.count = count
        self.failed = failed
        self.calls = []
        self.futures = []
        self.enqueued = asyncio.Event()

    async def send(self, topic: str, value: bytes, *, key: bytes):
        """Queue without resolving deliveries until the whole batch has arrived."""
        self.calls.append((topic, key, json.loads(value)))
        future = asyncio.get_running_loop().create_future()
        self.futures.append(future)
        if len(self.calls) == self.count:
            self.enqueued.set()
        return future

    def acknowledge(self) -> None:
        """Resolve confirmed and failed records independently."""
        for index, future in enumerate(self.futures):
            if index < self.failed:
                future.set_exception(RuntimeError('injected Kafka delivery failure'))
            else:
                future.set_result(None)


@pytest.mark.parametrize('entrypoint', ['method', 'function'])
@pytest.mark.parametrize('batch_size', [1, 2])
async def test_batch_size_from_settings(
    notification_database: AsyncEngine, monkeypatch, entrypoint, batch_size
) -> None:
    """Read the batch size exclusively from relay settings."""
    await seed(notification_database, 3)
    monkeypatch.setenv('AUTH_RELAY_BATCH_SIZE', str(batch_size))

    class AcknowledgedProducer:
        async def send(self, topic, value, *, key):
            future = asyncio.get_running_loop().create_future()
            future.set_result(None)
            return future

    relay = AuthRelay(notification_database, AcknowledgedProducer())

    @asynccontextmanager
    async def runtime():
        yield relay

    monkeypatch.setattr(relay_module, '_runtime', runtime)
    publish = (
        relay.publish_batch if entrypoint == 'method' else relay_module.publish_batch
    )
    count = await publish()
    assert count == batch_size
    async with AsyncSession(notification_database) as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(NotificationOutbox)
                .where(NotificationOutbox.published_at.is_(None))
            )
            == 3 - batch_size
        )


async def test_batch_cap_parallel_delivery_partial_ack_and_lock(
    notification_database: AsyncEngine,
) -> None:
    """Submit 500 before waiting; exclude a second relay; update only confirmed IDs."""
    await seed(notification_database, 501)
    producer = ControlledProducer(500, failed=1)
    relay = AuthRelay(notification_database, producer)
    task = asyncio.create_task(relay.publish_batch())
    await asyncio.wait_for(producer.enqueued.wait(), 5)
    assert len(producer.calls) == 500
    competitor = ControlledProducer(1)
    assert await AuthRelay(notification_database, competitor).publish_batch() == 0
    assert competitor.calls == []
    producer.acknowledge()
    with pytest.raises(BatchPublishError):
        await task
    async with AsyncSession(notification_database) as session:
        confirmed = (
            await session.scalars(
                select(NotificationOutbox).where(
                    NotificationOutbox.published_at.is_not(None)
                )
            )
        ).all()
        assert len(confirmed) == 499
        assert {str(row.event_id) for row in confirmed} == {
            call[2]['event_id'] for call in producer.calls[1:]
        }
    for topic, key, payload in producer.calls:
        assert topic == 'auth.users.v1'
        assert key == payload['user_id'].encode()


async def test_cancel_releases_lock_and_preserves_unconfirmed(
    notification_database: AsyncEngine,
) -> None:
    """Cancellation leaves retryable rows and releases PostgreSQL leadership."""
    await seed(notification_database, 1)
    producer = ControlledProducer(1)
    task = asyncio.create_task(
        AuthRelay(notification_database, producer).publish_batch()
    )
    await asyncio.wait_for(producer.enqueued.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    retry = ControlledProducer(1)
    task = asyncio.create_task(AuthRelay(notification_database, retry).publish_batch())
    await asyncio.wait_for(retry.enqueued.wait(), 5)
    assert retry.calls == producer.calls
    retry.acknowledge()
    assert await task == 1


async def test_live_kafka_ack_then_database_failure_replays_identical_event(
    notification_database: AsyncEngine,
    kafka_topic: str,
) -> None:
    """Real Kafka preserves the keyed snapshot across a post-ack database failure."""
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    brokers = '127.0.0.1:19094'
    config = AuthRelaySettings(topic=kafka_topic)
    rows = await seed(notification_database, 1)
    event_id = str(rows[0]['event_id'])
    producer = AIOKafkaProducer(bootstrap_servers=brokers, enable_idempotence=True)
    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers=brokers,
        group_id=None,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
    )
    try:
        await producer.start()
        await consumer.start()
        async with notification_database.begin() as connection:
            await connection.execute(
                text("""
                CREATE FUNCTION reject_ack() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'injected post-ack failure'; END $$
            """)
            )
            await connection.execute(
                text("""
                CREATE TRIGGER reject_ack BEFORE UPDATE ON notification_outbox
                FOR EACH ROW EXECUTE FUNCTION reject_ack()
            """)
            )
        with pytest.raises(DBAPIError, match='injected post-ack failure'):
            await AuthRelay(
                notification_database, producer, config=config
            ).publish_batch()
        async with notification_database.begin() as connection:
            assert (
                await connection.scalar(
                    select(func.count())
                    .select_from(NotificationOutbox)
                    .where(NotificationOutbox.published_at.is_(None))
                )
                == 1
            )
            await connection.execute(
                text('DROP TRIGGER reject_ack ON notification_outbox')
            )
        # A new relay object represents restart; committed snapshot is the only input.
        assert (
            await AuthRelay(
                notification_database, producer, config=config
            ).publish_batch()
            == 1
        )
        assert (
            await AuthRelay(
                notification_database, producer, config=config
            ).publish_batch()
            == 0
        )
        received = []
        async with asyncio.timeout(20):
            async for message in consumer:
                assert message.value is not None
                payload = json.loads(message.value)
                if payload['event_id'] == event_id:
                    received.append((message.topic, message.key, message.value))
                    if len(received) == 2:
                        break
        assert received[0] == received[1]
        assert received[0][0] == kafka_topic
        assert received[0][1] == str(rows[0]['user_id']).encode()
        assert json.loads(received[0][2]) == rows[0]['payload']
    finally:
        await consumer.stop()
        await producer.stop()


async def test_deadline_commits_successes_and_retries_stalled_delivery(
    notification_database: AsyncEngine,
) -> None:
    """A stalled delivery cannot discard other confirmed records in its batch."""
    await seed(notification_database, 2)
    producer = ControlledProducer(2)
    config = AuthRelaySettings(delivery_timeout_seconds=0.1)
    task = asyncio.create_task(
        AuthRelay(notification_database, producer, config=config).publish_batch()
    )
    await asyncio.wait_for(producer.enqueued.wait(), 5)
    producer.futures[0].set_result(None)
    with pytest.raises(BatchPublishError):
        await task
    async with notification_database.connect() as connection:
        assert (
            await connection.scalar(
                select(func.count())
                .select_from(NotificationOutbox)
                .where(NotificationOutbox.published_at.is_not(None))
            )
            == 1
        )


async def test_live_kafka_batch_of_501_and_standalone_sigterm(
    notification_database: AsyncEngine,
    kafka_topic: str,
) -> None:
    """Publish 500+1 to real Kafka, then stop the standalone polling process cleanly."""
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    rows = await seed(notification_database, 501)
    expected = {str(row['event_id']): row['payload'] for row in rows}
    producer = AIOKafkaProducer(
        bootstrap_servers='127.0.0.1:19094', enable_idempotence=True
    )
    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers='127.0.0.1:19094',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=None,
    )
    try:
        await producer.start()
        await consumer.start()
        relay = AuthRelay(
            notification_database, producer, config=AuthRelaySettings(topic=kafka_topic)
        )
        assert await relay.publish_batch() == 500
        assert await relay.publish_batch() == 1
        actual = {}
        async with asyncio.timeout(20):
            async for message in consumer:
                assert message.value is not None
                payload = json.loads(message.value)
                if payload['event_id'] in expected:
                    assert message.topic == kafka_topic
                    assert message.key == payload['user_id'].encode()
                    actual[payload['event_id']] = payload
                    if len(actual) == 501:
                        break
        assert actual == expected
    finally:
        await consumer.stop()
        await producer.stop()

    root = Path(__file__).parents[1]
    environment = dict(
        os.environ,
        POSTGRES_DB=str(notification_database.url.database),
        PYTHONPATH=str(root / 'src'),
        KAFKA_BOOTSTRAP_SERVERS='127.0.0.1:19094',
        AUTH_RELAY_TOPIC=kafka_topic,
    )
    cli_row = (await seed(notification_database, 1))[0]
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-m',
        'relay',
        cwd=root,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        # Wait for an actual CLI publication before asking the process to stop.
        async with asyncio.timeout(10):
            async with notification_database.connect() as connection:
                while not await connection.scalar(
                    select(NotificationOutbox.published_at).where(
                        NotificationOutbox.event_id == cli_row['event_id']
                    )
                ):
                    await asyncio.sleep(0.05)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, (stdout + stderr).decode()
        assert b'Auth relay stopped' in stderr
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


async def test_process_crash_after_live_kafka_ack_preserves_retry_identity(
    notification_database: AsyncEngine,
    kafka_topic: str,
) -> None:
    """Kill after a real broker ack, restart, and read the same event twice."""
    from aiokafka import AIOKafkaConsumer

    row = (await seed(notification_database, 1))[0]
    root = Path(__file__).parents[1]
    environment = dict(
        os.environ,
        POSTGRES_DB=str(notification_database.url.database),
        PYTHONPATH=str(root / 'src'),
        KAFKA_BOOTSTRAP_SERVERS='127.0.0.1:19094',
        AUTH_RELAY_TOPIC=kafka_topic,
    )
    # The harness kills precisely after the production sender receives a real ack.
    crash_script = """
import asyncio, os, signal
import relay
send = relay.AuthRelay._send
async def crash(self, row):
    await send(self, row)
    os.kill(os.getpid(), signal.SIGKILL)
relay.AuthRelay._send = crash
asyncio.run(relay.publish_batch())
"""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-c',
        crash_script,
        cwd=root,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == -signal.SIGKILL, (stdout + stderr).decode()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    async with notification_database.connect() as connection:
        assert await connection.scalar(select(NotificationOutbox.published_at)) is None
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        '-c',
        'import asyncio, relay; print(asyncio.run(relay.publish_batch()))',
        cwd=root,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, (stdout + stderr).decode()
        assert stdout.strip() == b'1'
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers='127.0.0.1:19094',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=None,
    )
    try:
        await consumer.start()
        messages = []
        async with asyncio.timeout(10):
            async for message in consumer:
                messages.append(message)
                if len(messages) == 2:
                    break
        assert messages[0].key == messages[1].key == str(row['user_id']).encode()
        assert messages[0].value == messages[1].value
        assert json.loads(messages[0].value) == row['payload']
        assert messages[0].offset != messages[1].offset
    finally:
        await consumer.stop()


async def test_polling_pauses_only_for_empty_or_failed_batches(monkeypatch) -> None:
    """Full batches flow immediately; empty and failed attempts pause one second."""
    import relay as relay_module

    attempts = iter(
        [500, 0, RuntimeError('delivery failed'), 1, asyncio.CancelledError()]
    )
    pauses = []

    async def publish():
        outcome = next(attempts)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def sleep(delay):
        pauses.append(delay)

    instance = AuthRelay(
        create_autospec(AsyncEngine, instance=True),
        create_autospec(Producer, instance=True),
    )
    monkeypatch.setattr(instance, 'publish_batch', publish)
    monkeypatch.setattr(relay_module.asyncio, 'sleep', sleep)
    with pytest.raises(asyncio.CancelledError):
        await instance.run()
    assert pauses == [1, 1]
