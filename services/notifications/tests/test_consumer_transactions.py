"""Atomic ingest against PostgreSQL and Kafka, including abrupt process death."""

import asyncio
import json
import multiprocessing
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.structs import TopicPartition
from contact_fixtures import contact_profile
from sqlalchemy import func, select

from notifications.contracts import AuthEvent, SourcePosition
from notifications.db.models import Delivery, KafkaOffset, ProcessedEvent, Profile
from notifications.db.session import Database
from notifications.ingest.consumer import KafkaIngestConsumer
from notifications.ingest.transactions import (
    AtomicIngest,
    OwnershipLost,
    PositionError,
    RetentionError,
)


def event():
    return AuthEvent(
        event_id=uuid4(),
        event_type="user.registered",
        user_id=uuid4(),
        user_version=1,
        tz=None,
    )


async def apply_profile(session, value):
    session.add(
        contact_profile(
            user_id=value.user_id,
            user_version=value.user_version,
            likes=True,
            new_films=True,
            marketing=False,
        )
    )
    await session.flush()
    session.add(
        Delivery(
            user_id=value.user_id,
            kind="welcome",
            logical_key=f"welcome:{value.user_id}",
        )
    )
    await session.flush()


async def counts(db):
    async with db.transaction() as session:
        return [
            await session.scalar(select(func.count()).select_from(model))
            for model in (ProcessedEvent, Profile, Delivery)
        ]


async def offset(db, topic="test"):
    async with db.transaction() as session:
        return await session.scalar(
            select(KafkaOffset.next_offset).where(KafkaOffset.topic == topic)
        )


async def test_atomic_rollback_retry_and_duplicate_on_next_offset(database_url):
    db = Database(database_url)
    failed = True

    async def handler(session, value):
        await apply_profile(session, value)
        assert await session.get(ProcessedEvent, value.event_id) is not None
        if failed:
            raise RuntimeError("after all writes")

    ingest = AtomicIngest(db, handler)
    await ingest.claim("test", 0, earliest=0, latest=10)
    value = event()
    try:
        with pytest.raises(RuntimeError, match="after all writes"):
            await ingest.handle_record(
                value, SourcePosition(topic="test", partition=0, next_offset=1)
            )
        assert await counts(db) == [0, 0, 0]
        assert await offset(db) == 0
        failed = False
        await ingest.handle_record(value, SourcePosition(topic="test", partition=0, next_offset=1))
        await ingest.handle_record(value, SourcePosition(topic="test", partition=0, next_offset=2))
        assert await counts(db) == [1, 1, 1]
        assert await offset(db) == 2
    finally:
        await db.close()


async def test_ownership_waits_for_transaction_then_fences_old_owner(database_url):
    db = Database(database_url)
    entered, finish = asyncio.Event(), asyncio.Event()

    async def blocked_handler(session, value):
        await apply_profile(session, value)
        entered.set()
        await finish.wait()

    first, second = AtomicIngest(db, blocked_handler), AtomicIngest(db, apply_profile)
    await first.claim("test", 0, earliest=0, latest=10)
    applying = asyncio.create_task(
        first.handle_record(event(), SourcePosition(topic="test", partition=0, next_offset=1))
    )
    await asyncio.wait_for(entered.wait(), 5)
    claiming = asyncio.create_task(second.claim("test", 0, earliest=0, latest=10))
    await asyncio.sleep(0.1)
    assert not claiming.done()
    finish.set()
    await applying
    assert await claiming == 1
    with pytest.raises(OwnershipLost):
        await first.handle_record(event(), SourcePosition(topic="test", partition=0, next_offset=2))
    with pytest.raises(PositionError):
        await second.handle_record(
            event(), SourcePosition(topic="test", partition=0, next_offset=3)
        )
    await second.handle_record(event(), SourcePosition(topic="test", partition=0, next_offset=2))
    assert await counts(db) == [2, 2, 2]
    await db.close()


async def test_cancellation_rolls_back_and_releases_owner(database_url):
    db = Database(database_url)
    entered = asyncio.Event()

    async def blocked(session, value):
        await apply_profile(session, value)
        entered.set()
        await asyncio.Event().wait()

    ingest = AtomicIngest(db, blocked)
    await ingest.claim("test", 0, earliest=0, latest=10)
    task = asyncio.create_task(
        ingest.handle_record(event(), SourcePosition(topic="test", partition=0, next_offset=1))
    )
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await ingest.release("test", 0)
    assert await counts(db) == [0, 0, 0]
    assert await offset(db) == 0
    await db.close()


async def test_earliest_and_out_of_retention_never_reset(database_url):
    db = Database(database_url)
    ingest = AtomicIngest(db, apply_profile)
    assert await ingest.claim("test", 0, earliest=5, latest=10) == 5
    await ingest.release("test", 0)
    for earliest, latest in [(6, 10), (0, 4)]:
        with pytest.raises(RetentionError, match="test"):
            await ingest.claim("test", 0, earliest=earliest, latest=latest)
        assert await offset(db) == 5
    await db.close()


def crash_worker(url, payload, committed, pipe):
    async def run():
        db = Database(url)

        async def handler(session, value):
            await apply_profile(session, value)
            if not committed:
                pipe.send("before")
                await asyncio.Event().wait()

        ingest = AtomicIngest(db, handler)
        await ingest.claim("test", 0, earliest=0, latest=10)
        await ingest.handle_record(
            AuthEvent.model_validate(payload),
            SourcePosition(topic="test", partition=0, next_offset=1),
        )
        pipe.send("after")
        await asyncio.Event().wait()

    asyncio.run(run())


@pytest.mark.parametrize("committed", [False, True])
async def test_process_killed_before_or_after_commit(database_url, committed):
    value = event()
    context = multiprocessing.get_context("spawn")
    reader, writer = context.Pipe(duplex=False)
    process = context.Process(
        target=crash_worker, args=(database_url, value.model_dump(mode="json"), committed, writer)
    )
    process.start()
    try:
        assert await asyncio.to_thread(reader.poll, 15)
        assert reader.recv() == ("after" if committed else "before")
    finally:
        process.kill()
        await asyncio.to_thread(process.join, 10)
        reader.close()
        writer.close()
    db = Database(database_url)
    ingest = AtomicIngest(db, apply_profile)
    start = await ingest.claim("test", 0, earliest=0, latest=10)
    assert start == int(committed)
    assert await counts(db) == ([1, 1, 1] if committed else [0, 0, 0])
    if not committed:
        await ingest.handle_record(value, SourcePosition(topic="test", partition=0, next_offset=1))
    assert await counts(db) == [1, 1, 1]
    await db.close()


@pytest.fixture
async def kafka_topic():
    root = Path(__file__).resolve().parents[3]
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            "notifications-integration",
            "-f",
            str(root / "tests/notifications/compose.yaml"),
            "port",
            "kafka",
            os.environ.get("NOTIFICATIONS_KAFKA_PORT", "19094"),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    server = result.stdout.strip()
    assert server.startswith("127.0.0.1:")
    topic = "notifications_test_" + uuid4().hex
    admin = AIOKafkaAdminClient(bootstrap_servers=server)
    await admin.start()
    await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    try:
        yield server, topic
    finally:
        await admin.delete_topics([topic])
        await admin.close()


async def produce(server, topic, *values):
    producer = AIOKafkaProducer(bootstrap_servers=server)
    await producer.start()
    try:
        for value in values:
            await producer.send_and_wait(
                topic, value=value, key=b"deleted-key" if value is None else None, partition=0
            )
    finally:
        await producer.stop()


async def wait_until(predicate):
    async with asyncio.timeout(20):
        while not await predicate():
            await asyncio.sleep(0.05)


async def wait_for_paused(consumer, task, partition):
    async def paused_or_stopped():
        return task.done() or partition in consumer.client.paused()

    await wait_until(paused_or_stopped)
    assert not task.done(), "An invalid record stopped the entire consumer"


@pytest.mark.parametrize(
    "invalid",
    [b'{"broken":true}', b"not-json", b"\xff", None],
    ids=["schema", "json", "utf8", "tombstone"],
)
async def test_real_kafka_invalid_payload_pauses_without_advancing(
    database_url, kafka_topic, invalid
):
    server, topic = kafka_topic
    value = event()
    await produce(
        server,
        topic,
        value.model_dump_json().encode(),
        invalid,
        event().model_dump_json().encode(),
    )
    db = Database(database_url)
    consumer = KafkaIngestConsumer(
        db,
        apply_profile,
        bootstrap_servers=server,
        group_id=uuid4().hex,
        topic_models={topic: AuthEvent},
    )
    task = asyncio.create_task(consumer.run())
    try:
        tp = TopicPartition(topic, 0)
        await wait_for_paused(consumer, task, tp)
        assert await counts(db) == [1, 1, 1]
        assert await offset(db, topic) == 1
        assert consumer.client.paused() == {tp}
        assert await consumer.client.position(tp) == 1
        assert await consumer.client.committed(tp) is None
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await db.close()


async def test_real_kafka_rebalance_cancels_transaction_then_pg_seek(database_url, kafka_topic):
    server, topic = kafka_topic
    value = event()
    await produce(server, topic, value.model_dump_json().encode())
    db = Database(database_url)
    entered = asyncio.Event()

    async def blocked(session, record):
        await apply_profile(session, record)
        entered.set()
        await asyncio.Event().wait()

    group = uuid4().hex
    first = KafkaIngestConsumer(
        db, blocked, bootstrap_servers=server, group_id=group, topic_models={topic: AuthEvent}
    )
    second = KafkaIngestConsumer(
        db, apply_profile, bootstrap_servers=server, group_id=group, topic_models={topic: AuthEvent}
    )
    task1 = asyncio.create_task(first.run())
    task2 = None
    try:
        await asyncio.wait_for(entered.wait(), 20)
        # Joining the same real group revokes the first assignment. Either member
        # may win the one partition; stop first after observing its revocation.
        previous_generation = first.assignment_generation
        task2 = asyncio.create_task(second.run())

        async def rebalanced():
            return first.assignment_generation > previous_generation

        await wait_until(rebalanced)
        task1.cancel()
        await asyncio.gather(task1, return_exceptions=True)

        async def applied():
            return await offset(db, topic) == 1

        await wait_until(applied)
        assert await counts(db) == [1, 1, 1]
        tp = TopicPartition(topic, 0)
        assert isinstance(second.client, AIOKafkaConsumer)
        assert await second.client.committed(tp) is None
    finally:
        task1.cancel()
        if task2 is not None:
            task2.cancel()
        await asyncio.gather(*[task for task in (task1, task2) if task], return_exceptions=True)
        await db.close()


async def test_real_kafka_aborted_transactions_and_control_offset_gaps(database_url, kafka_topic):
    server, topic = kafka_topic
    producer = AIOKafkaProducer(bootstrap_servers=server, transactional_id=uuid4().hex)
    await producer.start()
    try:
        await producer.begin_transaction()
        await producer.send_and_wait(topic, event().model_dump_json().encode(), partition=0)
        await producer.abort_transaction()
        async with producer.transaction():
            await producer.send_and_wait(topic, event().model_dump_json().encode(), partition=0)
    finally:
        await producer.stop()
    # This last ordinary record crosses the preceding transaction's control marker.
    await produce(server, topic, event().model_dump_json().encode(), b"invalid")
    db = Database(database_url)
    consumer = KafkaIngestConsumer(
        db,
        apply_profile,
        bootstrap_servers=server,
        group_id=uuid4().hex,
        topic_models={topic: AuthEvent},
    )
    task = asyncio.create_task(consumer.run())
    try:
        await wait_for_paused(consumer, task, TopicPartition(topic, 0))
        assert await counts(db) == [2, 2, 2]
        assert await offset(db, topic) == 5
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await db.close()


async def test_real_kafka_deleted_prefix_rejects_saved_offset(database_url, kafka_topic):
    server, topic = kafka_topic
    await produce(
        server, topic, event().model_dump_json().encode(), event().model_dump_json().encode()
    )
    db = Database(database_url)
    seed = AtomicIngest(db, apply_profile)
    await seed.claim(topic, 0, earliest=0, latest=2)
    await seed.release(topic, 0)
    root = Path(__file__).resolve().parents[3]
    subprocess.run(
        [
            "docker",
            "compose",
            "-p",
            "notifications-integration",
            "-f",
            str(root / "tests/notifications/compose.yaml"),
            "exec",
            "-T",
            "kafka",
            "kafka-delete-records.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--offset-json-file",
            "/dev/stdin",
        ],
        input=json.dumps(
            {"partitions": [{"topic": topic, "partition": 0, "offset": 1}], "version": 1}
        ),
        text=True,
        capture_output=True,
        check=True,
    )
    consumer = KafkaIngestConsumer(
        db,
        apply_profile,
        bootstrap_servers=server,
        group_id=uuid4().hex,
        topic_models={topic: AuthEvent},
    )
    try:
        with pytest.raises(RetentionError, match="outside Kafka range"):
            await asyncio.wait_for(consumer.run(), 20)
        assert await counts(db) == [0, 0, 0]
        assert await offset(db, topic) == 0
    finally:
        await db.close()


async def test_real_kafka_handler_cancellation_never_skips_record(database_url, kafka_topic):
    server, topic = kafka_topic
    await produce(
        server, topic, event().model_dump_json().encode(), event().model_dump_json().encode()
    )
    db = Database(database_url)

    async def cancelled(session, value):
        await apply_profile(session, value)
        raise asyncio.CancelledError

    consumer = KafkaIngestConsumer(
        db,
        cancelled,
        bootstrap_servers=server,
        group_id=uuid4().hex,
        topic_models={topic: AuthEvent},
    )
    try:
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(consumer.run(), 20)
        assert await counts(db) == [0, 0, 0]
        assert await offset(db, topic) == 0
    finally:
        await db.close()
