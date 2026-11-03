import asyncio
import json
import os
from pathlib import Path
from uuid import uuid4

import pytest
from bson import ObjectId
from pymongo.errors import OperationFailure

from tests.conftest import TEST_MONGO_DB
from tests.test_likes_bootstrap import run


def add_events(mongo, count=3):
    from models.documents import utc_now

    db = mongo[TEST_MONGO_DB]
    # This module's per-test outbox is isolated inside the session's UUID test database.
    db.ugc_outbox.delete_many({})
    rows = [
        dict(
            event_id=uuid4(),
            event_type='review.likes_updated',
            review_id=str(ObjectId()),
            user_id=uuid4(),
            likes_count=n,
            likes_version=n,
            load_id=None,
            created_at=utc_now(),
            published_at=None,
        )
        for n in range(count)
    ]
    db.ugc_outbox.insert_many(rows)
    return rows


class ControlledProducer:
    def __init__(self, failure=None, gate=None):
        self.failure = failure
        self.gate = gate
        self.sent = []

    async def send(self, topic, value, key):
        payload = json.loads(value)
        self.sent.append((topic, key, payload))

        async def acknowledge():
            if self.gate:
                await self.gate.wait()
            if payload['likes_count'] == self.failure:
                raise RuntimeError('broker rejected message')
            return object()

        return asyncio.create_task(acknowledge())


def test_partial_ack_marks_only_confirmed_and_retries_same_id(client, mongo):
    from relay import publish_batch

    rows = add_events(mongo)
    producer = ControlledProducer(failure=1)

    async def publish():
        return await publish_batch(producer=producer)

    assert run(client, publish) == 2
    stored = list(mongo[TEST_MONGO_DB].ugc_outbox.find({}).sort('likes_count', 1))
    assert [row['published_at'] is not None for row in stored] == [True, False, True]
    assert [sent[1].decode() for sent in producer.sent] == [row['review_id'] for row in rows]
    producer.failure = None
    assert run(client, publish) == 1
    assert producer.sent[-1][2]['event_id'] == str(rows[1]['event_id'])
    assert run(client, publish) == 0


def test_ack_before_commit_crash_replays_same_payload(client, mongo, monkeypatch):
    import relay

    add_events(mongo, 1)
    producer = ControlledProducer()
    original = relay.mark_published

    async def crash_after_ack(*args):
        await original(*args)
        raise RuntimeError('crash before commit')

    async def publish():
        return await relay.publish_batch(producer=producer)

    monkeypatch.setattr(relay, 'mark_published', crash_after_ack)
    with pytest.raises(RuntimeError, match='crash before commit'):
        run(client, publish)
    assert mongo[TEST_MONGO_DB].ugc_outbox.count_documents({'published_at': None}) == 1
    monkeypatch.setattr(relay, 'mark_published', original)
    assert run(client, publish) == 1
    assert producer.sent[0] == producer.sent[1]


def test_lock_excludes_second_relay_and_timeout_releases_it(client, mongo, monkeypatch):
    import relay

    add_events(mongo, 1)
    monkeypatch.setattr(relay.settings, 'relay_batch_timeout_seconds', 0.3)

    async def exercise():
        gate = asyncio.Event()
        owner = ControlledProducer(gate=gate)
        contender = ControlledProducer()
        first = asyncio.create_task(relay.publish_batch(producer=owner))
        while not owner.sent:
            await asyncio.sleep(0.005)
        assert mongo[TEST_MONGO_DB].ugc_outbox.count_documents({'published_at': None}) == 1
        with pytest.raises(OperationFailure):
            await relay.publish_batch(producer=contender)
        assert not contender.sent
        with pytest.raises(asyncio.TimeoutError):
            await first
        assert await relay.publish_batch(producer=contender) == 1
        assert owner.sent[0] == contender.sent[0]

    run(client, exercise)


@pytest.mark.parametrize('mode', ['normal', 'partial', 'crash', 'cli'])
def test_real_kafka_ack_key_and_json_contract(client, mongo, monkeypatch, mode):
    import jsonschema
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    import relay

    rows = add_events(mongo, 3)
    topic = f'ugc-test-{uuid4().hex}'
    monkeypatch.setattr(relay.settings, 'kafka_likes_topic', topic)
    brokers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:19094')

    async def exercise():
        producer = AIOKafkaProducer(bootstrap_servers=brokers, enable_idempotence=True)
        consumer = AIOKafkaConsumer(
            topic, bootstrap_servers=brokers, auto_offset_reset='earliest', enable_auto_commit=False
        )
        admin = AIOKafkaAdminClient(bootstrap_servers=brokers)
        await admin.start()
        try:
            await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
            try:
                await producer.start()
                await consumer.start()
                expected = 3
                if mode == 'cli':
                    import subprocess
                    import sys

                    service = Path(__file__).resolve().parents[1]
                    env = {
                        **os.environ,
                        'PYTHONPATH': str(service / 'src'),
                        'KAFKA_BOOTSTRAP_SERVERS': brokers,
                        'KAFKA_LIKES_TOPIC': topic,
                    }
                    result = await asyncio.to_thread(
                        subprocess.run,
                        [sys.executable, '-m', 'relay', '--once'],
                        env=env,
                        cwd=service,
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    assert result.returncode == 0, result.stderr
                elif mode == 'partial':

                    class RejectOne:
                        async def send(self, topic, value, key):
                            if json.loads(value)['likes_count'] == 1:
                                raise RuntimeError('injected per-message failure')
                            return await producer.send(topic, value, key=key)

                    assert await relay.publish_batch(producer=RejectOne()) == 2
                    assert (
                        mongo[TEST_MONGO_DB].ugc_outbox.count_documents({'published_at': None}) == 1
                    )
                    assert await relay.publish_batch(producer=producer) == 1
                elif mode == 'crash':
                    mark = relay.mark_published

                    async def crash(session, ids):
                        await mark(session, ids)
                        raise RuntimeError('crash after real Kafka ack')

                    monkeypatch.setattr(relay, 'mark_published', crash)
                    with pytest.raises(RuntimeError, match='after real Kafka ack'):
                        await relay.publish_batch(producer=producer)
                    assert (
                        mongo[TEST_MONGO_DB].ugc_outbox.count_documents({'published_at': None}) == 3
                    )
                    monkeypatch.setattr(relay, 'mark_published', mark)
                    assert await relay.publish_batch(producer=producer) == 3
                    expected = 6
                else:
                    assert await relay.publish_batch(producer=producer) == 3
                return [await asyncio.wait_for(consumer.getone(), 15) for _ in range(expected)]
            finally:
                await consumer.stop()
                await producer.stop()
                await admin.delete_topics([topic])
        finally:
            await admin.close()

    records = run(client, exercise)
    schema = json.loads(
        (
            Path(__file__).resolve().parents[3] / 'contracts/notifications/ugc.schema.json'
        ).read_text()
    )
    assert {json.loads(record.value)['event_id'] for record in records} == {
        str(row['event_id']) for row in rows
    }
    for record in records:
        value = json.loads(record.value)
        assert record.key.decode() == value['review_id']
        jsonschema.validate(value, schema, format_checker=jsonschema.FormatChecker())
    assert mongo[TEST_MONGO_DB].ugc_outbox.count_documents({'published_at': None}) == 0
    if mode == 'crash':
        assert [record.value for record in records[:3]] == [record.value for record in records[3:]]


def test_relay_batch_size_comes_from_settings(client, mongo, monkeypatch):
    import relay
    from core.config import Settings

    monkeypatch.setenv('RELAY_BATCH_SIZE', '2')
    # Required mongo_uri is supplied by the test environment.
    settings = Settings()  # pyright: ignore[reportCallIssue]
    monkeypatch.setattr(relay, 'settings', settings)
    add_events(mongo, 3)
    producer = ControlledProducer()

    async def publish():
        return await relay.publish_batch(producer=producer)

    assert run(client, publish) == 2
    assert mongo[TEST_MONGO_DB].ugc_outbox.count_documents({'published_at': None}) == 1
    assert run(client, publish) == 1
