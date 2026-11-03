import asyncio
from types import SimpleNamespace
from typing import Optional
from unittest.mock import AsyncMock, Mock

import pytest
from aiokafka.errors import KafkaConnectionError, MessageSizeTooLargeError

from services.event_broker import EventEnvelopeTooLargeError, PublishedEvent
from services.kafka_broker import AIOKafkaEventBroker


class _ProducerStub:
    def __init__(self, partitions: Optional[set[int]]) -> None:
        self._partitions = partitions
        self.client = SimpleNamespace(
            force_metadata_update=AsyncMock(return_value=True),
            cluster=SimpleNamespace(partitions_for_topic=Mock(return_value=partitions)),
        )

    async def partitions_for(self, topic: str) -> Optional[set[int]]:
        del topic
        return self._partitions


async def test_ping_true_when_topic_has_partitions() -> None:
    broker = AIOKafkaEventBroker(_ProducerStub({0, 1}), topic='ugc.events')

    assert await broker.ping() is True


@pytest.mark.parametrize('partitions', [None, set()])
async def test_ping_false_when_topic_missing(partitions: Optional[set[int]]) -> None:
    broker = AIOKafkaEventBroker(_ProducerStub(partitions), topic='ugc.events')

    assert await broker.ping() is False


async def test_ping_false_when_metadata_update_fails_with_cached_partitions() -> None:
    producer = _ProducerStub({0, 1})
    producer.client.force_metadata_update.return_value = False
    broker = AIOKafkaEventBroker(producer, topic='ugc.events')

    assert await broker.ping() is False


@pytest.mark.parametrize('error', [KafkaConnectionError('unavailable'), TimeoutError()])
async def test_ping_false_when_metadata_update_raises(error: Exception) -> None:
    producer = _ProducerStub({0, 1})
    producer.client.force_metadata_update.side_effect = error
    broker = AIOKafkaEventBroker(producer, topic='ugc.events')

    assert await broker.ping() is False


async def test_ping_uses_partitions_from_refreshed_metadata() -> None:
    producer = _ProducerStub({0, 1})

    async def remove_topic() -> bool:
        producer.client.cluster.partitions_for_topic.return_value = None
        return True

    producer.client.force_metadata_update.side_effect = remove_topic
    broker = AIOKafkaEventBroker(producer, topic='ugc.events')

    assert await broker.ping() is False


async def test_ping_times_out_when_metadata_update_hangs() -> None:
    producer = _ProducerStub({0, 1})
    producer.client.force_metadata_update.side_effect = asyncio.Event().wait
    broker = AIOKafkaEventBroker(producer, topic='ugc.events')

    assert await asyncio.wait_for(broker.ping(), timeout=3) is False


async def test_ping_propagates_cancellation() -> None:
    producer = _ProducerStub({0, 1})
    producer.client.force_metadata_update.side_effect = asyncio.CancelledError
    broker = AIOKafkaEventBroker(producer, topic='ugc.events')

    with pytest.raises(asyncio.CancelledError):
        await broker.ping()


class _RejectingProducer:
    async def send_and_wait(self, topic: str, value: bytes, key: bytes) -> None:
        del topic, value, key
        raise MessageSizeTooLargeError('too large')


async def test_publish_batch_maps_message_too_large_to_envelope_error() -> None:
    broker = AIOKafkaEventBroker(_RejectingProducer(), topic='ugc.events')

    with pytest.raises(EventEnvelopeTooLargeError):
        await broker.publish_batch([PublishedEvent(key='user', value=b'{"event":1}')])
