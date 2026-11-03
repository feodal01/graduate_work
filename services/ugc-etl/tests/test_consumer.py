from typing import Optional

from aiokafka import TopicPartition

from services.consumer import KafkaEventConsumer
from services.contracts import ConsumedRecord


class _FakeMessage:
    def __init__(
        self,
        value: bytes,
        offset: int,
        *,
        partition: int = 0,
        topic: str = 'ugc.events',
        key: Optional[bytes] = None,
    ) -> None:
        self.value = value
        self.offset = offset
        self.partition = partition
        self.topic = topic
        self.key = key


class _FakeKafkaConsumer:
    def __init__(self, batches: list[dict[TopicPartition, list[_FakeMessage]]]) -> None:
        self._batches = list(batches)
        self.getmany_calls = 0
        self.committed: Optional[dict[TopicPartition, int]] = None

    async def getmany(
        self,
        timeout_ms: int,
        max_records: int,
    ) -> dict[TopicPartition, list[_FakeMessage]]:
        del timeout_ms, max_records
        self.getmany_calls += 1
        if not self._batches:
            return {}
        return self._batches.pop(0)

    async def commit(self, offsets: Optional[dict[TopicPartition, int]] = None) -> None:
        self.committed = offsets


class _FakeProducer:
    pass


def _consumer(
    batches: list[dict[TopicPartition, list[_FakeMessage]]],
    *,
    batch_max_bytes: int,
) -> tuple[KafkaEventConsumer, _FakeKafkaConsumer]:
    kafka = _FakeKafkaConsumer(batches)
    wrapper = KafkaEventConsumer(kafka, _FakeProducer())  # type: ignore[arg-type]
    wrapper._batch_max_bytes = batch_max_bytes
    wrapper._batch_size = 100
    wrapper._flush_interval_seconds = 0
    return wrapper, kafka


async def test_byte_limit_keeps_unread_getmany_tail_for_next_poll() -> None:
    topic = 'ugc.events'
    tp = TopicPartition(topic, 0)
    messages = [
        _FakeMessage(b'aaaaaa', offset=10, key=b'k'),
        _FakeMessage(b'bbbbbb', offset=11, key=b'k'),
        _FakeMessage(b'cccccc', offset=12, key=b'k'),
    ]
    next_fetch = [_FakeMessage(b'dddddd', offset=99, key=b'k')]
    wrapper, kafka = _consumer([{tp: messages}, {tp: next_fetch}], batch_max_bytes=10)

    first = await wrapper.poll_batch()
    second = await wrapper.poll_batch()
    third = await wrapper.poll_batch()

    assert [record.offset for record in first] == [10]
    assert [record.offset for record in second] == [11]
    assert [record.offset for record in third] == [12]
    assert kafka.getmany_calls == 1

    fourth = await wrapper.poll_batch()
    assert [record.offset for record in fourth] == [99]
    assert kafka.getmany_calls == 2


async def test_first_leftover_record_is_taken_even_when_over_batch_budget() -> None:
    tp = TopicPartition('ugc.events', 0)
    huge = _FakeMessage(b'x' * 20, offset=1)
    small = _FakeMessage(b'y', offset=2)
    wrapper, kafka = _consumer([{tp: [huge, small]}], batch_max_bytes=10)

    first = await wrapper.poll_batch()
    second = await wrapper.poll_batch()

    assert [record.offset for record in first] == [1]
    assert len(first[0].value) == 20
    assert [record.offset for record in second] == [2]
    assert kafka.getmany_calls == 1


async def test_commit_uses_last_processed_offset_plus_one() -> None:
    tp0 = TopicPartition('ugc.events', 0)
    tp1 = TopicPartition('ugc.events', 1)
    wrapper, kafka = _consumer([], batch_max_bytes=100)

    await wrapper.commit(
        [
            ConsumedRecord(value=b'a', topic='ugc.events', partition=0, offset=10),
            ConsumedRecord(value=b'b', topic='ugc.events', partition=0, offset=11),
            ConsumedRecord(value=b'c', topic='ugc.events', partition=1, offset=4),
        ]
    )

    assert kafka.committed == {tp0: 12, tp1: 5}
