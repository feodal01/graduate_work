import asyncio
import time
from collections import deque
from typing import Optional, Self

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.structs import ConsumerRecord

from core.config import settings
from services.contracts import ConsumedRecord


class KafkaEventConsumer:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._topic = settings.kafka_topic
        self._dlq_topic = settings.kafka_dlq_topic
        self._batch_size = settings.etl_batch_size
        self._batch_max_bytes = settings.etl_batch_max_bytes
        self._flush_interval_seconds = settings.etl_flush_interval_seconds
        self._leftover: deque[ConsumedRecord] = deque()

    @classmethod
    async def start(cls) -> Self:
        servers = settings.kafka_bootstrap_servers.split(',')
        consumer = AIOKafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=servers,
            group_id=settings.kafka_group_id,
            client_id=settings.project_name,
            enable_auto_commit=False,
            auto_offset_reset='earliest',
            max_poll_records=settings.etl_batch_size,
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=servers,
            client_id=settings.project_name,
            acks='all',
        )
        await producer.start()
        try:
            await consumer.start()
        except BaseException:
            await producer.stop()
            raise
        return cls(consumer, producer)

    def _consumed(self, message: ConsumerRecord[bytes, bytes]) -> ConsumedRecord:
        return ConsumedRecord(
            value=message.value or b'',
            key=message.key,
            topic=message.topic,
            partition=message.partition,
            offset=message.offset,
        )

    def _take_from_leftover(self) -> list[ConsumedRecord]:
        records: list[ConsumedRecord] = []
        total_bytes = 0
        while self._leftover:
            if len(records) >= self._batch_size:
                break
            nxt = self._leftover[0]
            size = len(nxt.value)
            if records and total_bytes + size > self._batch_max_bytes:
                break
            self._leftover.popleft()
            records.append(nxt)
            total_bytes += size
        return records

    async def poll_batch(self) -> list[ConsumedRecord]:
        if self._leftover:
            return self._take_from_leftover()
        deadline = time.monotonic() + self._flush_interval_seconds
        while True:
            remaining_ms = max(0, int((deadline - time.monotonic()) * 1000))
            fetched = await self._consumer.getmany(
                timeout_ms=remaining_ms,
                max_records=self._batch_size,
            )
            for messages in fetched.values():
                for message in messages:
                    self._leftover.append(self._consumed(message))
            if self._leftover:
                return self._take_from_leftover()
            if remaining_ms == 0:
                return []

    async def commit(self, records: list[ConsumedRecord]) -> None:
        if not records:
            return
        offsets: dict[TopicPartition, int] = {}
        for record in records:
            offsets[TopicPartition(record.topic, record.partition)] = record.offset + 1
        await self._consumer.commit(offsets)

    async def send_dlq(self, value: bytes, key: Optional[bytes]) -> None:
        await self._producer.send_and_wait(self._dlq_topic, value=value, key=key)

    async def ping(self) -> bool:
        try:
            updated = await asyncio.wait_for(
                self._producer.client.force_metadata_update(),
                timeout=2,
            )
            if not updated:
                return False
            partitions = self._producer.client.cluster.partitions_for_topic(self._topic)
        except Exception:  # pylint: disable=broad-exception-caught
            return False
        return partitions is not None and len(partitions) > 0

    async def stop(self) -> None:
        await self._consumer.stop()
        await self._producer.stop()
