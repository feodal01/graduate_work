import asyncio
import logging

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, MessageSizeTooLargeError

from services.event_broker import (
    EventBrokerUnavailableError,
    EventEnvelopeTooLargeError,
    PublishedEvent,
)

logger = logging.getLogger(__name__)


class AIOKafkaEventBroker:
    def __init__(self, producer: AIOKafkaProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def publish_batch(self, records: list[PublishedEvent]) -> None:
        try:
            await asyncio.gather(
                *[
                    self._producer.send_and_wait(
                        self._topic,
                        value=record.value,
                        key=record.key.encode('utf-8'),
                    )
                    for record in records
                ]
            )
        except MessageSizeTooLargeError as exc:
            logger.exception('Kafka message exceeds max_request_size')
            raise EventEnvelopeTooLargeError from exc
        except KafkaError as exc:
            logger.exception('Kafka publish failed')
            raise EventBrokerUnavailableError from exc

    async def ping(self) -> bool:
        try:
            updated = await asyncio.wait_for(
                self._producer.client.force_metadata_update(),
                timeout=2,
            )
            if not updated:
                return False
            partitions = self._producer.client.cluster.partitions_for_topic(self._topic)
        except (KafkaError, TimeoutError):
            logger.exception('Kafka metadata lookup failed')
            return False
        return partitions is not None and len(partitions) > 0
