from dataclasses import dataclass
from typing import Protocol


class EventBrokerUnavailableError(Exception):
    """Kafka недоступна или не подтвердила запись."""


class EventEnvelopeTooLargeError(Exception):
    """Сериализованная запись больше max_request_size продюсера."""


@dataclass(frozen=True)
class PublishedEvent:
    key: str
    value: bytes


class EventBroker(Protocol):
    async def publish_batch(self, records: list[PublishedEvent]) -> None: ...

    async def ping(self) -> bool: ...
