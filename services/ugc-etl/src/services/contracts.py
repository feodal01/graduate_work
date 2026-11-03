from dataclasses import dataclass
from typing import Optional, Protocol, Union

from services.transform import EventRow


@dataclass(frozen=True)
class ConsumedRecord:
    value: bytes
    key: Optional[bytes] = None
    topic: str = ''
    partition: int = 0
    offset: int = 0


class EventConsumer(Protocol):
    async def poll_batch(self) -> list[ConsumedRecord]: ...

    async def commit(self, records: list[ConsumedRecord]) -> None: ...

    async def send_dlq(self, value: bytes, key: Union[bytes, None]) -> None: ...

    async def ping(self) -> bool: ...


class Warehouse(Protocol):
    async def ensure_schema(self) -> None: ...

    async def insert_batch(self, events: list[EventRow]) -> None: ...

    async def ping(self) -> bool: ...
