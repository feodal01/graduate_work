from abc import ABC, abstractmethod


class AbstractCache(ABC):
    @abstractmethod
    async def get(self, key: str) -> bytes | str | None: ...

    @abstractmethod
    async def set(self, key: str, value: bytes | str, ttl: int) -> None: ...

    @abstractmethod
    async def ping(self) -> bool: ...
