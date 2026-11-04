from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager

from sqlalchemy.ext.asyncio import AsyncSession


class AbstractDatabase(ABC):
    @abstractmethod
    def session(self) -> AbstractAsyncContextManager[AsyncSession]: ...

    @abstractmethod
    async def close(self) -> None: ...
