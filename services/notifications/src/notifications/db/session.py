"""One SQL transaction per unit of work; no connection/session shared across tasks."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from functools import lru_cache

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from notifications.config import Settings


class Database:
    def __init__(self, url: str) -> None:
        self.engine = create_async_engine(url, pool_pre_ping=True)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[AsyncSession, None]:
        async with self.sessions.begin() as session:
            yield session

    async def close(self) -> None:
        await self.engine.dispose()


@lru_cache(maxsize=1)
def get_database() -> Database:
    return Database(Settings().database_url)  # type: ignore[call-arg]


@asynccontextmanager
async def transaction() -> AsyncGenerator[AsyncSession, None]:
    async with get_database().transaction() as session:
        yield session
