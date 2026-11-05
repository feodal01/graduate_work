from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager

from fastapi import Request
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from db.sql.base import AbstractDatabase


class PostgresDatabase(AbstractDatabase):
    def __init__(
        self,
        dsn: str,
        *,
        connect_timeout_seconds: float = 5.0,
        pool_timeout_seconds: float = 10.0,
        command_timeout_seconds: float = 15.0,
        statement_timeout_ms: int = 15_000,
        pool_size: int = 20,
        max_overflow: int = 30,
    ) -> None:
        self.engine = create_async_engine(
            dsn,
            echo=False,
            future=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout_seconds,
            connect_args={
                'timeout': connect_timeout_seconds,
                'command_timeout': command_timeout_seconds,
                'server_settings': {
                    'statement_timeout': str(statement_timeout_ms),
                    'synchronous_commit': 'on',
                },
            },
        )
        self._sessionmaker = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )

    @property
    def sessionmaker(self) -> async_sessionmaker[AsyncSession]:
        return self._sessionmaker

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        return self._sessionmaker()

    async def close(self) -> None:
        await self.engine.dispose()


async def get_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    database: AbstractDatabase = request.app.state.database
    async with database.session() as session:
        yield session
