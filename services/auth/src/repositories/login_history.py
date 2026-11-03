from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from db.backoff_policy import POSTGRES_RETRYABLE, on_postgres_exception
from db.sql.errors import DatabaseUnavailableError
from models import LoginHistory


class LoginHistoryRepository(Protocol):
    async def record(
        self,
        *,
        user_id: UUID,
        ip_address: str | None,
        user_agent: str | None,
    ) -> LoginHistory: ...

    async def list_for_user(self, user_id: UUID) -> list[LoginHistory]: ...


class SQLAlchemyLoginHistoryRepository:
    """PostgreSQL-репозиторий успешных входов."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def record(
        self,
        *,
        user_id: UUID,
        ip_address: str | None,
        user_agent: str | None,
    ) -> LoginHistory:
        history = LoginHistory(
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent,
        )
        self._session.add(history)
        try:
            await self._session.commit()
            await self._session.refresh(history)
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc
        return history

    async def list_for_user(self, user_id: UUID) -> list[LoginHistory]:
        try:
            return await self._list_for_user(user_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc

    @on_postgres_exception()
    async def _list_for_user(self, user_id: UUID) -> list[LoginHistory]:
        try:
            result = await self._session.execute(
                select(LoginHistory)
                .where(LoginHistory.user_id == user_id)
                .order_by(LoginHistory.logged_at.desc())
            )
        except POSTGRES_RETRYABLE:
            await self._session.rollback()
            raise
        return list(result.scalars().all())
