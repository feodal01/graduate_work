from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from db.backoff_policy import POSTGRES_RETRYABLE, on_postgres_exception
from db.sql.errors import DatabaseUnavailableError
from models import User
from repositories.errors import LoginAlreadyTakenError
from services.profile_events import (
    apply_contact_changes,
    create_profile_event,
    delete_profile,
    lock_profile,
)
from validation.timezones import validate_timezone


class UserRepository(Protocol):
    async def create(
        self,
        *,
        login: str,
        password_hash: str | None,
        first_name: str | None,
        last_name: str | None,
        is_superuser: bool = False,
        email: str | None = None,
        locale: str | None = None,
        tz: str,
    ) -> User: ...

    async def get_by_login(self, login: str) -> User | None: ...

    async def get_id_by_login(self, login: str) -> UUID | None: ...

    async def get_by_id(self, user_id: UUID) -> User | None: ...

    async def promote_to_superuser(self, user: User) -> User: ...

    async def update(
        self,
        user: User,
        *,
        login: str | None,
        password_hash: str | None,
        contacts: dict[str, str | None] | None = None,
    ) -> User: ...

    async def delete(self, user_id: UUID) -> None: ...


class SQLAlchemyUserRepository:
    """PostgreSQL-репозиторий пользователей."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        *,
        login: str,
        password_hash: str | None,
        first_name: str | None,
        last_name: str | None,
        is_superuser: bool = False,
        email: str | None = None,
        locale: str | None = None,
        tz: str,
    ) -> User:
        tz = validate_timezone(tz)
        user = User(
            login=login,
            password_hash=password_hash,
            first_name=first_name,
            last_name=last_name,
            is_superuser=is_superuser,
            email=email,
            locale=locale,
            tz=tz,
        )
        self._session.add(user)
        try:
            await create_profile_event(self._session, user)
            await self._session.commit()
            await self._session.refresh(user)
        except IntegrityError as exc:
            await self._session.rollback()
            if getattr(exc.orig, 'sqlstate', None) == '23505':
                raise LoginAlreadyTakenError from exc
            raise DatabaseUnavailableError from exc
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc
        except BaseException:
            await self._session.rollback()
            raise
        return user

    async def promote_to_superuser(self, user: User) -> User:
        user.is_superuser = True
        try:
            await self._session.commit()
            await self._session.refresh(user)
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc
        return user

    async def get_by_login(self, login: str) -> User | None:
        try:
            return await self._get_by_login(login)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc

    @on_postgres_exception()
    async def _get_by_login(self, login: str) -> User | None:
        try:
            result = await self._session.execute(
                select(User).where(User.login == login)
            )
        except POSTGRES_RETRYABLE:
            await self._session.rollback()
            raise
        return result.scalar_one_or_none()

    async def get_id_by_login(self, login: str) -> UUID | None:
        try:
            return await self._get_id_by_login(login)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc

    @on_postgres_exception()
    async def _get_id_by_login(self, login: str) -> UUID | None:
        try:
            result = await self._session.execute(
                select(User.id).where(User.login == login)
            )
        except POSTGRES_RETRYABLE:
            await self._session.rollback()
            raise
        return result.scalar_one_or_none()

    async def get_by_id(self, user_id: UUID) -> User | None:
        try:
            return await self._get_by_id(user_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc

    @on_postgres_exception()
    async def _get_by_id(self, user_id: UUID) -> User | None:
        try:
            return await self._session.get(User, user_id)
        except POSTGRES_RETRYABLE:
            await self._session.rollback()
            raise

    async def update(
        self,
        user: User,
        *,
        login: str | None,
        password_hash: str | None,
        contacts: dict[str, str | None] | None = None,
    ) -> User:
        try:
            user = await lock_profile(self._session, user.id)
            if login is not None:
                user.login = login
            if password_hash is not None:
                user.password_hash = password_hash
            await apply_contact_changes(self._session, user, contacts or {})
            await self._session.commit()
            await self._session.refresh(user)
        except IntegrityError as exc:
            await self._session.rollback()
            if getattr(exc.orig, 'sqlstate', None) == '23505':
                raise LoginAlreadyTakenError from exc
            raise DatabaseUnavailableError from exc
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc
        except BaseException:
            await self._session.rollback()
            raise
        return user

    async def delete(self, user_id: UUID) -> None:
        try:
            await delete_profile(self._session, user_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc
