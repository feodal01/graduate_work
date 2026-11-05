import builtins
from collections.abc import Collection
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import delete, insert, or_, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from db.backoff_policy import POSTGRES_RETRYABLE, on_postgres_exception
from db.sql.errors import DatabaseUnavailableError
from models import (
    Permission,
    Role,
    User,
    UserSubscriptionAccessState,
    role_permissions,
    user_roles,
)
from repositories.errors import (
    RoleAlreadyAssignedError,
    RoleAlreadyExistsError,
    RoleAssignmentNotFoundError,
    RoleNotFoundError,
    UnknownPermissionError,
    UserNotFoundError,
)


class RoleRepository(Protocol):
    async def list(self) -> list[Role]: ...

    async def create(
        self,
        *,
        name: str,
        permission_codes: Collection[str],
    ) -> Role: ...

    async def update(
        self,
        role_id: UUID,
        *,
        name: str | None,
        permission_codes: Collection[str] | None,
    ) -> Role: ...

    async def delete(self, role_id: UUID) -> None: ...

    async def assign(self, user_id: UUID, role_id: UUID) -> None: ...

    async def revoke(self, user_id: UUID, role_id: UUID) -> None: ...

    async def set_subscription_access(
        self,
        user_id: UUID,
        *,
        version: int,
        expires_at: datetime | None,
    ) -> tuple[bool, int]: ...

    async def get_permissions_for_user(self, user_id: UUID) -> set[str]: ...

    async def get_active_permissions_and_nearest_expiry(
        self,
        user_id: UUID,
    ) -> tuple[set[str], datetime | None]: ...


class SQLAlchemyRoleRepository:
    """PostgreSQL persistence for roles, permissions, and assignments."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def list(self) -> list[Role]:
        try:
            return await self._list()
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc

    @on_postgres_exception()
    async def _list(self) -> builtins.list[Role]:
        try:
            result = await self._session.execute(
                select(Role).options(selectinload(Role.permissions)).order_by(Role.name)
            )
        except POSTGRES_RETRYABLE:
            await self._session.rollback()
            raise
        return list(result.scalars().unique().all())

    async def create(
        self,
        *,
        name: str,
        permission_codes: Collection[str],
    ) -> Role:
        permissions = await self._resolve_permissions(permission_codes)
        role = Role(name=name, permissions=permissions)
        self._session.add(role)
        try:
            await self._session.commit()
            await self._session.refresh(role)
        except IntegrityError as exc:
            await self._session.rollback()
            raise RoleAlreadyExistsError from exc
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc
        return role

    async def update(
        self,
        role_id: UUID,
        *,
        name: str | None,
        permission_codes: Collection[str] | None,
    ) -> Role:
        role = await self._get_by_id(role_id)
        if role is None:
            raise RoleNotFoundError
        if name is not None:
            role.name = name
        if permission_codes is not None:
            role.permissions = await self._resolve_permissions(permission_codes)
        try:
            await self._session.commit()
            await self._session.refresh(role)
        except IntegrityError as exc:
            await self._session.rollback()
            raise RoleAlreadyExistsError from exc
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc
        return role

    async def delete(self, role_id: UUID) -> None:
        role = await self._get_by_id(role_id)
        if role is None:
            raise RoleNotFoundError
        try:
            await self._session.delete(role)
            await self._session.commit()
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc

    async def assign(self, user_id: UUID, role_id: UUID) -> None:
        await self._require_user_and_role(user_id, role_id)
        try:
            await self._session.execute(
                insert(user_roles).values(user_id=user_id, role_id=role_id)
            )
            await self._session.commit()
        except IntegrityError as exc:
            await self._session.rollback()
            raise RoleAlreadyAssignedError from exc
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc

    async def revoke(self, user_id: UUID, role_id: UUID) -> None:
        await self._require_user_and_role(user_id, role_id)
        try:
            result = await self._session.execute(
                delete(user_roles)
                .where(
                    user_roles.c.user_id == user_id,
                    user_roles.c.role_id == role_id,
                )
                .returning(user_roles.c.role_id)
            )
            if result.scalar_one_or_none() is None:
                await self._session.rollback()
                raise RoleAssignmentNotFoundError
            await self._session.commit()
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc

    async def set_subscription_access(
        self,
        user_id: UUID,
        *,
        version: int,
        expires_at: datetime | None,
    ) -> tuple[bool, int]:
        """Атомарно обновляет версию доступа и роль (AUTH-FR-01, AUTH-FR-02)."""
        try:
            await self._session.execute(
                text('SELECT pg_advisory_xact_lock(hashtext(:uid))'),
                {'uid': str(user_id)},
            )
            user = await self._session.get(User, user_id)
            if user is None:
                await self._session.rollback()
                raise UserNotFoundError

            subscriber_role = await self._session.scalar(
                select(Role).where(Role.name == 'subscriber')
            )
            if subscriber_role is None:
                await self._session.rollback()
                raise RoleNotFoundError

            state = await self._session.get(
                UserSubscriptionAccessState,
                user_id,
                with_for_update=True,
                populate_existing=True,
            )
            if state is not None and version <= state.access_version:
                current_version = int(state.access_version)
                await self._session.commit()
                return False, current_version

            now_utc = datetime.now(UTC)
            if state is None:
                self._session.add(
                    UserSubscriptionAccessState(
                        user_id=user_id,
                        access_version=version,
                        updated_at=now_utc,
                    )
                )
            else:
                state.access_version = version
                state.updated_at = now_utc

            if expires_at is not None:
                upsert_role = (
                    pg_insert(user_roles)
                    .values(
                        user_id=user_id,
                        role_id=subscriber_role.id,
                        expires_at=expires_at,
                    )
                    .on_conflict_do_update(
                        index_elements=[user_roles.c.user_id, user_roles.c.role_id],
                        set_={'expires_at': expires_at},
                    )
                )
                await self._session.execute(upsert_role)
            else:
                await self._session.execute(
                    delete(user_roles).where(
                        user_roles.c.user_id == user_id,
                        user_roles.c.role_id == subscriber_role.id,
                    )
                )

            await self._session.commit()
            return True, version
        except (UserNotFoundError, RoleNotFoundError):
            raise
        except (SQLAlchemyError, OSError) as exc:
            await self._session.rollback()
            raise DatabaseUnavailableError from exc

    async def get_permissions_for_user(self, user_id: UUID) -> set[str]:
        permissions, _ = await self.get_active_permissions_and_nearest_expiry(user_id)
        return permissions

    async def get_active_permissions_and_nearest_expiry(
        self,
        user_id: UUID,
    ) -> tuple[set[str], datetime | None]:
        """Возвращает активные права и ближайший expires_at (AUTH-FR-03)."""
        try:
            return await self._get_active_permissions_and_nearest_expiry(user_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc

    @on_postgres_exception()
    async def _get_active_permissions_and_nearest_expiry(
        self,
        user_id: UUID,
    ) -> tuple[set[str], datetime | None]:
        now_utc = datetime.now(UTC)
        try:
            result = await self._session.execute(
                select(
                    role_permissions.c.permission_code,
                    user_roles.c.expires_at,
                )
                .join(
                    user_roles,
                    user_roles.c.role_id == role_permissions.c.role_id,
                )
                .where(
                    user_roles.c.user_id == user_id,
                    or_(
                        user_roles.c.expires_at.is_(None),
                        user_roles.c.expires_at > now_utc,
                    ),
                )
            )
        except POSTGRES_RETRYABLE:
            await self._session.rollback()
            raise
        rows = result.all()
        permissions = {str(code) for code, _ in rows}
        expirations = [exp for _, exp in rows if exp is not None]
        nearest_expires_at = min(expirations) if expirations else None
        return permissions, nearest_expires_at

    async def _resolve_permissions(
        self,
        permission_codes: Collection[str],
    ) -> builtins.list[Permission]:
        codes = set(permission_codes)
        if not codes:
            return []
        try:
            result = await self._session.execute(
                select(Permission).where(Permission.code.in_(codes))
            )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc
        permissions = list(result.scalars().all())
        found = {permission.code for permission in permissions}
        if found != codes:
            raise UnknownPermissionError(', '.join(sorted(codes - found)))
        return permissions

    async def _get_by_id(self, role_id: UUID) -> Role | None:
        try:
            result = await self._session.execute(
                select(Role)
                .options(selectinload(Role.permissions))
                .where(Role.id == role_id)
            )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc
        return result.scalar_one_or_none()

    async def _require_user_and_role(
        self,
        user_id: UUID,
        role_id: UUID,
    ) -> None:
        try:
            user = await self._session.get(User, user_id)
            role = await self._session.get(Role, role_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc
        if user is None:
            raise UserNotFoundError
        if role is None:
            raise RoleNotFoundError
