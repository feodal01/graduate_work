import builtins
from collections.abc import Collection
from typing import Protocol
from uuid import UUID

from sqlalchemy import delete, insert, select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from db.backoff_policy import POSTGRES_RETRYABLE, on_postgres_exception
from db.sql.errors import DatabaseUnavailableError
from models import Permission, Role, User, role_permissions, user_roles
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

    async def get_permissions_for_user(self, user_id: UUID) -> set[str]: ...


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

    async def get_permissions_for_user(self, user_id: UUID) -> set[str]:
        try:
            return await self._get_permissions_for_user(user_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError from exc

    @on_postgres_exception()
    async def _get_permissions_for_user(self, user_id: UUID) -> set[str]:
        try:
            result = await self._session.execute(
                select(role_permissions.c.permission_code)
                .join(
                    user_roles,
                    user_roles.c.role_id == role_permissions.c.role_id,
                )
                .where(user_roles.c.user_id == user_id)
            )
        except POSTGRES_RETRYABLE:
            await self._session.rollback()
            raise
        return set(result.scalars().all())

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
