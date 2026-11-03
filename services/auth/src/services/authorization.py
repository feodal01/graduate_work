"""Проверка прав и управление ролями пользователей.

Основные данные механизма:

- PostgreSQL хранит пользователей, роли, права и назначения ролей. Эти данные
  считаются исходными.
- Действующие права пользователя — объединение прав всех назначенных ему ролей.
- Снимок содержит признак суперпользователя и уже вычисленные действующие
  права. Redis хранит снимок, чтобы не обращаться к PostgreSQL при каждой
  проверке.
- Ревизия ролей обозначает текущее состояние общих ролевых правил. Ревизия
  снимка должна совпадать с текущей ревизией.
- Маркер блокировки — временный ключ Redis. Пока права меняются, маркер
  запрещает использовать потенциально устаревший снимок.

Проверка прав происходит так:

1. Вызывающий код или политика определяет права, необходимые для действия.
2. Сервис запрашивает снимок в ``AuthorizationCache``.
3. Если кэш вернул подходящий снимок, сервис проверяет в нём признак
   суперпользователя и необходимые права.
4. Если снимка нет, он устарел или Redis недоступен, сервис читает пользователя
   и его действующие права из PostgreSQL.
5. После чтения PostgreSQL сервис пытается сохранить новый снимок. Запись
   выполняется только при прежней ревизии ролей и отсутствии маркеров.

Изменение прав происходит так:

1. При назначении или отзыве роли устанавливается маркер одного пользователя.
   После изменения в PostgreSQL удаляются его снимок и маркер.
2. При изменении прав роли или удалении роли устанавливается общий маркер.
   После изменения в PostgreSQL публикуется новая ревизия, поэтому все снимки
   с прежней ревизией перестают использоваться.
"""

import logging
from collections.abc import Collection
from uuid import UUID

from db.cache.authorization import AuthorizationCache, AuthorizationSnapshot
from db.cache.errors import CacheUnavailableError
from models import Role
from repositories.roles import RoleRepository
from repositories.users import UserRepository
from services.errors import InvalidAccessTokenError, PermissionDeniedError
from services.film_access import (
    FILM_EARLY_ACCESS_READ,
    FilmAccessContext,
    FilmAccessPolicy,
)
from services.types import CurrentUser

ROLES_MANAGE = 'roles:manage'
USER_ROLES_MANAGE = 'user_roles:manage'


logger = logging.getLogger(__name__)


class AuthorizationService:
    """Проверяет права и управляет ролями пользователей."""

    def __init__(
        self,
        *,
        users: UserRepository,
        roles: RoleRepository,
        authorization_cache: AuthorizationCache,
        film_policy: FilmAccessPolicy,
    ) -> None:
        self._users = users
        self._roles = roles
        self._authorization_cache = authorization_cache
        self._film_policy = film_policy

    async def can_read_film(
        self,
        current_user: CurrentUser | None,
        context: FilmAccessContext,
    ) -> bool:
        required_permissions = self._film_policy.required_permissions(context)
        if not required_permissions:
            return True
        if current_user is None:
            return False
        return await self._has_permissions(
            current_user,
            required_permissions,
        )

    async def has_permission(
        self,
        current_user: CurrentUser,
        permission: str,
    ) -> bool:
        return await self._has_permissions(current_user, (permission,))

    async def _has_permissions(
        self,
        current_user: CurrentUser,
        required_permissions: Collection[str],
    ) -> bool:
        snapshot = await self._authorization_snapshot(current_user.user_id)
        if snapshot.is_superuser:
            return True
        return set(required_permissions) <= snapshot.permissions

    async def _authorization_snapshot(
        self,
        user_id: UUID,
    ) -> AuthorizationSnapshot:
        # Сначала пробуем взять уже вычисленные права из Redis. Ревизия нужна,
        # чтобы позднее не записать результат, устаревший за время чтения.
        roles_revision_at_read: str | None = None
        try:
            cache_read = await self._authorization_cache.read_snapshot(user_id)
        except CacheUnavailableError:
            logger.warning(
                'Authorization cache unavailable; using database user_id=%s',
                user_id,
            )
        else:
            if cache_read.cached_snapshot is not None:
                return cache_read.cached_snapshot
            roles_revision_at_read = cache_read.roles_revision_at_read

        # Если подходящего снимка нет или Redis недоступен, читаем исходные
        # данные из PostgreSQL. Отсутствующий пользователь означает
        # недействительный токен.
        user = await self._users.get_by_id(user_id)
        if user is None:
            raise InvalidAccessTokenError
        permissions: frozenset[str] = (
            frozenset()
            if user.is_superuser
            else frozenset(await self._roles.get_permissions_for_user(user.id))
        )
        snapshot = AuthorizationSnapshot(
            is_superuser=user.is_superuser,
            permissions=permissions,
        )

        # Если Redis был доступен, пытаемся сохранить новый снимок. Lua-скрипт
        # отклонит запись, если во время чтения PostgreSQL права изменились.
        if roles_revision_at_read is not None:
            try:
                await self._authorization_cache.store_snapshot(
                    user_id,
                    snapshot,
                    roles_revision_at_read,
                )
            except CacheUnavailableError:
                logger.warning(
                    'Could not fill authorization cache user_id=%s',
                    user_id,
                )
        return snapshot

    async def list_roles(self, current_user: CurrentUser) -> list[Role]:
        await self._require_permission(current_user, ROLES_MANAGE)
        return await self._roles.list()

    async def create_role(
        self,
        current_user: CurrentUser,
        *,
        name: str,
        permission_codes: Collection[str],
    ) -> Role:
        await self._require_permission(current_user, ROLES_MANAGE)
        return await self._roles.create(
            name=name,
            permission_codes=permission_codes,
        )

    async def update_role(
        self,
        current_user: CurrentUser,
        role_id: UUID,
        *,
        name: str | None,
        permission_codes: Collection[str] | None,
    ) -> Role:
        await self._require_permission(current_user, ROLES_MANAGE)
        if permission_codes is None:
            # Переименование роли не меняет права и не требует инвалидации.
            return await self._roles.update(
                role_id,
                name=name,
                permission_codes=None,
            )
        # Изменение состава прав влияет на всех носителей роли, поэтому
        # после успешной операции меняется общая ревизия снимков.
        async with self._authorization_cache.roles_mutation():
            return await self._roles.update(
                role_id,
                name=name,
                permission_codes=permission_codes,
            )

    async def delete_role(
        self,
        current_user: CurrentUser,
        role_id: UUID,
    ) -> None:
        await self._require_permission(current_user, ROLES_MANAGE)
        # Удаление роли меняет итоговые права всех её пользователей.
        async with self._authorization_cache.roles_mutation():
            await self._roles.delete(role_id)

    async def assign_role(
        self,
        current_user: CurrentUser,
        user_id: UUID,
        role_id: UUID,
    ) -> None:
        await self._require_permission(current_user, USER_ROLES_MANAGE)
        # Назначение затрагивает только снимок указанного пользователя.
        async with self._authorization_cache.user_mutation(user_id):
            await self._roles.assign(user_id, role_id)

    async def revoke_role(
        self,
        current_user: CurrentUser,
        user_id: UUID,
        role_id: UUID,
    ) -> None:
        await self._require_permission(current_user, USER_ROLES_MANAGE)
        # Отзыв роли также требует удалить только один снимок.
        async with self._authorization_cache.user_mutation(user_id):
            await self._roles.revoke(user_id, role_id)

    async def _require_permission(
        self,
        current_user: CurrentUser,
        permission: str,
    ) -> None:
        if not await self.has_permission(current_user, permission):
            raise PermissionDeniedError


__all__ = [
    'FILM_EARLY_ACCESS_READ',
    'ROLES_MANAGE',
    'USER_ROLES_MANAGE',
    'AuthorizationService',
]
