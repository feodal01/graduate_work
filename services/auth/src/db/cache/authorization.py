"""Общая точка работы сервиса с кэшем прав.

Здесь используются три части:

1. Хранилище снимков запоминает уже вычисленные права пользователя.
2. Изменение пользователя временно запрещает доверять снимку этого
   пользователя и удаляет снимок после изменения его ролей.
3. Изменение ролей временно запрещает доверять всем снимкам и после успешного
   изменения публикует новую ревизию ролевых правил.

``AuthorizationCache`` объединяет эти части в один интерфейс. Сервис вызывает
методы этого интерфейса, а ``RedisAuthorizationCache`` передаёт вызов нужной
части. Если Redis недоступен, фасад преобразует его ошибку в
``CacheUnavailableError``. После этого ``AuthorizationService`` читает права
напрямую из PostgreSQL.
"""

import logging
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

from db.cache.authorization_mutations import (
    AuthorizationMutationInProgressError,
    RedisRoleAuthorizationMutation,
    RedisUserAuthorizationMutation,
)
from db.cache.authorization_snapshot import (
    AuthorizationKeyspace,
    AuthorizationRevisionUnavailableError,
    AuthorizationSnapshot,
    AuthorizationSnapshotReadResult,
    RedisAuthorizationSnapshotStore,
)
from db.cache.errors import CacheUnavailableError

logger = logging.getLogger(__name__)


class AuthorizationCache(Protocol):
    """Контракт кэша уже вычисленных прав пользователей."""

    async def read_snapshot(
        self,
        user_id: UUID,
    ) -> AuthorizationSnapshotReadResult:
        """Прочитать снимок и ревизию ролей на момент чтения."""
        ...

    async def store_snapshot(
        self,
        user_id: UUID,
        snapshot: AuthorizationSnapshot,
        expected_roles_revision: str,
        *,
        expected_user_generation: str,
    ) -> None:
        """Сохранить снимок, если ревизия и поколение с момента чтения те же."""
        ...

    def user_mutation(
        self,
        user_id: UUID,
    ) -> AbstractAsyncContextManager[None]:
        """Не использовать снимок пользователя во время изменения его прав."""
        ...

    def roles_mutation(self) -> AbstractAsyncContextManager[None]:
        """Не использовать снимки во время изменения общих ролевых прав."""
        ...


class RedisAuthorizationCache:
    """Единый фасад над снимками и двумя видами инвалидации.

    Внутренние компоненты работают с Redis напрямую. Фасад скрывает их от
    сервисного слоя и переводит инфраструктурные ошибки в CacheUnavailableError.
    """

    def __init__(
        self,
        redis: Redis,
        *,
        ttl_seconds: int,
        namespace: str = 'authz',
    ) -> None:
        keys = AuthorizationKeyspace.create(namespace)
        # Если очистка не сработает, маркер блокировки исчезнет сам. Он живёт
        # чуть дольше снимка, поэтому снимок успеет устареть раньше маркера.
        marker_ttl_seconds = ttl_seconds + 30
        self._snapshots = RedisAuthorizationSnapshotStore(
            redis,
            ttl_seconds=ttl_seconds,
            keys=keys,
        )
        self._user_mutations = RedisUserAuthorizationMutation(
            redis,
            marker_ttl_seconds=marker_ttl_seconds,
            keys=keys,
        )
        self._role_mutations = RedisRoleAuthorizationMutation(
            redis,
            marker_ttl_seconds=marker_ttl_seconds,
            keys=keys,
        )

    async def read_snapshot(
        self,
        user_id: UUID,
    ) -> AuthorizationSnapshotReadResult:
        try:
            return await self._snapshots.read_snapshot(user_id)
        except AuthorizationRevisionUnavailableError as exc:
            raise CacheUnavailableError(
                'Authorization revision is unavailable'
            ) from exc
        except RedisError as exc:
            logger.exception('Error reading authorization cache')
            raise CacheUnavailableError from exc

    async def store_snapshot(
        self,
        user_id: UUID,
        snapshot: AuthorizationSnapshot,
        expected_roles_revision: str,
        *,
        expected_user_generation: str,
    ) -> None:
        try:
            await self._snapshots.store_snapshot(
                user_id,
                snapshot,
                expected_roles_revision,
                expected_user_generation=expected_user_generation,
            )
        except RedisError as exc:
            logger.exception('Error writing authorization cache')
            raise CacheUnavailableError from exc

    @asynccontextmanager
    async def user_mutation(self, user_id: UUID) -> AsyncGenerator[None, None]:
        try:
            async with self._user_mutations.mutate(user_id):
                # Пока установлен маркер изменения пользователя, управление
                # передаётся сервису для выполнения операции в PostgreSQL.
                yield
        except AuthorizationMutationInProgressError as exc:
            raise CacheUnavailableError(
                'Authorization cache mutation is already in progress'
            ) from exc
        except RedisError as exc:
            logger.exception('Could not acquire authorization mutation marker')
            raise CacheUnavailableError from exc

    @asynccontextmanager
    async def roles_mutation(self) -> AsyncGenerator[None, None]:
        try:
            async with self._role_mutations.mutate():
                # Операция в PostgreSQL выполняется, пока установлен общий
                # маркер изменения ролевых правил.
                yield
        except AuthorizationMutationInProgressError as exc:
            raise CacheUnavailableError(
                'Authorization cache mutation is already in progress'
            ) from exc
        except RedisError as exc:
            logger.exception('Could not acquire authorization mutation marker')
            raise CacheUnavailableError from exc


__all__ = [
    'AuthorizationCache',
    'AuthorizationSnapshot',
    'AuthorizationSnapshotReadResult',
    'RedisAuthorizationCache',
]
