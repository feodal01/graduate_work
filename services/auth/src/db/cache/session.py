import logging
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Protocol
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

from db.backoff_policy import on_redis_exception
from db.cache.access_token_denylist import RedisAccessTokenDenylist
from db.cache.errors import CacheUnavailableError
from db.cache.refresh_tokens import RedisRefreshTokenStore
from db.cache.session_state import (
    RedisSessionStateStore,
    SessionKeyspace,
    SessionMutationFinalizationError,
    SessionMutationInProgressError,
)

logger = logging.getLogger(__name__)


class TokenSessionStore(Protocol):
    async def get_or_create_epoch(self, user_id: UUID) -> UUID: ...

    async def store_refresh(
        self,
        token_id: UUID,
        user_id: UUID,
        session_epoch: UUID,
        ttl: int,
    ) -> bool: ...

    async def rotate_refresh(
        self,
        *,
        old_token_id: UUID,
        new_token_id: UUID,
        user_id: UUID,
        session_epoch: UUID,
        ttl: int,
    ) -> bool: ...

    async def is_access_valid(
        self,
        *,
        user_id: UUID,
        token_id: UUID,
        session_epoch: UUID,
    ) -> bool: ...

    def password_change(
        self,
        user_id: UUID,
        expected_epoch: UUID,
    ) -> AbstractAsyncContextManager[None]: ...

    async def revoke_session(
        self,
        *,
        user_id: UUID,
        refresh_token_id: UUID | None,
        access_token_id: UUID,
        access_ttl: int,
    ) -> None: ...


class RedisTokenSessionStore:
    """Единая Redis-граница для epoch, refresh и отзыва access."""

    def __init__(
        self,
        redis: Redis,
        *,
        namespace: str = 'auth-session',
        marker_ttl_seconds: int = 30,
    ) -> None:
        self._redis = redis
        keys = SessionKeyspace(namespace)
        self._session_state = RedisSessionStateStore(
            redis,
            keys=keys,
            marker_ttl_seconds=marker_ttl_seconds,
        )
        self._refresh_tokens = RedisRefreshTokenStore(redis, keys=keys)
        self._access_denylist = RedisAccessTokenDenylist(keys=keys)

    async def get_or_create_epoch(self, user_id: UUID) -> UUID:
        try:
            epoch = await self._session_state.get_or_create_epoch(user_id)
        except RedisError as exc:
            logger.exception('Error reading session epoch user_id=%s', user_id)
            raise CacheUnavailableError from exc
        if epoch is None:
            raise CacheUnavailableError('Session state is changing or invalid')
        return epoch

    async def store_refresh(
        self,
        token_id: UUID,
        user_id: UUID,
        session_epoch: UUID,
        ttl: int,
    ) -> bool:
        try:
            return await self._refresh_tokens.store(
                token_id,
                user_id,
                session_epoch,
                ttl,
            )
        except RedisError as exc:
            logger.exception('Error storing refresh session')
            raise CacheUnavailableError from exc

    async def rotate_refresh(
        self,
        *,
        old_token_id: UUID,
        new_token_id: UUID,
        user_id: UUID,
        session_epoch: UUID,
        ttl: int,
    ) -> bool:
        try:
            return await self._refresh_tokens.rotate(
                old_token_id=old_token_id,
                new_token_id=new_token_id,
                user_id=user_id,
                session_epoch=session_epoch,
                ttl=ttl,
            )
        except RedisError as exc:
            logger.exception('Error rotating refresh session')
            raise CacheUnavailableError from exc

    async def is_access_valid(
        self,
        *,
        user_id: UUID,
        token_id: UUID,
        session_epoch: UUID,
    ) -> bool:
        try:
            return await self._session_state.is_access_valid(
                user_id=user_id,
                token_id=token_id,
                session_epoch=session_epoch,
            )
        except RedisError as exc:
            logger.exception('Error validating access session')
            raise CacheUnavailableError from exc

    @asynccontextmanager
    async def password_change(
        self,
        user_id: UUID,
        expected_epoch: UUID,
    ) -> AsyncGenerator[None, None]:
        try:
            async with self._session_state.password_change(
                user_id,
                expected_epoch,
            ):
                yield
        except SessionMutationInProgressError as exc:
            raise CacheUnavailableError(
                'Password change is already in progress'
            ) from exc
        except SessionMutationFinalizationError as exc:
            logger.error(
                'Session mutation marker ownership lost user_id=%s',
                user_id,
            )
            raise CacheUnavailableError(
                'Password change session finalization failed'
            ) from exc
        except RedisError as exc:
            logger.exception(
                'Password change session state failed user_id=%s',
                user_id,
            )
            raise CacheUnavailableError from exc

    async def revoke_session(
        self,
        *,
        user_id: UUID,
        refresh_token_id: UUID | None,
        access_token_id: UUID,
        access_ttl: int,
    ) -> None:
        try:
            await self._revoke_session(
                user_id=user_id,
                refresh_token_id=refresh_token_id,
                access_token_id=access_token_id,
                access_ttl=access_ttl,
            )
        except RedisError as exc:
            logger.exception('Error revoking session')
            raise CacheUnavailableError from exc

    @on_redis_exception()
    async def _revoke_session(
        self,
        *,
        user_id: UUID,
        refresh_token_id: UUID | None,
        access_token_id: UUID,
        access_ttl: int,
    ) -> None:
        async with self._redis.pipeline(transaction=True) as pipeline:
            if refresh_token_id is not None:
                self._refresh_tokens.queue_delete(
                    pipeline,
                    user_id,
                    refresh_token_id,
                )
            self._access_denylist.queue_add(
                pipeline,
                user_id,
                access_token_id,
                access_ttl,
            )
            await pipeline.execute()
