import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from uuid import UUID, uuid4

from redis.asyncio import Redis
from redis.exceptions import RedisError

from db.backoff_policy import on_redis_exception
from db.cache.errors import SessionEpochMismatchError

logger = logging.getLogger(__name__)

_GET_OR_CREATE_EPOCH_SCRIPT = """
if redis.call('EXISTS', KEYS[2]) == 1 then
    return false
end

local current_epoch = redis.call('GET', KEYS[1])
if current_epoch then
    return current_epoch
end

redis.call('SET', KEYS[1], ARGV[1], 'NX')
return redis.call('GET', KEYS[1])
"""

_BEGIN_PASSWORD_CHANGE_SCRIPT = """
if redis.call('GET', KEYS[2]) ~= ARGV[2] then
    return -1
end
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[4]) then
    redis.call('SET', KEYS[2], ARGV[3])
    return 1
end
return 0
"""

_FINISH_PASSWORD_CHANGE_SCRIPT = """
local marker_owner = redis.call('GET', KEYS[1])
if marker_owner == ARGV[1] then
    if ARGV[3] == '1' then
        redis.call('SET', KEYS[2], ARGV[2])
    end
    redis.call('DEL', KEYS[1])
    return 1
end
if not marker_owner then
    if ARGV[3] == '1' then
        redis.call('SET', KEYS[2], ARGV[2])
    end
    return 1
end
return 0
"""

_VALIDATE_ACCESS_SCRIPT = """
if redis.call('GET', KEYS[1]) ~= ARGV[1]
    or redis.call('EXISTS', KEYS[2]) == 1
    or redis.call('EXISTS', KEYS[3]) == 1 then
    return 0
end
return 1
"""


@dataclass(frozen=True, slots=True)
class SessionKeyspace:
    namespace: str = 'auth-session'

    def prefix(self, user_id: UUID) -> str:
        return f'{{{self.namespace}:{user_id}}}'

    def epoch(self, user_id: UUID) -> str:
        return f'{self.prefix(user_id)}:epoch'

    def mutation_marker(self, user_id: UUID) -> str:
        return f'{self.prefix(user_id)}:changing'

    def refresh(self, user_id: UUID, token_id: UUID) -> str:
        return f'{self.prefix(user_id)}:refresh:{token_id}'

    def revoked_access(self, user_id: UUID, token_id: UUID) -> str:
        return f'{self.prefix(user_id)}:revoked-access:{token_id}'


class SessionMutationInProgressError(Exception):
    """Другой процесс уже изменяет пароль пользователя."""


class SessionMutationFinalizationError(Exception):
    """Процесс потерял владение marker до завершения смены пароля."""


class RedisSessionStateStore:
    def __init__(
        self,
        redis: Redis,
        *,
        keys: SessionKeyspace,
        marker_ttl_seconds: int,
    ) -> None:
        self._keys = keys
        self._marker_ttl_seconds = marker_ttl_seconds
        self._get_or_create_script = redis.register_script(_GET_OR_CREATE_EPOCH_SCRIPT)
        self._begin_script = redis.register_script(_BEGIN_PASSWORD_CHANGE_SCRIPT)
        self._finish_script = redis.register_script(_FINISH_PASSWORD_CHANGE_SCRIPT)
        self._access_script = redis.register_script(_VALIDATE_ACCESS_SCRIPT)

    @on_redis_exception()
    async def get_or_create_epoch(self, user_id: UUID) -> UUID | None:
        value = await self._get_or_create_script(
            keys=(
                self._keys.epoch(user_id),
                self._keys.mutation_marker(user_id),
            ),
            args=(str(uuid4()),),
        )
        return self._parse_epoch(value)

    @asynccontextmanager
    async def password_change(
        self,
        user_id: UUID,
        expected_epoch: UUID,
    ) -> AsyncGenerator[None, None]:
        owner = str(uuid4())
        new_epoch = str(uuid4())
        outcome = await self._begin(
            user_id=user_id,
            owner=owner,
            expected_epoch=str(expected_epoch),
            new_epoch=new_epoch,
        )
        if outcome == -1:
            raise SessionEpochMismatchError
        if outcome != 1:
            raise SessionMutationInProgressError

        try:
            yield
        except BaseException:
            try:
                finalized = await self._finish(
                    user_id=user_id,
                    owner=owner,
                    final_epoch=str(uuid4()),
                    succeeded=False,
                )
                if not finalized:
                    logger.error(
                        'Could not clean up session mutation marker user_id=%s',
                        user_id,
                    )
            except RedisError:
                logger.exception(
                    'Could not clean up session mutation user_id=%s',
                    user_id,
                )
            raise
        else:
            finalized = await self._finish(
                user_id=user_id,
                owner=owner,
                final_epoch=str(uuid4()),
                succeeded=True,
            )
            if not finalized:
                raise SessionMutationFinalizationError

    @on_redis_exception()
    async def is_access_valid(
        self,
        *,
        user_id: UUID,
        token_id: UUID,
        session_epoch: UUID,
    ) -> bool:
        result = await self._access_script(
            keys=(
                self._keys.epoch(user_id),
                self._keys.mutation_marker(user_id),
                self._keys.revoked_access(user_id, token_id),
            ),
            args=(str(session_epoch),),
        )
        return bool(result)

    @on_redis_exception()
    async def _begin(
        self,
        *,
        user_id: UUID,
        owner: str,
        expected_epoch: str,
        new_epoch: str,
    ) -> int:
        result = await self._begin_script(
            keys=(
                self._keys.mutation_marker(user_id),
                self._keys.epoch(user_id),
            ),
            args=(
                owner,
                expected_epoch,
                new_epoch,
                self._marker_ttl_seconds,
            ),
        )
        return int(result)

    @on_redis_exception()
    async def _finish(
        self,
        *,
        user_id: UUID,
        owner: str,
        final_epoch: str,
        succeeded: bool,
    ) -> bool:
        result = await self._finish_script(
            keys=(
                self._keys.mutation_marker(user_id),
                self._keys.epoch(user_id),
            ),
            args=(owner, final_epoch, int(succeeded)),
        )
        return bool(result)

    @staticmethod
    def _parse_epoch(value: bytes | str | None) -> UUID | None:
        if value is None:
            return None
        try:
            text = value.decode() if isinstance(value, bytes) else value
            return UUID(text)
        except (TypeError, UnicodeDecodeError, ValueError):
            return None
