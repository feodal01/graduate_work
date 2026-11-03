from uuid import UUID

from redis.asyncio import Redis
from redis.asyncio.client import Pipeline

from db.backoff_policy import on_redis_exception
from db.cache.session_state import SessionKeyspace

_STORE_REFRESH_SCRIPT = """
if redis.call('EXISTS', KEYS[2]) == 1
    or redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end
redis.call('SET', KEYS[3], ARGV[2], 'EX', ARGV[3])
return 1
"""

_ROTATE_REFRESH_SCRIPT = """
if redis.call('EXISTS', KEYS[2]) == 1
    or redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return 0
end

local current_user = redis.call('GET', KEYS[3])
if current_user then
    if current_user ~= ARGV[2] then
        return 0
    end
    redis.call('DEL', KEYS[3])
    redis.call('SET', KEYS[4], ARGV[2], 'EX', ARGV[3])
    return 1
end

if redis.call('GET', KEYS[4]) == ARGV[2] then
    return 1
end
return 0
"""


class RedisRefreshTokenStore:
    def __init__(self, redis: Redis, *, keys: SessionKeyspace) -> None:
        self._keys = keys
        self._store_script = redis.register_script(_STORE_REFRESH_SCRIPT)
        self._rotate_script = redis.register_script(_ROTATE_REFRESH_SCRIPT)

    @on_redis_exception()
    async def store(
        self,
        token_id: UUID,
        user_id: UUID,
        session_epoch: UUID,
        ttl: int,
    ) -> bool:
        result = await self._store_script(
            keys=(
                self._keys.epoch(user_id),
                self._keys.mutation_marker(user_id),
                self._keys.refresh(user_id, token_id),
            ),
            args=(str(session_epoch), str(user_id), ttl),
        )
        return bool(result)

    @on_redis_exception()
    async def rotate(
        self,
        *,
        old_token_id: UUID,
        new_token_id: UUID,
        user_id: UUID,
        session_epoch: UUID,
        ttl: int,
    ) -> bool:
        result = await self._rotate_script(
            keys=(
                self._keys.epoch(user_id),
                self._keys.mutation_marker(user_id),
                self._keys.refresh(user_id, old_token_id),
                self._keys.refresh(user_id, new_token_id),
            ),
            args=(str(session_epoch), str(user_id), ttl),
        )
        return bool(result)

    def queue_delete(
        self,
        pipeline: Pipeline,
        user_id: UUID,
        token_id: UUID,
    ) -> None:
        pipeline.delete(self._keys.refresh(user_id, token_id))
