import logging
from typing import cast

from redis.asyncio import Redis
from redis.exceptions import RedisError

from db.backoff_policy import on_redis_exception

from .base import AbstractCache

logger = logging.getLogger(__name__)


class RedisCache(AbstractCache):
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def get(self, key: str) -> bytes | str | None:
        try:
            return await self._get(key)
        except RedisError:
            logger.exception('Error getting from cache')
            return None

    async def set(self, key: str, value: bytes | str, ttl: int) -> None:
        try:
            await self._set(key, value, ttl)
        except RedisError:
            logger.exception('Error putting to cache')

    @on_redis_exception()
    async def _get(self, key: str) -> bytes | str | None:
        return cast(bytes | str | None, await self.redis.get(key))

    @on_redis_exception()
    async def _set(self, key: str, value: bytes | str, ttl: int) -> None:
        await self.redis.set(key, value, ttl)

    async def ping(self) -> bool:
        try:
            return bool(await self.redis.ping())
        except RedisError:
            logger.exception('Error pinging cache')
            return False
