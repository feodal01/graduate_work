import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

from db.health import HealthCheck

logger = logging.getLogger(__name__)


class RedisHealthCheck(HealthCheck):
    """Проверка доступности Redis."""

    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    async def ping(self) -> bool:
        try:
            return bool(await self.redis.ping())
        except RedisError:
            logger.exception('Error pinging cache')
            return False
