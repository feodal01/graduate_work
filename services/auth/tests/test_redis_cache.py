from collections.abc import AsyncGenerator

import pytest
from redis.asyncio import Redis

from core.config import RedisSettings
from db.cache.redis_health import RedisHealthCheck


@pytest.fixture
async def redis_client() -> AsyncGenerator[Redis, None]:
    """Создаёт Redis-клиент и закрывает его после теста."""
    settings = RedisSettings()
    client = Redis(host=settings.host, port=settings.port)
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_redis_health_check_reports_available_redis(
    redis_client: Redis,
) -> None:
    """Health check возвращает успех для доступного Redis."""
    health_check = RedisHealthCheck(redis_client)

    assert await health_check.ping() is True


@pytest.mark.asyncio
async def test_redis_health_check_reports_unavailable_redis() -> None:
    """Health check возвращает ошибку для недоступного Redis."""
    client = Redis(
        host='127.0.0.1',
        port=1,
        socket_connect_timeout=0.05,
        socket_timeout=0.05,
    )
    health_check = RedisHealthCheck(client)

    assert await health_check.ping() is False
    await client.aclose()
