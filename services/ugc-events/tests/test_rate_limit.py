import asyncio
import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from services.rate_limit import RedisEventRateLimiter


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
    redis = Redis(
        host=os.environ.get('REDIS_HOST', '127.0.0.1'),
        port=int(os.environ.get('REDIS_PORT', '6379')),
    )
    try:
        yield redis
    finally:
        await redis.aclose()


@pytest.fixture
async def namespace(redis_client: Redis) -> AsyncIterator[str]:
    prefix = f'ugc-events-test:{uuid4()}'
    try:
        yield prefix
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{prefix}:*')]
        if keys:
            await redis_client.unlink(*keys)


async def test_concurrent_api_instances_share_user_capacity(
    redis_client: Redis,
    namespace: str,
) -> None:
    limiters = [
        RedisEventRateLimiter(
            redis_client,
            rate=1,
            window_seconds=3600,
            capacity=5,
            namespace=namespace,
        )
        for _ in range(2)
    ]
    user_id = uuid4()

    results = await asyncio.gather(*(limiters[index % 2].check(user_id) for index in range(25)))

    assert results.count(0) == 5
    assert all(1 <= retry_after <= 3600 for retry_after in results if retry_after)
    assert await limiters[0].check(uuid4()) == 0
    assert 1 <= await redis_client.ttl(f'{namespace}:{user_id}') <= 18001


async def test_bucket_refills_after_retry_after(
    redis_client: Redis,
    namespace: str,
) -> None:
    limiter = RedisEventRateLimiter(
        redis_client,
        rate=1,
        window_seconds=1,
        capacity=1,
        namespace=namespace,
    )
    user_id = uuid4()

    assert await limiter.check(user_id) == 0
    assert await limiter.check(user_id) == 1
    await asyncio.sleep(1.1)
    assert await limiter.check(user_id) == 0
