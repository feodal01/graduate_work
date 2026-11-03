import asyncio
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from core.config import RedisSettings
from db.cache.errors import RateLimitStoreUnavailableError
from db.cache.rate_limit import RedisTokenBucketRateLimiter
from services.rate_limit import RateLimitDecision, RateLimitPolicy


@pytest.fixture
async def redis_client() -> AsyncGenerator[Redis, None]:
    """Create and close the Redis client used by integration-level tests."""
    settings = RedisSettings()
    client = Redis(host=settings.host, port=settings.port)
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_first_request_consumes_one_token_and_sets_expiring_safe_key(
    redis_client: Redis,
) -> None:
    """A first request leaves no identity text and no permanent Redis state."""
    namespace = f'rate-limit-test-{uuid4()}'
    limiter = RedisTokenBucketRateLimiter(redis_client, namespace=namespace)
    policy = RateLimitPolicy(
        name='login',
        rate=5,
        window_seconds=60,
        capacity=5,
    )

    try:
        decision = await limiter.check(policy=policy, identity='Alice')

        assert decision == RateLimitDecision(
            allowed=True,
            remaining=policy.capacity - 1,
            retry_after_seconds=0,
        )
        keys = [key async for key in redis_client.scan_iter(match=f'{namespace}:*')]
        assert len(keys) == 1
        assert 1 <= await redis_client.ttl(keys[0])
        assert b'Alice' not in keys[0]
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{namespace}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
async def test_concurrent_requests_cannot_exceed_bucket_capacity(
    redis_client: Redis,
) -> None:
    """The atomic transition never spends the same token twice."""
    namespace = f'rate-limit-test-{uuid4()}'
    limiter = RedisTokenBucketRateLimiter(redis_client, namespace=namespace)
    policy = RateLimitPolicy(
        name='service',
        rate=1,
        window_seconds=3600,
        capacity=5,
    )

    try:
        results = await asyncio.gather(
            *(
                limiter.check(policy=policy, identity='same-user')
                for _ in range(policy.capacity + 20)
            )
        )

        assert sum(result.allowed for result in results) == policy.capacity
        assert all(
            result.retry_after_seconds >= 1 for result in results if not result.allowed
        )
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{namespace}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
async def test_bucket_refills_from_redis_time(redis_client: Redis) -> None:
    """Elapsed Redis time restores a token without an application clock."""
    namespace = f'rate-limit-test-{uuid4()}'
    limiter = RedisTokenBucketRateLimiter(redis_client, namespace=namespace)
    policy = RateLimitPolicy(
        name='refresh',
        rate=10,
        window_seconds=1,
        capacity=1,
    )

    try:
        first = await limiter.check(policy=policy, identity='refresh-token')
        rejected = await limiter.check(policy=policy, identity='refresh-token')
        await asyncio.sleep(0.2)
        refilled = await limiter.check(policy=policy, identity='refresh-token')

        assert first.allowed is True
        assert rejected.allowed is False
        assert rejected.retry_after_seconds == 1
        assert refilled.allowed is True
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{namespace}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
async def test_limiter_uses_registered_script_instead_of_raw_eval(
    redis_client: Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The client can cache and reload the Lua script by its SHA."""
    namespace = f'rate-limit-test-{uuid4()}'
    limiter = RedisTokenBucketRateLimiter(redis_client, namespace=namespace)
    policy = RateLimitPolicy('login', rate=5, window_seconds=60, capacity=5)
    raw_eval = AsyncMock(side_effect=AssertionError('raw EVAL called'))
    monkeypatch.setattr(redis_client, 'eval', raw_eval)

    try:
        decision = await limiter.check(policy=policy, identity='Alice')

        assert decision.allowed is True
        raw_eval.assert_not_awaited()
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{namespace}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'redis_error',
    [
        RedisConnectionError('offline'),
        RedisTimeoutError('timed out'),
    ],
)
async def test_connection_failures_are_classified_without_retry(
    redis_error: Exception,
) -> None:
    """A failed store check makes exactly one Lua-script call."""
    script = AsyncMock(side_effect=redis_error)
    redis = MagicMock()
    redis.register_script.return_value = script
    limiter = RedisTokenBucketRateLimiter(redis)
    policy = RateLimitPolicy('login', rate=5, window_seconds=60, capacity=5)

    with pytest.raises(RateLimitStoreUnavailableError):
        await limiter.check(policy=policy, identity='Alice')

    script.assert_awaited_once()


@pytest.mark.asyncio
async def test_lua_response_error_is_not_hidden_as_store_unavailable() -> None:
    """A broken script remains an internal error instead of enabling fail-open."""
    script = AsyncMock(side_effect=ResponseError('bad script'))
    redis = MagicMock()
    redis.register_script.return_value = script
    limiter = RedisTokenBucketRateLimiter(redis)
    policy = RateLimitPolicy('login', rate=5, window_seconds=60, capacity=5)

    with pytest.raises(ResponseError, match='bad script'):
        await limiter.check(policy=policy, identity='Alice')

    script.assert_awaited_once()


@pytest.mark.asyncio
async def test_malformed_lua_result_is_rejected() -> None:
    """A malformed script contract cannot become an authorization decision."""
    script = AsyncMock(return_value=[1, 4])
    redis = MagicMock()
    redis.register_script.return_value = script
    limiter = RedisTokenBucketRateLimiter(redis)
    policy = RateLimitPolicy('login', rate=5, window_seconds=60, capacity=5)

    with pytest.raises(ValueError, match='Malformed rate-limit script result'):
        await limiter.check(policy=policy, identity='Alice')
