from hashlib import sha256
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from db.cache.errors import OAuthStateStoreUnavailableError
from db.cache.oauth_state import OAuthStateData, RedisOAuthStateStore


@pytest.mark.asyncio
async def test_state_is_saved_under_a_hashed_expiring_key() -> None:
    """The raw state must not appear in Redis keys or survive without TTL."""
    redis = MagicMock()
    redis.set = AsyncMock(return_value=True)
    store = RedisOAuthStateStore(redis)

    created = await store.save(
        provider='yandex',
        state='raw-oauth-state',
        code_verifier='test-verifier',
        tz='Europe/Moscow',
        ttl_seconds=600,
    )

    digest = sha256(b'raw-oauth-state').hexdigest()
    redis.set.assert_awaited_once_with(
        f'oauth-state:yandex:{digest}',
        OAuthStateData(
            code_verifier='test-verifier', tz='Europe/Moscow'
        ).model_dump_json(),
        ex=600,
        nx=True,
    )
    assert created is True


@pytest.mark.asyncio
async def test_state_is_consumed_only_once() -> None:
    """GETDEL must make a replay indistinguishable from an expired state."""
    redis = MagicMock()
    redis.getdel = AsyncMock(
        side_effect=[
            OAuthStateData(code_verifier='test-verifier', tz='Europe/Moscow')
            .model_dump_json()
            .encode(),
            None,
        ]
    )
    store = RedisOAuthStateStore(redis)

    first = await store.consume(provider='yandex', state='test-state')
    replay = await store.consume(provider='yandex', state='test-state')

    assert first == OAuthStateData(code_verifier='test-verifier', tz='Europe/Moscow')
    assert replay is None
    assert redis.getdel.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'redis_error',
    [
        RedisConnectionError('offline'),
        RedisTimeoutError('timed out'),
    ],
)
async def test_transient_store_failure_is_fail_closed(
    redis_error: Exception,
) -> None:
    """A connection failure must not become a missing or accepted state."""
    redis = MagicMock()
    redis.getdel = AsyncMock(side_effect=redis_error)
    store = RedisOAuthStateStore(redis)

    with pytest.raises(OAuthStateStoreUnavailableError):
        await store.consume(provider='yandex', state='test-state')

    redis.getdel.assert_awaited_once()


@pytest.mark.asyncio
async def test_redis_command_error_is_not_hidden_as_missing_state() -> None:
    """A Redis protocol error must remain visible to the application."""
    redis = MagicMock()
    redis.getdel = AsyncMock(side_effect=ResponseError('bad command'))
    store = RedisOAuthStateStore(redis)

    with pytest.raises(ResponseError, match='bad command'):
        await store.consume(provider='yandex', state='test-state')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'value',
    [
        123,
        b'test-verifier',
        b'{}',
        b'{"code_verifier":"v","tz":null}',
        b'{"code_verifier":"v","tz":"Unknown/Zone"}',
    ],
)
async def test_malformed_state_value_is_rejected(value: object) -> None:
    """Unexpected Redis value types must not validate OAuth state."""
    redis = MagicMock()
    redis.getdel = AsyncMock(return_value=value)
    store = RedisOAuthStateStore(redis)

    with pytest.raises(ValueError, match='Malformed OAuth state value'):
        await store.consume(provider='yandex', state='test-state')
