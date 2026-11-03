import asyncio
import importlib
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from core.config import RedisSettings
from db.cache.errors import CacheUnavailableError, SessionEpochMismatchError
from db.cache.session import RedisTokenSessionStore


@pytest.fixture
async def redis_client() -> AsyncGenerator[Redis, None]:
    """Создаёт Redis-клиент и закрывает его после теста."""
    redis_settings = RedisSettings()
    client = Redis(host=redis_settings.host, port=redis_settings.port)
    yield client
    await client.aclose()


def _session_keyspace(namespace: str):
    module = importlib.import_module('db.cache.session_state')
    return module.SessionKeyspace(namespace)


def _redis_uuid(value: bytes | str | None) -> UUID:
    assert value is not None
    text = value.decode() if isinstance(value, bytes) else value
    return UUID(text)


def test_session_keyspace_keeps_user_keys_in_one_cluster_slot() -> None:
    """Потеря hash tag сделает пользовательские Lua-операции неатомарными."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    user_id = uuid4()
    token_id = uuid4()
    prefix = f'{{{namespace}:{user_id}}}'

    assert keys.epoch(user_id) == f'{prefix}:epoch'
    assert keys.mutation_marker(user_id) == f'{prefix}:changing'
    assert keys.refresh(user_id, token_id) == f'{prefix}:refresh:{token_id}'
    assert keys.revoked_access(user_id, token_id) == (
        f'{prefix}:revoked-access:{token_id}'
    )


@pytest.mark.asyncio
async def test_epoch_is_stable_and_has_no_ttl(redis_client: Redis) -> None:
    """Повторная аутентификация не должна сама отзывать другие сессии."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()

    try:
        first = await store.get_or_create_epoch(user_id)
        second = await store.get_or_create_epoch(user_id)

        assert first == second
        assert await redis_client.ttl(keys.epoch(user_id)) == -1
    finally:
        await redis_client.unlink(keys.epoch(user_id))


@pytest.mark.asyncio
async def test_password_change_rotates_epoch_and_blocks_sessions(
    redis_client: Redis,
) -> None:
    """Marker закрывает выпуск, access и refresh на время смены пароля."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    old_refresh_id = uuid4()
    new_refresh_id = uuid4()
    access_id = uuid4()
    old_epoch = await store.get_or_create_epoch(user_id)
    assert await store.store_refresh(
        old_refresh_id,
        user_id,
        old_epoch,
        ttl=30,
    )

    try:
        async with store.password_change(user_id, old_epoch):
            epoch_value = await redis_client.get(keys.epoch(user_id))
            new_epoch = _redis_uuid(epoch_value)
            assert new_epoch != old_epoch
            assert await redis_client.get(keys.mutation_marker(user_id))
            assert not await store.store_refresh(uuid4(), user_id, new_epoch, ttl=30)
            assert not await store.rotate_refresh(
                old_token_id=old_refresh_id,
                new_token_id=new_refresh_id,
                user_id=user_id,
                session_epoch=old_epoch,
                ttl=30,
            )
            assert not await store.is_access_valid(
                user_id=user_id,
                token_id=access_id,
                session_epoch=new_epoch,
            )

        assert await redis_client.get(keys.mutation_marker(user_id)) is None
        assert await store.get_or_create_epoch(user_id) != old_epoch
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.mutation_marker(user_id),
            keys.refresh(user_id, old_refresh_id),
            keys.refresh(user_id, new_refresh_id),
        )


@pytest.mark.asyncio
async def test_password_change_error_keeps_new_epoch(
    redis_client: Redis,
) -> None:
    """Ошибка PostgreSQL не должна возвращать отозванные токены к жизни."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    old_epoch = await store.get_or_create_epoch(user_id)

    try:
        with pytest.raises(RuntimeError, match='database failed'):
            async with store.password_change(user_id, old_epoch):
                raise RuntimeError('database failed')

        assert await store.get_or_create_epoch(user_id) != old_epoch
        assert await redis_client.get(keys.mutation_marker(user_id)) is None
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.mutation_marker(user_id),
        )


@pytest.mark.asyncio
async def test_successful_password_change_revokes_session_created_after_marker_expiry(
    redis_client: Redis,
) -> None:
    """Медленный UPDATE не оставляет сессию со старым паролем активной."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    old_epoch = await store.get_or_create_epoch(user_id)
    slipped_refresh_id = uuid4()
    rotated_refresh_id = uuid4()

    try:
        async with store.password_change(user_id, old_epoch):
            begin_epoch_value = await redis_client.get(keys.epoch(user_id))
            begin_epoch = _redis_uuid(begin_epoch_value)
            await redis_client.delete(keys.mutation_marker(user_id))
            assert await store.store_refresh(
                slipped_refresh_id,
                user_id,
                begin_epoch,
                ttl=30,
            )

        final_epoch = await store.get_or_create_epoch(user_id)
        assert final_epoch != begin_epoch
        assert not await store.rotate_refresh(
            old_token_id=slipped_refresh_id,
            new_token_id=rotated_refresh_id,
            user_id=user_id,
            session_epoch=begin_epoch,
            ttl=30,
        )
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.mutation_marker(user_id),
            keys.refresh(user_id, slipped_refresh_id),
            keys.refresh(user_id, rotated_refresh_id),
        )


@pytest.mark.asyncio
async def test_concurrent_password_change_is_rejected(
    redis_client: Redis,
) -> None:
    """Два процесса не должны одновременно владеть mutation marker."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    expected_epoch = await store.get_or_create_epoch(user_id)

    try:
        async with store.password_change(user_id, expected_epoch):
            with pytest.raises(SessionEpochMismatchError):
                async with store.password_change(user_id, expected_epoch):
                    pytest.fail('second mutation entered protected section')
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.mutation_marker(user_id),
        )


@pytest.mark.asyncio
async def test_password_change_rejects_busy_marker(
    redis_client: Redis,
) -> None:
    """A marker for the current epoch is a transient 503 condition."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    expected_epoch = await store.get_or_create_epoch(user_id)
    await redis_client.set(
        keys.mutation_marker(user_id),
        'another-owner',
        ex=30,
    )

    try:
        with pytest.raises(CacheUnavailableError):
            async with store.password_change(user_id, expected_epoch):
                pytest.fail('busy mutation entered protected section')
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.mutation_marker(user_id),
        )


@pytest.mark.asyncio
async def test_password_change_rejects_stale_expected_epoch(
    redis_client: Redis,
) -> None:
    """An already authenticated stale PATCH must not overwrite newer state."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    stale_epoch = await store.get_or_create_epoch(user_id)
    current_epoch = uuid4()
    await redis_client.set(keys.epoch(user_id), str(current_epoch))

    try:
        with pytest.raises(SessionEpochMismatchError):
            async with store.password_change(user_id, stale_epoch):
                pytest.fail('stale mutation entered protected section')

        assert _redis_uuid(await redis_client.get(keys.epoch(user_id))) == (
            current_epoch
        )
        assert await redis_client.get(keys.mutation_marker(user_id)) is None
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.mutation_marker(user_id),
        )


@pytest.mark.asyncio
async def test_foreign_owner_marker_is_not_deleted(
    redis_client: Redis,
) -> None:
    """Завершившийся процесс не удаляет marker нового владельца."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    expected_epoch = await store.get_or_create_epoch(user_id)
    marker_key = keys.mutation_marker(user_id)
    begin_epoch: UUID | None = None

    try:
        with pytest.raises(CacheUnavailableError):
            async with store.password_change(user_id, expected_epoch):
                begin_epoch_value = await redis_client.get(keys.epoch(user_id))
                begin_epoch = _redis_uuid(begin_epoch_value)
                await redis_client.set(marker_key, 'foreign-owner', ex=30)

        assert begin_epoch is not None
        assert await redis_client.get(marker_key) == b'foreign-owner'
        final_epoch_value = await redis_client.get(keys.epoch(user_id))
        assert _redis_uuid(final_epoch_value) == begin_epoch
    finally:
        await redis_client.unlink(keys.epoch(user_id), marker_key)


@pytest.mark.asyncio
async def test_finish_failure_leaves_marker_with_ttl(
    redis_client: Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Сбой cleanup временно блокирует вход, но не отменяет новый epoch."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(
        redis_client,
        namespace=namespace,
        marker_ttl_seconds=30,
    )
    user_id = uuid4()
    old_epoch = await store.get_or_create_epoch(user_id)
    finish_script = AsyncMock(side_effect=ResponseError('finish failed'))
    monkeypatch.setattr(store._session_state, '_finish_script', finish_script)

    try:
        with pytest.raises(CacheUnavailableError):
            async with store.password_change(user_id, old_epoch):
                pass

        epoch_value = await redis_client.get(keys.epoch(user_id))
        assert _redis_uuid(epoch_value) != old_epoch
        marker_ttl = await redis_client.ttl(keys.mutation_marker(user_id))
        assert 1 <= marker_ttl <= 30
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.mutation_marker(user_id),
        )


@pytest.mark.asyncio
async def test_body_error_wins_over_finish_failure(
    redis_client: Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redis cleanup failure must not replace the PostgreSQL body error."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(
        redis_client,
        namespace=namespace,
        marker_ttl_seconds=30,
    )
    user_id = uuid4()
    old_epoch = await store.get_or_create_epoch(user_id)
    finish_script = AsyncMock(side_effect=ResponseError('finish failed'))
    monkeypatch.setattr(store._session_state, '_finish_script', finish_script)

    try:
        with pytest.raises(RuntimeError, match='database failed'):
            async with store.password_change(user_id, old_epoch):
                raise RuntimeError('database failed')

        epoch_value = await redis_client.get(keys.epoch(user_id))
        assert _redis_uuid(epoch_value) != old_epoch
        marker_ttl = await redis_client.ttl(keys.mutation_marker(user_id))
        assert 1 <= marker_ttl <= 30
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.mutation_marker(user_id),
        )


@pytest.mark.asyncio
async def test_store_rotate_and_access_require_current_epoch(
    redis_client: Redis,
) -> None:
    """Несовпадающий или отсутствующий epoch закрывает все token paths."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    epoch = await store.get_or_create_epoch(user_id)
    wrong_epoch = uuid4()
    old_refresh_id = uuid4()
    new_refresh_id = uuid4()
    access_id = uuid4()

    try:
        assert not await store.store_refresh(
            old_refresh_id, user_id, wrong_epoch, ttl=30
        )
        assert await store.store_refresh(old_refresh_id, user_id, epoch, ttl=30)
        assert await store.is_access_valid(
            user_id=user_id,
            token_id=access_id,
            session_epoch=epoch,
        )
        assert not await store.is_access_valid(
            user_id=user_id,
            token_id=access_id,
            session_epoch=wrong_epoch,
        )

        await redis_client.delete(keys.epoch(user_id))
        assert not await store.rotate_refresh(
            old_token_id=old_refresh_id,
            new_token_id=new_refresh_id,
            user_id=user_id,
            session_epoch=epoch,
            ttl=30,
        )
        assert not await store.is_access_valid(
            user_id=user_id,
            token_id=access_id,
            session_epoch=epoch,
        )
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.refresh(user_id, old_refresh_id),
            keys.refresh(user_id, new_refresh_id),
        )


@pytest.mark.asyncio
async def test_refresh_rotation_is_idempotent_and_replay_safe(
    redis_client: Redis,
) -> None:
    """Один refresh создаёт только одну новую сессию, включая retry."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    epoch = await store.get_or_create_epoch(user_id)
    old_token_id = uuid4()
    new_token_id = uuid4()
    replay_token_id = uuid4()
    await store.store_refresh(old_token_id, user_id, epoch, ttl=30)

    try:
        assert await store.rotate_refresh(
            old_token_id=old_token_id,
            new_token_id=new_token_id,
            user_id=user_id,
            session_epoch=epoch,
            ttl=30,
        )
        assert await store.rotate_refresh(
            old_token_id=old_token_id,
            new_token_id=new_token_id,
            user_id=user_id,
            session_epoch=epoch,
            ttl=30,
        )
        assert not await store.rotate_refresh(
            old_token_id=old_token_id,
            new_token_id=replay_token_id,
            user_id=user_id,
            session_epoch=epoch,
            ttl=30,
        )
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.refresh(user_id, old_token_id),
            keys.refresh(user_id, new_token_id),
            keys.refresh(user_id, replay_token_id),
        )


@pytest.mark.asyncio
async def test_concurrent_refresh_rotation_has_one_winner(
    redis_client: Redis,
) -> None:
    """Два новых refresh не должны родиться из одного старого."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    epoch = await store.get_or_create_epoch(user_id)
    old_token_id = uuid4()
    first_new_id = uuid4()
    second_new_id = uuid4()
    await store.store_refresh(old_token_id, user_id, epoch, ttl=30)

    try:
        results = await asyncio.gather(
            store.rotate_refresh(
                old_token_id=old_token_id,
                new_token_id=first_new_id,
                user_id=user_id,
                session_epoch=epoch,
                ttl=30,
            ),
            store.rotate_refresh(
                old_token_id=old_token_id,
                new_token_id=second_new_id,
                user_id=user_id,
                session_epoch=epoch,
                ttl=30,
            ),
        )

        assert results.count(True) == 1
        assert results.count(False) == 1
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.refresh(user_id, old_token_id),
            keys.refresh(user_id, first_new_id),
            keys.refresh(user_id, second_new_id),
        )


@pytest.mark.asyncio
async def test_password_change_wins_against_old_refresh(
    redis_client: Redis,
) -> None:
    """Старый refresh не работает ни во время marker, ни после него."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    old_epoch = await store.get_or_create_epoch(user_id)
    old_token_id = uuid4()
    new_token_id = uuid4()
    await store.store_refresh(old_token_id, user_id, old_epoch, ttl=30)

    try:
        async with store.password_change(user_id, old_epoch):
            assert not await store.rotate_refresh(
                old_token_id=old_token_id,
                new_token_id=new_token_id,
                user_id=user_id,
                session_epoch=old_epoch,
                ttl=30,
            )

        assert not await store.rotate_refresh(
            old_token_id=old_token_id,
            new_token_id=new_token_id,
            user_id=user_id,
            session_epoch=old_epoch,
            ttl=30,
        )
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.refresh(user_id, old_token_id),
            keys.refresh(user_id, new_token_id),
        )


@pytest.mark.asyncio
async def test_logout_revokes_only_selected_session(redis_client: Redis) -> None:
    """Обычный logout не меняет epoch и не отключает второе устройство."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    epoch = await store.get_or_create_epoch(user_id)
    first_refresh = uuid4()
    second_refresh = uuid4()
    first_access = uuid4()
    second_access = uuid4()
    second_rotated = uuid4()
    await store.store_refresh(first_refresh, user_id, epoch, ttl=30)
    await store.store_refresh(second_refresh, user_id, epoch, ttl=30)

    try:
        await store.revoke_session(
            user_id=user_id,
            refresh_token_id=first_refresh,
            access_token_id=first_access,
            access_ttl=20,
        )

        assert await store.get_or_create_epoch(user_id) == epoch
        assert not await store.is_access_valid(
            user_id=user_id,
            token_id=first_access,
            session_epoch=epoch,
        )
        assert await store.is_access_valid(
            user_id=user_id,
            token_id=second_access,
            session_epoch=epoch,
        )
        assert not await store.rotate_refresh(
            old_token_id=first_refresh,
            new_token_id=uuid4(),
            user_id=user_id,
            session_epoch=epoch,
            ttl=30,
        )
        assert await store.rotate_refresh(
            old_token_id=second_refresh,
            new_token_id=second_rotated,
            user_id=user_id,
            session_epoch=epoch,
            ttl=30,
        )
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.refresh(user_id, first_refresh),
            keys.refresh(user_id, second_refresh),
            keys.refresh(user_id, second_rotated),
            keys.revoked_access(user_id, first_access),
        )


@pytest.mark.asyncio
async def test_refresh_store_retries_same_operation() -> None:
    """Временный Redis-сбой повторяет условную запись без смены аргументов."""
    redis = MagicMock()
    store = RedisTokenSessionStore(redis)
    store_script = AsyncMock(side_effect=[RedisConnectionError('response lost'), 1])
    store._refresh_tokens._store_script = store_script
    user_id = uuid4()
    epoch = uuid4()
    token_id = uuid4()

    with patch('asyncio.sleep', new_callable=AsyncMock):
        stored = await store.store_refresh(token_id, user_id, epoch, ttl=30)

    assert stored is True
    assert store_script.await_count == 2
    assert store_script.await_args_list[0] == store_script.await_args_list[1]


@pytest.mark.asyncio
async def test_refresh_store_does_not_retry_non_transient_error() -> None:
    """Постоянная Redis-ошибка сразу закрывает выпуск сессии."""
    redis = MagicMock()
    store = RedisTokenSessionStore(redis)
    store_script = AsyncMock(side_effect=ResponseError('invalid command'))
    store._refresh_tokens._store_script = store_script

    with patch('asyncio.sleep', new_callable=AsyncMock) as sleep:
        with pytest.raises(CacheUnavailableError):
            await store.store_refresh(uuid4(), uuid4(), uuid4(), ttl=30)

    assert store_script.await_count == 1
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_access_validation_retries_same_operation() -> None:
    """Временный Redis-сбой не превращается в разрешённый access."""
    redis = MagicMock()
    store = RedisTokenSessionStore(redis)
    access_script = AsyncMock(side_effect=[RedisConnectionError('response lost'), 1])
    store._session_state._access_script = access_script

    with patch('asyncio.sleep', new_callable=AsyncMock):
        valid = await store.is_access_valid(
            user_id=uuid4(),
            token_id=uuid4(),
            session_epoch=uuid4(),
        )

    assert valid is True
    assert access_script.await_count == 2
    assert access_script.await_args_list[0] == access_script.await_args_list[1]


@pytest.mark.asyncio
async def test_token_session_facade_does_not_use_raw_eval(
    redis_client: Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Все session Lua-операции вызываются как зарегистрированные scripts."""
    namespace = f'test-session-{uuid4()}'
    keys = _session_keyspace(namespace)
    store = RedisTokenSessionStore(redis_client, namespace=namespace)
    user_id = uuid4()
    epoch = await store.get_or_create_epoch(user_id)
    old_token_id = uuid4()
    new_token_id = uuid4()
    raw_eval = AsyncMock(side_effect=AssertionError('raw EVAL called'))
    monkeypatch.setattr(redis_client, 'eval', raw_eval)

    try:
        assert await store.store_refresh(old_token_id, user_id, epoch, ttl=30)
        assert await store.rotate_refresh(
            old_token_id=old_token_id,
            new_token_id=new_token_id,
            user_id=user_id,
            session_epoch=epoch,
            ttl=30,
        )
        raw_eval.assert_not_awaited()
    finally:
        await redis_client.unlink(
            keys.epoch(user_id),
            keys.refresh(user_id, old_token_id),
            keys.refresh(user_id, new_token_id),
        )
