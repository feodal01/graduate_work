from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from core.config import RedisSettings
from db.cache.authorization import (
    AuthorizationSnapshot,
    RedisAuthorizationCache,
)
from db.cache.authorization_snapshot import (
    AuthorizationKeyspace,
    RedisAuthorizationSnapshotStore,
)
from db.cache.errors import CacheUnavailableError


@pytest.fixture
async def redis_client() -> AsyncGenerator[Redis, None]:
    """Создаёт Redis-клиент и закрывает его после теста."""
    settings = RedisSettings()
    client = Redis(host=settings.host, port=settings.port)
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_snapshot_component_round_trip(redis_client: Redis) -> None:
    """Компонент снимков сохраняет и возвращает права пользователя."""
    namespace = f'authz-component-test-{uuid4()}'
    store = RedisAuthorizationSnapshotStore(
        redis_client,
        ttl_seconds=30,
        keys=AuthorizationKeyspace.create(namespace),
    )
    user_id = uuid4()
    snapshot = AuthorizationSnapshot(
        is_superuser=False,
        permissions=frozenset({'roles:manage'}),
    )

    try:
        read_result = await store.read_snapshot(user_id)
        await store.store_snapshot(
            user_id,
            snapshot,
            read_result.roles_revision_at_read,
        )
        cached = await store.read_snapshot(user_id)
        assert cached.cached_snapshot == snapshot
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
async def test_authorization_snapshot_round_trip(redis_client: Redis) -> None:
    """Фасад RBAC-кэша сохраняет и читает пользовательский снимок."""
    namespace = f'authz-test-{uuid4()}'
    store = RedisAuthorizationCache(
        redis_client,
        ttl_seconds=30,
        namespace=namespace,
    )
    user_id = uuid4()
    snapshot = AuthorizationSnapshot(
        is_superuser=False,
        permissions=frozenset({'films.early_access:read'}),
    )

    try:
        miss = await store.read_snapshot(user_id)
        assert miss.cached_snapshot is None

        await store.store_snapshot(
            user_id,
            snapshot,
            miss.roles_revision_at_read,
        )

        hit = await store.read_snapshot(user_id)
        assert hit.cached_snapshot == snapshot
        assert hit.roles_revision_at_read == miss.roles_revision_at_read
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
async def test_cache_does_not_use_raw_eval(
    redis_client: Redis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RBAC-кэш выполняет зарегистрированные скрипты без прямого EVAL."""
    namespace = f'authz-test-{uuid4()}'
    store = RedisAuthorizationCache(
        redis_client,
        ttl_seconds=30,
        namespace=namespace,
    )
    user_id = uuid4()
    snapshot = AuthorizationSnapshot(
        is_superuser=False,
        permissions=frozenset({'roles:manage'}),
    )
    raw_eval = AsyncMock(side_effect=AssertionError('raw EVAL called'))
    monkeypatch.setattr(redis_client, 'eval', raw_eval)

    try:
        revision = (await store.read_snapshot(user_id)).roles_revision_at_read
        await store.store_snapshot(user_id, snapshot, revision)
        async with store.user_mutation(user_id):
            pass
        async with store.roles_mutation():
            pass

        raw_eval.assert_not_awaited()
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'malformed_fields',
    (
        '"is_superuser":"false","permissions":[]',
        '"is_superuser":false,"permissions":[1]',
    ),
)
async def test_malformed_snapshot_is_treated_as_cache_miss(
    redis_client: Redis,
    malformed_fields: str,
) -> None:
    """Повреждённый снимок считается отсутствующим значением кэша."""
    namespace = f'authz-test-{uuid4()}'
    store = RedisAuthorizationCache(
        redis_client,
        ttl_seconds=30,
        namespace=namespace,
    )
    user_id = uuid4()

    try:
        revision = (await store.read_snapshot(user_id)).roles_revision_at_read
        await redis_client.set(
            f'{{{namespace}}}:user:{user_id}',
            f'{{"roles_revision":"{revision}",{malformed_fields}}}',
        )

        read_result = await store.read_snapshot(user_id)
        assert read_result.cached_snapshot is None
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
async def test_user_mutation_invalidates_only_target_user(
    redis_client: Redis,
) -> None:
    """Изменение пользователя инвалидирует только его снимок."""
    namespace = f'authz-test-{uuid4()}'
    store = RedisAuthorizationCache(
        redis_client,
        ttl_seconds=30,
        namespace=namespace,
    )
    first_user_id = uuid4()
    second_user_id = uuid4()
    first_snapshot = AuthorizationSnapshot(
        is_superuser=False,
        permissions=frozenset({'films.early_access:read'}),
    )
    second_snapshot = AuthorizationSnapshot(
        is_superuser=True,
        permissions=frozenset(),
    )

    try:
        revision = (await store.read_snapshot(first_user_id)).roles_revision_at_read
        await store.store_snapshot(first_user_id, first_snapshot, revision)
        await store.store_snapshot(second_user_id, second_snapshot, revision)

        async with store.user_mutation(first_user_id):
            first_read = await store.read_snapshot(first_user_id)
            second_read = await store.read_snapshot(second_user_id)
            assert first_read.cached_snapshot is None
            assert second_read.cached_snapshot == second_snapshot

        first_read = await store.read_snapshot(first_user_id)
        second_read = await store.read_snapshot(second_user_id)
        assert first_read.cached_snapshot is None
        assert second_read.cached_snapshot == second_snapshot
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
async def test_roles_mutation_invalidates_all_snapshots_by_revision(
    redis_client: Redis,
) -> None:
    """Изменение ролей инвалидирует все снимки сменой ревизии."""
    namespace = f'authz-test-{uuid4()}'
    store = RedisAuthorizationCache(
        redis_client,
        ttl_seconds=30,
        namespace=namespace,
    )
    user_ids = (uuid4(), uuid4())
    snapshot = AuthorizationSnapshot(
        is_superuser=False,
        permissions=frozenset({'roles:manage'}),
    )

    try:
        old_revision = (await store.read_snapshot(user_ids[0])).roles_revision_at_read
        for user_id in user_ids:
            await store.store_snapshot(user_id, snapshot, old_revision)

        async with store.roles_mutation():
            for user_id in user_ids:
                read_result = await store.read_snapshot(user_id)
                assert read_result.cached_snapshot is None

        for user_id in user_ids:
            read_result = await store.read_snapshot(user_id)
            assert read_result.cached_snapshot is None
            assert read_result.roles_revision_at_read != old_revision
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
async def test_stale_snapshot_is_not_written_after_roles_revision_changes(
    redis_client: Redis,
) -> None:
    """Снимок со старой ревизией не возвращается в кэш."""
    namespace = f'authz-test-{uuid4()}'
    store = RedisAuthorizationCache(
        redis_client,
        ttl_seconds=30,
        namespace=namespace,
    )
    user_id = uuid4()
    snapshot = AuthorizationSnapshot(
        is_superuser=False,
        permissions=frozenset({'roles:manage'}),
    )

    try:
        old_revision = (await store.read_snapshot(user_id)).roles_revision_at_read

        async with store.roles_mutation():
            pass

        await store.store_snapshot(user_id, snapshot, old_revision)

        read_result = await store.read_snapshot(user_id)
        assert read_result.cached_snapshot is None
        assert read_result.roles_revision_at_read != old_revision
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
async def test_failed_roles_mutation_keeps_current_revision(
    redis_client: Redis,
) -> None:
    """Ошибка изменения ролей сохраняет текущую ревизию."""
    namespace = f'authz-test-{uuid4()}'
    store = RedisAuthorizationCache(
        redis_client,
        ttl_seconds=30,
        namespace=namespace,
    )
    user_id = uuid4()

    try:
        old_revision = (await store.read_snapshot(user_id)).roles_revision_at_read

        with pytest.raises(RuntimeError):
            async with store.roles_mutation():
                raise RuntimeError('database mutation failed')

        read_result = await store.read_snapshot(user_id)
        assert read_result.roles_revision_at_read == old_revision
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis_client.unlink(*keys)


@pytest.mark.asyncio
async def test_overlapping_mutations_for_same_user_are_rejected(
    redis_client: Redis,
) -> None:
    """Параллельные изменения одного пользователя отклоняются."""
    namespace = f'authz-test-{uuid4()}'
    store = RedisAuthorizationCache(
        redis_client,
        ttl_seconds=30,
        namespace=namespace,
    )
    user_id = uuid4()

    try:
        async with store.user_mutation(user_id):
            with pytest.raises(CacheUnavailableError):
                async with store.user_mutation(user_id):
                    pass
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis_client.unlink(*keys)
