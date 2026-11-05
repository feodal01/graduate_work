import asyncio
from datetime import UTC, date, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from db.cache.authorization import AuthorizationSnapshot, RedisAuthorizationCache
from db.cache.authorization_snapshot import (
    AuthorizationKeyspace,
    RedisAuthorizationSnapshotStore,
    compute_effective_snapshot_ttl,
)
from repositories.roles import SQLAlchemyRoleRepository
from services.authorization import AuthorizationService
from services.film_access import FilmAccessContext, create_film_access_policy
from services.types import CurrentUser
from tests.fakes import (
    InMemoryAuthorizationCache,
    InMemoryRoleRepository,
    InMemoryUserRepository,
)
from tests.test_authorization_cache import redis_client  # noqa: F401
from tests.test_role_repositories import create_user, session  # noqa: F401


def test_compute_effective_snapshot_ttl_bounds_by_nearest_expires_at() -> None:
    """AUTH-FR-03: TTL кэша ограничен сверху ближайшим expires_at."""
    now = datetime(2026, 11, 5, 12, 0, 0, tzinfo=UTC)

    # 1. Нет срочных ролей (nearest_expires_at=None) -> используется базовый TTL (300 с)
    assert compute_effective_snapshot_ttl(300, None, now_utc=now) == 300

    # 2. Ближайшее истечение через 42.2 секунды -> ceil(42.2) = 43 с
    soon = now + timedelta(seconds=42, milliseconds=200)
    assert compute_effective_snapshot_ttl(300, soon, now_utc=now) == 43

    # 3. Ближайшее истечение через 0.1 секунды -> max(1, ceil(0.1)) = 1 с
    very_soon = now + timedelta(milliseconds=100)
    assert compute_effective_snapshot_ttl(300, very_soon, now_utc=now) == 1

    # 4. Истечение через 30 дней -> используется базовый TTL (300 с)
    far_future = now + timedelta(days=30)
    assert compute_effective_snapshot_ttl(300, far_future, now_utc=now) == 300

    # 5. Уже истёкший момент (на случай гонки часов) -> минимум 1 с
    already_past = now - timedelta(seconds=5)
    assert compute_effective_snapshot_ttl(300, already_past, now_utc=now) == 1


async def test_authorization_service_ignores_expired_role_and_caps_cache_ttl() -> None:
    """AUTH-FR-03: истёкшая роль игнорируется, для активной есть nearest_expires_at."""
    users = InMemoryUserRepository()
    roles = InMemoryRoleRepository()
    cache = InMemoryAuthorizationCache()
    user = await users.create(
        login='ExpiringUser',
        password_hash='hash',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    roles.users.add(user.id)
    await roles.create(
        name='subscriber',
        permission_codes={'films.early_access:read'},
    )
    auth_service = AuthorizationService(
        users=users,
        roles=roles,
        authorization_cache=cache,
        film_policy=create_film_access_policy(),
    )
    cur_user = CurrentUser(
        user_id=user.id,
        token_id=uuid4(),
        session_epoch=uuid4(),
        expires_at=datetime(2027, 1, 1, tzinfo=UTC),
    )

    # 1. Назначаем роль со сроком через 120 мс -> доступ есть, затем истекает сам
    subsecond_exp = datetime.now(UTC) + timedelta(milliseconds=120)
    await auth_service.put_subscription_access(
        user.id,
        version=1,
        expires_at=subsecond_exp,
    )
    assert (
        await auth_service.can_read_film(
            cur_user,
            FilmAccessContext(creation_date=date(9999, 1, 1)),
        )
        is True
    )
    assert user.id in cache.items
    _, cached_snap = cache.items[user.id]
    assert cached_snap.nearest_expires_at == subsecond_exp

    # Ждём 160 мс (без put_subscription_access) -> снимок и роль истекают сами
    await asyncio.sleep(0.16)
    assert (
        await auth_service.can_read_film(
            cur_user,
            FilmAccessContext(creation_date=date(9999, 1, 1)),
        )
        is False
    )
    _, expired_snap = cache.items[user.id]
    assert expired_snap.nearest_expires_at is None


async def test_snapshot_store_passes_capped_ttl_to_redis_lua_script() -> None:
    """AUTH-FR-03: RedisAuthorizationSnapshotStore передаёт усечённый TTL."""
    fake_script = AsyncMock(return_value=1)
    fake_redis = AsyncMock(spec=Redis)
    fake_redis.register_script.return_value = fake_script

    store = RedisAuthorizationSnapshotStore(
        fake_redis,
        ttl_seconds=300,
        keys=AuthorizationKeyspace.create('authz-ttl-unit'),
    )
    user_id = uuid4()
    nearest = datetime.now(UTC) + timedelta(seconds=18)
    snapshot = AuthorizationSnapshot(
        is_superuser=False,
        permissions=frozenset({'films.early_access:read'}),
        nearest_expires_at=nearest,
    )

    await store.store_snapshot(user_id, snapshot, 'rev-1')

    fake_script.assert_awaited_once()
    passed_args = fake_script.call_args.kwargs['args']
    passed_ttl = passed_args[2]
    assert 1 <= passed_ttl <= 19


async def test_postgresql_and_redis_role_expiration_and_ttl_integration(
    session: AsyncSession,  # noqa: F811
    redis_client: Redis,  # noqa: F811
) -> None:
    """AUTH-FR-03 (PostgreSQL + Redis): игнор истёкших ролей и реальный TTL в Redis."""
    repo = SQLAlchemyRoleRepository(session)
    user = await create_user(session, login='PgExpiringUser')
    admin_role = await repo.create(name='admin', permission_codes={'roles:manage'})
    await repo.create(
        name='subscriber',
        permission_codes={'films.early_access:read'},
    )
    user_id = user.id
    await repo.assign(user_id, admin_role.id)

    # 1. Назначаем subscriber со сроком через 12 секунд
    future_exp = datetime.now(UTC) + timedelta(seconds=12)
    await repo.set_subscription_access(user_id, version=1, expires_at=future_exp)

    perms = await repo.get_permissions_for_user(user_id)
    assert perms == {'roles:manage', 'films.early_access:read'}
    perms_with_exp, nearest = await repo.get_active_permissions_and_nearest_expiry(
        user_id
    )
    assert perms_with_exp == {'roles:manage', 'films.early_access:read'}
    assert nearest == future_exp

    # Проверяем запись снимка в живой Redis: TTL ключа должен быть <= 12 с (а не 300 с)
    namespace = f'authz-t15-{uuid4().hex[:8]}'
    cache = RedisAuthorizationCache(
        redis_client,
        ttl_seconds=300,
        namespace=namespace,
    )
    try:
        read_res = await cache.read_snapshot(user_id)
        await cache.store_snapshot(
            user_id,
            AuthorizationSnapshot(
                is_superuser=False,
                permissions=frozenset(perms_with_exp),
                nearest_expires_at=nearest,
            ),
            read_res.roles_revision_at_read,
        )
        key_ttl = await redis_client.ttl(f'{{{namespace}}}:user:{user_id}')
        assert 1 <= key_ttl <= 13

        # Проверяем субсекундную границу (EX 1 в Redis, но expires_at через 150 мс)
        short_exp = datetime.now(UTC) + timedelta(milliseconds=150)
        await cache.store_snapshot(
            user_id,
            AuthorizationSnapshot(
                is_superuser=False,
                permissions=frozenset(perms_with_exp),
                nearest_expires_at=short_exp,
            ),
            read_res.roles_revision_at_read,
        )
        hit_before = await cache.read_snapshot(user_id)
        assert hit_before.cached_snapshot is not None
        assert hit_before.cached_snapshot.nearest_expires_at == short_exp

        await asyncio.sleep(0.2)
        # Ключ в Redis с EX 1 ещё жив, но read_snapshot обязан вернуть промах
        miss_after = await cache.read_snapshot(user_id)
        assert miss_after.cached_snapshot is None
    finally:
        keys = [key async for key in redis_client.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis_client.unlink(*keys)

    # 2. Истёкший subscriber игнорируется, бессрочная роль admin остаётся
    past_exp = datetime.now(UTC) - timedelta(seconds=1)
    await repo.set_subscription_access(user_id, version=2, expires_at=past_exp)

    assert await repo.get_permissions_for_user(user_id) == {'roles:manage'}
    perms_after, nearest_after = await repo.get_active_permissions_and_nearest_expiry(
        user_id
    )
    assert perms_after == {'roles:manage'}
    assert nearest_after is None
