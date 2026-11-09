"""Регрессия: гонка кэша прав Auth (реальный Redis, реальные Lua-скрипты).

До поколения пользователя устаревший снимок записывался после снятия
маркера и жил 300 с: после возврата доступ оставался, после оплаты — нет.

Порядок событий:
1. movies-api проверяет права: промах кэша, читает «PostgreSQL» (права ещё
   с подпиской) и замирает перед store_snapshot.
2. Billing доставляет возврат: put_subscription_access(version=2, None)
   целиком проходит user_mutation (маркер → «коммит» → DEL снимка и маркера).
3. Первый запрос просыпается и вызывает store_snapshot со старыми правами.
4. Следующая проверка прав должна совпасть с PostgreSQL, а не с кэшем.
"""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, date, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from redis.asyncio import Redis

from core.config import RedisSettings
from db.cache.authorization import RedisAuthorizationCache
from services.authorization import AuthorizationService
from services.film_access import FilmAccessContext, create_film_access_policy
from services.types import CurrentUser
from tests.fakes import InMemoryRoleRepository, InMemoryUserRepository

PREMIUM = FilmAccessContext(creation_date=date(9999, 1, 1))


@pytest.fixture
async def redis_client() -> AsyncGenerator[Redis, None]:
    """Клиент настоящего Redis: гонку проверяют боевые Lua-скрипты."""
    settings = RedisSettings()
    client = Redis(host=settings.host, port=settings.port)
    yield client
    await client.aclose()


class PausingRoleRepository(InMemoryRoleRepository):
    """Чтение прав возвращает результат, но отдаёт его только по сигналу."""

    def __init__(self) -> None:
        super().__init__()
        self.pause_next_read = False
        self.read_done = asyncio.Event()
        self.resume = asyncio.Event()

    async def get_active_permissions_and_nearest_expiry(
        self,
        user_id: UUID,
    ) -> tuple[set[str], datetime | None]:
        """Читает права, затем ждёт сигнала перед возвратом результата."""
        result = await super().get_active_permissions_and_nearest_expiry(user_id)
        if self.pause_next_read:
            self.pause_next_read = False
            self.read_done.set()  # данные из «PostgreSQL» уже прочитаны
            await self.resume.wait()  # медленный ответ / GC / переключение
        return result


@pytest.mark.parametrize('scenario', ['refund', 'payment'])
async def test_snapshot_read_before_access_change_is_not_cached(
    scenario: str,
    redis_client: Redis,
) -> None:
    """Снимок, прочитанный до смены доступа, не попадает в кэш после неё."""
    redis = redis_client
    namespace = f'authz-race-{uuid4()}'
    cache = RedisAuthorizationCache(redis, ttl_seconds=300, namespace=namespace)
    users = InMemoryUserRepository()
    roles = PausingRoleRepository()
    service = AuthorizationService(
        users=users,
        roles=roles,
        authorization_cache=cache,
        film_policy=create_film_access_policy(),
    )
    user = await users.create(
        login='RaceUser',
        password_hash='h',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    roles.users.add(user.id)
    await roles.create(name='subscriber', permission_codes={'films.early_access:read'})
    current = CurrentUser(
        user_id=user.id,
        token_id=uuid4(),
        session_epoch=uuid4(),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )
    paid_until = datetime.now(UTC) + timedelta(days=30)

    try:
        if scenario == 'refund':
            await service.put_subscription_access(
                user.id, version=1, expires_at=paid_until
            )
            before, after_expected, new_expiry = True, False, None
        else:
            before, after_expected, new_expiry = False, True, paid_until

        # 1. Проверка прав читает «PostgreSQL» и замирает до store_snapshot.
        roles.pause_next_read = True
        reader = asyncio.create_task(service.can_read_film(current, PREMIUM))
        await roles.read_done.wait()

        # 2. Billing доставляет новую версию доступа — мутация завершается целиком.
        applied, _ = await service.put_subscription_access(
            user.id,
            version=2,
            expires_at=new_expiry,
        )
        assert applied
        marker = await redis.exists(f'{{{namespace}}}:user-changing:{user.id}')
        assert marker == 0, 'маркер уже снят: защищает только поколение'

        # 3. Старый запрос дописывает снимок.
        roles.resume.set()
        assert await reader is before

        # 4. Новая проверка совпадает с PostgreSQL, а не с устаревшим снимком.
        db_answer = 'films.early_access:read' in await roles.get_permissions_for_user(
            user.id
        )
        assert db_answer is after_expected
        assert await service.can_read_film(current, PREMIUM) is after_expected
    finally:
        keys = [k async for k in redis.scan_iter(match=f'{{{namespace}}}:*')]
        if keys:
            await redis.unlink(*keys)
