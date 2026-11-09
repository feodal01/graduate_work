"""Гонка кэша прав на фейке: ловится и обычным `pytest` без Redis.

Тот же сценарий, что в test_authorization_cache_race.py (там — боевые
Lua-скрипты на настоящем Redis, тест помечен integration). Здесь фейк
кэша повторяет семантику поколения пользователя.
"""

import asyncio
from datetime import UTC, date, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from services.authorization import AuthorizationService
from services.film_access import FilmAccessContext, create_film_access_policy
from services.types import CurrentUser
from tests.fakes import (
    InMemoryAuthorizationCache,
    InMemoryRoleRepository,
    InMemoryUserRepository,
)

PREMIUM = FilmAccessContext(creation_date=date(9999, 1, 1))


class _PausingRoles(InMemoryRoleRepository):
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
            self.read_done.set()
            await self.resume.wait()
        return result


@pytest.mark.parametrize('scenario', ['refund', 'payment'])
async def test_snapshot_read_before_access_change_is_not_cached(scenario: str) -> None:
    """Снимок, прочитанный до смены доступа, не попадает в кэш после неё."""
    users = InMemoryUserRepository()
    roles = _PausingRoles()
    cache = InMemoryAuthorizationCache()
    service = AuthorizationService(
        users=users,
        roles=roles,
        authorization_cache=cache,
        film_policy=create_film_access_policy(),
    )
    user = await users.create(
        login='RaceUnit',
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
    if scenario == 'refund':
        await service.put_subscription_access(user.id, version=1, expires_at=paid_until)
        new_expiry, expected = None, False
    else:
        new_expiry, expected = paid_until, True

    roles.pause_next_read = True
    reader = asyncio.create_task(service.can_read_film(current, PREMIUM))
    await roles.read_done.wait()
    await service.put_subscription_access(user.id, version=2, expires_at=new_expiry)
    roles.resume.set()
    await reader

    assert await service.can_read_film(current, PREMIUM) is expected
