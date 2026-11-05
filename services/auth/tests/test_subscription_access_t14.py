from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from dependencies import (
    get_authorization_cache,
    get_optional_current_user,
    get_rate_limiter,
    get_service_credentials,
)
from main import create_app
from models import (
    Base,
    User,
    UserSubscriptionAccessState,
    user_roles,
)
from repositories.errors import UserNotFoundError
from security.service_credentials import ServiceCredentials
from services.authorization import AuthorizationService
from services.film_access import FilmAccessContext, create_film_access_policy
from services.rate_limit import RateLimitDecision
from services.types import CurrentUser
from tests.fakes import (
    InMemoryAuthorizationCache,
    InMemoryRoleRepository,
    InMemoryUserRepository,
    ProgrammableRateLimiter,
)
from tests.test_role_repositories import create_user, session  # noqa: F401

SERVICE_KEY = 'billing-test-service-key-that-is-long-enough'


def test_subscription_access_models_and_columns_registered() -> None:
    """AUTH-FR-01, AUTH-FR-02: таблица user_subscription_access_state и expires_at."""
    assert 'user_subscription_access_state' in Base.metadata.tables
    state_table = Base.metadata.tables['user_subscription_access_state']
    assert 'user_id' in state_table.c
    assert 'access_version' in state_table.c
    assert 'updated_at' in state_table.c
    fk = next(iter(state_table.c.user_id.foreign_keys))
    assert fk.target_fullname == 'users.id'
    assert fk.ondelete == 'CASCADE'

    assert 'expires_at' in user_roles.c
    assert user_roles.c.expires_at.nullable is True


async def test_subscription_access_out_of_order_versions_in_service() -> None:
    """AUTH-FR-01, инв. 11: внеочередная доставка версий 1..4 сохраняет версию 4."""
    users = InMemoryUserRepository()
    roles = InMemoryRoleRepository()
    cache = InMemoryAuthorizationCache()
    user = await users.create(
        login='SubscriberUser',
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

    future_t1 = datetime(2026, 12, 1, 12, 0, tzinfo=UTC)
    future_t3 = datetime(2027, 1, 1, 12, 0, tzinfo=UTC)

    # 1. Первой приходит версия 4 («доступа нет»), когда в БД ещё ничего не было
    applied_4, current_4 = await auth_service.put_subscription_access(
        user.id,
        version=4,
        expires_at=None,
    )
    assert applied_4 is True
    assert current_4 == 4
    assert user.id in cache.user_mutations

    # 2. Запоздало приходят версии 1, 2, 3 и повтор 4 -> applied=False
    for old_ver, exp in [
        (1, future_t1),
        (2, None),
        (3, future_t3),
        (4, future_t3),
    ]:
        applied, current = await auth_service.put_subscription_access(
            user.id,
            version=old_ver,
            expires_at=exp,
        )
        assert applied is False
        assert current == 4

    assert await roles.get_permissions_for_user(user.id) == set()

    # 3. Приходит новая версия 5 -> заполняем кэш и проверяем отзыв (версия 6)
    applied_5, current_5 = await auth_service.put_subscription_access(
        user.id,
        version=5,
        expires_at=future_t3,
    )
    assert applied_5 is True
    assert current_5 == 5
    cur_user = CurrentUser(
        user_id=user.id,
        token_id=uuid4(),
        session_epoch=uuid4(),
        expires_at=future_t3,
    )
    assert await auth_service.can_read_film(
        cur_user,
        FilmAccessContext(creation_date=date(9999, 1, 1)),
    )
    assert user.id in cache.items

    # Версия 6 («доступа нет») немедленно удаляет снимок из cache.items (AUTH-NFR-R-01)
    applied_6, current_6 = await auth_service.put_subscription_access(
        user.id,
        version=6,
        expires_at=None,
    )
    assert (applied_6, current_6) == (True, 6)
    assert user.id not in cache.items
    assert not await auth_service.can_read_film(
        cur_user,
        FilmAccessContext(creation_date=date(9999, 1, 1)),
    )

    # 4. Неизвестный пользователь -> UserNotFoundError (404)
    with pytest.raises(UserNotFoundError):
        await auth_service.put_subscription_access(
            uuid4(),
            version=1,
            expires_at=future_t3,
        )


@pytest.mark.parametrize(
    ('header_name', 'header_value', 'expected_status'),
    [
        ('X-Service-Api-Key', SERVICE_KEY, 200),
        ('X-Service-Key', SERVICE_KEY, 200),
        ('X-Service-Api-Key', 'wrong-key', 401),
        (None, None, 401),
    ],
)
async def test_subscription_access_api_auth_validation_and_cache_invalidation(
    header_name: str | None,
    header_value: str | None,
    expected_status: int,
) -> None:
    """AUTH-API-01, AUTH-NFR-SEC-01, AUTH-NFR-R-01: проверка ключа, версий и кэша."""
    users = InMemoryUserRepository()
    roles = InMemoryRoleRepository()
    cache = InMemoryAuthorizationCache()
    limiter = ProgrammableRateLimiter()
    user = await users.create(
        login='ApiSubUser',
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

    from dependencies import get_authorization_service

    cur_user = CurrentUser(
        user_id=user.id,
        token_id=uuid4(),
        session_epoch=uuid4(),
        expires_at=datetime(2027, 1, 1, tzinfo=UTC),
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )
    app.dependency_overrides[get_rate_limiter] = lambda: limiter
    app.dependency_overrides[get_authorization_cache] = lambda: cache
    app.dependency_overrides[get_optional_current_user] = lambda: cur_user
    app.dependency_overrides[get_authorization_service] = lambda: AuthorizationService(
        users=users,
        roles=roles,
        authorization_cache=cache,
        film_policy=create_film_access_policy(),
    )

    headers = {'X-Request-Id': 't14-req'}
    if header_name and header_value:
        headers[header_name] = header_value

    expires = datetime(2026, 12, 31, 15, 0, tzinfo=UTC)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers=headers,
    ) as client:
        resp = await client.put(
            f'/api/v1/internal/users/{user.id}/subscription-access',
            json={'version': 1, 'expires_at': expires.isoformat()},
        )
        assert resp.status_code == expected_status
        if expected_status == 200:
            assert resp.json() == {'applied': True, 'current_version': 1}

            # Проверяем AUTH-NFR-R-01 через /api/v1/auth/permissions/check:
            # 1) заполняем кэш -> allowed: True
            chk_allowed = await client.post(
                '/api/v1/auth/permissions/check',
                json={
                    'resource': 'film',
                    'action': 'read',
                    'attributes': {'creation_date': '9999-01-01'},
                },
            )
            assert chk_allowed.status_code == 200
            assert chk_allowed.json() == {'allowed': True}
            assert user.id in cache.items

            # 2) отзываем доступ -> снимок удалён, permissions/check -> allowed: False
            revoke_resp = await client.put(
                f'/api/v1/internal/users/{user.id}/subscription-access',
                json={'version': 2, 'expires_at': None},
            )
            assert revoke_resp.status_code == 200
            assert revoke_resp.json() == {'applied': True, 'current_version': 2}
            chk_denied = await client.post(
                '/api/v1/auth/permissions/check',
                json={
                    'resource': 'film',
                    'action': 'read',
                    'attributes': {'creation_date': '9999-01-01'},
                },
            )
            assert chk_denied.status_code == 200
            assert chk_denied.json() == {'allowed': False}

            # Невалидные тела запросов -> 422
            invalid_payloads: list[dict[str, Any]] = [
                {'version': 0, 'expires_at': None},
                {'version': '3', 'expires_at': None},
                {'version': True, 'expires_at': None},
                {'version': 3, 'expires_at': '2026-12-31T15:00:00'},
                {'version': 3, 'expires_at': 'not-a-date'},
                {'version': 3, 'expires_at': None, 'extra_field': 1},
            ]
            for bad_body in invalid_payloads:
                bad_res = await client.put(
                    f'/api/v1/internal/users/{user.id}/subscription-access',
                    json=bad_body,
                )
                assert bad_res.status_code == 422

            # Несуществующий пользователь -> 404
            missing_user = await client.put(
                f'/api/v1/internal/users/{uuid4()}/subscription-access',
                json={'version': 1, 'expires_at': None},
            )
            assert missing_user.status_code == 404

            # Отказ БД -> 503
            roles.fail_db = True
            db_down = await client.put(
                f'/api/v1/internal/users/{user.id}/subscription-access',
                json={'version': 3, 'expires_at': None},
            )
            assert db_down.status_code == 503
            roles.fail_db = False

            # При недоступном Redis (fail_mutations=True) возвращается 503
            cache.fail_mutations = True
            redis_down = await client.put(
                f'/api/v1/internal/users/{user.id}/subscription-access',
                json={'version': 3, 'expires_at': None},
            )
            assert redis_down.status_code == 503
            cache.fail_mutations = False

            # Превышение сервисного rate limit -> 429 (AUTH-NFR-SEC-01)
            limiter.decision = RateLimitDecision(
                allowed=False,
                remaining=0,
                retry_after_seconds=3,
            )
            rate_limited = await client.put(
                f'/api/v1/internal/users/{user.id}/subscription-access',
                json={'version': 3, 'expires_at': None},
            )
            assert rate_limited.status_code == 429
            assert rate_limited.headers.get('Retry-After') == '3'


async def test_sqlalchemy_subscription_access_atomic_persistence_and_admin_preservation(
    session: AsyncSession,  # noqa: F811
) -> None:
    """AUTH-FR-01, AUTH-FR-02 (PostgreSQL): версии, срок subscriber и роль admin."""
    from repositories.roles import SQLAlchemyRoleRepository

    repo = SQLAlchemyRoleRepository(session)
    user = await create_user(session, login='PostgresSubUser')
    admin_role = await repo.create(name='admin', permission_codes={'roles:manage'})
    sub_role = await repo.create(
        name='subscriber',
        permission_codes={'films.early_access:read'},
    )
    user_id = user.id
    admin_role_id = admin_role.id
    sub_role_id = sub_role.id
    await repo.assign(user_id, admin_role_id)

    t1 = datetime.now(UTC) + timedelta(days=30)
    t2 = datetime.now(UTC) + timedelta(days=60)

    # 1. Применяем версию 2 (доступ открыт до t1)
    applied_2, cur_2 = await repo.set_subscription_access(
        user_id,
        version=2,
        expires_at=t1,
    )
    assert (applied_2, cur_2) == (True, 2)

    # 2. Старая версия 1 игнорируется (applied=False, current_version=2)
    applied_1, cur_1 = await repo.set_subscription_access(
        user_id,
        version=1,
        expires_at=None,
    )
    assert (applied_1, cur_1) == (False, 2)

    # Проверяем, что expires_at записан у subscriber, а у admin остаётся NULL
    sub_row = (
        await session.execute(
            select(user_roles.c.expires_at).where(
                user_roles.c.user_id == user_id,
                user_roles.c.role_id == sub_role_id,
            )
        )
    ).one()
    assert sub_row[0] == t1

    admin_row = (
        await session.execute(
            select(user_roles.c.expires_at).where(
                user_roles.c.user_id == user_id,
                user_roles.c.role_id == admin_role_id,
            )
        )
    ).one()
    assert admin_row[0] is None

    # 3. Обновление срока (версия 3 -> t2) обновляет expires_at у subscriber
    applied_3, cur_3 = await repo.set_subscription_access(
        user_id,
        version=3,
        expires_at=t2,
    )
    assert (applied_3, cur_3) == (True, 3)

    # 4. Отзыв доступа (версия 4) удаляет subscriber, сохраняя версию 4 и admin
    applied_4, cur_4 = await repo.set_subscription_access(
        user_id,
        version=4,
        expires_at=None,
    )
    assert (applied_4, cur_4) == (True, 4)

    state = await session.get(UserSubscriptionAccessState, user_id)
    assert state is not None and state.access_version == 4
    assert await repo.get_permissions_for_user(user_id) == {'roles:manage'}

    # 5. Проверяем ON DELETE CASCADE при удалении пользователя
    await session.execute(delete(user_roles).where(user_roles.c.user_id == user_id))
    await session.execute(delete(User).where(User.id == user_id))
    await session.commit()
    session.expire_all()
    assert await session.get(UserSubscriptionAccessState, user_id) is None
