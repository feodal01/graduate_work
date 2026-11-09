from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import date

import httpx
import pytest

from dependencies import (
    get_authentication_service,
    get_authorization_service,
    get_rate_limiter,
    get_service_credentials,
)
from main import create_app
from models import User
from schemas.authorization import PermissionCheckRequest
from security.passwords import PasswordHasher
from security.service_credentials import ServiceCredentials
from security.tokens import TokenService
from services.authentication import AuthenticationService
from services.authorization import (
    FILM_EARLY_ACCESS_READ,
    AuthorizationService,
)
from services.film_access import (
    FilmAccessContext,
    FilmAccessPolicy,
    RecentFilmAccessRule,
)
from services.types import ClientInfo
from tests.fakes import (
    InMemoryAuthorizationCache,
    InMemoryLoginHistoryRepository,
    InMemoryRoleRepository,
    InMemoryTokenSessionStore,
    InMemoryUserRepository,
    ProgrammableRateLimiter,
)
from validation.credentials import PasswordPolicy

SECRET = 'test-secret-that-is-long-enough-for-hs256'
SERVICE_KEY = 'test-internal-service-key-with-32-characters'


@dataclass
class ApiContext:
    client: httpx.AsyncClient
    authentication: AuthenticationService
    authorization: AuthorizationService
    users: InMemoryUserRepository
    roles: InMemoryRoleRepository
    authorization_cache: InMemoryAuthorizationCache
    rate_limiter: ProgrammableRateLimiter


@pytest.fixture
async def context() -> AsyncGenerator[ApiContext, None]:
    """Создаёт HTTP-клиент и зависимости API авторизации."""
    users = InMemoryUserRepository()
    roles = InMemoryRoleRepository()
    authentication = AuthenticationService(
        users=users,
        login_history=InMemoryLoginHistoryRepository(),
        token_sessions=InMemoryTokenSessionStore(),
        password_hasher=PasswordHasher(),
        password_policy=PasswordPolicy(min_length=8, min_score=3),
        token_service=TokenService(
            secret_key=SECRET,
            algorithm='HS256',
            access_ttl_seconds=900,
            refresh_ttl_seconds=604800,
        ),
    )
    authorization_cache = InMemoryAuthorizationCache()
    authorization = AuthorizationService(
        users=users,
        roles=roles,
        authorization_cache=authorization_cache,
        film_policy=FilmAccessPolicy(
            rules=(RecentFilmAccessRule(today=lambda: date(2026, 8, 15)),)
        ),
    )
    rate_limiter = ProgrammableRateLimiter()
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_authentication_service] = lambda: authentication
    app.dependency_overrides[get_authorization_service] = lambda: authorization
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )
    app.dependency_overrides[get_rate_limiter] = lambda: rate_limiter
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'auth-test-request'},
    ) as client:
        yield ApiContext(
            client,
            authentication,
            authorization,
            users,
            roles,
            authorization_cache,
            rate_limiter,
        )


async def create_user(
    context: ApiContext,
    *,
    login: str,
    superuser: bool = False,
) -> tuple[User, str]:
    """Создаёт пользователя и возвращает его access-токен."""
    user = await context.authentication.signup(
        login=login,
        password='correct horse',
        first_name=None,
        last_name=None,
        tz='Europe/Moscow',
    )
    user.is_superuser = superuser
    context.roles.users.add(user.id)
    pair = await context.authentication.login(
        login=login,
        password='correct horse',
        client=ClientInfo(ip_address=None, user_agent=None),
    )
    return user, pair.access.value


def service_headers(access_token: str | None = None) -> dict[str, str]:
    """Формирует служебные заголовки для проверки разрешений."""
    headers = {'X-Service-Key': SERVICE_KEY}
    if access_token is not None:
        headers['Authorization'] = f'Bearer {access_token}'
    return headers


def film_request(creation_date: str | None) -> dict[str, object]:
    """Формирует запрос на проверку доступа к фильму."""
    return {
        'resource': 'film',
        'action': 'read',
        'attributes': {'creation_date': creation_date},
    }


@pytest.mark.asyncio
async def test_service_rate_limit_runs_only_after_service_authentication(
    context: ApiContext,
) -> None:
    """Only a verified Movies API call can spend the stable service bucket."""
    invalid = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request('2020-01-01'),
        headers={'X-Service-Key': 'wrong-key'},
    )

    assert invalid.status_code == 401
    assert context.rate_limiter.calls == []

    valid = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request('2020-01-01'),
        headers=service_headers(),
    )

    assert valid.status_code == 200
    assert [(call[0].name, call[1]) for call in context.rate_limiter.calls] == [
        ('service', 'internal-service:permission-check')
    ]


@pytest.mark.asyncio
async def test_admin_rate_limit_uses_actor_instead_of_target_user(
    context: ApiContext,
) -> None:
    """Role assignment spends the administrator bucket, not the target bucket."""
    root, root_token = await create_user(
        context,
        login='Root',
        superuser=True,
    )
    target, _target_token = await create_user(context, login='Alice')
    role = await context.roles.create(name='viewer', permission_codes=set())
    context.rate_limiter.calls.clear()

    response = await context.client.post(
        f'/api/v1/auth/users/{target.id}/roles',
        json={'role_id': str(role.id)},
        headers={'Authorization': f'Bearer {root_token}'},
    )

    assert response.status_code == 204
    assert [(call[0].name, call[1]) for call in context.rate_limiter.calls] == [
        ('admin', str(root.id))
    ]


def test_permission_check_request_builds_film_access_context() -> None:
    """HTTP attributes must reach the access policy as its domain context."""
    request = PermissionCheckRequest.model_validate(film_request('2023-08-16'))

    assert request.attributes == FilmAccessContext(creation_date=date(2023, 8, 16))


@pytest.mark.parametrize('attributes', ({}, {'creation_date': None}))
def test_permission_check_accepts_missing_creation_date(
    attributes: dict[str, object],
) -> None:
    """A missing date must become an explicit unknown film date."""
    request = PermissionCheckRequest.model_validate(
        {
            'resource': 'film',
            'action': 'read',
            'attributes': attributes,
        }
    )

    assert request.attributes == FilmAccessContext(creation_date=None)


@pytest.mark.asyncio
async def test_permission_check_denies_unknown_date_for_anonymous_user(
    context: ApiContext,
) -> None:
    """An unknown date requires early-access permission."""
    response = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request(None),
        headers=service_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {'allowed': False}


@pytest.mark.asyncio
async def test_permission_check_allows_unknown_date_for_subscriber(
    context: ApiContext,
) -> None:
    """A subscriber can read a film with an unknown date."""
    user, token = await create_user(context, login='Alice')
    subscriber = await context.roles.create(
        name='subscriber',
        permission_codes={FILM_EARLY_ACCESS_READ},
    )
    await context.roles.assign(user.id, subscriber.id)

    response = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request(None),
        headers=service_headers(token),
    )

    assert response.status_code == 200
    assert response.json() == {'allowed': True}


@pytest.mark.asyncio
async def test_permission_check_allows_unknown_date_for_superuser(
    context: ApiContext,
) -> None:
    """A superuser can read a film with an unknown date."""
    _user, token = await create_user(context, login='Root', superuser=True)

    response = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request(None),
        headers=service_headers(token),
    )

    assert response.status_code == 200
    assert response.json() == {'allowed': True}


@pytest.mark.asyncio
async def test_permission_check_requires_valid_internal_service_key(
    context: ApiContext,
) -> None:
    """Проверка прав отклоняет отсутствующий и неверный service key."""
    missing = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request('2020-01-01'),
    )
    invalid = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request('2020-01-01'),
        headers={'X-Service-Key': 'wrong-key'},
    )

    assert missing.status_code == invalid.status_code == 401
    assert missing.json() == invalid.json() == {'detail': 'Invalid service credentials'}


@pytest.mark.asyncio
async def test_permission_check_allows_public_film_for_anonymous_user(
    context: ApiContext,
) -> None:
    """Анонимный пользователь получает доступ к публичному фильму."""
    response = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request('2023-08-15'),
        headers=service_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {'allowed': True}


@pytest.mark.asyncio
async def test_permission_check_denies_early_film_for_anonymous_user(
    context: ApiContext,
) -> None:
    """Анонимный пользователь не получает ранний доступ к фильму."""
    response = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request('2023-08-16'),
        headers=service_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {'allowed': False}


@pytest.mark.asyncio
async def test_permission_check_allows_subscriber_role(
    context: ApiContext,
) -> None:
    """Роль subscriber разрешает ранний доступ к фильму."""
    user, token = await create_user(context, login='Alice')
    subscriber = await context.roles.create(
        name='subscriber',
        permission_codes={FILM_EARLY_ACCESS_READ},
    )
    await context.roles.assign(user.id, subscriber.id)

    response = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request('2023-08-16'),
        headers=service_headers(token),
    )

    assert response.status_code == 200
    assert response.json() == {'allowed': True}


@pytest.mark.asyncio
async def test_permission_check_preserves_invalid_user_token_error(
    context: ApiContext,
) -> None:
    """Невалидный JWT пользователя возвращает HTTP 401."""
    response = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request('2020-01-01'),
        headers=service_headers('not-a-jwt'),
    )

    assert response.status_code == 401
    assert response.json() == {'detail': 'Invalid token'}


@pytest.mark.asyncio
async def test_non_admin_cannot_manage_roles(context: ApiContext) -> None:
    """Пользователь без разрешения не может управлять ролями."""
    _user, token = await create_user(context, login='Alice')

    response = await context.client.get(
        '/api/v1/auth/roles',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == 403
    assert response.json() == {'detail': 'Forbidden'}


@pytest.mark.asyncio
async def test_superuser_can_manage_roles(context: ApiContext) -> None:
    """Суперпользователь может выполнить все операции с ролями."""
    _root, token = await create_user(context, login='Root', superuser=True)
    headers = {'Authorization': f'Bearer {token}'}

    created = await context.client.post(
        '/api/v1/auth/roles',
        json={
            'name': 'subscriber',
            'permissions': [FILM_EARLY_ACCESS_READ],
        },
        headers=headers,
    )
    role_id = created.json()['id']
    listed = await context.client.get('/api/v1/auth/roles', headers=headers)
    updated = await context.client.patch(
        f'/api/v1/auth/roles/{role_id}',
        json={'name': 'premium'},
        headers=headers,
    )
    deleted = await context.client.delete(
        f'/api/v1/auth/roles/{role_id}',
        headers=headers,
    )

    assert created.status_code == 201
    assert created.json()['permissions'] == [FILM_EARLY_ACCESS_READ]
    assert listed.status_code == 200
    assert [item['name'] for item in listed.json()] == ['subscriber']
    assert updated.status_code == 200
    assert updated.json()['name'] == 'premium'
    assert deleted.status_code == 204
    assert deleted.content == b''


@pytest.mark.asyncio
async def test_role_api_translates_duplicates_and_missing_roles(
    context: ApiContext,
) -> None:
    """API переводит дубликат и отсутствие роли в HTTP 409 и 404."""
    _root, token = await create_user(context, login='Root', superuser=True)
    headers = {'Authorization': f'Bearer {token}'}
    payload = {'name': 'viewer', 'permissions': []}
    await context.client.post('/api/v1/auth/roles', json=payload, headers=headers)

    duplicate = await context.client.post(
        '/api/v1/auth/roles',
        json=payload,
        headers=headers,
    )
    missing = await context.client.delete(
        '/api/v1/auth/roles/00000000-0000-0000-0000-000000000099',
        headers=headers,
    )

    assert duplicate.status_code == 409
    assert duplicate.json() == {'detail': 'Role already exists'}
    assert missing.status_code == 404
    assert missing.json() == {'detail': 'Role not found'}


@pytest.mark.asyncio
async def test_superuser_can_assign_and_revoke_role(context: ApiContext) -> None:
    """Суперпользователь может назначить и отозвать роль."""
    root, root_token = await create_user(
        context,
        login='Root',
        superuser=True,
    )
    target, _target_token = await create_user(context, login='Alice')
    role = await context.roles.create(name='viewer', permission_codes=set())
    headers = {'Authorization': f'Bearer {root_token}'}

    assigned = await context.client.post(
        f'/api/v1/auth/users/{target.id}/roles',
        json={'role_id': str(role.id)},
        headers=headers,
    )
    duplicate = await context.client.post(
        f'/api/v1/auth/users/{target.id}/roles',
        json={'role_id': str(role.id)},
        headers=headers,
    )
    revoked = await context.client.delete(
        f'/api/v1/auth/users/{target.id}/roles/{role.id}',
        headers=headers,
    )
    missing = await context.client.delete(
        f'/api/v1/auth/users/{target.id}/roles/{role.id}',
        headers=headers,
    )

    assert root.is_superuser is True
    assert assigned.status_code == 204
    assert duplicate.status_code == 409
    assert duplicate.json() == {'detail': 'Role already assigned'}
    assert revoked.status_code == 204
    assert missing.status_code == 404
    assert missing.json() == {'detail': 'Role assignment not found'}


@pytest.mark.asyncio
async def test_role_api_rejects_unknown_permission(
    context: ApiContext,
) -> None:
    """Неизвестный код разрешения отклоняется с HTTP 422."""
    _root, token = await create_user(context, login='Root', superuser=True)

    response = await context.client.post(
        '/api/v1/auth/roles',
        json={'name': 'invalid', 'permissions': ['films:unknown']},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == 422
    assert response.json()['detail'][0]['loc'] == [
        'body',
        'permissions',
        0,
    ]


@pytest.mark.asyncio
async def test_role_assignment_reports_unknown_user(
    context: ApiContext,
) -> None:
    """Назначение роли отсутствующему пользователю возвращает HTTP 404."""
    _root, token = await create_user(context, login='Root', superuser=True)
    role = await context.roles.create(name='viewer', permission_codes=set())

    response = await context.client.post(
        '/api/v1/auth/users/00000000-0000-0000-0000-000000000099/roles',
        json={'role_id': str(role.id)},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == 404
    assert response.json() == {'detail': 'User not found'}


@pytest.mark.asyncio
async def test_role_assignment_reports_unavailable_cache(
    context: ApiContext,
) -> None:
    """Недоступная защита инвалидации возвращает HTTP 503 до записи."""
    _root, token = await create_user(context, login='Root', superuser=True)
    target, _target_token = await create_user(context, login='Alice')
    role = await context.roles.create(name='viewer', permission_codes=set())
    context.authorization_cache.fail_mutations = True

    response = await context.client.post(
        f'/api/v1/auth/users/{target.id}/roles',
        json={'role_id': str(role.id)},
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == 503
    assert response.json() == {'detail': 'Cache is unavailable'}
    assert (target.id, role.id) not in context.roles.assignments


@pytest.mark.asyncio
async def test_internal_endpoints_spend_separate_service_buckets(
    context: ApiContext,
) -> None:
    """Проверки прав от movies-api не тратят квоту доставки доступа из Billing.

    Регрессия: все внутренние вызовы делили одну корзину `movies-api`, и при
    потоке просмотров фильмов Billing получал 429 на AUTH-API-01.
    """
    user, _ = await create_user(context, login='BucketUser')
    context.roles.users.add(user.id)
    await context.roles.create(
        name='subscriber',
        permission_codes={FILM_EARLY_ACCESS_READ},
    )

    check = await context.client.post(
        '/api/v1/auth/permissions/check',
        json=film_request('2020-01-01'),
        headers=service_headers(),
    )
    access = await context.client.put(
        f'/api/v1/internal/users/{user.id}/subscription-access',
        json={'version': 1, 'expires_at': None},
        headers=service_headers(),
    )

    assert check.status_code == 200
    assert access.status_code == 200, access.text
    identities = [identity for _, identity in context.rate_limiter.calls]
    assert identities == [
        'internal-service:permission-check',
        'internal-service:subscription-access',
    ]
