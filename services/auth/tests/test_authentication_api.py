from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import jwt
import pytest

from dependencies import get_authentication_service, get_rate_limiter
from main import create_app
from security.passwords import PasswordHasher
from security.tokens import TokenService
from services.authentication import AuthenticationService
from tests.fakes import (
    InMemoryLoginHistoryRepository,
    InMemoryTokenSessionStore,
    InMemoryUserRepository,
    ProgrammableRateLimiter,
)
from validation.credentials import PasswordPolicy, validate_login

SECRET = 'test-secret-that-is-long-enough-for-hs256'


@pytest.fixture
def user_repository() -> InMemoryUserRepository:
    """Создаёт изолированное хранилище пользователей."""
    return InMemoryUserRepository()


@pytest.fixture
def authentication_service(
    user_repository: InMemoryUserRepository,
) -> AuthenticationService:
    """Создаёт сервис аутентификации с изолированными хранилищами."""
    return AuthenticationService(
        users=user_repository,
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


@pytest.fixture
def rate_limiter() -> ProgrammableRateLimiter:
    """Allow requests that are unrelated to rate-limit behavior."""
    return ProgrammableRateLimiter()


@pytest.fixture
async def client(
    authentication_service: AuthenticationService,
    rate_limiter: ProgrammableRateLimiter,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    """Создаёт HTTP-клиент с тестовым сервисом аутентификации."""
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_authentication_service] = lambda: (
        authentication_service
    )
    app.dependency_overrides[get_rate_limiter] = lambda: rate_limiter
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'auth-test-request'},
    ) as test_client:
        yield test_client


async def signup(
    client: httpx.AsyncClient,
    login: str = 'Alice',
) -> dict[str, object]:
    """Регистрирует пользователя через HTTP API и возвращает ответ."""
    response = await client.post(
        '/api/v1/auth/signup',
        json={
            'tz': 'Europe/Moscow',
            'login': login,
            'password': 'correct horse',
            'first_name': None,
            'last_name': None,
        },
    )
    assert response.status_code == 201
    return response.json()


async def login(
    client: httpx.AsyncClient,
    login_name: str = 'Alice',
) -> dict[str, str]:
    """Выполняет вход через HTTP API и возвращает пару токенов."""
    response = await client.post(
        '/api/v1/auth/login',
        json={'login': login_name, 'password': 'correct horse'},
        headers={'user-agent': 'api-test'},
    )
    assert response.status_code == 200
    return response.json()


def bearer(token: str) -> dict[str, str]:
    """Формирует заголовок авторизации с bearer-токеном."""
    return {'Authorization': f'Bearer {token}'}


@pytest.mark.asyncio
async def test_signup_rate_limit_uses_validated_case_sensitive_login(
    client: httpx.AsyncClient,
    rate_limiter: ProgrammableRateLimiter,
) -> None:
    """Signup strips outer spaces but preserves login case for its bucket."""
    response = await client.post(
        '/api/v1/auth/signup',
        json={'tz': 'Europe/Moscow', 'login': '  Alice  ', 'password': 'correct horse'},
    )

    assert response.status_code == 201
    assert [(call[0].name, call[1]) for call in rate_limiter.calls] == [
        ('signup', 'Alice')
    ]


@pytest.mark.asyncio
async def test_login_rate_limit_uses_validated_case_sensitive_login(
    client: httpx.AsyncClient,
    rate_limiter: ProgrammableRateLimiter,
) -> None:
    """Login uses the same normalized identity as credential validation."""
    await signup(client)
    rate_limiter.calls.clear()

    response = await client.post(
        '/api/v1/auth/login',
        json={'login': '  Alice  ', 'password': 'correct horse'},
    )

    assert response.status_code == 200
    assert [(call[0].name, call[1]) for call in rate_limiter.calls] == [
        ('login', 'Alice')
    ]


@pytest.mark.asyncio
async def test_refresh_rate_limit_uses_verified_user_id_across_rotations(
    client: httpx.AsyncClient,
    rate_limiter: ProgrammableRateLimiter,
) -> None:
    """Rotated refresh credentials must share one stable user bucket."""
    user = await signup(client)
    pair = await login(client)
    rate_limiter.calls.clear()

    first = await client.post(
        '/api/v1/auth/refresh',
        json={'refresh_token': pair['refresh_token']},
    )
    assert first.status_code == 200

    second = await client.post(
        '/api/v1/auth/refresh',
        json={'refresh_token': first.json()['refresh_token']},
    )

    assert second.status_code == 200
    assert [(call[0].name, call[1]) for call in rate_limiter.calls] == [
        ('refresh', user['id']),
        ('refresh', user['id']),
    ]


@pytest.mark.asyncio
async def test_profile_rate_limit_uses_authenticated_user_id(
    client: httpx.AsyncClient,
    rate_limiter: ProgrammableRateLimiter,
) -> None:
    """A user bucket starts only after access-token authentication succeeds."""
    user = await signup(client)
    pair = await login(client)
    rate_limiter.calls.clear()

    response = await client.get(
        '/api/v1/auth/profile',
        headers=bearer(pair['access_token']),
    )

    assert response.status_code == 200
    assert [(call[0].name, call[1]) for call in rate_limiter.calls] == [
        ('user', user['id'])
    ]


@pytest.mark.asyncio
async def test_signup_returns_public_user_without_password(
    client: httpx.AsyncClient,
) -> None:
    """HTTP response не содержит plaintext или hash."""
    body = await signup(client)

    assert body['login'] == 'Alice'
    assert 'id' in body
    assert 'created_at' in body
    assert 'password' not in body
    assert 'password_hash' not in body


@pytest.mark.asyncio
async def test_signup_duplicate_login_is_conflict(
    client: httpx.AsyncClient,
) -> None:
    """Unique violation переводится в HTTP 409."""
    await signup(client)

    response = await client.post(
        '/api/v1/auth/signup',
        json={'tz': 'Europe/Moscow', 'login': 'Alice', 'password': 'another horse'},
    )

    assert response.status_code == 409
    assert response.json() == {'detail': 'Login already taken'}


@pytest.mark.asyncio
async def test_login_returns_bearer_pair_and_records_client_context(
    client: httpx.AsyncClient,
) -> None:
    """Login возвращает оба JWT и пишет history."""
    await signup(client)

    body = await login(client)

    assert set(body) == {'access_token', 'refresh_token', 'token_type'}
    assert body['token_type'] == 'bearer'
    assert body['access_token'] != body['refresh_token']


@pytest.mark.asyncio
async def test_invalid_login_credentials_share_one_response(
    client: httpx.AsyncClient,
) -> None:
    """HTTP API не раскрывает наличие login."""
    await signup(client)

    unknown = await client.post(
        '/api/v1/auth/login',
        json={'login': 'Unknown', 'password': 'correct horse'},
    )
    wrong = await client.post(
        '/api/v1/auth/login',
        json={'login': 'Alice', 'password': 'wrong horse'},
    )

    assert unknown.status_code == wrong.status_code == 401
    assert unknown.json() == wrong.json()
    assert wrong.json() == {'detail': 'Invalid credentials'}


@pytest.mark.asyncio
async def test_social_only_user_cannot_use_local_password_login(
    client: httpx.AsyncClient,
    user_repository: InMemoryUserRepository,
) -> None:
    """A social user without a local password gets the standard 401."""
    await user_repository.create(
        login='alice@yandex.ru',
        password_hash=None,
        first_name='Alice',
        last_name='Viewer',
        tz='Europe/Moscow',
    )

    response = await client.post(
        '/api/v1/auth/login',
        json={'login': 'alice@yandex.ru', 'password': 'correct horse'},
    )

    assert response.status_code == 401
    assert response.json() == {'detail': 'Invalid credentials'}


@pytest.mark.asyncio
async def test_refresh_rotates_token_and_rejects_old_value(
    client: httpx.AsyncClient,
) -> None:
    """HTTP refresh сохраняет одноразовую семантику."""
    await signup(client)
    original = await login(client)

    rotated = await client.post(
        '/api/v1/auth/refresh',
        json={'refresh_token': original['refresh_token']},
    )
    reused = await client.post(
        '/api/v1/auth/refresh',
        json={'refresh_token': original['refresh_token']},
    )

    assert rotated.status_code == 200
    assert rotated.json()['refresh_token'] != original['refresh_token']
    assert reused.status_code == 401
    assert reused.json() == {'detail': 'Invalid refresh token'}


@pytest.mark.parametrize(
    'payload',
    [
        {'tz': 'Europe/Moscow', 'login': 'ab', 'password': 'correct horse'},
        {'tz': 'Europe/Moscow', 'login': 'Alice', 'password': 'short'},
        {'tz': 'Europe/Moscow', 'login': 'Alice', 'password': 'password123!'},
    ],
)
@pytest.mark.asyncio
async def test_signup_rejects_invalid_credentials_schema(
    client: httpx.AsyncClient,
    payload: dict[str, str],
) -> None:
    """Mini-module validation должна работать на HTTP boundary."""
    response = await client.post('/api/v1/auth/signup', json=payload)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_credential_validation_outside_request_schema_returns_422() -> None:
    """Service-level credential errors не должны превращаться в HTTP 500."""
    app = create_app(app_lifespan=None)

    @app.get('/test/invalid-credential')
    async def invalid_credential() -> None:
        validate_login('ab')

    transport = httpx.ASGITransport(
        app=app,
        raise_app_exceptions=False,
    )
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'auth-test-request'},
    ) as test_client:
        response = await test_client.get('/test/invalid-credential')

    assert response.status_code == 422
    assert response.json() == {
        'detail': 'login must contain from 3 to 255 characters',
    }


@pytest.mark.asyncio
async def test_protected_route_requires_valid_bearer(
    client: httpx.AsyncClient,
) -> None:
    """Dependency различает missing, expired и invalid JWT."""
    missing = await client.get('/api/v1/auth/login-history')
    invalid = await client.get(
        '/api/v1/auth/login-history',
        headers=bearer('not-a-jwt'),
    )
    now = datetime.now(UTC)
    expired_token = jwt.encode(
        {
            'sub': str(uuid4()),
            'jti': str(uuid4()),
            'type': 'access',
            'session_epoch': str(uuid4()),
            'iat': now - timedelta(minutes=2),
            'exp': now - timedelta(minutes=1),
        },
        SECRET,
        algorithm='HS256',
    )
    expired = await client.get(
        '/api/v1/auth/login-history',
        headers=bearer(expired_token),
    )

    assert missing.status_code == invalid.status_code
    assert invalid.status_code == expired.status_code == 401
    assert missing.json() == {'detail': 'Invalid token'}
    assert invalid.json() == {'detail': 'Invalid token'}
    assert expired.json() == {'detail': 'Token expired'}
    assert missing.headers['www-authenticate'] == 'Bearer'
    assert invalid.headers['www-authenticate'] == 'Bearer'
    assert expired.headers['www-authenticate'] == 'Bearer'


@pytest.mark.asyncio
async def test_profile_updates_login_and_password(
    client: httpx.AsyncClient,
) -> None:
    """Protected PATCH возвращает public user."""
    await signup(client)
    pair = await login(client)

    response = await client.patch(
        '/api/v1/auth/profile',
        json={
            'new_login': 'AliceUpdated',
            'new_password': 'new correct horse',
        },
        headers=bearer(pair['access_token']),
    )

    assert response.status_code == 200
    assert response.json()['login'] == 'AliceUpdated'
    assert 'password' not in response.json()
    assert 'password_hash' not in response.json()
    revoked_access = await client.get(
        '/api/v1/auth/profile',
        headers=bearer(pair['access_token']),
    )
    revoked_refresh = await client.post(
        '/api/v1/auth/refresh',
        json={'refresh_token': pair['refresh_token']},
    )
    old_password = await client.post(
        '/api/v1/auth/login',
        json={'login': 'AliceUpdated', 'password': 'correct horse'},
    )

    assert revoked_access.status_code == 401
    assert revoked_access.json() == {'detail': 'Token revoked'}
    assert revoked_refresh.status_code == 401
    assert revoked_refresh.json() == {'detail': 'Invalid refresh token'}
    assert old_password.status_code == 401
    assert old_password.json() == {'detail': 'Invalid credentials'}
    new_login = await client.post(
        '/api/v1/auth/login',
        json={'login': 'AliceUpdated', 'password': 'new correct horse'},
    )
    assert new_login.status_code == 200


@pytest.mark.asyncio
async def test_stale_profile_password_update_returns_token_revoked(
    client: httpx.AsyncClient,
    authentication_service: AuthenticationService,
    user_repository: InMemoryUserRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis epoch change after auth but before PATCH mutation returns 401."""
    signed_up = await signup(client)
    pair = await login(client)
    token_sessions = authentication_service._token_sessions
    assert isinstance(token_sessions, InMemoryTokenSessionStore)
    validate_access = token_sessions.is_access_valid

    async def validate_then_advance_epoch(
        *,
        user_id: UUID,
        token_id: UUID,
        session_epoch: UUID,
    ) -> bool:
        valid = await validate_access(
            user_id=user_id,
            token_id=token_id,
            session_epoch=session_epoch,
        )
        if valid:
            token_sessions.epochs[user_id] = uuid4()
        return valid

    monkeypatch.setattr(
        token_sessions,
        'is_access_valid',
        validate_then_advance_epoch,
    )

    response = await client.patch(
        '/api/v1/auth/profile',
        json={'new_password': 'new correct horse'},
        headers=bearer(pair['access_token']),
    )

    assert response.status_code == 401
    assert response.json() == {'detail': 'Token revoked'}
    assert user_repository.update_calls == 0
    assert isinstance(signed_up['id'], str)
    user = user_repository.items[UUID(signed_up['id'])]
    assert user.password_hash is not None
    assert PasswordHasher().verify('correct horse', user.password_hash)


@pytest.mark.asyncio
async def test_current_profile_returns_access_token_owner(
    client: httpx.AsyncClient,
) -> None:
    """GET profile должен вернуть профиль владельца access-токена."""
    signed_up = await signup(client)
    pair = await login(client)

    response = await client.get(
        '/api/v1/auth/profile',
        headers=bearer(pair['access_token']),
    )

    assert response.status_code == 200
    assert response.json() == signed_up


@pytest.mark.asyncio
async def test_current_profile_rejects_missing_expired_and_revoked_access(
    client: httpx.AsyncClient,
) -> None:
    """GET profile не должен принимать отсутствующий, истёкший или отозванный JWT."""
    await signup(client)
    pair = await login(client)
    logged_out = await client.post(
        '/api/v1/auth/logout',
        json={'refresh_token': pair['refresh_token']},
        headers=bearer(pair['access_token']),
    )
    assert logged_out.status_code == 204

    now = datetime.now(UTC)
    expired_token = jwt.encode(
        {
            'sub': str(uuid4()),
            'jti': str(uuid4()),
            'type': 'access',
            'session_epoch': str(uuid4()),
            'iat': now - timedelta(minutes=2),
            'exp': now - timedelta(minutes=1),
        },
        SECRET,
        algorithm='HS256',
    )

    missing = await client.get('/api/v1/auth/profile')
    expired = await client.get(
        '/api/v1/auth/profile',
        headers=bearer(expired_token),
    )
    revoked = await client.get(
        '/api/v1/auth/profile',
        headers=bearer(pair['access_token']),
    )

    assert missing.status_code == expired.status_code == revoked.status_code == 401
    assert missing.json() == {'detail': 'Invalid token'}
    assert expired.json() == {'detail': 'Token expired'}
    assert revoked.json() == {'detail': 'Token revoked'}


@pytest.mark.asyncio
async def test_current_profile_rejects_deleted_user(
    client: httpx.AsyncClient,
    user_repository: InMemoryUserRepository,
) -> None:
    """Access-токен удалённого пользователя не должен возвращать профиль."""
    signed_up = await signup(client)
    pair = await login(client)
    assert isinstance(signed_up['id'], str)
    user_repository.items.pop(UUID(signed_up['id']))

    response = await client.get(
        '/api/v1/auth/profile',
        headers=bearer(pair['access_token']),
    )

    assert response.status_code == 401
    assert response.json() == {'detail': 'Invalid token'}


@pytest.mark.asyncio
async def test_profile_rejects_empty_patch_and_conflicting_login(
    client: httpx.AsyncClient,
) -> None:
    """PATCH отличает schema error от unique conflict."""
    await signup(client)
    await signup(client, login='Bob')
    pair = await login(client)

    empty = await client.patch(
        '/api/v1/auth/profile',
        json={},
        headers=bearer(pair['access_token']),
    )
    conflict = await client.patch(
        '/api/v1/auth/profile',
        json={'new_login': 'Bob'},
        headers=bearer(pair['access_token']),
    )
    old_names = await client.patch(
        '/api/v1/auth/profile',
        json={'login': 'LegacyLogin', 'password': 'legacy correct horse'},
        headers=bearer(pair['access_token']),
    )

    assert empty.status_code == 422
    assert conflict.status_code == 409
    assert conflict.json() == {'detail': 'Login already taken'}
    assert old_names.status_code == 422


@pytest.mark.asyncio
async def test_login_history_returns_only_current_user_entries(
    client: httpx.AsyncClient,
) -> None:
    """History не смешивает пользователей и сортируется newest first."""
    await signup(client)
    await signup(client, login='Bob')
    first_alice = await login(client)
    await login(client, login_name='Bob')
    current_alice = await login(client)

    response = await client.get(
        '/api/v1/auth/login-history',
        headers=bearer(current_alice['access_token']),
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert [item['user_agent'] for item in body] == ['api-test', 'api-test']
    assert all(item['user_id'] == body[0]['user_id'] for item in body)
    assert first_alice['access_token'] != current_alice['access_token']


@pytest.mark.asyncio
async def test_logout_returns_empty_204_and_revokes_both_tokens(
    client: httpx.AsyncClient,
) -> None:
    """Logout HTTP contract обязан отозвать access и refresh."""
    await signup(client)
    pair = await login(client)

    response = await client.post(
        '/api/v1/auth/logout',
        json={'refresh_token': pair['refresh_token']},
        headers=bearer(pair['access_token']),
    )
    revoked_access = await client.get(
        '/api/v1/auth/login-history',
        headers=bearer(pair['access_token']),
    )
    revoked_refresh = await client.post(
        '/api/v1/auth/refresh',
        json={'refresh_token': pair['refresh_token']},
    )

    assert response.status_code == 204
    assert response.content == b''
    assert revoked_access.status_code == 401
    assert revoked_access.json() == {'detail': 'Token revoked'}
    assert revoked_refresh.status_code == 401
    assert revoked_refresh.json() == {'detail': 'Invalid refresh token'}


async def test_contact_profile_and_delete_api(client: httpx.AsyncClient) -> None:
    """Contact PATCH distinguishes null and omitted fields; DELETE revokes sessions."""
    response = await client.post(
        '/api/v1/auth/signup',
        json={
            'login': 'ContactUser',
            'password': 'correct horse',
            'email': 'user@example.org',
            'locale': 'ru-RU',
            'tz': 'Europe/Moscow',
            'first_name': 'First',
        },
    )
    assert response.status_code == 201
    assert response.json()['email'] == 'user@example.org'
    pair = await login(client, 'ContactUser')
    headers = bearer(pair['access_token'])
    updated = await client.patch(
        '/api/v1/auth/profile', json={'first_name': None}, headers=headers
    )
    assert updated.status_code == 200
    assert updated.json()['first_name'] is None
    assert updated.json()['email'] == 'user@example.org'
    assert updated.json()['user_version'] == 2
    assert (await client.delete('/api/v1/auth/profile')).status_code == 401
    assert (
        await client.delete('/api/v1/auth/profile', headers=headers)
    ).status_code == 204
    assert (
        await client.get('/api/v1/auth/profile', headers=headers)
    ).status_code == 401
    assert (
        await client.post(
            '/api/v1/auth/refresh', json={'refresh_token': pair['refresh_token']}
        )
    ).status_code == 401


@pytest.mark.parametrize(
    'payload_tz', [{}, {'tz': None}, {'tz': ''}, {'tz': 'Unknown/Zone'}]
)
async def test_signup_rejects_unknown_timezone_without_user(
    client: httpx.AsyncClient,
    user_repository: InMemoryUserRepository,
    payload_tz: dict[str, str | None],
) -> None:
    """Invalid timezone is rejected before profile persistence."""
    response = await client.post(
        '/api/v1/auth/signup',
        json={'login': 'Alice', 'password': 'correct horse', **payload_tz},
    )
    assert response.status_code == 422
    assert not user_repository.items


@pytest.mark.parametrize('tz', [None, '', 'Unknown/Zone'])
async def test_profile_cannot_clear_timezone(
    client: httpx.AsyncClient, tz: str | None
) -> None:
    """Rejected timezone updates keep the previous profile value."""
    await signup(client)
    pair = await login(client)
    headers = bearer(pair['access_token'])
    response = await client.patch(
        '/api/v1/auth/profile', json={'tz': tz}, headers=headers
    )
    assert response.status_code == 422
    profile = await client.get('/api/v1/auth/profile', headers=headers)
    assert profile.json()['tz'] == 'Europe/Moscow'
