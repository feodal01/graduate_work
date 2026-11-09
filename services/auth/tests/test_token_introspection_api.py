"""Internal token status uses the same access checks as the public Auth API."""

import httpx
import pytest

from core.config import settings
from db.cache.errors import CacheUnavailableError
from dependencies import (
    get_authentication_service,
    get_rate_limiter,
    get_service_credentials,
)
from main import create_app
from security.service_credentials import ServiceCredentials
from tests.test_authentication_api import (  # noqa: F401
    authentication_service,
    bearer,
    login,
    rate_limiter,
    signup,
    user_repository,
)

PATH = '/api/v1/internal/introspect'
SERVICE_KEY = 'introspection-test-service-key'


@pytest.fixture
async def client(authentication_service, rate_limiter):  # noqa: F811
    """Keep authentication real and isolate its external stores."""
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_authentication_service] = lambda: (
        authentication_service
    )
    app.dependency_overrides[get_rate_limiter] = lambda: rate_limiter
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://auth',
        headers={'X-Request-Id': 'introspection-test'},
    ) as result:
        yield result


async def test_active_then_revoked_token(client):
    """A completed logout changes introspection from active to inactive."""
    user = await signup(client)
    pair = await login(client)
    request = {
        'headers': {'X-Service-Key': SERVICE_KEY},
        'json': {'token': pair['access_token']},
    }
    before = await client.post(PATH, **request)
    assert before.status_code == 200
    assert before.json() == {'active': True, 'sub': user['id']}
    assert before.headers['cache-control'] == 'no-store'
    logout = await client.post(
        '/api/v1/auth/logout',
        headers=bearer(pair['access_token']),
        json={'refresh_token': pair['refresh_token']},
    )
    assert logout.status_code == 204
    after = await client.post(PATH, **request)
    assert after.status_code == 200
    assert after.json() == {'active': False}


@pytest.mark.parametrize('key', [None, 'incorrect'])
async def test_service_authentication_is_required(client, key):
    """Reject missing and incorrect service credentials even for an invalid token."""
    result = await client.post(
        PATH,
        headers={} if key is None else {'X-Service-Key': key},
        json={'token': 'invalid'},
    )
    assert result.status_code == 401


async def test_invalid_and_refresh_tokens_are_inactive(client):
    """Never accept a refresh token or disclose the rejected token in a response."""
    await signup(client)
    pair = await login(client)
    for token in ('invalid-private-token', pair['refresh_token']):
        result = await client.post(
            PATH, headers={'X-Service-Key': SERVICE_KEY}, json={'token': token}
        )
        assert result.status_code == 200
        assert result.json() == {'active': False}


async def test_session_store_failure_is_unavailable(
    client,
    authentication_service,  # noqa: F811
    monkeypatch,
):
    """An unavailable session store is not a negative token-status result."""

    async def unavailable(*args, **kwargs):
        raise CacheUnavailableError

    monkeypatch.setattr(authentication_service, 'authenticate_access', unavailable)
    result = await client.post(
        PATH, headers={'X-Service-Key': SERVICE_KEY}, json={'token': 'test-token'}
    )
    assert result.status_code == 503
    assert 'active' not in result.json()


async def test_introspection_has_separate_service_rate_bucket(
    client,
    rate_limiter,  # noqa: F811
    monkeypatch,
):
    """Preference traffic cannot consume the Movies authorization quota."""
    monkeypatch.setattr(settings.rate_limit, 'enabled', True)
    result = await client.post(
        PATH, headers={'X-Service-Key': SERVICE_KEY}, json={'token': 'invalid'}
    )
    assert result.status_code == 200
    assert rate_limiter.calls[-1][1] == 'internal-service:token-introspection'
