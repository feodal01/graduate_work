import logging
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from unittest.mock import MagicMock

import httpx
import pytest
from fastapi import FastAPI, Request

from api import rate_limit as rate_limit_api
from core.config import RateLimitSettings, settings
from db.cache.errors import RateLimitStoreUnavailableError
from dependencies import (
    get_authentication_service,
    get_cache_health,
    get_database_health,
    get_rate_limiter,
)
from main import create_app
from security.passwords import PasswordHasher
from security.tokens import TokenService
from services.authentication import AuthenticationService
from services.rate_limit import RateLimitDecision, RateLimitPolicy
from tests.fakes import (
    InMemoryLoginHistoryRepository,
    InMemoryTokenSessionStore,
    InMemoryUserRepository,
    ProgrammableRateLimiter,
)
from validation.credentials import PasswordPolicy

SECRET = 'test-secret-that-is-long-enough-for-hs256'


class BackendHealth:
    async def ping(self) -> bool:
        """Report a ready dependency for routes excluded from limiting."""
        return True


@dataclass
class RateLimitApiContext:
    app: FastAPI
    client: httpx.AsyncClient
    login_history: InMemoryLoginHistoryRepository


@pytest.fixture
async def context(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[RateLimitApiContext, None]:
    """Create an Auth API with in-memory business dependencies."""
    monkeypatch.setattr(settings.tracing, 'enabled', False)
    login_history = InMemoryLoginHistoryRepository()
    authentication = AuthenticationService(
        users=InMemoryUserRepository(),
        login_history=login_history,
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
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_authentication_service] = lambda: authentication
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'rate-limit-api-test'},
    ) as client:
        yield RateLimitApiContext(app, client, login_history)


@pytest.mark.asyncio
async def test_exhausted_login_bucket_returns_429_before_authentication(
    context: RateLimitApiContext,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A rejected login cannot reach password checks or login-history writes."""
    limiter = ProgrammableRateLimiter(
        decision=RateLimitDecision(
            allowed=False,
            remaining=0,
            retry_after_seconds=12,
        )
    )
    context.app.dependency_overrides[get_rate_limiter] = lambda: limiter

    with caplog.at_level(logging.WARNING, logger='api.rate_limit'):
        response = await context.client.post(
            '/api/v1/auth/login',
            json={'login': 'Alice', 'password': 'correct horse'},
        )

    assert response.status_code == 429
    assert response.json() == {'detail': 'Too many requests'}
    assert response.headers['retry-after'] == '12'
    assert context.login_history.items == []
    assert caplog.records[-1].getMessage() == (
        'Auth request rejected by rate limit '
        'request_id=rate-limit-api-test policy=login '
        'allowed=false retry_after=12'
    )
    assert [(call[0].name, call[1]) for call in limiter.calls] == [('login', 'Alice')]


@pytest.mark.asyncio
async def test_store_unavailable_is_fail_open_and_observable(
    context: RateLimitApiContext,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Redis outage preserves the business response and safe diagnostics."""
    limiter = ProgrammableRateLimiter(error=RateLimitStoreUnavailableError())
    context.app.dependency_overrides[get_rate_limiter] = lambda: limiter
    span = MagicMock()
    span.is_recording.return_value = True
    monkeypatch.setattr(
        rate_limit_api.trace,
        'get_current_span',
        lambda *_args, **_kwargs: span,
    )

    with caplog.at_level(logging.WARNING, logger='api.rate_limit'):
        response = await context.client.post(
            '/api/v1/auth/login',
            json={'login': 'Unknown', 'password': 'correct horse'},
            headers={'X-Request-Id': 'fail-open-request'},
        )

    assert response.status_code == 401
    record = caplog.records[-1]
    assert record.getMessage() == (
        'Rate-limit store is unavailable; request continues '
        'request_id=fail-open-request policy=login store_available=false'
    )
    assert getattr(record, 'request_id') == 'fail-open-request'
    assert getattr(record, 'rate_limit.policy') == 'login'
    assert getattr(record, 'rate_limit.store_available') is False
    span.set_attribute.assert_any_call('rate_limit.policy', 'login')
    span.set_attribute.assert_any_call('rate_limit.store_available', False)


@pytest.mark.asyncio
async def test_disabled_rate_limit_does_not_call_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The global switch disables Redis access without changing policies."""
    limiter = ProgrammableRateLimiter(error=AssertionError('backend called'))
    request = MagicMock(spec=Request)
    monkeypatch.setattr(rate_limit_api.settings.rate_limit, 'enabled', False)

    await rate_limit_api.enforce_rate_limit(
        request=request,
        limiter=limiter,
        policy=RateLimitPolicy('login', rate=5, window_seconds=60, capacity=5),
        identity='Alice',
    )

    assert limiter.calls == []


def test_policy_uses_explicit_settings_for_each_route_group() -> None:
    """Each public policy maps to its own three configuration fields."""
    config = RateLimitSettings(
        # BaseSettings supports _env_file at runtime; isolate this test from dotenv.
        _env_file=None,  # pyright: ignore[reportCallIssue]
        login_rate=1,
        login_window_seconds=11,
        login_capacity=21,
        signup_rate=2,
        signup_window_seconds=12,
        signup_capacity=22,
        refresh_rate=3,
        refresh_window_seconds=13,
        refresh_capacity=23,
        user_rate=4,
        user_window_seconds=14,
        user_capacity=24,
        admin_rate=5,
        admin_window_seconds=15,
        admin_capacity=25,
        service_rate=6,
        service_window_seconds=16,
        service_capacity=26,
    )

    assert [
        rate_limit_api.policy(name, config)
        for name in ('login', 'signup', 'refresh', 'user', 'admin', 'service')
    ] == [
        RateLimitPolicy('login', 1, 11, 21),
        RateLimitPolicy('signup', 2, 12, 22),
        RateLimitPolicy('refresh', 3, 13, 23),
        RateLimitPolicy('user', 4, 14, 24),
        RateLimitPolicy('admin', 5, 15, 25),
        RateLimitPolicy('service', 6, 16, 26),
    ]


@pytest.mark.asyncio
async def test_health_and_openapi_routes_do_not_use_auth_rate_limiter() -> None:
    """Readiness and API documentation remain outside Token Bucket policies."""
    app = create_app(app_lifespan=None)
    limiter = ProgrammableRateLimiter(error=AssertionError('backend called'))
    app.dependency_overrides[get_rate_limiter] = lambda: limiter
    app.dependency_overrides[get_database_health] = BackendHealth
    app.dependency_overrides[get_cache_health] = BackendHealth
    transport = httpx.ASGITransport(app=app)

    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'excluded-routes-test'},
    ) as client:
        responses = [
            await client.get('/api/v1/health/'),
            await client.get('/auth/docs'),
            await client.get('/auth/openapi.json'),
        ]

    assert [response.status_code for response in responses] == [200, 200, 200]
    assert limiter.calls == []
