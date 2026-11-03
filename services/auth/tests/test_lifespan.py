from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import FastAPI
from pydantic import SecretStr

import main
from clients.yandex_oauth import YandexOAuthClient
from db.cache.oauth_state import RedisOAuthStateStore
from db.cache.rate_limit import RedisTokenBucketRateLimiter
from db.cache.redis_health import RedisHealthCheck
from db.sql.postgres_health import PostgresHealthCheck
from security.service_credentials import ServiceCredentials


@pytest.mark.asyncio
async def test_lifespan_registers_resources_and_health_checks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Lifespan создаёт ресурсы, health check и корректно их закрывает."""
    app = FastAPI()
    tracer_provider = MagicMock()
    app.state.tracer_provider = tracer_provider
    engine = MagicMock()
    database = MagicMock()
    database.engine = engine
    database.close = AsyncMock()
    redis = MagicMock()
    redis.aclose = AsyncMock()
    redis_type = MagicMock(return_value=redis)
    token_settings = MagicMock()
    token_settings.jwt_secret.get_secret_value.return_value = 'test-secret'
    token_settings.jwt_algorithm = 'HS256'
    token_settings.access_token_ttl_seconds = 900
    token_settings.refresh_token_ttl_seconds = 604800
    service_settings = MagicMock()
    service_settings.api_key.get_secret_value.return_value = (
        'test-internal-service-key-with-32-characters'
    )
    authorization_cache = MagicMock()
    authorization_cache_type = MagicMock(return_value=authorization_cache)
    database_type = MagicMock(return_value=database)
    token_sessions = object()
    token_session_type = MagicMock(return_value=token_sessions)
    monkeypatch.setattr(main.settings.yandex_oauth, 'enabled', False)

    monkeypatch.setattr(
        main,
        'PostgresDatabase',
        database_type,
    )
    monkeypatch.setattr(main, 'Redis', redis_type)
    monkeypatch.setattr(
        main,
        'RedisAuthorizationCache',
        authorization_cache_type,
    )
    monkeypatch.setattr(
        main,
        'TokenSettings',
        MagicMock(return_value=token_settings),
    )
    monkeypatch.setattr(
        main,
        'ServiceSettings',
        MagicMock(return_value=service_settings),
    )
    monkeypatch.setattr(
        main,
        'RedisTokenSessionStore',
        token_session_type,
    )

    async with main.lifespan(app):
        assert app.state.database is database
        assert app.state.token_sessions is token_sessions
        assert isinstance(app.state.database_health, PostgresHealthCheck)
        assert app.state.database_health._engine is engine
        assert isinstance(app.state.cache_health, RedisHealthCheck)
        assert app.state.cache_health.redis is redis
        assert app.state.authorization_cache is authorization_cache
        assert isinstance(
            app.state.rate_limiter,
            RedisTokenBucketRateLimiter,
        )
        assert isinstance(app.state.oauth_state_store, RedisOAuthStateStore)
        assert app.state.yandex_oauth_client is None
        authorization_cache_type.assert_called_once_with(
            redis,
            ttl_seconds=(main.settings.authorization_cache.ttl_seconds),
        )
        assert isinstance(app.state.service_credentials, ServiceCredentials)

    database_type.assert_called_once_with(
        main.settings.postgres.dsn,
        connect_timeout_seconds=(main.settings.postgres.connect_timeout_seconds),
        pool_timeout_seconds=main.settings.postgres.pool_timeout_seconds,
        command_timeout_seconds=(main.settings.postgres.command_timeout_seconds),
        statement_timeout_ms=main.settings.postgres.statement_timeout_ms,
    )
    token_session_type.assert_called_once_with(
        redis,
        marker_ttl_seconds=(main.settings.session.password_change_marker_ttl_seconds),
    )

    redis_type.assert_called_once_with(
        host=main.settings.redis.host,
        port=main.settings.redis.port,
        socket_connect_timeout=main.settings.redis.socket_connect_timeout,
        socket_timeout=main.settings.redis.socket_timeout,
    )

    database.close.assert_awaited_once_with()
    redis.aclose.assert_awaited_once_with()
    tracer_provider.shutdown.assert_called_once_with()


@pytest.mark.asyncio
async def test_lifespan_registers_and_closes_enabled_yandex_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enabled OAuth must use one bounded HTTP client and close it."""
    app = FastAPI()
    database = MagicMock()
    database.engine = MagicMock()
    database.close = AsyncMock()
    redis = MagicMock()
    redis.aclose = AsyncMock()
    yandex_http_client = MagicMock()
    yandex_http_client.aclose = AsyncMock()
    token_settings = MagicMock()
    token_settings.jwt_secret.get_secret_value.return_value = 'test-secret'
    token_settings.jwt_algorithm = 'HS256'
    token_settings.access_token_ttl_seconds = 900
    token_settings.refresh_token_ttl_seconds = 604800
    service_settings = MagicMock()
    service_settings.api_key.get_secret_value.return_value = (
        'test-internal-service-key-with-32-characters'
    )

    monkeypatch.setattr(
        main,
        'PostgresDatabase',
        MagicMock(return_value=database),
    )
    monkeypatch.setattr(main, 'Redis', MagicMock(return_value=redis))
    monkeypatch.setattr(
        main,
        'TokenSettings',
        MagicMock(return_value=token_settings),
    )
    monkeypatch.setattr(
        main,
        'ServiceSettings',
        MagicMock(return_value=service_settings),
    )
    async_client_type = MagicMock(return_value=yandex_http_client)
    monkeypatch.setattr(main.httpx, 'AsyncClient', async_client_type)
    yandex = main.settings.yandex_oauth
    monkeypatch.setattr(yandex, 'enabled', True)
    monkeypatch.setattr(yandex, 'client_id', 'test-client-id')
    monkeypatch.setattr(yandex, 'client_secret', SecretStr('test-client-secret'))
    monkeypatch.setattr(
        yandex,
        'redirect_uri',
        'http://testserver/api/v1/auth/social/yandex/callback',
    )

    async with main.lifespan(app):
        assert isinstance(app.state.yandex_oauth_client, YandexOAuthClient)
        assert app.state.yandex_oauth_client._http_client is yandex_http_client

    async_client_type.assert_called_once()
    timeout = async_client_type.call_args.kwargs['timeout']
    assert timeout.connect == yandex.connect_timeout_seconds
    assert timeout.read == yandex.request_timeout_seconds
    yandex_http_client.aclose.assert_awaited_once_with()
    database.close.assert_awaited_once_with()
    redis.aclose.assert_awaited_once_with()
