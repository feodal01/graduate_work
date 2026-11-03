from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from logging.config import dictConfig

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from opentelemetry.sdk.trace import TracerProvider
from redis.asyncio import Redis

from api.errors import register_exception_handlers
from api.request_id import require_request_id
from api.v1 import (
    authentication,
    authorization,
    health,
    notification_contacts,
    social_authentication,
    token_introspection,
)
from clients.yandex_oauth import YandexOAuthClient
from core.config import ServiceSettings, TokenSettings, settings
from core.logger import LOGGING, add_logstash_handler
from core.observability import bind_request_id, init_sentry
from core.tracing import create_tracer_provider, instrument_fastapi
from db.cache.authorization import RedisAuthorizationCache
from db.cache.oauth_state import RedisOAuthStateStore
from db.cache.rate_limit import RedisTokenBucketRateLimiter
from db.cache.redis_health import RedisHealthCheck
from db.cache.session import RedisTokenSessionStore
from db.sql.postgres import PostgresDatabase
from db.sql.postgres_health import PostgresHealthCheck
from security.passwords import PasswordHasher
from security.service_credentials import ServiceCredentials
from security.tokens import TokenService
from validation.credentials import PasswordPolicy

if settings.logstash.host:
    add_logstash_handler(
        settings.logstash.host,
        settings.logstash.port,
        settings.tracing.service_name,
    )
dictConfig(LOGGING)
init_sentry(
    dsn=settings.sentry.dsn,
    environment=settings.sentry.environment,
    traces_sample_rate=settings.sentry.traces_sample_rate,
    service=settings.tracing.service_name,
)

Lifespan = Callable[[FastAPI], AbstractAsyncContextManager[None]]


class UTF8JSONResponse(JSONResponse):
    """JSON с явной UTF-8 кодировкой."""

    media_type = 'application/json; charset=utf-8'


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # These required secrets are supplied by BaseSettings from the environment.
    token_settings = TokenSettings()  # pyright: ignore[reportCallIssue]
    service_settings = ServiceSettings()  # pyright: ignore[reportCallIssue]
    database = PostgresDatabase(
        settings.postgres.dsn,
        connect_timeout_seconds=settings.postgres.connect_timeout_seconds,
        pool_timeout_seconds=settings.postgres.pool_timeout_seconds,
        command_timeout_seconds=settings.postgres.command_timeout_seconds,
        statement_timeout_ms=settings.postgres.statement_timeout_ms,
    )
    redis = Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        socket_connect_timeout=settings.redis.socket_connect_timeout,
        socket_timeout=settings.redis.socket_timeout,
    )
    yandex_http_client: httpx.AsyncClient | None = None

    app.state.database = database
    app.state.database_health = PostgresHealthCheck(database.engine)
    app.state.cache_health = RedisHealthCheck(redis)
    app.state.token_sessions = RedisTokenSessionStore(
        redis,
        marker_ttl_seconds=(settings.session.password_change_marker_ttl_seconds),
    )
    app.state.authorization_cache = RedisAuthorizationCache(
        redis,
        ttl_seconds=settings.authorization_cache.ttl_seconds,
    )
    app.state.rate_limiter = RedisTokenBucketRateLimiter(redis)
    app.state.oauth_state_store = RedisOAuthStateStore(redis)
    app.state.yandex_oauth_client = None
    app.state.service_credentials = ServiceCredentials(
        service_settings.api_key.get_secret_value()
    )
    app.state.password_hasher = PasswordHasher()
    app.state.password_policy = PasswordPolicy(
        min_length=settings.password_policy.min_length,
        min_score=settings.password_policy.min_score,
    )
    app.state.token_service = TokenService(
        secret_key=token_settings.jwt_secret.get_secret_value(),
        algorithm=token_settings.jwt_algorithm,
        access_ttl_seconds=token_settings.access_token_ttl_seconds,
        refresh_ttl_seconds=token_settings.refresh_token_ttl_seconds,
    )

    yandex = settings.yandex_oauth
    if yandex.enabled:
        if (
            yandex.client_id is None
            or yandex.client_secret is None
            or yandex.redirect_uri is None
        ):
            raise RuntimeError('Validated Yandex OAuth settings are incomplete')
        yandex_http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                yandex.request_timeout_seconds,
                connect=yandex.connect_timeout_seconds,
            )
        )
        app.state.yandex_oauth_client = YandexOAuthClient(
            http_client=yandex_http_client,
            client_id=yandex.client_id,
            client_secret=yandex.client_secret.get_secret_value(),
            redirect_uri=str(yandex.redirect_uri),
            userinfo_max_tries=yandex.userinfo_max_tries,
            userinfo_backoff_max_seconds=(yandex.userinfo_backoff_max_seconds),
        )

    try:
        yield
    finally:
        try:
            try:
                if yandex_http_client is not None:
                    await yandex_http_client.aclose()
            finally:
                try:
                    await database.close()
                finally:
                    await redis.aclose()
        finally:
            tracer_provider = getattr(
                app.state,
                'tracer_provider',
                None,
            )
            if tracer_provider is not None:
                tracer_provider.shutdown()


def create_app(
    app_lifespan: Lifespan | None = lifespan,
    tracer_provider: TracerProvider | None = None,
) -> FastAPI:
    application = FastAPI(
        title=settings.project_name,
        docs_url='/auth/docs',
        openapi_url='/auth/openapi.json',
        default_response_class=UTF8JSONResponse,
        lifespan=app_lifespan,
    )
    application.middleware('http')(require_request_id)
    application.middleware('http')(bind_request_id)
    if settings.tracing.enabled or tracer_provider is not None:
        provider = tracer_provider or create_tracer_provider(settings.tracing)
        instrument_fastapi(application, provider)
        application.state.tracer_provider = provider
    register_exception_handlers(application)
    application.include_router(authentication.router, prefix='/api/v1/auth')
    application.include_router(
        social_authentication.router,
        prefix='/api/v1/auth',
    )
    application.include_router(authorization.router, prefix='/api/v1/auth')
    application.include_router(health.router, prefix='/api/v1/health')
    application.include_router(notification_contacts.router, prefix='/api/v1/internal')
    application.include_router(token_introspection.router, prefix='/api/v1/internal')
    return application


app = create_app()
