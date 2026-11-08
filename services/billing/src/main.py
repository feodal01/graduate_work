from collections.abc import AsyncGenerator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from logging.config import dictConfig

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from opentelemetry.sdk.trace import TracerProvider

from api import ui
from api.errors import register_exception_handlers
from api.request_id import require_request_id
from api.v1 import health, internal, payments, webhooks
from clients.auth import HttpAuthGateway
from clients.notifications import HttpNotificationsGateway
from clients.payment_provider import YooKassaProviderAdapter
from core.config import ServiceSettings, TokenSettings, settings
from core.logger import LOGGING, add_logstash_handler
from core.observability import bind_request_id, init_sentry
from core.tracing import create_tracer_provider, instrument_fastapi
from db.sql.postgres import PostgresDatabase
from db.sql.postgres_health import PostgresHealthCheck
from security.service_credentials import ServiceCredentials

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
    token_settings = TokenSettings()  # pyright: ignore[reportCallIssue]
    service_settings = ServiceSettings()  # pyright: ignore[reportCallIssue]
    database = PostgresDatabase(
        settings.postgres.dsn,
        connect_timeout_seconds=settings.postgres.connect_timeout_seconds,
        pool_timeout_seconds=settings.postgres.pool_timeout_seconds,
        command_timeout_seconds=settings.postgres.command_timeout_seconds,
        statement_timeout_ms=settings.postgres.statement_timeout_ms,
    )

    app.state.database = database
    app.state.database_health = PostgresHealthCheck(database.engine)
    app.state.token_settings = token_settings
    app.state.service_credentials = ServiceCredentials(
        service_settings.api_key.get_secret_value()
    )
    auth_http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(settings.auth_client.timeout_seconds)
    )
    notifications_http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(settings.notifications_client.timeout_seconds)
    )
    yookassa_http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(settings.yookassa.timeout_seconds)
    )
    app.state.auth_gateway = HttpAuthGateway(
        http_client=auth_http_client,
        base_url=str(settings.auth_client.service_url),
        service_api_key=service_settings.api_key.get_secret_value(),
        logout_timeout_seconds=settings.auth_client.logout_timeout_seconds,
    )
    app.state.notifications_gateway = HttpNotificationsGateway(
        http_client=notifications_http_client,
        base_url=str(settings.notifications_client.campaign_url),
        api_key=settings.notifications_client.campaign_api_key.get_secret_value(),
    )
    app.state.payment_provider = YooKassaProviderAdapter(
        http_client=yookassa_http_client,
        shop_id=settings.yookassa.shop_id,
        secret_key=settings.yookassa.secret_key.get_secret_value(),
        base_url=str(settings.yookassa.api_base_url),
    )

    try:
        yield
    finally:
        try:
            await yookassa_http_client.aclose()
        finally:
            try:
                await notifications_http_client.aclose()
            finally:
                try:
                    await auth_http_client.aclose()
                finally:
                    try:
                        await database.close()
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
        docs_url='/billing/docs',
        openapi_url='/billing/openapi.json',
        default_response_class=UTF8JSONResponse,
        lifespan=app_lifespan,
    )
    application.middleware('http')(bind_request_id)
    application.middleware('http')(require_request_id)
    if settings.tracing.enabled or tracer_provider is not None:
        provider = tracer_provider or create_tracer_provider(settings.tracing)
        instrument_fastapi(application, provider)
        application.state.tracer_provider = provider
    register_exception_handlers(application)
    application.include_router(health.router, prefix='/api/v1/health')
    application.include_router(payments.router, prefix='/api/v1/billing')
    application.include_router(webhooks.router, prefix='/api/v1/billing')
    application.include_router(internal.router, prefix='/api/v1/internal/billing')
    application.include_router(ui.router, prefix='/billing')
    return application


app = create_app()
