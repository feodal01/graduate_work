import logging
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from logging.config import dictConfig

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff

from api import openapi as openapi_meta
from api.v1 import events, health
from core.config import settings
from core.logger import LOGGING, add_logstash_handler
from core.observability import bind_request_id, init_sentry
from services.event_broker import (
    EventBrokerUnavailableError,
    EventEnvelopeTooLargeError,
)
from services.kafka_broker import AIOKafkaEventBroker
from services.rate_limit import RedisEventRateLimiter

logger = logging.getLogger(__name__)

if settings.logstash_host:
    add_logstash_handler(settings.logstash_host, settings.logstash_port, settings.project_name)
dictConfig(LOGGING)
init_sentry(
    dsn=settings.sentry_dsn,
    environment=settings.sentry_environment,
    traces_sample_rate=settings.sentry_traces_sample_rate,
    service=settings.project_name,
)


class UTF8JSONResponse(JSONResponse):
    """JSON с charset=utf-8 (Starlette ставит charset только для text/*)."""

    media_type = 'application/json; charset=utf-8'


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(','),
        acks='all',
        client_id=settings.project_name,
        max_request_size=settings.kafka_max_request_size,
    )
    producer_started = False
    try:
        await producer.start()
        producer_started = True
        app.state.event_broker = AIOKafkaEventBroker(
            producer=producer,
            topic=settings.kafka_topic,
        )
        redis = Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            socket_connect_timeout=settings.redis_socket_connect_timeout,
            socket_timeout=settings.redis_socket_timeout,
            retry=Retry(NoBackoff(), 0),
        )
        try:
            app.state.event_rate_limiter = RedisEventRateLimiter(
                redis,
                rate=settings.events_rate_limit_rate,
                window_seconds=settings.events_rate_limit_window_seconds,
                capacity=settings.events_rate_limit_capacity,
            )
            yield
        finally:
            await redis.aclose()
    finally:
        try:
            await producer.stop()
        except Exception:
            if producer_started:
                raise
            logger.exception('Kafka producer cleanup failed after startup failure')


app = FastAPI(
    title=settings.project_name,
    description=openapi_meta.DESCRIPTION,
    version='1.0.0',
    docs_url='/ugc/docs',
    redoc_url=None,
    openapi_url='/ugc/openapi.json',
    openapi_tags=openapi_meta.TAGS,
    default_response_class=UTF8JSONResponse,
    lifespan=lifespan,
)
app.middleware('http')(bind_request_id)


@app.exception_handler(EventEnvelopeTooLargeError)
async def event_envelope_too_large_handler(
    _request: Request,
    _exc: EventEnvelopeTooLargeError,
) -> UTF8JSONResponse:
    return UTF8JSONResponse(
        status_code=413,
        content={'detail': 'Event payload is too large'},
    )


@app.exception_handler(EventBrokerUnavailableError)
async def event_broker_unavailable_handler(
    _request: Request,
    _exc: EventBrokerUnavailableError,
) -> UTF8JSONResponse:
    return UTF8JSONResponse(
        status_code=503,
        content={'detail': 'Event broker is unavailable'},
    )


@app.middleware('http')
async def ensure_json_utf8_charset(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    response = await call_next(request)
    content_type = response.headers.get('content-type', '')
    if content_type.startswith('application/json') and 'charset=' not in content_type.lower():
        response.headers['content-type'] = 'application/json; charset=utf-8'
    return response


app.include_router(events.router, prefix='/api/v1/ugc/events', tags=['events'])
app.include_router(health.router, prefix='/api/v1/health', tags=['health'])
