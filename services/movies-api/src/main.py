from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from logging.config import dictConfig

import aiohttp
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from redis.asyncio import Redis

from api import openapi as openapi_meta
from api.v1 import films, health
from clients.auth import AuthClient
from core.config import settings
from core.logger import LOGGING, add_logstash_handler
from core.observability import bind_request_id, init_sentry
from db.cache.redis import RedisCache
from db.storage.elastic import ElasticStorage
from db.storage.elastic_errors import StorageUnavailableError
from db.storage.elastic_query import ElasticQueryTranslator
from models.film import Film

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
    es = AsyncElasticsearch(
        hosts=[f'http://{settings.elasticsearch_host}:{settings.elasticsearch_port}'],
        request_timeout=settings.elasticsearch_request_timeout,
        retry_on_timeout=False,
    )
    redis = Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        socket_connect_timeout=settings.redis_socket_connect_timeout,
        socket_timeout=settings.redis_socket_timeout,
    )
    auth_session = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(
            connect=settings.auth_connect_timeout,
            total=settings.auth_request_timeout,
        )
    )

    app.state.film_storage = ElasticStorage[Film](
        index=settings.elasticsearch_index,
        es=es,
        model=Film,
        translator=ElasticQueryTranslator(nested_paths=frozenset[str]({'genres'})),
    )
    app.state.cache = RedisCache(redis)
    app.state.auth_client = AuthClient(
        session=auth_session,
        base_url=settings.auth_service_url,
        service_api_key=settings.auth_service_api_key.get_secret_value(),
    )

    try:
        yield
    finally:
        try:
            await auth_session.close()
        finally:
            try:
                await es.close()
            finally:
                await redis.aclose()


app = FastAPI(
    title=settings.project_name,
    description=openapi_meta.DESCRIPTION,
    version='1.0.0',
    docs_url='/movies/docs',
    redoc_url=None,
    openapi_url='/movies/openapi.json',
    openapi_tags=openapi_meta.TAGS,
    default_response_class=UTF8JSONResponse,
    lifespan=lifespan,
)
app.middleware('http')(bind_request_id)


@app.exception_handler(StorageUnavailableError)
async def storage_unavailable_handler(
    _request: Request,
    exc: StorageUnavailableError,
) -> UTF8JSONResponse:
    return UTF8JSONResponse(
        status_code=503,
        content={'detail': str(exc) or 'Storage is unavailable'},
    )


@app.middleware('http')
async def ensure_json_utf8_charset(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    # OpenAPI JSON идёт через JSONResponse, минуя default_response_class
    response = await call_next(request)
    content_type = response.headers.get('content-type', '')
    if content_type.startswith('application/json') and 'charset=' not in content_type.lower():
        response.headers['content-type'] = 'application/json; charset=utf-8'
    return response


app.include_router(films.router, prefix='/api/v1/films', tags=['films'])
app.include_router(health.router, prefix='/api/v1/health', tags=['health'])
