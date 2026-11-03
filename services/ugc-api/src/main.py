from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from logging.config import dictConfig
from typing import Any

from beanie import init_beanie
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pymongo import AsyncMongoClient

from api import openapi as openapi_meta
from api.v1 import films, health, reviews, users
from core.config import settings
from core.logger import LOGGING, add_logstash_handler
from core.observability import bind_request_id, init_sentry
from dependencies import set_mongo_client
from models.documents import DOCUMENT_MODELS
from services.errors import ConflictError, ForbiddenError, NotFoundError, WritesPausedError
from services.review_writes import initialize_review_write_gate

if settings.logstash_host:
    add_logstash_handler(settings.logstash_host, settings.logstash_port, settings.project_name)
dictConfig(LOGGING)
init_sentry(
    dsn=settings.sentry_dsn,
    environment=settings.sentry_environment,
    traces_sample_rate=settings.sentry_traces_sample_rate,
    service=settings.project_name,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
        settings.mongo_uri,
        uuidRepresentation='standard',
        tz_aware=True,
        serverSelectionTimeoutMS=settings.mongo_server_selection_timeout_ms,
    )
    set_mongo_client(client)
    try:
        await init_beanie(database=client[settings.mongo_db], document_models=DOCUMENT_MODELS)
        await initialize_review_write_gate()
        app.state.mongo_client = client
        yield
    finally:
        set_mongo_client(None)
        await client.close()


app = FastAPI(
    title=settings.project_name,
    description=openapi_meta.DESCRIPTION,
    version='0.1.0',
    docs_url='/ugc-api/docs',
    redoc_url=None,
    openapi_url='/ugc-api/openapi.json',
    openapi_tags=openapi_meta.TAGS,
    lifespan=lifespan,
)
app.middleware('http')(bind_request_id)


@app.exception_handler(NotFoundError)
async def not_found_handler(_request: Request, _exc: NotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={'detail': 'Not found'})


@app.exception_handler(ForbiddenError)
async def forbidden_handler(_request: Request, _exc: ForbiddenError) -> JSONResponse:
    return JSONResponse(status_code=403, content={'detail': 'Forbidden'})


@app.exception_handler(ConflictError)
async def conflict_handler(_request: Request, _exc: ConflictError) -> JSONResponse:
    return JSONResponse(status_code=409, content={'detail': 'Already exists'})


@app.exception_handler(WritesPausedError)
async def writes_paused_handler(_request: Request, _exc: WritesPausedError) -> JSONResponse:
    return JSONResponse(status_code=503, content={'detail': 'Review writes are temporarily paused'})


app.include_router(films.router, prefix='/api/v1/ugc/films', tags=['films'])
app.include_router(reviews.router, prefix='/api/v1/ugc/reviews', tags=['reviews'])
app.include_router(users.router, prefix='/api/v1/ugc/users/me', tags=['users'])
app.include_router(health.router, prefix='/api/v1/health', tags=['health'])
