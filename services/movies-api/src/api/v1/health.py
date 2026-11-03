from typing import Literal

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from db.cache.base import AbstractCache
from db.health import HealthCheck
from dependencies import get_cache, get_storage_health

router = APIRouter()

ComponentStatus = Literal['ok', 'degraded', 'unavailable']


class HealthStatus(BaseModel):
    status: Literal['ok', 'degraded', 'unavailable'] = Field(
        description='Общее состояние: ok — всё доступно, degraded — без кэша, '
        'unavailable — нет хранилища',
        examples=['ok'],
    )
    elasticsearch: ComponentStatus = Field(
        description='Состояние Elasticsearch',
        examples=['ok'],
    )
    redis: ComponentStatus = Field(
        description='Состояние Redis-кэша (degraded = сервис жив без кэша)',
        examples=['ok'],
    )


@router.get(
    '/',
    response_model=HealthStatus,
    summary='Проверка здоровья',
    description=(
        'Готовность сервиса: Elasticsearch обязателен (иначе 503), '
        'Redis опционален (degraded при недоступности).'
    ),
    responses={
        200: {'description': 'Сервис готов (возможно без кэша)'},
        503: {'description': 'Elasticsearch недоступен'},
    },
)
async def health(
    storage: HealthCheck = Depends(get_storage_health),
    cache: AbstractCache = Depends(get_cache),
) -> HealthStatus | JSONResponse:
    es_ok = await storage.ping()
    redis_ok = await cache.ping()

    body = HealthStatus(
        status=('ok' if es_ok and redis_ok else 'degraded' if es_ok else 'unavailable'),
        elasticsearch='ok' if es_ok else 'unavailable',
        redis='ok' if redis_ok else 'degraded',
    )
    if not es_ok:
        return JSONResponse(status_code=503, content=body.model_dump())
    return body
