from typing import Literal

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from db.health import HealthCheck
from dependencies import get_cache_health, get_database_health

router = APIRouter()

ComponentStatus = Literal['ok', 'unavailable']


class HealthStatus(BaseModel):
    status: ComponentStatus = Field(
        description='Общее состояние: ok — всё доступно, degraded — без кэша, '
        'unavailable — нет базы данных',
        examples=['ok'],
    )
    database: ComponentStatus = Field(
        description='Состояние Database',
        examples=['ok'],
    )
    cache: ComponentStatus = Field(
        description='Состояние Кэша',
        examples=['ok'],
    )


@router.get(
    '/',
    response_model=HealthStatus,
    summary='Проверка здоровья',
    description=('Готовность сервиса: Database и Cache обязателен (иначе 503)'),
    responses={
        200: {'description': 'Сервис готов'},
        503: {'description': 'Database или Cache недоступен'},
    },
)
async def health(
    database_health: HealthCheck = Depends(get_database_health),
    cache_health: HealthCheck = Depends(get_cache_health),
) -> HealthStatus | JSONResponse:
    database_ok = await database_health.ping()
    cache_ok = await cache_health.ping()

    body = HealthStatus(
        status=('ok' if database_ok and cache_ok else 'unavailable'),
        database='ok' if database_ok else 'unavailable',
        cache='ok' if cache_ok else 'unavailable',
    )
    if not database_ok or not cache_ok:
        return JSONResponse(status_code=503, content=body.model_dump())
    return body
