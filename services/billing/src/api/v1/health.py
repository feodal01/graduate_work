from typing import Literal

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from db.health import HealthCheck
from dependencies import get_database_health

router = APIRouter()

ComponentStatus = Literal['ok', 'unavailable']


class HealthStatus(BaseModel):
    status: ComponentStatus = Field(
        description='Общее состояние сервиса Billing (ok / unavailable)',
        examples=['ok'],
    )
    database: ComponentStatus = Field(
        description='Состояние PostgreSQL (billing-db)',
        examples=['ok'],
    )


@router.get(
    '/',
    response_model=HealthStatus,
    summary='Проверка работоспособности сервиса и его БД (BIL-API-15)',
    description='Проверяет соединение с billing-db и возвращает 200 ok (или 503).',
    responses={
        200: {'description': 'Сервис и БД доступны'},
        503: {'description': 'БД недоступна'},
    },
)
async def health(
    database_health: HealthCheck = Depends(get_database_health),
) -> HealthStatus | JSONResponse:
    database_ok = await database_health.ping()
    state: ComponentStatus = 'ok' if database_ok else 'unavailable'
    body = HealthStatus(status=state, database=state)
    if not database_ok:
        return JSONResponse(status_code=503, content=body.model_dump())
    return body
