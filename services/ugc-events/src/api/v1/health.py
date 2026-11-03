from typing import Literal

from fastapi import APIRouter, Response
from pydantic import BaseModel, Field

from dependencies import EventBrokerDep, EventRateLimiterDep

router = APIRouter()

ComponentStatus = Literal['ok', 'unavailable']


class HealthStatus(BaseModel):
    status: Literal['ok', 'degraded', 'unavailable'] = Field(
        description='ok: сервис готов; degraded: Redis недоступен; unavailable: Kafka недоступна',
        examples=['ok'],
    )
    kafka: ComponentStatus = Field(
        description='Состояние Kafka и топик ugc.events',
        examples=['ok'],
    )
    redis: ComponentStatus = Field(description='Хранилище лимитов доступно', examples=['ok'])


@router.get(
    '/',
    response_model=HealthStatus,
    summary='Проверка здоровья',
    description='Kafka и топик ugc.events обязательны (иначе 503); отказ Redis не блокирует приём.',
    responses={
        200: {
            'description': 'Сервис готов, в том числе без пользовательского лимита при отказе Redis'
        },
        503: {'description': 'Kafka недоступна либо топик отсутствует'},
    },
)
async def health(
    broker: EventBrokerDep,
    limiter: EventRateLimiterDep,
    response: Response,
) -> HealthStatus:
    kafka_ok = await broker.ping()
    redis_ok = await limiter.ping()
    if not kafka_ok:
        response.status_code = 503
    return HealthStatus(
        status=('ok' if redis_ok else 'degraded') if kafka_ok else 'unavailable',
        kafka='ok' if kafka_ok else 'unavailable',
        redis='ok' if redis_ok else 'unavailable',
    )
