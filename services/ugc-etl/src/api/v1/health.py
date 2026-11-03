from typing import Literal, Optional

from fastapi import APIRouter, Request, Response
from pydantic import BaseModel, Field

from core.config import settings
from dependencies import EventConsumerDep, WarehouseDep
from services.memory import read_memory
from services.pipeline import pending_age_seconds, pipeline_is_running, stats

router = APIRouter()

ComponentStatus = Literal['ok', 'unavailable']


class HealthStatus(BaseModel):
    status: ComponentStatus = Field(
        description='ok только если Kafka, ClickHouse, RSS и пайплайн в норме',
        examples=['ok'],
    )
    kafka: ComponentStatus = Field(examples=['ok'])
    clickhouse: ComponentStatus = Field(examples=['ok'])
    pipeline: ComponentStatus = Field(
        description='ok если задача работает и возраст текущего пакета меньше порога остановки',
        examples=['ok'],
    )
    rss_bytes: int = Field(ge=0)
    rss_limit_bytes: int = Field(ge=0)
    batch_size: int = Field(ge=0)
    pending_age_seconds: Optional[float] = Field(
        ge=0, description='Возраст неподтверждённого пакета; null, если пакета нет'
    )
    last_success_age_seconds: Optional[float] = Field(
        ge=0, description='Секунды после последнего успешного commit; null до первой обработки'
    )
    consecutive_errors: int = Field(ge=0, description='Ошибки подряд до успешной обработки пакета')


@router.get(
    '/',
    response_model=HealthStatus,
    summary='Проверка здоровья',
    responses={
        200: {'description': 'Kafka, ClickHouse, RSS и пайплайн в норме'},
        503: {'description': 'Kafka, ClickHouse, RSS или пайплайн вне нормы'},
    },
)
async def health(
    request: Request,
    consumer: EventConsumerDep,
    warehouse: WarehouseDep,
    response: Response,
) -> HealthStatus:
    kafka_ok = await consumer.ping()
    clickhouse_ok = await warehouse.ping()
    memory = read_memory()
    rss_ok = memory.rss_bytes <= settings.etl_memory_limit_bytes
    pipeline_ok = pipeline_is_running(getattr(request.app.state, 'pipeline_task', None))
    pending_age = pending_age_seconds()
    if pending_age is not None and pending_age >= settings.etl_stall_timeout_seconds:
        pipeline_ok = False
    all_ok = kafka_ok and clickhouse_ok and rss_ok and pipeline_ok
    if not all_ok:
        response.status_code = 503
    return HealthStatus(
        status='ok' if all_ok else 'unavailable',
        kafka='ok' if kafka_ok else 'unavailable',
        clickhouse='ok' if clickhouse_ok else 'unavailable',
        pipeline='ok' if pipeline_ok else 'unavailable',
        rss_bytes=memory.rss_bytes,
        rss_limit_bytes=settings.etl_memory_limit_bytes,
        batch_size=stats.batch_size,
        pending_age_seconds=pending_age,
        last_success_age_seconds=stats.last_success_age_seconds(),
        consecutive_errors=stats.consecutive_errors,
    )
