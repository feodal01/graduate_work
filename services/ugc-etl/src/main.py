import asyncio
import tracemalloc
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from typing import Optional

from fastapi import FastAPI

from api.v1 import health
from core.config import settings
from core.logger import add_logstash_handler, configure_logging
from core.observability import bind_request_id, init_sentry
from services.consumer import KafkaEventConsumer
from services.contracts import EventConsumer, Warehouse
from services.loader import ClickHouseLoader
from services.pipeline import log_if_pipeline_stopped, run_forever

init_sentry(
    dsn=settings.sentry_dsn,
    environment=settings.sentry_environment,
    traces_sample_rate=settings.sentry_traces_sample_rate,
    service=settings.project_name,
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    if settings.logstash_host:
        add_logstash_handler(settings.logstash_host, settings.logstash_port, settings.project_name)
    configure_logging(settings.log_level)
    if getattr(app.state, 'injected', False):
        yield
        return

    consumer = await KafkaEventConsumer.start()
    warehouse = await ClickHouseLoader.from_settings()
    app.state.consumer = consumer
    app.state.warehouse = warehouse
    await warehouse.ensure_schema()
    tracemalloc.start()
    task = asyncio.create_task(run_forever(consumer, warehouse), name='etl-pipeline')
    task.add_done_callback(log_if_pipeline_stopped)
    app.state.pipeline_task = task
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await task
        await consumer.stop()
        await warehouse.close()


def create_app(
    consumer: Optional[EventConsumer] = None,
    warehouse: Optional[Warehouse] = None,
) -> FastAPI:
    if (consumer is None) != (warehouse is None):
        raise ValueError('consumer and warehouse must be provided together')

    application = FastAPI(
        title=settings.project_name,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    if consumer is not None and warehouse is not None:
        application.state.consumer = consumer
        application.state.warehouse = warehouse
        application.state.injected = True
    application.middleware('http')(bind_request_id)
    application.include_router(health.router, prefix='/api/v1/health')
    return application


app = create_app()
