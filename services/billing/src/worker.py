import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from logging.config import dictConfig

import httpx
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from clients.auth import HttpAuthGateway
from clients.payment_provider import YooKassaProviderAdapter
from core.config import ServiceSettings, settings
from core.logger import LOGGING, add_logstash_handler
from core.observability import init_sentry
from core.tracing import create_tracer_provider
from db.sql.postgres import PostgresDatabase
from repositories.purchase import SQLAlchemyPurchaseRepository
from services.outbox_dispatcher import OutboxDispatcher
from services.reconciler import PaymentReconciler

logger = logging.getLogger(__name__)


async def _sleep_or_stop(stop_event: asyncio.Event, delay_seconds: float) -> bool:
    """Wait for delay_seconds or return True immediately if stop_event is set."""
    try:
        async with asyncio.timeout(delay_seconds):
            await stop_event.wait()
        return True
    except TimeoutError:
        return stop_event.is_set()


async def run_periodic_loop(
    name: str,
    step: Callable[[], Awaitable[None]],
    interval_seconds: float,
    stop_event: asyncio.Event,
) -> None:
    """Run step immediately on startup and then every interval_seconds until stopped."""
    while not stop_event.is_set():
        try:
            await step()
        except Exception:
            logger.exception('Unexpected error in billing-worker %s loop', name)
        if await _sleep_or_stop(stop_event, interval_seconds):
            break


class BillingWorkerRunner:
    """Background runner for reconciliation and outbox with graceful shutdown."""

    def __init__(
        self,
        database: PostgresDatabase,
        *,
        reconcile_interval_seconds: float,
        outbox_interval_seconds: float,
        reconcile_step: Callable[[], Awaitable[None]] | None = None,
        outbox_step: Callable[[], Awaitable[None]] | None = None,
        tracer_provider: TracerProvider | None = None,
    ) -> None:
        self._database = database
        self._reconcile_interval_seconds = reconcile_interval_seconds
        self._outbox_interval_seconds = outbox_interval_seconds
        self._reconcile_step = reconcile_step or self._noop_step
        self._outbox_step = outbox_step or self._noop_step
        self._tracer_provider = tracer_provider
        self.stop_event = asyncio.Event()

    @staticmethod
    async def _noop_step() -> None:
        return None

    def request_stop(self) -> None:
        logger.info('Stopping billing-worker gracefully')
        self.stop_event.set()

    async def run(self) -> None:
        logger.info(
            'Starting billing-worker (reconcile_interval=%ss, outbox_interval=%ss)',
            self._reconcile_interval_seconds,
            self._outbox_interval_seconds,
        )
        try:
            async with asyncio.TaskGroup() as task_group:
                task_group.create_task(
                    run_periodic_loop(
                        'reconcile',
                        self._reconcile_step,
                        self._reconcile_interval_seconds,
                        self.stop_event,
                    )
                )
                task_group.create_task(
                    run_periodic_loop(
                        'outbox',
                        self._outbox_step,
                        self._outbox_interval_seconds,
                        self.stop_event,
                    )
                )
        finally:
            try:
                await self._database.close()
            finally:
                if self._tracer_provider is not None:
                    self._tracer_provider.shutdown()
            logger.info('billing-worker stopped')


def install_signal_handlers(
    loop: asyncio.AbstractEventLoop,
    runner: BillingWorkerRunner,
) -> None:
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, runner.request_stop)
        except NotImplementedError:  # pragma: no cover
            signal.signal(sig, lambda *_: runner.request_stop())


async def main() -> None:
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
    tracer_provider: TracerProvider | None = None
    if settings.tracing.enabled:
        tracer_provider = create_tracer_provider(settings.tracing)
        trace.set_tracer_provider(tracer_provider)
    database = PostgresDatabase(
        settings.postgres.dsn,
        connect_timeout_seconds=settings.postgres.connect_timeout_seconds,
        pool_timeout_seconds=settings.postgres.pool_timeout_seconds,
        command_timeout_seconds=settings.postgres.command_timeout_seconds,
        statement_timeout_ms=settings.postgres.statement_timeout_ms,
    )
    service_settings = ServiceSettings()  # pyright: ignore[reportCallIssue]
    auth_http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(settings.auth_client.timeout_seconds)
    )
    yookassa_http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(settings.yookassa.timeout_seconds)
    )
    auth_gateway = HttpAuthGateway(
        http_client=auth_http_client,
        base_url=str(settings.auth_client.service_url),
        service_api_key=service_settings.api_key.get_secret_value(),
    )
    payment_provider = YooKassaProviderAdapter(
        http_client=yookassa_http_client,
        shop_id=settings.yookassa.shop_id,
        secret_key=settings.yookassa.secret_key.get_secret_value(),
        base_url=str(settings.yookassa.api_base_url),
    )
    repo = SQLAlchemyPurchaseRepository(database.sessionmaker)
    outbox_dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth_gateway,
        max_attempts=settings.worker.billing_outbox_max_attempts,
    )
    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=payment_provider,
        public_base_url=str(settings.worker.billing_public_base_url),
        outbox_dispatcher=outbox_dispatcher,
    )

    async def reconcile_step() -> None:
        await reconciler.reconcile_due_pending_payments(
            min_age_seconds=settings.worker.reconcile_min_age_seconds,
        )

    async def outbox_step() -> None:
        await outbox_dispatcher.process_due_tasks()

    runner = BillingWorkerRunner(
        database,
        reconcile_interval_seconds=settings.worker.billing_reconcile_interval_seconds,
        outbox_interval_seconds=settings.worker.billing_outbox_interval_seconds,
        reconcile_step=reconcile_step,
        outbox_step=outbox_step,
        tracer_provider=tracer_provider,
    )
    install_signal_handlers(asyncio.get_running_loop(), runner)
    try:
        await runner.run()
    finally:
        try:
            await yookassa_http_client.aclose()
        finally:
            await auth_http_client.aclose()


if __name__ == '__main__':  # pragma: no cover
    asyncio.run(main())
