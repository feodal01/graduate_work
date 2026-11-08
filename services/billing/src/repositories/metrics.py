from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from db.sql.errors import DatabaseUnavailableError
from models import (
    OutboxTask,
    Payment,
    Refund,
)
from repositories._sqlalchemy import SQLAlchemyRepository
from repositories.read_models import (
    OperationalMetricsSnapshot,
)


class OperationalMetricsRepository(Protocol):
    """Счётчики зависших операций и outbox для мониторинга (6.4, T-35)."""

    async def get_operational_metrics(
        self,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> OperationalMetricsSnapshot: ...


class SQLAlchemyOperationalMetricsRepository(SQLAlchemyRepository):
    """Счётчики зависших операций и outbox для мониторинга (6.4, T-35)."""

    async def get_operational_metrics(
        self,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> OperationalMetricsSnapshot:
        now = now_utc or datetime.now(UTC)
        stuck_cutoff = now - timedelta(seconds=stuck_threshold_seconds)
        try:
            async with self._session_factory() as session:
                stuck_payments_count = int(
                    await session.scalar(
                        select(func.count(Payment.id)).where(
                            Payment.status == 'pending',
                            Payment.created_at <= stuck_cutoff,
                        )
                    )
                    or 0
                )
                stuck_refunds_count = int(
                    await session.scalar(
                        select(func.count(Refund.id)).where(
                            Refund.status == 'processing',
                            Refund.created_at <= stuck_cutoff,
                        )
                    )
                    or 0
                )
                outbox_pending_count = int(
                    await session.scalar(
                        select(func.count(OutboxTask.id)).where(
                            OutboxTask.status == 'pending'
                        )
                    )
                    or 0
                )
                outbox_stuck_pending_count = int(
                    await session.scalar(
                        select(func.count(OutboxTask.id)).where(
                            OutboxTask.status == 'pending',
                            OutboxTask.created_at <= stuck_cutoff,
                        )
                    )
                    or 0
                )
                outbox_requires_intervention_count = int(
                    await session.scalar(
                        select(func.count(OutboxTask.id)).where(
                            OutboxTask.status == 'requires_intervention'
                        )
                    )
                    or 0
                )
                return OperationalMetricsSnapshot(
                    stuck_payments_count=stuck_payments_count,
                    stuck_refunds_count=stuck_refunds_count,
                    stuck_operations_count=(stuck_payments_count + stuck_refunds_count),
                    outbox_pending_count=outbox_pending_count,
                    outbox_stuck_pending_count=outbox_stuck_pending_count,
                    outbox_requires_intervention_count=(
                        outbox_requires_intervention_count
                    ),
                    undelivered_outbox_count=(
                        outbox_requires_intervention_count + outbox_stuck_pending_count
                    ),
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc
