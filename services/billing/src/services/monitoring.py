import logging
from datetime import UTC, datetime, timedelta
from typing import Protocol

import sentry_sdk
from opentelemetry import trace

from core.observability import correlation_request_id
from repositories.purchase import OperationalMetricsSnapshot

logger = logging.getLogger(__name__)


class OperationalMetricsRepositoryProtocol(Protocol):
    async def get_operational_metrics(
        self,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> OperationalMetricsSnapshot: ...


class OperationalMonitor:
    """Мониторинг зависших операций и сбоев доставки по разделу 6.4 ТЗ (T-35)."""

    def __init__(
        self,
        *,
        repository: OperationalMetricsRepositoryProtocol,
        stuck_threshold_seconds: int = 600,
        alert_cooldown_seconds: float = 300.0,
    ) -> None:
        self._repo = repository
        self._stuck_threshold_seconds = stuck_threshold_seconds
        self._alert_cooldown = timedelta(seconds=alert_cooldown_seconds)
        self._last_stuck_payments = 0
        self._last_stuck_refunds = 0
        self._last_stuck_alert_at: datetime | None = None
        self._last_intervene_outbox = 0
        self._last_stuck_pending_outbox = 0
        self._last_undelivered_alert_at: datetime | None = None

    async def check_and_emit_signals(
        self,
        *,
        now_utc: datetime | None = None,
    ) -> OperationalMetricsSnapshot:
        """Снимает операционные метрики и поднимает сигналы в GlitchTip + ERROR-лог."""
        with correlation_request_id('monitor-operational'):
            now = now_utc or datetime.now(UTC)
            snapshot = await self._repo.get_operational_metrics(
                stuck_threshold_seconds=self._stuck_threshold_seconds,
                now_utc=now,
            )

            span = trace.get_current_span()
            if span.is_recording():
                span.set_attribute(
                    'billing.stuck_payments_count', snapshot.stuck_payments_count
                )
                span.set_attribute(
                    'billing.stuck_refunds_count', snapshot.stuck_refunds_count
                )
                span.set_attribute(
                    'billing.stuck_operations_count', snapshot.stuck_operations_count
                )
                span.set_attribute(
                    'billing.outbox_pending_count', snapshot.outbox_pending_count
                )
                span.set_attribute(
                    'billing.outbox_stuck_pending_count',
                    snapshot.outbox_stuck_pending_count,
                )
                span.set_attribute(
                    'billing.outbox_requires_intervention_count',
                    snapshot.outbox_requires_intervention_count,
                )
                span.set_attribute(
                    'billing.undelivered_outbox_count',
                    snapshot.undelivered_outbox_count,
                )

            if snapshot.stuck_operations_count > 0:
                if self._should_emit(
                    current_components=(
                        snapshot.stuck_payments_count,
                        snapshot.stuck_refunds_count,
                    ),
                    previous_components=(
                        self._last_stuck_payments,
                        self._last_stuck_refunds,
                    ),
                    last_alert_at=self._last_stuck_alert_at,
                    now=now,
                ):
                    msg = (
                        f'Сигнал мониторинга «Зависшие операции»: обнаружено '
                        f'{snapshot.stuck_operations_count} операций старше '
                        f'{self._stuck_threshold_seconds} с '
                        f'(платежей pending: {snapshot.stuck_payments_count}, '
                        f'возвратов processing: {snapshot.stuck_refunds_count})'
                    )
                    logger.error(msg)
                    sentry_sdk.capture_message(msg, level='error')
                    self._last_stuck_alert_at = now
            self._last_stuck_payments = snapshot.stuck_payments_count
            self._last_stuck_refunds = snapshot.stuck_refunds_count

            if snapshot.undelivered_outbox_count > 0:
                if self._should_emit(
                    current_components=(
                        snapshot.outbox_requires_intervention_count,
                        snapshot.outbox_stuck_pending_count,
                    ),
                    previous_components=(
                        self._last_intervene_outbox,
                        self._last_stuck_pending_outbox,
                    ),
                    last_alert_at=self._last_undelivered_alert_at,
                    now=now,
                ):
                    msg = (
                        f'Сигнал мониторинга «Не доставлено в Auth или Notifications»: '
                        f'обнаружено {snapshot.undelivered_outbox_count} проблемных '
                        f'задач (requires_intervention: '
                        f'{snapshot.outbox_requires_intervention_count}, '
                        f'зависших pending: {snapshot.outbox_stuck_pending_count})'
                    )
                    logger.error(msg)
                    sentry_sdk.capture_message(msg, level='error')
                    self._last_undelivered_alert_at = now
            self._last_intervene_outbox = snapshot.outbox_requires_intervention_count
            self._last_stuck_pending_outbox = snapshot.outbox_stuck_pending_count

            return snapshot

    def _should_emit(
        self,
        *,
        current_components: tuple[int, int],
        previous_components: tuple[int, int],
        last_alert_at: datetime | None,
        now: datetime,
    ) -> bool:
        if any(
            curr > prev
            for curr, prev in zip(current_components, previous_components, strict=True)
        ):
            return True
        if last_alert_at is None:
            return True
        return (now - last_alert_at) >= self._alert_cooldown
