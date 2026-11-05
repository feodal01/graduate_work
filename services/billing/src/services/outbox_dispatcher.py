import logging
from collections.abc import Callable, Sequence
from typing import Protocol
from uuid import UUID

from opentelemetry import trace

from clients.auth import (
    AuthGateway,
    AuthPermanentDeliveryError,
    AuthServiceUnavailableError,
)
from clients.notifications import (
    NotificationsGateway,
    NotificationsPermanentError,
    NotificationsTransientError,
)
from core.observability import correlation_request_id
from repositories.purchase import (
    DeliveryOutcome,
    OutboxDeliverFn,
    OutboxTaskRecordProtocol,
)

__all__ = [
    'NotificationsGateway',
    'NotificationsPermanentError',
    'NotificationsTransientError',
    'OutboxDispatcher',
    'OutboxRepositoryProtocol',
]

logger = logging.getLogger(__name__)

_EMAIL_BATCH_TASK_TYPES = frozenset(
    {
        'payment_succeeded_email',
        'payment_failed_email',
        'subscription_expiration_email',
        'refund_succeeded_email',
    }
)


class OutboxRepositoryProtocol(Protocol):
    async def process_due_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        target_services: Sequence[str] = ('auth', 'notifications'),
        payment_id: UUID | None = None,
        task_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int: ...


class OutboxDispatcher:
    """Диспетчер доставки задач из outbox_tasks в Auth и Notifications (T-16, T-19)."""

    def __init__(
        self,
        *,
        repository: OutboxRepositoryProtocol,
        auth_gateway: AuthGateway,
        notifications_gateway: NotificationsGateway | None = None,
        max_attempts: int = 20,
    ) -> None:
        self._repo = repository
        self._auth = auth_gateway
        self._notifications = notifications_gateway
        self._max_attempts = max_attempts

    @property
    def _enabled_target_services(self) -> tuple[str, ...]:
        if self._notifications is not None:
            return ('auth', 'notifications')
        return ('auth',)

    async def try_deliver_payment_tasks(self, payment_id: UUID) -> int:
        """Немедленная первая отправка после коммита в БД (US-S-02, BIL-FR-11)."""
        try:
            return await self._repo.process_due_outbox_tasks(
                self._deliver_task,
                target_services=self._enabled_target_services,
                payment_id=payment_id,
                limit=5,
                max_attempts=self._max_attempts,
            )
        except Exception as exc:
            logger.warning(
                'Immediate outbox dispatch skipped for payment_id=%s: %s',
                payment_id,
                exc,
            )
            return 0

    async def try_deliver_task(self, task_id: UUID) -> int:
        """Немедленная попытка доставки конкретной задачи (для ручного повтора)."""
        try:
            return await self._repo.process_due_outbox_tasks(
                self._deliver_task,
                target_services=self._enabled_target_services,
                task_id=task_id,
                limit=1,
                max_attempts=self._max_attempts,
            )
        except Exception as exc:
            logger.warning(
                'Immediate outbox dispatch skipped for task_id=%s: %s',
                task_id,
                exc,
            )
            return 0

    async def process_due_tasks(
        self,
        *,
        limit: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int:
        """Периодический проход по готовым задачам в очереди (для воркера)."""
        return await self._repo.process_due_outbox_tasks(
            self._deliver_task,
            target_services=self._enabled_target_services,
            payment_id=None,
            limit=limit,
            max_attempts=self._max_attempts,
            should_stop=should_stop,
        )

    async def _deliver_task(
        self,
        task: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        tracer = trace.get_tracer('billing.outbox')
        with (
            correlation_request_id(f'outbox-{task.id}') as request_id,
            tracer.start_as_current_span(
                'billing.outbox_deliver',
                attributes={
                    'request_id': request_id,
                    'task_id': str(task.id),
                    'payment_id': str(task.payment_id),
                    'target_service': task.target_service,
                    'task_type': task.task_type,
                },
            ),
        ):
            if task.target_service == 'auth':
                return await self._deliver_auth_task(task, request_id=request_id)
            if task.target_service == 'notifications':
                return await self._deliver_notifications_task(
                    task,
                    request_id=request_id,
                )
            return (
                'permanent_error',
                f'Unsupported target_service: {task.target_service}',
            )

    async def _deliver_auth_task(
        self,
        task: OutboxTaskRecordProtocol,
        *,
        request_id: str,
    ) -> tuple[DeliveryOutcome, str | None]:
        if task.task_type != 'sync_access':
            return 'permanent_error', f'Unsupported auth task_type: {task.task_type}'
        try:
            raw_version = (
                task.access_version
                if task.access_version is not None
                else task.payload.get('version')
            )
            if raw_version is None or isinstance(raw_version, bool):
                raise ValueError('Missing or invalid access version in outbox task')
            version = int(raw_version)
            if version < 1:
                raise ValueError(f'Access version must be >= 1, got {version}')
        except (ValueError, TypeError) as exc:
            return 'permanent_error', f'Malformed outbox payload: {exc}'

        raw_expires = task.payload.get('expires_at')
        expires_at = str(raw_expires) if raw_expires is not None else None

        try:
            await self._auth.put_subscription_access(
                task.user_id,
                version=version,
                expires_at=expires_at,
                request_id=request_id,
            )
            return 'completed', None
        except AuthPermanentDeliveryError as exc:
            return 'permanent_error', str(exc)
        except AuthServiceUnavailableError as exc:
            logger.warning(
                'Transient Auth error for outbox task %s (attempt %s): %s',
                task.id,
                task.attempts + 1,
                exc,
            )
            return 'transient_error', str(exc)

    async def _deliver_notifications_task(
        self,
        task: OutboxTaskRecordProtocol,
        *,
        request_id: str,
    ) -> tuple[DeliveryOutcome, str | None]:
        if self._notifications is None:
            return 'transient_error', 'Notifications gateway is not configured'

        try:
            raw_campaign_id = task.payload.get('campaign_id')
            if not isinstance(raw_campaign_id, str) or not raw_campaign_id.strip():
                raise ValueError('Missing campaign_id in notifications payload')
            campaign_id = UUID(raw_campaign_id)

            if task.task_type == 'cancel_expiration_email':
                await self._notifications.cancel_campaign(
                    campaign_id,
                    request_id=request_id,
                )
                return 'completed', None

            if task.task_type not in _EMAIL_BATCH_TASK_TYPES:
                return (
                    'permanent_error',
                    f'Unsupported notifications task_type: {task.task_type}',
                )

            raw_batch_id = task.payload.get('batch_id')
            if not isinstance(raw_batch_id, str) or not raw_batch_id.strip():
                raise ValueError('Missing batch_id in notifications payload')
            batch_id = UUID(raw_batch_id)

            template_id = str(
                task.payload.get('template_id') or 'billing_notification'
            ).strip()
            subject = str(task.payload.get('subject') or '').strip()
            text = str(task.payload.get('text') or '').strip()
            if not subject or not text:
                raise ValueError('Missing subject or text in notifications payload')

            raw_available_at = task.payload.get('available_at')
            available_at = (
                str(raw_available_at) if raw_available_at is not None else None
            )
        except (ValueError, TypeError) as exc:
            return 'permanent_error', f'Malformed notifications payload: {exc}'

        try:
            await self._notifications.send_campaign_batch(
                campaign_id=campaign_id,
                batch_id=batch_id,
                user_id=task.user_id,
                template_id=template_id,
                subject=subject,
                text=text,
                available_at=available_at,
                request_id=request_id,
            )
            return 'completed', None
        except NotificationsPermanentError as exc:
            return 'permanent_error', str(exc)
        except NotificationsTransientError as exc:
            logger.warning(
                'Transient Notifications error for outbox task %s (attempt %s): %s',
                task.id,
                task.attempts + 1,
                exc,
            )
            return 'transient_error', str(exc)
