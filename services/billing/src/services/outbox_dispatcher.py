import logging
from typing import Protocol
from uuid import UUID

from clients.auth import (
    AuthGateway,
    AuthPermanentDeliveryError,
    AuthServiceUnavailableError,
)
from core.observability import correlation_request_id
from repositories.purchase import (
    DeliveryOutcome,
    OutboxDeliverFn,
    OutboxTaskRecordProtocol,
)

logger = logging.getLogger(__name__)


class OutboxRepositoryProtocol(Protocol):
    async def process_due_auth_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        payment_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
    ) -> int: ...


class OutboxDispatcher:
    """Диспетчер доставки задач доступа из outbox_tasks в Auth (T-16, BIL-FR-11)."""

    def __init__(
        self,
        *,
        repository: OutboxRepositoryProtocol,
        auth_gateway: AuthGateway,
        max_attempts: int = 20,
    ) -> None:
        self._repo = repository
        self._auth = auth_gateway
        self._max_attempts = max_attempts

    async def try_deliver_payment_tasks(self, payment_id: UUID) -> int:
        """Немедленная первая отправка после коммита в БД (US-S-02, BIL-FR-11)."""
        try:
            return await self._repo.process_due_auth_outbox_tasks(
                self._deliver_auth_task,
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

    async def process_due_tasks(self, *, limit: int = 20) -> int:
        """Периодический проход по готовым задачам доступа (для воркера)."""
        return await self._repo.process_due_auth_outbox_tasks(
            self._deliver_auth_task,
            payment_id=None,
            limit=limit,
            max_attempts=self._max_attempts,
        )

    async def _deliver_auth_task(
        self,
        task: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        with correlation_request_id(f'outbox-{task.id}') as request_id:
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
