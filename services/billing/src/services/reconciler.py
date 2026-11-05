import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from opentelemetry import trace

from clients.payment_provider import (
    CreateProviderPaymentRequest,
    PaymentProviderError,
    PaymentProviderGateway,
    ProviderPermanentError,
)
from core.observability import correlation_request_id
from domain.subscription import ReconcileSource
from repositories.purchase import PaymentRecordProtocol

logger = logging.getLogger(__name__)

CHECK_THROTTLE_WINDOW = timedelta(seconds=5)
IDEMPOTENCY_KEY_TTL = timedelta(hours=24)


@dataclass(frozen=True, slots=True)
class ReconcileOutcome:
    payment_id: UUID
    user_id: UUID
    status: str
    confirmation_url: str | None
    failure_reason: str | None
    transitioned: bool
    throttled: bool


class ReconcileRepositoryProtocol(Protocol):
    async def get_payment(
        self,
        payment_id: UUID,
    ) -> PaymentRecordProtocol | None: ...

    async def get_payment_by_provider_id(
        self,
        provider_code: str,
        provider_payment_id: str,
    ) -> PaymentRecordProtocol | None: ...

    async def list_due_pending_payments(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
        limit: int = 20,
    ) -> Sequence[PaymentRecordProtocol]: ...

    async def claim_next_due_pending_payment(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
    ) -> PaymentRecordProtocol | None: ...

    async def record_payment_reconcile_error(
        self,
        payment_id: UUID,
        *,
        error_message: str,
    ) -> PaymentRecordProtocol: ...

    async def save_provider_payment_created(
        self,
        payment_id: UUID,
        *,
        provider_payment_id: str,
        confirmation_url: str | None,
        provider_details: dict[str, Any],
    ) -> PaymentRecordProtocol: ...

    async def record_payment_checked(
        self,
        payment_id: UUID,
        *,
        confirmation_url: str | None = None,
        provider_details: dict[str, Any] | None = None,
    ) -> PaymentRecordProtocol: ...

    async def finalize_payment_succeeded(
        self,
        payment_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[PaymentRecordProtocol, bool]: ...

    async def finalize_payment_failed(
        self,
        payment_id: UUID,
        *,
        failure_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[PaymentRecordProtocol, bool]: ...


class OutboxDispatcherProtocol(Protocol):
    async def try_deliver_payment_tasks(self, payment_id: UUID) -> int: ...


class PaymentReconciler:
    """Единый доменный сервис сверки платежей через GET (T-11, T-16, T-17)."""

    def __init__(
        self,
        *,
        repository: ReconcileRepositoryProtocol,
        payment_provider: PaymentProviderGateway,
        public_base_url: str = 'http://localhost',
        outbox_dispatcher: OutboxDispatcherProtocol | None = None,
    ) -> None:
        self._repo = repository
        self._provider = payment_provider
        self._public_base_url = public_base_url.rstrip('/')
        self._outbox_dispatcher = outbox_dispatcher

    async def notify_payment_tasks(self, payment_id: UUID) -> None:
        """Немедленно запускает отправку задач outbox для платежа (US-S-02)."""
        if self._outbox_dispatcher is not None:
            await self._outbox_dispatcher.try_deliver_payment_tasks(payment_id)

    async def reconcile_payment(
        self,
        payment_id: UUID,
        *,
        source: ReconcileSource,
        respect_throttle: bool = True,
    ) -> ReconcileOutcome:
        payment = await self._repo.get_payment(payment_id)
        if payment is None:
            raise LookupError(f'Payment {payment_id} not found')

        return await self.reconcile_payment_record(
            payment,
            source=source,
            respect_throttle=respect_throttle,
        )

    async def reconcile_by_provider_payment_id(
        self,
        provider_payment_id: str,
        *,
        source: ReconcileSource = 'webhook',
        respect_throttle: bool = False,
    ) -> ReconcileOutcome | None:
        payment = await self._repo.get_payment_by_provider_id(
            self._provider.provider_code,
            provider_payment_id.strip(),
        )
        if payment is None:
            return None

        return await self.reconcile_payment_record(
            payment,
            source=source,
            respect_throttle=respect_throttle,
        )

    async def reconcile_due_pending_payments(
        self,
        *,
        min_age_seconds: float,
        limit: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int:
        """Фоновая сверка платежей старше min_age_seconds (T-17, BIL-FR-05)."""
        processed = 0
        throttle_sec = CHECK_THROTTLE_WINDOW.total_seconds()
        tracer = trace.get_tracer('billing.reconciler')
        for _ in range(limit):
            if should_stop is not None and should_stop():
                break
            payment = await self._repo.claim_next_due_pending_payment(
                min_age_seconds=min_age_seconds,
                throttle_seconds=throttle_sec,
            )
            if payment is None:
                break
            with (
                correlation_request_id(f'reconcile-{payment.id}') as req_id,
                tracer.start_as_current_span(
                    'billing.reconcile_payment',
                    attributes={
                        'request_id': req_id,
                        'payment_id': str(payment.id),
                    },
                ),
            ):
                try:
                    await self._reconcile_background_payment(payment)
                    processed += 1
                except PaymentProviderError as exc:
                    logger.warning(
                        'Background reconcile deferred for payment_id=%s: %s',
                        payment.id,
                        exc,
                    )
                    await self._repo.record_payment_reconcile_error(
                        payment.id,
                        error_message=str(exc),
                    )
        return processed

    async def _reconcile_background_payment(
        self,
        payment: PaymentRecordProtocol,
    ) -> ReconcileOutcome:
        if payment.status != 'pending':
            return ReconcileOutcome(
                payment_id=payment.id,
                user_id=payment.user_id,
                status=payment.status,
                confirmation_url=payment.confirmation_url,
                failure_reason=payment.failure_reason,
                transitioned=False,
                throttled=False,
            )

        if not payment.provider_payment_id:
            now_utc = datetime.now(UTC)
            created_at = (
                payment.created_at
                if payment.created_at.tzinfo is not None
                else payment.created_at.replace(tzinfo=UTC)
            )
            # Инвариант 13: если за 24 часа жизни ключа номер ЮKassa не получен,
            # переводим платёж в Не оплачен (средства не списаны).
            if (now_utc - created_at) >= IDEMPOTENCY_KEY_TTL:
                updated, transitioned = await self._repo.finalize_payment_failed(
                    payment.id,
                    failure_reason='idempotency_key_expired',
                    source='reconcile',
                    public_base_url=self._public_base_url,
                )
                if transitioned and self._outbox_dispatcher is not None:
                    await self._outbox_dispatcher.try_deliver_payment_tasks(payment.id)
                return ReconcileOutcome(
                    payment_id=updated.id,
                    user_id=updated.user_id,
                    status=updated.status,
                    confirmation_url=updated.confirmation_url,
                    failure_reason=updated.failure_reason,
                    transitioned=transitioned,
                    throttled=False,
                )

            # До 24 часов: досоздаём платёж по тому же ключу и проверяем через GET
            return_url = f'{self._public_base_url}/billing/payments/{payment.id}'
            try:
                created_snapshot = await self._provider.create_payment(
                    CreateProviderPaymentRequest(
                        payment_id=payment.id,
                        idempotency_key=payment.idempotency_key,
                        amount_kopeks=payment.amount_kopeks,
                        currency=payment.currency.strip(),
                        description='Подписка «Премиум» на 30 дней',
                        return_url=return_url,
                    )
                )
            except ProviderPermanentError as exc:
                updated, transitioned = await self._repo.finalize_payment_failed(
                    payment.id,
                    failure_reason=exc.message,
                    source='reconcile',
                    public_base_url=self._public_base_url,
                )
                if transitioned and self._outbox_dispatcher is not None:
                    await self._outbox_dispatcher.try_deliver_payment_tasks(payment.id)
                return ReconcileOutcome(
                    payment_id=updated.id,
                    user_id=updated.user_id,
                    status=updated.status,
                    confirmation_url=updated.confirmation_url,
                    failure_reason=updated.failure_reason,
                    transitioned=transitioned,
                    throttled=False,
                )

            payment = await self._repo.save_provider_payment_created(
                payment.id,
                provider_payment_id=created_snapshot.provider_payment_id,
                confirmation_url=created_snapshot.confirmation_url,
                provider_details=created_snapshot.provider_details,
            )

        return await self.reconcile_payment_record(
            payment,
            source='reconcile',
            respect_throttle=False,
        )

    async def reconcile_payment_record(
        self,
        payment: PaymentRecordProtocol,
        *,
        source: ReconcileSource,
        respect_throttle: bool,
    ) -> ReconcileOutcome:
        # Инвариант 7: если платёж в итоговом статусе, не вызываем ЮKassa
        if payment.status != 'pending':
            return ReconcileOutcome(
                payment_id=payment.id,
                user_id=payment.user_id,
                status=payment.status,
                confirmation_url=payment.confirmation_url,
                failure_reason=payment.failure_reason,
                transitioned=False,
                throttled=False,
            )

        if not payment.provider_payment_id:
            return ReconcileOutcome(
                payment_id=payment.id,
                user_id=payment.user_id,
                status='pending',
                confirmation_url=payment.confirmation_url,
                failure_reason=None,
                transitioned=False,
                throttled=False,
            )

        now_utc = datetime.now(UTC)
        if (
            respect_throttle
            and payment.last_checked_at is not None
            and (now_utc - payment.last_checked_at) < CHECK_THROTTLE_WINDOW
        ):
            return ReconcileOutcome(
                payment_id=payment.id,
                user_id=payment.user_id,
                status='pending',
                confirmation_url=payment.confirmation_url,
                failure_reason=None,
                transitioned=False,
                throttled=True,
            )

        snapshot = await self._provider.get_payment(
            payment.provider_payment_id,
            expected_payment_id=payment.id,
            expected_amount_kopeks=payment.amount_kopeks,
            expected_currency=payment.currency.strip(),
        )

        if snapshot.domain_status == 'pending':
            confirmation_url = snapshot.confirmation_url or payment.confirmation_url
            updated = await self._repo.record_payment_checked(
                payment.id,
                confirmation_url=confirmation_url,
                provider_details=snapshot.provider_details,
            )
            return ReconcileOutcome(
                payment_id=updated.id,
                user_id=updated.user_id,
                status=updated.status,
                confirmation_url=updated.confirmation_url,
                failure_reason=updated.failure_reason,
                transitioned=False,
                throttled=False,
            )

        if snapshot.domain_status == 'succeeded':
            updated, transitioned = await self._repo.finalize_payment_succeeded(
                payment.id,
                source=source,
                provider_details=snapshot.provider_details,
                public_base_url=self._public_base_url,
            )
            if transitioned and self._outbox_dispatcher is not None:
                await self._outbox_dispatcher.try_deliver_payment_tasks(payment.id)
            return ReconcileOutcome(
                payment_id=updated.id,
                user_id=updated.user_id,
                status=updated.status,
                confirmation_url=updated.confirmation_url,
                failure_reason=updated.failure_reason,
                transitioned=transitioned,
                throttled=False,
            )

        reason = snapshot.failure_reason or 'canceled_by_provider'
        updated, transitioned = await self._repo.finalize_payment_failed(
            payment.id,
            failure_reason=reason,
            source=source,
            provider_details=snapshot.provider_details,
            public_base_url=self._public_base_url,
        )
        if transitioned and self._outbox_dispatcher is not None:
            await self._outbox_dispatcher.try_deliver_payment_tasks(payment.id)
        return ReconcileOutcome(
            payment_id=updated.id,
            user_id=updated.user_id,
            status=updated.status,
            confirmation_url=updated.confirmation_url,
            failure_reason=updated.failure_reason,
            transitioned=transitioned,
            throttled=False,
        )
