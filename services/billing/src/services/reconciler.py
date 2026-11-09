import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol
from uuid import UUID

from opentelemetry import trace

from clients.payment_provider import (
    CreateProviderPaymentRequest,
    CreateProviderRefundRequest,
    PaymentProviderError,
    ProviderPermanentError,
)
from clients.payment_provider_registry import PaymentProviderRegistry
from core.observability import correlation_request_id
from domain.subscription import ReconcileSource
from repositories.payments import PaymentRepository
from repositories.records import PaymentRecordProtocol, RefundRecordProtocol
from repositories.refunds import RefundRepository

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


@dataclass(frozen=True, slots=True)
class RefundReconcileOutcome:
    refund_id: UUID
    payment_id: UUID
    subscription_id: UUID
    provider_code: str
    provider_refund_id: str | None
    amount_kopeks: int
    currency: str
    status: str
    reason: str
    rejection_reason: str | None
    admin_user_id: UUID
    created_at: datetime
    updated_at: datetime
    transitioned: bool
    throttled: bool


class OutboxDispatcherProtocol(Protocol):
    async def try_deliver_payment_tasks(self, payment_id: UUID) -> int: ...


class PaymentReconciler:
    """Единый доменный сервис сверки платежей через GET (T-11, T-16, T-17)."""

    def __init__(
        self,
        *,
        payments: PaymentRepository,
        refunds: RefundRepository,
        payment_providers: PaymentProviderRegistry,
        public_base_url: str = 'http://localhost',
        outbox_dispatcher: OutboxDispatcherProtocol | None = None,
    ) -> None:
        self._payments = payments
        self._refunds = refunds
        self._providers = payment_providers
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
        payment = await self._payments.get_payment(payment_id)
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
        provider_code: str,
        source: ReconcileSource = 'webhook',
        respect_throttle: bool = False,
    ) -> ReconcileOutcome | None:
        cleaned_id = provider_payment_id.strip()
        payment = await self._payments.get_payment_by_provider_id(
            provider_code,
            cleaned_id,
        )
        if payment is None:
            return None

        return await self.reconcile_payment_record(
            payment,
            source=source,
            respect_throttle=respect_throttle,
        )

    async def reconcile_by_provider_operation_id(
        self,
        provider_operation_id: str,
        *,
        provider_code: str,
        source: ReconcileSource = 'webhook',
        respect_throttle: bool = False,
    ) -> ReconcileOutcome | RefundReconcileOutcome | None:
        """Сверяет операцию из вебхука провайдера `provider_code` (BIL-FR-07)."""
        cleaned_id = provider_operation_id.strip()
        pay_outcome = await self.reconcile_by_provider_payment_id(
            cleaned_id,
            provider_code=provider_code,
            source=source,
            respect_throttle=respect_throttle,
        )
        if pay_outcome is not None:
            return pay_outcome

        refund = await self._refunds.get_refund_by_provider_id(
            provider_code,
            cleaned_id,
        )
        if refund is not None:
            return await self.reconcile_refund_record(
                refund,
                source=source,
                respect_throttle=respect_throttle,
            )
        return None

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
            payment = await self._payments.claim_next_due_pending_payment(
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
                    await self._payments.record_payment_reconcile_error(
                        payment.id,
                        error_message=str(exc),
                    )
        processed += await self.reconcile_due_processing_refunds(
            min_age_seconds=min_age_seconds,
            limit=limit,
            should_stop=should_stop,
        )
        return processed

    async def _reconcile_background_payment(
        self,
        payment: PaymentRecordProtocol,
        *,
        source: ReconcileSource = 'reconcile',
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
                updated, transitioned = await self._payments.finalize_payment_failed(
                    payment.id,
                    failure_reason='idempotency_key_expired',
                    source=source,
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
                created_snapshot = await self._providers.get(
                    payment.provider_code
                ).create_payment(
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
                updated, transitioned = await self._payments.finalize_payment_failed(
                    payment.id,
                    failure_reason=exc.message,
                    source=source,
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

            payment = await self._payments.save_provider_payment_created(
                payment.id,
                provider_payment_id=created_snapshot.provider_payment_id,
                confirmation_url=created_snapshot.confirmation_url,
                provider_details=created_snapshot.provider_details,
            )

        return await self.reconcile_payment_record(
            payment,
            source=source,
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

        if respect_throttle and not await self._payments.claim_payment_check(
            payment.id,
            throttle_seconds=CHECK_THROTTLE_WINDOW.total_seconds(),
        ):
            # Окно занято другим запросом (или платёж только что завершился):
            # отдаём актуальное состояние из БД без запроса в ЮKassa.
            current = await self._payments.get_payment(payment.id) or payment
            return ReconcileOutcome(
                payment_id=current.id,
                user_id=current.user_id,
                status=current.status,
                confirmation_url=current.confirmation_url,
                failure_reason=current.failure_reason,
                transitioned=False,
                throttled=current.status == 'pending',
            )

        snapshot = await self._providers.get(payment.provider_code).get_payment(
            payment.provider_payment_id,
            expected_payment_id=payment.id,
            expected_amount_kopeks=payment.amount_kopeks,
            expected_currency=payment.currency.strip(),
        )

        if snapshot.domain_status == 'pending':
            confirmation_url = snapshot.confirmation_url or payment.confirmation_url
            updated = await self._payments.record_payment_checked(
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
            updated, transitioned = await self._payments.finalize_payment_succeeded(
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
        updated, transitioned = await self._payments.finalize_payment_failed(
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

    async def create_or_reconcile_refund(
        self,
        payment_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        refund_id: UUID | None = None,
    ) -> RefundReconcileOutcome:
        """Создаёт или повторяет полный возврат и запускает сверку GET (BIL-FR-10)."""

        refund, _ = await self._refunds.get_or_create_processing_refund(
            payment_id,
            admin_user_id=admin_user_id,
            reason=reason,
            refund_id=refund_id,
        )
        if refund.status != 'processing':
            # Повтор формы для уже выполненной или отклонённой попытки:
            # возвращаем её итог без новых запросов в ЮKassa.
            return self._build_refund_outcome(
                refund,
                transitioned=False,
                throttled=False,
            )
        if (
            not refund.provider_refund_id
            and datetime.now(UTC) - refund.created_at >= IDEMPOTENCY_KEY_TTL
        ):
            # Инв. 14: ключ в ЮKassa истёк, повторный POST мог бы создать
            # второй возврат (и получить 400 «сумма больше остатка», после чего
            # уже выполненный возврат считался бы отклонённым). Ищем попытку
            # по metadata, как фоновая сверка.
            try:
                return await self._reconcile_background_refund(refund, source='admin')
            except PaymentProviderError as exc:
                logger.warning(
                    'Refund lookup by metadata deferred for refund_id=%s: %s',
                    refund.id,
                    exc,
                )
                return self._build_refund_outcome(
                    refund,
                    transitioned=False,
                    throttled=False,
                )
        if not refund.provider_refund_id:
            payment = await self._payments.get_payment(refund.payment_id)
            if payment is None or not payment.provider_payment_id:
                return self._build_refund_outcome(
                    refund,
                    transitioned=False,
                    throttled=False,
                )
            try:
                created_snap = await self._providers.get(
                    refund.provider_code
                ).create_refund(
                    CreateProviderRefundRequest(
                        refund_id=refund.id,
                        idempotency_key=refund.idempotency_key,
                        provider_payment_id=payment.provider_payment_id,
                        amount_kopeks=refund.amount_kopeks,
                        currency=refund.currency.strip(),
                        description=f'Возврат по подписке (платёж {payment.id})',
                    )
                )
            except ProviderPermanentError as exc:
                updated, transitioned = await self._refunds.finalize_refund_rejected(
                    refund.id,
                    rejection_reason=exc.message,
                    source='admin',
                )
                return self._build_refund_outcome(
                    updated,
                    transitioned=transitioned,
                    throttled=False,
                )
            except PaymentProviderError as exc:
                logger.warning(
                    'YooKassa refund creation deferred for refund_id=%s: %s',
                    refund.id,
                    exc,
                )
                return self._build_refund_outcome(
                    refund,
                    transitioned=False,
                    throttled=False,
                )

            refund = await self._refunds.save_provider_refund_created(
                refund.id,
                provider_refund_id=created_snap.provider_refund_id,
                provider_details=created_snap.provider_details,
            )

        try:
            return await self.reconcile_refund_record(
                refund,
                source='admin',
                respect_throttle=False,
            )
        except PaymentProviderError as exc:
            logger.warning(
                'Immediate refund GET reconcile deferred for refund_id=%s: %s',
                refund.id,
                exc,
            )
            return self._build_refund_outcome(
                refund,
                transitioned=False,
                throttled=False,
            )

    async def reconcile_refund(
        self,
        refund_id: UUID,
        *,
        source: ReconcileSource,
        respect_throttle: bool = True,
    ) -> RefundReconcileOutcome:
        refund = await self._refunds.get_refund(refund_id)
        if refund is None:
            raise LookupError(f'Refund {refund_id} not found')
        return await self.reconcile_refund_record(
            refund,
            source=source,
            respect_throttle=respect_throttle,
        )

    async def reconcile_due_processing_refunds(
        self,
        *,
        min_age_seconds: float,
        limit: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int:
        processed = 0
        throttle_sec = CHECK_THROTTLE_WINDOW.total_seconds()
        tracer = trace.get_tracer('billing.reconciler')
        for _ in range(limit):
            if should_stop is not None and should_stop():
                break
            refund = await self._refunds.claim_next_due_processing_refund(
                min_age_seconds=min_age_seconds,
                throttle_seconds=throttle_sec,
            )
            if refund is None:
                break
            with (
                correlation_request_id(f'reconcile-refund-{refund.id}') as req_id,
                tracer.start_as_current_span(
                    'billing.reconcile_refund',
                    attributes={
                        'request_id': req_id,
                        'refund_id': str(refund.id),
                        'payment_id': str(refund.payment_id),
                    },
                ),
            ):
                try:
                    await self._reconcile_background_refund(refund)
                    processed += 1
                except PaymentProviderError as exc:
                    logger.warning(
                        'Background refund reconcile deferred for refund_id=%s: %s',
                        refund.id,
                        exc,
                    )
                    await self._refunds.record_refund_reconcile_error(
                        refund.id,
                        error_message=str(exc),
                    )
        return processed

    async def _reconcile_background_refund(
        self,
        refund: RefundRecordProtocol,
        *,
        source: ReconcileSource = 'reconcile',
    ) -> RefundReconcileOutcome:
        if refund.status != 'processing':
            return self._build_refund_outcome(
                refund,
                transitioned=False,
                throttled=False,
            )

        if not refund.provider_refund_id:
            payment = await self._payments.get_payment(refund.payment_id)
            if payment is None or not payment.provider_payment_id:
                return self._build_refund_outcome(
                    refund,
                    transitioned=False,
                    throttled=False,
                )
            now_utc = datetime.now(UTC)
            created_at = (
                refund.created_at
                if refund.created_at.tzinfo is not None
                else refund.created_at.replace(tzinfo=UTC)
            )
            # Инвариант 14: после истечения 24 часов ищем возврат в списке возвратов
            # по нашему refund_id в metadata.
            if (now_utc - created_at) >= IDEMPOTENCY_KEY_TTL:
                found_snap = await self._providers.get(
                    refund.provider_code
                ).find_refund_by_metadata(
                    payment.provider_payment_id,
                    refund_id=refund.id,
                    expected_amount_kopeks=refund.amount_kopeks,
                    expected_currency=refund.currency.strip(),
                )
                if found_snap is None:
                    (
                        updated,
                        transitioned,
                    ) = await self._refunds.finalize_refund_rejected(
                        refund.id,
                        rejection_reason='not_created_in_provider',
                        source=source,
                    )
                    return self._build_refund_outcome(
                        updated,
                        transitioned=transitioned,
                        throttled=False,
                    )
                refund = await self._refunds.save_provider_refund_created(
                    refund.id,
                    provider_refund_id=found_snap.provider_refund_id,
                    provider_details=found_snap.provider_details,
                )
            else:
                try:
                    created_snap = await self._providers.get(
                        refund.provider_code
                    ).create_refund(
                        CreateProviderRefundRequest(
                            refund_id=refund.id,
                            idempotency_key=refund.idempotency_key,
                            provider_payment_id=payment.provider_payment_id,
                            amount_kopeks=refund.amount_kopeks,
                            currency=refund.currency.strip(),
                            description=f'Возврат по подписке (платёж {payment.id})',
                        )
                    )
                except ProviderPermanentError as exc:
                    (
                        updated,
                        transitioned,
                    ) = await self._refunds.finalize_refund_rejected(
                        refund.id,
                        rejection_reason=exc.message,
                        source=source,
                    )
                    return self._build_refund_outcome(
                        updated,
                        transitioned=transitioned,
                        throttled=False,
                    )
                refund = await self._refunds.save_provider_refund_created(
                    refund.id,
                    provider_refund_id=created_snap.provider_refund_id,
                    provider_details=created_snap.provider_details,
                )

        return await self.reconcile_refund_record(
            refund,
            source=source,
            respect_throttle=False,
        )

    async def reconcile_operation_by_admin(
        self,
        operation_type: Literal['payment', 'refund'],
        operation_id: UUID,
    ) -> tuple[UUID, str, bool]:
        """Принудительная сверка по кнопке администратора без лимита 5 с (BIL-FR-15)."""
        if operation_type == 'payment':
            payment = await self._payments.get_payment(operation_id)
            if payment is None:
                raise LookupError(f'Payment {operation_id} not found')
            pay_out = await self._reconcile_background_payment(
                payment,
                source='admin',
            )
            return pay_out.payment_id, pay_out.status, pay_out.transitioned

        refund = await self._refunds.get_refund(operation_id)
        if refund is None:
            raise LookupError(f'Refund {operation_id} not found')
        ref_out = await self._reconcile_background_refund(
            refund,
            source='admin',
        )
        return ref_out.payment_id, ref_out.status, ref_out.transitioned

    async def reconcile_refund_record(
        self,
        refund: RefundRecordProtocol,
        *,
        source: ReconcileSource,
        respect_throttle: bool,
    ) -> RefundReconcileOutcome:
        if refund.status != 'processing':
            return self._build_refund_outcome(
                refund,
                transitioned=False,
                throttled=False,
            )
        if not refund.provider_refund_id:
            return self._build_refund_outcome(
                refund,
                transitioned=False,
                throttled=False,
            )

        now_utc = datetime.now(UTC)
        if (
            respect_throttle
            and refund.last_checked_at is not None
            and (now_utc - refund.last_checked_at) < CHECK_THROTTLE_WINDOW
        ):
            return self._build_refund_outcome(
                refund,
                transitioned=False,
                throttled=True,
            )

        snapshot = await self._providers.get(refund.provider_code).get_refund(
            refund.provider_refund_id,
            expected_refund_id=refund.id,
            expected_amount_kopeks=refund.amount_kopeks,
            expected_currency=refund.currency.strip(),
        )
        if snapshot.domain_status == 'processing':
            updated = await self._refunds.record_refund_checked(
                refund.id,
                provider_details=snapshot.provider_details,
            )
            return self._build_refund_outcome(
                updated,
                transitioned=False,
                throttled=False,
            )

        if snapshot.domain_status == 'succeeded':
            updated, transitioned = await self._refunds.finalize_refund_succeeded(
                refund.id,
                source=source,
                provider_details=snapshot.provider_details,
                public_base_url=self._public_base_url,
            )
            if transitioned and self._outbox_dispatcher is not None:
                await self._outbox_dispatcher.try_deliver_payment_tasks(
                    refund.payment_id
                )
            return self._build_refund_outcome(
                updated,
                transitioned=transitioned,
                throttled=False,
            )

        rej_reason = snapshot.rejection_reason or 'canceled_by_provider'
        updated, transitioned = await self._refunds.finalize_refund_rejected(
            refund.id,
            rejection_reason=rej_reason,
            source=source,
            provider_details=snapshot.provider_details,
        )
        return self._build_refund_outcome(
            updated,
            transitioned=transitioned,
            throttled=False,
        )

    @staticmethod
    def _build_refund_outcome(
        refund: RefundRecordProtocol,
        *,
        transitioned: bool,
        throttled: bool,
    ) -> RefundReconcileOutcome:
        return RefundReconcileOutcome(
            refund_id=refund.id,
            payment_id=refund.payment_id,
            subscription_id=refund.subscription_id,
            provider_code=refund.provider_code,
            provider_refund_id=refund.provider_refund_id,
            amount_kopeks=refund.amount_kopeks,
            currency=refund.currency.strip(),
            status=refund.status,
            reason=refund.reason,
            rejection_reason=refund.rejection_reason,
            admin_user_id=refund.admin_user_id,
            created_at=refund.created_at,
            updated_at=refund.updated_at,
            transitioned=transitioned,
            throttled=throttled,
        )
