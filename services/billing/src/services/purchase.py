from dataclasses import dataclass
from uuid import UUID

from api.errors import BillingDomainError
from clients.payment_provider import (
    CreateProviderPaymentRequest,
    PaymentProviderError,
    PaymentProviderGateway,
    ProviderPermanentError,
)
from domain.subscription import (
    PaymentResultBlock,
    format_amount_rubles,
    format_user_datetime,
    humanize_failure_reason,
)
from repositories.purchase import PaymentRecordProtocol, PurchaseRepository
from services.reconciler import CHECK_THROTTLE_WINDOW, PaymentReconciler

__all__ = [
    'CHECK_THROTTLE_WINDOW',
    'PaymentResultView',
    'PurchaseOutcome',
    'PurchaseService',
]


@dataclass(frozen=True, slots=True)
class PurchaseOutcome:
    payment_id: UUID
    status: str
    redirect_url: str
    confirmation_url: str | None


@dataclass(frozen=True, slots=True)
class PaymentResultView:
    payment_id: UUID
    tariff_title: str
    amount_display: str
    block: PaymentResultBlock
    subscription_ends_display: str | None
    failure_reason_display: str | None
    auto_refresh_seconds: int | None


class PurchaseService:
    """Доменный сервис создания и продолжения покупки подписки (T-10..T-13)."""

    def __init__(
        self,
        *,
        repository: PurchaseRepository,
        payment_provider: PaymentProviderGateway,
        public_base_url: str,
        reconciler: PaymentReconciler | None = None,
    ) -> None:
        self._repo = repository
        self._provider = payment_provider
        self._public_base_url = public_base_url.rstrip('/')
        self._reconciler = reconciler or PaymentReconciler(
            repository=repository,
            payment_provider=payment_provider,
            public_base_url=self._public_base_url,
        )

    async def start_or_continue_purchase(self, user_id: UUID) -> PurchaseOutcome:
        payment, is_new = await self._repo.get_or_create_pending_payment(user_id)
        if not is_new:
            return await self._resolve_pending_payment(user_id, payment)
        return await self._create_in_provider(user_id, payment)

    async def continue_payment_for_user(
        self,
        *,
        payment_id: UUID,
        user_id: UUID,
    ) -> PurchaseOutcome:
        """Продолжить оплату (/continue, T-13, BIL-FR-04, BIL-NFR-R-06)."""
        payment = await self._repo.get_payment_for_user(payment_id, user_id)
        if payment is None:
            raise BillingDomainError(
                'Платёж не найден',
                error_code='payment_not_found',
                status_code=404,
            )

        if payment.status == 'succeeded':
            result_url = f'/billing/payments/{payment.id}'
            return PurchaseOutcome(
                payment_id=payment.id,
                status='succeeded',
                redirect_url=result_url,
                confirmation_url=None,
            )

        if payment.status == 'failed':
            return await self.start_or_continue_purchase(user_id)

        return await self._resolve_pending_payment(user_id, payment)

    async def _resolve_pending_payment(
        self,
        user_id: UUID,
        payment: PaymentRecordProtocol,
    ) -> PurchaseOutcome:
        if payment.provider_payment_id:
            return await self._verify_existing_pending_and_resolve(
                user_id,
                payment,
                respect_throttle=True,
            )
        return await self._create_in_provider(user_id, payment)

    async def get_payment_result_view(
        self,
        *,
        payment_id: UUID,
        user_id: UUID,
        user_tz: str,
    ) -> PaymentResultView:
        """Читает только локальный статус из БД для Э3 без ЮKassa (BIL-FR-04)."""
        local = await self._repo.get_local_payment_result_for_user(payment_id, user_id)
        if local is None:
            raise BillingDomainError(
                'Платёж не найден',
                error_code='payment_not_found',
                status_code=404,
            )

        amount_display = format_amount_rubles(local.amount_kopeks)
        if local.status == 'succeeded':
            if local.auth_access_delivered and local.subscription_ends_at is not None:
                return PaymentResultView(
                    payment_id=local.payment_id,
                    tariff_title=local.tariff_title,
                    amount_display=amount_display,
                    block='succeeded_ready',
                    subscription_ends_display=format_user_datetime(
                        local.subscription_ends_at,
                        user_tz,
                    ),
                    failure_reason_display=None,
                    auto_refresh_seconds=None,
                )
            return PaymentResultView(
                payment_id=local.payment_id,
                tariff_title=local.tariff_title,
                amount_display=amount_display,
                block='succeeded_activating',
                subscription_ends_display=None,
                failure_reason_display=None,
                auto_refresh_seconds=5,
            )

        if local.status == 'failed':
            return PaymentResultView(
                payment_id=local.payment_id,
                tariff_title=local.tariff_title,
                amount_display=amount_display,
                block='failed',
                subscription_ends_display=None,
                failure_reason_display=humanize_failure_reason(local.failure_reason),
                auto_refresh_seconds=None,
            )

        return PaymentResultView(
            payment_id=local.payment_id,
            tariff_title=local.tariff_title,
            amount_display=amount_display,
            block='pending',
            subscription_ends_display=None,
            failure_reason_display=None,
            auto_refresh_seconds=5,
        )

    async def _verify_existing_pending_and_resolve(
        self,
        user_id: UUID,
        payment: PaymentRecordProtocol,
        *,
        respect_throttle: bool,
    ) -> PurchaseOutcome:
        try:
            outcome = await self._reconciler.reconcile_payment_record(
                payment,
                source='continue',
                respect_throttle=respect_throttle,
            )
        except PaymentProviderError as exc:
            raise BillingDomainError(
                'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                error_code='payment_provider_unavailable',
                status_code=503,
            ) from exc

        if outcome.status == 'pending':
            url = outcome.confirmation_url or payment.confirmation_url
            if not url:
                raise BillingDomainError(
                    'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                    error_code='payment_provider_unavailable',
                    status_code=503,
                )
            return PurchaseOutcome(
                payment_id=payment.id,
                status='pending',
                redirect_url=url,
                confirmation_url=url,
            )

        if outcome.status == 'failed':
            return await self.start_or_continue_purchase(user_id)

        # succeeded -> редирект на экран результата (подписка создана в Reconciler)
        result_url = f'/billing/payments/{payment.id}'
        return PurchaseOutcome(
            payment_id=payment.id,
            status='succeeded',
            redirect_url=result_url,
            confirmation_url=None,
        )

    async def _create_in_provider(
        self,
        user_id: UUID,
        payment: PaymentRecordProtocol,
    ) -> PurchaseOutcome:
        return_url = f'{self._public_base_url}/billing/payments/{payment.id}'
        try:
            snapshot = await self._provider.create_payment(
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
            await self._repo.finalize_payment_failed(
                payment.id,
                failure_reason=exc.message,
                source='continue',
                public_base_url=self._public_base_url,
            )
            raise BillingDomainError(
                'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                error_code='payment_provider_rejected',
                status_code=503,
            ) from exc
        except PaymentProviderError as exc:
            raise BillingDomainError(
                'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                error_code='payment_provider_unavailable',
                status_code=503,
            ) from exc

        payment = await self._repo.save_provider_payment_created(
            payment.id,
            provider_payment_id=snapshot.provider_payment_id,
            confirmation_url=snapshot.confirmation_url,
            provider_details=snapshot.provider_details,
        )

        # Инвариант 6 и BIL-NFR-SEC-04: если повтор POST вернул не-pending,
        # никогда не меняем статус по ответу POST, а проверяем через GET в Reconciler.
        if snapshot.domain_status != 'pending':
            return await self._verify_existing_pending_and_resolve(
                user_id,
                payment,
                respect_throttle=False,
            )

        if not snapshot.confirmation_url:
            raise BillingDomainError(
                'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                error_code='payment_provider_unavailable',
                status_code=503,
            )
        return PurchaseOutcome(
            payment_id=payment.id,
            status='pending',
            redirect_url=snapshot.confirmation_url,
            confirmation_url=snapshot.confirmation_url,
        )
