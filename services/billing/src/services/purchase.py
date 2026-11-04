from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from api.errors import BillingDomainError
from clients.payment_provider import (
    CreateProviderPaymentRequest,
    PaymentProviderError,
    PaymentProviderGateway,
    ProviderPermanentError,
)
from repositories.purchase import PaymentRecordProtocol, PurchaseRepository

CHECK_THROTTLE_WINDOW = timedelta(seconds=5)


@dataclass(frozen=True, slots=True)
class PurchaseOutcome:
    payment_id: UUID
    status: str
    redirect_url: str
    confirmation_url: str | None


class PurchaseService:
    """Доменный сервис создания и продолжения покупки подписки (T-10)."""

    def __init__(
        self,
        *,
        repository: PurchaseRepository,
        payment_provider: PaymentProviderGateway,
        public_base_url: str,
    ) -> None:
        self._repo = repository
        self._provider = payment_provider
        self._public_base_url = public_base_url.rstrip('/')

    async def start_or_continue_purchase(self, user_id: UUID) -> PurchaseOutcome:
        payment, is_new = await self._repo.get_or_create_pending_payment(user_id)

        # Если платёж уже существовал в pending и у него есть provider_payment_id:
        # проверяем статус в ЮKassa (GET, не чаще 1 раза в 5 с по BIL-NFR-R-06).
        if not is_new and payment.provider_payment_id:
            return await self._verify_existing_pending_and_resolve(
                user_id,
                payment,
                payment.provider_payment_id,
                respect_throttle=True,
            )

        return await self._create_in_provider(user_id, payment)

    async def _verify_existing_pending_and_resolve(
        self,
        user_id: UUID,
        payment: PaymentRecordProtocol,
        provider_payment_id: str,
        *,
        respect_throttle: bool,
    ) -> PurchaseOutcome:
        now_utc = datetime.now(UTC)
        if (
            respect_throttle
            and payment.last_checked_at is not None
            and (now_utc - payment.last_checked_at) < CHECK_THROTTLE_WINDOW
            and payment.confirmation_url
        ):
            return PurchaseOutcome(
                payment_id=payment.id,
                status='pending',
                redirect_url=payment.confirmation_url,
                confirmation_url=payment.confirmation_url,
            )

        try:
            snapshot = await self._provider.get_payment(
                provider_payment_id,
                expected_payment_id=payment.id,
                expected_amount_kopeks=payment.amount_kopeks,
                expected_currency=payment.currency.strip(),
            )
        except PaymentProviderError as exc:
            raise BillingDomainError(
                'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                error_code='payment_provider_unavailable',
                status_code=503,
            ) from exc

        if snapshot.domain_status == 'pending':
            url = snapshot.confirmation_url or payment.confirmation_url
            if not url:
                raise BillingDomainError(
                    'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                    error_code='payment_provider_unavailable',
                    status_code=503,
                )
            await self._repo.record_payment_checked(
                payment.id,
                confirmation_url=url,
                provider_details=snapshot.provider_details,
            )
            return PurchaseOutcome(
                payment_id=payment.id,
                status='pending',
                redirect_url=url,
                confirmation_url=url,
            )

        if snapshot.domain_status == 'failed':
            await self._repo.mark_payment_failed(
                payment.id,
                failure_reason=snapshot.failure_reason or 'canceled_by_provider',
                source='continue',
                provider_details=snapshot.provider_details,
            )
            new_payment, is_new = await self._repo.get_or_create_pending_payment(
                user_id
            )
            if not is_new and new_payment.provider_payment_id:
                return await self._verify_existing_pending_and_resolve(
                    user_id,
                    new_payment,
                    new_payment.provider_payment_id,
                    respect_throttle=True,
                )
            return await self._create_in_provider(user_id, new_payment)

        # succeeded -> редирект на экран результата оплаты
        await self._repo.record_payment_checked(
            payment.id,
            provider_details=snapshot.provider_details,
        )
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
            await self._repo.mark_payment_failed(
                payment.id,
                failure_reason=exc.message,
                source='continue',
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

        # Сохраняем полученный provider_payment_id (и confirmation_url для pending) в БД
        payment = await self._repo.save_provider_payment_created(
            payment.id,
            provider_payment_id=snapshot.provider_payment_id,
            confirmation_url=snapshot.confirmation_url,
            provider_details=snapshot.provider_details,
        )

        # Инвариант 6 и BIL-NFR-SEC-04: если повтор POST вернул не-pending,
        # никогда не меняем статус по ответу POST, а проверяем через GET.
        if snapshot.domain_status != 'pending':
            return await self._verify_existing_pending_and_resolve(
                user_id,
                payment,
                snapshot.provider_payment_id,
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
