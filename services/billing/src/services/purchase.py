from dataclasses import dataclass
from uuid import UUID

from clients.payment_provider import (
    CreateProviderPaymentRequest,
    PaymentProviderError,
    ProviderPermanentError,
)
from clients.payment_provider_registry import PaymentProviderRegistry
from domain.errors import DependencyUnavailableError, EntityNotFoundError
from domain.subscription import (
    MySubscriptionActionType,
    MySubscriptionStatusKey,
    PaymentResultBlock,
    compute_my_subscription_summary,
    format_amount_rubles,
    format_payment_status_display,
    format_refund_badge_display,
    format_user_datetime,
    humanize_failure_reason,
)
from repositories.payments import PaymentRepository
from repositories.records import PaymentRecordProtocol
from repositories.user_views import UserBillingViews
from services.reconciler import (
    CHECK_THROTTLE_WINDOW,
    PaymentReconciler,
    idempotency_key_expired,
)

__all__ = [
    'CHECK_THROTTLE_WINDOW',
    'MySubscriptionPaymentRowView',
    'MySubscriptionView',
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


@dataclass(frozen=True, slots=True)
class MySubscriptionPaymentRowView:
    payment_id: UUID
    created_at_display: str
    tariff_title: str
    amount_kopeks: int
    amount_display: str
    status: str
    status_display: str
    refund_status: str | None
    refund_badge_display: str | None
    result_url: str


@dataclass(frozen=True, slots=True)
class MySubscriptionView:
    rule_number: int
    status_key: MySubscriptionStatusKey
    status_display: str
    action_type: MySubscriptionActionType
    action_label: str | None
    action_url: str | None
    can_subscribe: bool
    subscription_ends_display: str | None
    payments: list[MySubscriptionPaymentRowView]


class PurchaseService:
    """Доменный сервис создания и продолжения покупки подписки (T-10..T-13)."""

    def __init__(
        self,
        *,
        payments: PaymentRepository,
        user_views: UserBillingViews,
        payment_providers: PaymentProviderRegistry,
        public_base_url: str,
        reconciler: PaymentReconciler,
    ) -> None:
        self._payments = payments
        self._views = user_views
        self._providers = payment_providers
        self._public_base_url = public_base_url.rstrip('/')
        self._reconciler = reconciler

    async def start_or_continue_purchase(self, user_id: UUID) -> PurchaseOutcome:
        # Новый платёж закрепляется за активным провайдером из конфигурации;
        # уже созданный продолжает идти через свой provider_code.
        payment, is_new = await self._payments.get_or_create_pending_payment(
            user_id,
            provider_code=self._providers.active_code,
        )
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
        payment = await self._payments.get_payment_for_user(payment_id, user_id)
        if payment is None:
            raise EntityNotFoundError(
                'Платёж не найден',
                error_code='payment_not_found',
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
        local = await self._views.get_local_payment_result_for_user(payment_id, user_id)
        if local is None:
            raise EntityNotFoundError(
                'Платёж не найден',
                error_code='payment_not_found',
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

    async def get_my_subscription_view(
        self,
        *,
        user_id: UUID,
        user_tz: str,
    ) -> MySubscriptionView:
        """Возвращает статус по 7 правилам US-U-05 и историю платежей (BIL-FR-09)."""
        overview = await self._views.get_user_billing_overview(user_id)
        tariff_price_display = format_amount_rubles(overview.tariff_price_kopeks)
        summary = compute_my_subscription_summary(
            pending_payment_id=overview.pending_payment_id,
            latest_subscription_ends_at=overview.latest_subscription_ends_at,
            latest_subscription_has_succeeded_refund=(
                overview.latest_subscription_has_succeeded_refund
            ),
            latest_subscription_has_processing_refund=(
                overview.latest_subscription_has_processing_refund
            ),
            latest_subscription_auth_delivered=(
                overview.latest_subscription_auth_delivered
            ),
            user_tz=user_tz,
            tariff_price_display=tariff_price_display,
        )
        rows = [
            MySubscriptionPaymentRowView(
                payment_id=item.payment_id,
                created_at_display=format_user_datetime(item.created_at, user_tz),
                tariff_title=item.tariff_title,
                amount_kopeks=item.amount_kopeks,
                amount_display=format_amount_rubles(item.amount_kopeks),
                status=item.status,
                status_display=format_payment_status_display(item.status),
                refund_status=item.refund_status,
                refund_badge_display=format_refund_badge_display(item.refund_status),
                result_url=f'/billing/payments/{item.payment_id}',
            )
            for item in overview.payments
        ]
        return MySubscriptionView(
            rule_number=summary.rule_number,
            status_key=summary.status_key,
            status_display=summary.status_display,
            action_type=summary.action_type,
            action_label=summary.action_label,
            action_url=summary.action_url,
            can_subscribe=summary.can_subscribe,
            subscription_ends_display=summary.subscription_ends_display,
            payments=rows,
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
            raise DependencyUnavailableError(
                'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                error_code='payment_provider_unavailable',
            ) from exc

        if outcome.status == 'pending':
            url = outcome.confirmation_url or payment.confirmation_url
            if not url:
                raise DependencyUnavailableError(
                    'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                    error_code='payment_provider_unavailable',
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
        if idempotency_key_expired(payment.created_at):
            # Инв. 13: ключ идемпотентности в ЮKassa жив 24 ч. POST с истёкшим
            # ключом создал бы в ЮKassa второй платёж. Номера нет, значит
            # пользователь не платил: закрываем платёж и начинаем новый.
            _, transitioned = await self._payments.finalize_payment_failed(
                payment.id,
                failure_reason='idempotency_key_expired',
                source='continue',
                public_base_url=self._public_base_url,
            )
            if transitioned and self._reconciler is not None:
                await self._reconciler.notify_payment_tasks(payment.id)
            return await self.start_or_continue_purchase(user_id)

        return_url = f'{self._public_base_url}/billing/payments/{payment.id}'
        try:
            snapshot = await self._providers.get(payment.provider_code).create_payment(
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
            _, transitioned = await self._payments.finalize_payment_failed(
                payment.id,
                failure_reason=exc.message,
                source='continue',
                public_base_url=self._public_base_url,
            )
            if transitioned and self._reconciler is not None:
                await self._reconciler.notify_payment_tasks(payment.id)
            raise DependencyUnavailableError(
                'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                error_code='payment_provider_rejected',
            ) from exc
        except PaymentProviderError as exc:
            raise DependencyUnavailableError(
                'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                error_code='payment_provider_unavailable',
            ) from exc

        payment = await self._payments.save_provider_payment_created(
            payment.id,
            provider_payment_id=snapshot.provider_payment_id,
            confirmation_url=snapshot.confirmation_url,
            provider_details=snapshot.provider_details,
        )
        if payment.provider_payment_id != snapshot.provider_payment_id:
            # Номер не сохранён: пока шёл POST, платёж закрыла фоновая сверка
            # (например, по истечении ключа). Инв. 13 разрешает отправить на
            # оплату только после сохранения номера, иначе оплата в ЮKassa
            # прошла бы мимо Биллинга. Платёж ЮKassa без оплаты отменится сам.
            return await self.continue_payment_for_user(
                payment_id=payment.id,
                user_id=user_id,
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
            raise DependencyUnavailableError(
                'Не удалось связаться с платёжной системой, попробуйте ещё раз',
                error_code='payment_provider_unavailable',
            )
        return PurchaseOutcome(
            payment_id=payment.id,
            status='pending',
            redirect_url=snapshot.confirmation_url,
            confirmation_url=snapshot.confirmation_url,
        )
