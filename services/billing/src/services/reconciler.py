import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol, TypeVar
from uuid import UUID

import sentry_sdk
from opentelemetry import trace

from clients.payment_provider import (
    CreateProviderPaymentRequest,
    CreateProviderRefundRequest,
    PaymentProviderError,
    ProviderIntegrityMismatchError,
    ProviderOperationAnomalyError,
    ProviderPaymentSnapshot,
    ProviderPermanentError,
    ProviderProtocolAnomalyError,
    ProviderReconciliationError,
    ProviderRefundSnapshot,
    is_safe_provider_operation_id,
)
from clients.payment_provider_registry import PaymentProviderRegistry
from core.config import WorkerSettings
from core.observability import correlation_request_id
from domain.subscription import (
    ReconcileSource,
    build_payment_description,
    ensure_utc,
)
from repositories.payments import PaymentRepository
from repositories.records import (
    DIVERGENCE_SIGNAL_KIND,
    DIVERGENCE_SIGNALED_KEY,
    POST_OUTCOME_UNKNOWN_KEY,
    POST_STOPPED_KEY,
    PaymentRecordProtocol,
    RefundRecordProtocol,
    has_anomaly_marker,
    lookup_not_found_state,
)
from repositories.refunds import RefundRepository
from services.webhook_probe import WebhookProbeLimiter

logger = logging.getLogger(__name__)

T = TypeVar('T')
# Вид отметки для прочих аномалий ответа ЮKassa (битое тело, нет полей).
PROTOCOL_ANOMALY_KIND = 'protocol'

CHECK_THROTTLE_WINDOW = timedelta(seconds=5)
IDEMPOTENCY_KEY_TTL = timedelta(hours=24)


@dataclass(frozen=True, slots=True)
class RefundKeyPolicy:
    """Возврат без номера ЮKassa у границы 24-часового ключа (инв. 14).

    Значения задаются настройками `BILLING_REFUND_*` (`WorkerSettings`);
    умолчания здесь совпадают с умолчаниями настроек.
    """

    # POST у самой границы мог бы получить ответ уже после розыска по списку.
    post_stop_before_key_expiry: timedelta = timedelta(hours=1)
    # Созданный возврат может появиться в списке ЮKassa не сразу.
    lookup_not_found_attempts: int = 2
    lookup_interval: timedelta = timedelta(minutes=5)

    @classmethod
    def from_settings(cls, worker: WorkerSettings) -> 'RefundKeyPolicy':
        return cls(
            post_stop_before_key_expiry=timedelta(
                seconds=worker.billing_refund_post_stop_before_key_expiry_seconds
            ),
            lookup_not_found_attempts=worker.billing_refund_lookup_not_found_attempts,
            lookup_interval=timedelta(
                seconds=worker.billing_refund_lookup_interval_seconds
            ),
        )


def idempotency_key_expired(
    created_at: datetime,
    *,
    now_utc: datetime | None = None,
) -> bool:
    """Истёк ли 24-часовой срок ключа идемпотентности ЮKassa (инв. 13, 14).

    Единая проверка для фоновой сверки, «Продолжить оплату» и повтора
    возврата; naive-время считается UTC.
    """
    now = now_utc or datetime.now(UTC)
    return now - ensure_utc(created_at) >= IDEMPOTENCY_KEY_TTL


def refund_post_cutoff_passed(
    created_at: datetime,
    *,
    stop_before_key_expiry: timedelta,
    now_utc: datetime | None = None,
) -> bool:
    """Поздно ли повторять POST /v3/refunds по ключу попытки (инв. 14)."""
    now = now_utc or datetime.now(UTC)
    cutoff = IDEMPOTENCY_KEY_TTL - stop_before_key_expiry
    return now - ensure_utc(created_at) >= cutoff


def _parse_uuid(value: str | None) -> UUID | None:
    try:
        return UUID(value) if value else None
    except ValueError:
        return None


def log_dropped_payment_number(
    payment: PaymentRecordProtocol,
    provider_payment_id: str,
) -> None:
    """Номер ЮKassa пришёл, когда платёж уже закрыт: не сохраняем, но пишем след.

    Без записи в логе объект ЮKassa не связать с нашим платежом при разборе
    (инв. 13: номер сохраняется только у ожидающего платежа).
    """
    logger.warning(
        'Provider payment %s arrived for payment %s that is already %r; '
        'the number is not stored',
        provider_payment_id,
        payment.id,
        payment.status,
    )


def _raise_monitoring_signal(message: str) -> None:
    """Сигнал в мониторинг (6.4): ERROR-лог и событие в GlitchTip."""
    logger.error(message)
    sentry_sdk.capture_message(message, level='error')


def _post_stopped(refund: RefundRecordProtocol) -> bool:
    """POST по попытке остановлен после 400/401/403 на повтор (инв. 15)."""
    return bool((refund.provider_details or {}).get(POST_STOPPED_KEY))


def _post_outcome_was_unknown(refund: RefundRecordProtocol) -> bool:
    """Был ли у попытки POST с неизвестным исходом (инв. 15).

    Счётчик `_reconcile_errors` учитывается для попыток, начатых до появления
    отметки: без номера возврата фоновая сверка ошибается только на POST.
    """
    details = refund.provider_details or {}
    return bool(
        details.get(POST_OUTCOME_UNKNOWN_KEY) or details.get('_reconcile_errors')
    )


@dataclass(frozen=True, slots=True)
class ReconcileOutcome:
    payment_id: UUID
    user_id: UUID
    status: str
    confirmation_url: str | None
    failure_reason: str | None
    transitioned: bool
    throttled: bool
    # ЮKassa отдала по платежу `waiting_for_capture`, неизвестный статус или
    # аномальный ответ, и нормального ответа с тех пор не было (инв. 16): на
    # оплату не уводим.
    provider_anomaly: bool = False


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
        refund_policy: RefundKeyPolicy | None = None,
        webhook_probe_limiter: WebhookProbeLimiter | None = None,
    ) -> None:
        self._payments = payments
        self._refunds = refunds
        self._providers = payment_providers
        self._public_base_url = public_base_url.rstrip('/')
        self._outbox_dispatcher = outbox_dispatcher
        self._refund_policy = refund_policy or RefundKeyPolicy()
        # Один на процесс: `billing-api` передаёт общий экземпляр (dependencies).
        self._probe_limiter = webhook_probe_limiter or WebhookProbeLimiter()

    async def call_for_operation(
        self,
        operation_type: Literal['payment', 'refund'],
        operation_id: UUID,
        call: Awaitable[T],
    ) -> T:
        """Вызов ЮKassa по нашей операции; аномалии сигналит её владелец (6.4).

        Аномалия вроде `waiting_for_capture` повторяется на каждом чтении, а
        адаптер не знает, видели ли её раньше. Сверка отмечает вид аномалии в
        записи операции и поднимает сигнал один раз; дальше — WARNING.
        """
        try:
            return await call
        except ProviderReconciliationError as exc:
            # 4xx на чтение нашей операции — ошибка ключей или договора с
            # провайдером (инв. 15). Адаптер её не сигналит: чтение без
            # владельца (незнакомый номер из вебхука) сигнала не стоит.
            _raise_monitoring_signal(
                f'{operation_type.capitalize()} {operation_id}: provider answered '
                f'HTTP {exc.status_code} on read: {exc}'
            )
            raise
        except ProviderProtocolAnomalyError as exc:
            # Прочие аномалии ответа (битое тело, нет суммы) адаптер сигналит
            # сам; отметка нужна, чтобы не уводить пользователя на оплату.
            kind = (
                exc.kind
                if isinstance(exc, ProviderOperationAnomalyError)
                else PROTOCOL_ANOMALY_KIND
            )
            if operation_type == 'payment':
                first = await self._payments.mark_payment_signaled(
                    operation_id, kind=kind
                )
            else:
                first = await self._refunds.mark_refund_signaled(
                    operation_id, kind=kind
                )
            if isinstance(exc, ProviderOperationAnomalyError):
                message = f'{operation_type.capitalize()} {operation_id}: {exc}'
                if first:
                    _raise_monitoring_signal(message)
                else:
                    logger.warning('Repeated provider anomaly: %s', message)
            raise

    async def payment_description(self, payment: PaymentRecordProtocol) -> str:
        """Описание платежа в ЮKassa из тарифа платежа (название и срок из БД)."""
        tariff = await self._payments.get_tariff(payment.tariff_id)
        if tariff is None:
            raise LookupError(f'Tariff {payment.tariff_id} not found')
        return build_payment_description(
            tariff_title=tariff.title,
            duration_days=tariff.duration_days,
        )

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
        object_hint: Literal['payment', 'refund'] | None = None,
    ) -> ReconcileOutcome | RefundReconcileOutcome | None:
        """Сверяет операцию из вебхука провайдера `provider_code` (BIL-FR-07).

        Вебхук по уже завершённой операции сверяется с ЮKassa только для
        мониторинга расхождений (6.4); итог операции не меняется (инв. 7).
        """
        cleaned_id = provider_operation_id.strip()
        payment = await self._payments.get_payment_by_provider_id(
            provider_code,
            cleaned_id,
        )
        if payment is not None:
            if source == 'webhook' and payment.status != 'pending':
                await self._check_final_payment_divergence(payment)
            return await self.reconcile_payment_record(
                payment,
                source=source,
                respect_throttle=respect_throttle,
            )

        refund = await self._refunds.get_refund_by_provider_id(
            provider_code,
            cleaned_id,
        )
        if refund is not None:
            if source == 'webhook' and refund.status != 'processing':
                await self._check_final_refund_divergence(refund)
            return await self.reconcile_refund_record(
                refund,
                source=source,
                respect_throttle=respect_throttle,
            )
        if source != 'webhook':
            return None
        return await self._adopt_unknown_object(
            cleaned_id, provider_code=provider_code, object_hint=object_hint
        )

    async def _adopt_unknown_object(
        self,
        provider_operation_id: str,
        *,
        provider_code: str,
        object_hint: Literal['payment', 'refund'] | None,
    ) -> ReconcileOutcome | RefundReconcileOutcome | None:
        """Вебхук с незнакомым номером: может быть, это наш объект (BIL-API-05).

        Ответ на POST создания мог потеряться, и вебхук — самый ранний способ
        узнать номер. Читаем объект у провайдера; если в его `metadata` номер
        нашей незавершённой операции без номера провайдера, привязываем его
        после тех же проверок, что и находку из списка (инв. 14, 17). Иначе —
        WARNING: повторять уведомление о чужом объекте незачем, ответ 200.

        Вебхук без подписи может прислать кто угодно (2.5.4), поэтому чтение
        бывает, только если есть наша операция без номера, не чаще раза в окно
        на номер и в пределах общего лимита (`WebhookProbeLimiter`).
        """
        # Без `event` уведомление не от ЮKassa (она присылает его всегда), а
        # угадывать тип стоило бы лишнего чтения — не читаем.
        if object_hint is None or not is_safe_provider_operation_id(
            provider_operation_id
        ):
            logger.debug(
                'Webhook object %s ignored: no type hint', provider_operation_id
            )
            return None
        # Номер может принадлежать только нашей операции без номера провайдера.
        # Нет такой — нет и запроса к ЮKassa: поддельный вебхук стоит один
        # запрос к своей БД.
        if object_hint == 'refund':
            has_candidate = await self._refunds.has_refund_awaiting_provider_number(
                provider_code
            )
        else:
            has_candidate = await self._payments.has_payment_awaiting_provider_number(
                provider_code
            )
        if not has_candidate:
            logger.debug(
                'Webhook object %s ignored: no %s awaiting a provider number',
                provider_operation_id,
                object_hint,
            )
            return None
        if not self._probe_limiter.allow(provider_code, provider_operation_id):
            logger.debug(
                'Webhook object %s not read: probe limit or repeat within window',
                provider_operation_id,
            )
            return None
        try:
            if object_hint == 'refund':
                refund_outcome = await self._adopt_refund(
                    provider_operation_id, provider_code=provider_code
                )
                if refund_outcome is not None:
                    return refund_outcome
            else:
                payment_outcome = await self._adopt_payment(
                    provider_operation_id, provider_code=provider_code
                )
                if payment_outcome is not None:
                    return payment_outcome
        except PaymentProviderError as exc:
            # Объект не наш или ЮKassa не ответила: сигнала нет (6.4), только
            # след в логе — число таких чтений ограничено лимитом выше.
            logger.warning(
                'Webhook object %s could not be read from provider %s as %s: %s',
                provider_operation_id,
                provider_code,
                object_hint,
                exc,
            )
            return None
        logger.warning(
            'Webhook for unknown provider object %s (%s) does not belong to an '
            'unfinished operation of Billing; ignored',
            provider_operation_id,
            provider_code,
        )
        return None

    async def _adopt_refund(
        self,
        provider_refund_id: str,
        *,
        provider_code: str,
    ) -> RefundReconcileOutcome | None:
        gateway = self._providers.get(provider_code)
        hint = await gateway.get_refund(provider_refund_id)
        refund_id = _parse_uuid(hint.metadata_refund_id)
        refund = (
            None if refund_id is None else await self._refunds.get_refund(refund_id)
        )
        if (
            refund is None
            or refund.status != 'processing'
            or refund.provider_refund_id
            or refund.provider_code != provider_code
        ):
            return None
        payment = await self._payments.get_payment(refund.payment_id)
        if payment is None or not payment.provider_payment_id:
            return None
        checked = await self.call_for_operation(
            'refund',
            refund.id,
            gateway.get_refund(
                provider_refund_id,
                expected_refund_id=refund.id,
                expected_provider_payment_id=payment.provider_payment_id,
                expected_amount_kopeks=refund.amount_kopeks,
                expected_currency=refund.currency.strip(),
            ),
        )
        saved = await self._save_created_refund(refund.id, checked)
        if saved.provider_refund_id != checked.provider_refund_id:
            return self._build_refund_outcome(
                saved, transitioned=False, throttled=False
            )
        logger.info(
            'Refund %s attached to provider refund %s from webhook',
            refund.id,
            checked.provider_refund_id,
        )
        return await self._apply_refund_snapshot(
            saved, checked, source='webhook', audit_unchanged=False
        )

    async def _adopt_payment(
        self,
        provider_payment_id: str,
        *,
        provider_code: str,
    ) -> ReconcileOutcome | None:
        gateway = self._providers.get(provider_code)
        hint = await gateway.get_payment(provider_payment_id)
        payment_id = _parse_uuid(hint.metadata_payment_id)
        payment = (
            None if payment_id is None else await self._payments.get_payment(payment_id)
        )
        if (
            payment is None
            or payment.status != 'pending'
            or payment.provider_payment_id
            or payment.provider_code != provider_code
        ):
            return None
        checked = await self.call_for_operation(
            'payment',
            payment.id,
            gateway.get_payment(
                provider_payment_id,
                expected_payment_id=payment.id,
                expected_amount_kopeks=payment.amount_kopeks,
                expected_currency=payment.currency.strip(),
            ),
        )
        saved = await self._payments.save_provider_payment_created(
            payment.id,
            provider_payment_id=checked.provider_payment_id,
            confirmation_url=checked.confirmation_url,
            provider_details=checked.provider_details,
        )
        if saved.provider_payment_id != checked.provider_payment_id:
            log_dropped_payment_number(saved, checked.provider_payment_id)
            return None
        logger.info(
            'Payment %s attached to provider payment %s from webhook',
            payment.id,
            checked.provider_payment_id,
        )
        # Номера не было, значит пользователь на страницу оплаты не попадал:
        # при отмене письмо об отказе не нужно (BIL-FR-06).
        return await self._apply_payment_snapshot(
            saved,
            checked,
            source='webhook',
            audit_unchanged=False,
            notify_failure=False,
        )

    async def _check_final_payment_divergence(
        self,
        payment: PaymentRecordProtocol,
    ) -> None:
        """Один сигнал, если ЮKassa противоречит итогу платежа (инв. 7)."""
        details = payment.provider_details or {}
        if not payment.provider_payment_id or details.get(DIVERGENCE_SIGNALED_KEY):
            return
        try:
            snapshot = await self.call_for_operation(
                'payment',
                payment.id,
                self._providers.get(payment.provider_code).get_payment(
                    payment.provider_payment_id,
                    expected_payment_id=payment.id,
                    expected_amount_kopeks=payment.amount_kopeks,
                    expected_currency=payment.currency.strip(),
                ),
            )
        except PaymentProviderError as exc:
            # Итог уже зафиксирован: сбой ЮKassa не повод просить повтор вебхука.
            logger.warning(
                'Divergence check skipped for final payment_id=%s: %s',
                payment.id,
                exc,
            )
            return
        if (payment.status, snapshot.domain_status) not in (
            ('failed', 'succeeded'),
            ('succeeded', 'failed'),
        ):
            return
        if not await self._payments.mark_payment_signaled(
            payment.id, kind=DIVERGENCE_SIGNAL_KIND
        ):
            return
        _raise_monitoring_signal(
            f'Payment {payment.id} is final {payment.status!r} in Billing, but '
            f'provider {payment.provider_code} payment '
            f'{payment.provider_payment_id} is {snapshot.domain_status!r}; '
            'local status is kept (invariant 7)'
        )

    async def _check_final_refund_divergence(
        self,
        refund: RefundRecordProtocol,
    ) -> None:
        """Один сигнал, если ЮKassa противоречит итогу возврата (инв. 7)."""
        details = refund.provider_details or {}
        if not refund.provider_refund_id or details.get(DIVERGENCE_SIGNALED_KEY):
            return
        payment = await self._payments.get_payment(refund.payment_id)
        if payment is None or not payment.provider_payment_id:
            return
        try:
            snapshot = await self.call_for_operation(
                'refund',
                refund.id,
                self._providers.get(refund.provider_code).get_refund(
                    refund.provider_refund_id,
                    expected_refund_id=refund.id,
                    expected_provider_payment_id=payment.provider_payment_id,
                    expected_amount_kopeks=refund.amount_kopeks,
                    expected_currency=refund.currency.strip(),
                ),
            )
        except PaymentProviderError as exc:
            logger.warning(
                'Divergence check skipped for final refund_id=%s: %s',
                refund.id,
                exc,
            )
            return
        if (refund.status, snapshot.domain_status) not in (
            ('rejected', 'succeeded'),
            ('succeeded', 'rejected'),
        ):
            return
        if not await self._refunds.mark_refund_signaled(
            refund.id, kind=DIVERGENCE_SIGNAL_KIND
        ):
            return
        _raise_monitoring_signal(
            f'Refund {refund.id} is final {refund.status!r} in Billing, but '
            f'provider {refund.provider_code} refund {refund.provider_refund_id} '
            f'is {snapshot.domain_status!r}; local status is kept (invariant 7)'
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
        audit_unchanged: bool = False,
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
            # Инвариант 13: если за 24 часа жизни ключа номер ЮKassa не получен,
            # переводим платёж в Не оплачен (средства не списаны). Письма нет
            # (BIL-FR-06): пользователь не попадал на страницу оплаты.
            if idempotency_key_expired(payment.created_at):
                updated, transitioned = await self._payments.finalize_payment_failed(
                    payment.id,
                    failure_reason='idempotency_key_expired',
                    source=source,
                    public_base_url=self._public_base_url,
                    notify_user=False,
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
                created_snapshot = await self.call_for_operation(
                    'payment',
                    payment.id,
                    self._providers.get(payment.provider_code).create_payment(
                        CreateProviderPaymentRequest(
                            payment_id=payment.id,
                            idempotency_key=payment.idempotency_key,
                            amount_kopeks=payment.amount_kopeks,
                            currency=payment.currency.strip(),
                            description=await self.payment_description(payment),
                            return_url=return_url,
                        )
                    ),
                )
            except ProviderPermanentError as exc:
                # Инв. 15: платёж не создан, пользователь на страницу оплаты не
                # попадал — письма об отказе нет (BIL-FR-06).
                updated, transitioned = await self._payments.finalize_payment_failed(
                    payment.id,
                    failure_reason=exc.message,
                    source=source,
                    public_base_url=self._public_base_url,
                    notify_user=False,
                )
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
            if payment.provider_payment_id != created_snapshot.provider_payment_id:
                log_dropped_payment_number(
                    payment, created_snapshot.provider_payment_id
                )

        return await self.reconcile_payment_record(
            payment,
            source=source,
            respect_throttle=False,
            audit_unchanged=audit_unchanged,
        )

    async def reconcile_payment_record(
        self,
        payment: PaymentRecordProtocol,
        *,
        source: ReconcileSource,
        respect_throttle: bool,
        audit_unchanged: bool = False,
        notify_failure: bool = True,
    ) -> ReconcileOutcome:
        """Сверка платежа через GET.

        `audit_unchanged` — ручная проверка администратора: запрос в ЮKassa без
        смены статуса тоже пишется в `operation_events` (US-A-06, BIL-FR-13).
        `notify_failure=False` — без письма об отказе: вызывающий сразу
        создаёт пользователю новый платёж (BIL-FR-06).
        """
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
                provider_anomaly=has_anomaly_marker(current.provider_details),
            )

        snapshot = await self.call_for_operation(
            'payment',
            payment.id,
            self._providers.get(payment.provider_code).get_payment(
                payment.provider_payment_id,
                expected_payment_id=payment.id,
                expected_amount_kopeks=payment.amount_kopeks,
                expected_currency=payment.currency.strip(),
            ),
        )
        return await self._apply_payment_snapshot(
            payment,
            snapshot,
            source=source,
            audit_unchanged=audit_unchanged,
            notify_failure=notify_failure,
        )

    async def _apply_payment_snapshot(
        self,
        payment: PaymentRecordProtocol,
        snapshot: ProviderPaymentSnapshot,
        *,
        source: ReconcileSource,
        audit_unchanged: bool,
        notify_failure: bool,
    ) -> ReconcileOutcome:
        """Применяет проверенный снимок ЮKassa к ожидающему платежу (инв. 6, 7)."""
        if snapshot.domain_status == 'pending':
            confirmation_url = snapshot.confirmation_url or payment.confirmation_url
            updated = await self._payments.record_payment_checked(
                payment.id,
                confirmation_url=confirmation_url,
                provider_details=snapshot.provider_details,
                event_source=source if audit_unchanged else None,
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
            notify_user=notify_failure,
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
        if not refund.provider_refund_id and (
            self._refund_post_cutoff_passed(refund.created_at) or _post_stopped(refund)
        ):
            # Инв. 14: ключ в ЮKassa истёк (или истекает: POST у самой границы
            # мог бы получить ответ уже после розыска),
            # повторный POST мог бы создать второй возврат (и получить 400
            # «сумма больше остатка», после чего уже выполненный возврат
            # считался бы отклонённым). Ждём и ищем попытку по metadata, как
            # фоновая сверка.
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
                created_snap = await self.call_for_operation(
                    'refund',
                    refund.id,
                    self._providers.get(refund.provider_code).create_refund(
                        CreateProviderRefundRequest(
                            refund_id=refund.id,
                            idempotency_key=refund.idempotency_key,
                            provider_payment_id=payment.provider_payment_id,
                            amount_kopeks=refund.amount_kopeks,
                            currency=refund.currency.strip(),
                            description=f'Возврат по подписке (платёж {payment.id})',
                        )
                    ),
                )
            except ProviderPermanentError as exc:
                if _post_outcome_was_unknown(refund):
                    refund = await self._stop_refund_post(refund, exc)
                    return self._build_refund_outcome(
                        refund,
                        transitioned=False,
                        throttled=False,
                    )
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
                refund = await self._refunds.mark_refund_post_outcome_unknown(refund.id)
                return self._build_refund_outcome(
                    refund,
                    transitioned=False,
                    throttled=False,
                )

            refund = await self._save_created_refund(refund.id, created_snap)

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
        audit_unchanged: bool = False,
    ) -> RefundReconcileOutcome:
        if refund.status != 'processing':
            return self._build_refund_outcome(
                refund,
                transitioned=False,
                throttled=False,
            )

        event_source = source if audit_unchanged else None
        if not refund.provider_refund_id:
            payment = await self._payments.get_payment(refund.payment_id)
            if payment is None or not payment.provider_payment_id:
                return self._build_refund_outcome(
                    refund,
                    transitioned=False,
                    throttled=False,
                )
            # Инвариант 14: после истечения 24 часов ищем возврат в списке возвратов
            # по нашему refund_id в metadata.
            if idempotency_key_expired(refund.created_at):
                misses, last_miss_at = lookup_not_found_state(refund.provider_details)
                if (
                    last_miss_at is not None
                    and datetime.now(UTC) - last_miss_at
                    < self._refund_policy.lookup_interval
                ):
                    # Инв. 14: повторный розыск не раньше чем через интервал.
                    return self._build_refund_outcome(
                        refund,
                        transitioned=False,
                        throttled=False,
                    )
                found_snap = await self.call_for_operation(
                    'refund',
                    refund.id,
                    self._providers.get(refund.provider_code).find_refund_by_metadata(
                        payment.provider_payment_id,
                        refund_id=refund.id,
                        expected_amount_kopeks=refund.amount_kopeks,
                        expected_currency=refund.currency.strip(),
                    ),
                )
                if found_snap is None:
                    if misses + 1 < self._refund_policy.lookup_not_found_attempts:
                        # Инв. 14: созданный возврат может появиться в списке
                        # не сразу, поэтому пустой розыск ещё не отказ.
                        updated = await self._refunds.record_refund_lookup_not_found(
                            refund.id,
                            event_source=event_source,
                        )
                        return self._build_refund_outcome(
                            updated,
                            transitioned=False,
                            throttled=False,
                        )
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
                refund = await self._save_created_refund(refund.id, found_snap)
            elif self._refund_post_cutoff_passed(refund.created_at) or _post_stopped(
                refund
            ):
                # Инв. 14, 15: у границы жизни ключа или после 400/401/403 на
                # повтор POST не шлём и ждём розыска по списку после 24 часов.
                return self._build_refund_outcome(
                    refund,
                    transitioned=False,
                    throttled=False,
                )
            else:
                try:
                    created_snap = await self.call_for_operation(
                        'refund',
                        refund.id,
                        self._providers.get(refund.provider_code).create_refund(
                            CreateProviderRefundRequest(
                                refund_id=refund.id,
                                idempotency_key=refund.idempotency_key,
                                provider_payment_id=payment.provider_payment_id,
                                amount_kopeks=refund.amount_kopeks,
                                currency=refund.currency.strip(),
                                description=(
                                    f'Возврат по подписке (платёж {payment.id})'
                                ),
                            )
                        ),
                    )
                except ProviderPermanentError as exc:
                    if _post_outcome_was_unknown(refund):
                        await self._stop_refund_post(refund, exc)
                        # Запрос к ЮKassa был, статус не изменился (US-A-06).
                        updated = await self._refunds.record_refund_checked(
                            refund.id,
                            event_source=event_source,
                        )
                        return self._build_refund_outcome(
                            updated,
                            transitioned=False,
                            throttled=False,
                        )
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
                except PaymentProviderError:
                    await self._refunds.mark_refund_post_outcome_unknown(refund.id)
                    raise
                refund = await self._save_created_refund(refund.id, created_snap)

        return await self.reconcile_refund_record(
            refund,
            source=source,
            respect_throttle=False,
            audit_unchanged=audit_unchanged,
        )

    async def _save_created_refund(
        self,
        refund_id: UUID,
        snapshot: ProviderRefundSnapshot,
    ) -> RefundRecordProtocol:
        """Сохраняет номер возврата; поздний ответ не теряется молча (инв. 14)."""
        refund = await self._refunds.save_provider_refund_created(
            refund_id,
            provider_refund_id=snapshot.provider_refund_id,
            provider_details=snapshot.provider_details,
        )
        if (
            refund.status != 'processing'
            and refund.provider_refund_id != snapshot.provider_refund_id
        ):
            # Итог попытки не меняется (инв. 7), но возврат в ЮKassa есть.
            _raise_monitoring_signal(
                f'Provider refund {snapshot.provider_refund_id} arrived for refund '
                f'{refund.id} that is already {refund.status!r}; '
                'final status is kept (invariant 7), manual review required'
            )
        return refund

    def _refund_post_cutoff_passed(self, created_at: datetime) -> bool:
        return refund_post_cutoff_passed(
            created_at,
            stop_before_key_expiry=self._refund_policy.post_stop_before_key_expiry,
        )

    async def _stop_refund_post(
        self,
        refund: RefundRecordProtocol,
        exc: ProviderPermanentError,
    ) -> RefundRecordProtocol:
        """400/401/403 после POST с неизвестным исходом — не отказ (инв. 15).

        Постоянный ответ не повторяем: POST по попытке больше не шлётся, она
        остаётся «В обработке» и ждёт розыска по metadata после 24 ч. Сигнал
        поднимается один раз — когда отметка ставится впервые.
        """
        if await self._refunds.mark_refund_post_stopped(refund.id):
            _raise_monitoring_signal(
                f'Refund {refund.id} got HTTP {exc.status_code} on repeated '
                'POST /v3/refunds after a send with unknown outcome; POST is '
                'stopped, refund stays processing until lookup by metadata: '
                f'{exc.message}'
            )
        return await self._refunds.get_refund(refund.id) or refund

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
                audit_unchanged=True,
            )
            return pay_out.payment_id, pay_out.status, pay_out.transitioned

        refund = await self._refunds.get_refund(operation_id)
        if refund is None:
            raise LookupError(f'Refund {operation_id} not found')
        ref_out = await self._reconcile_background_refund(
            refund,
            source='admin',
            audit_unchanged=True,
        )
        return ref_out.payment_id, ref_out.status, ref_out.transitioned

    async def reconcile_refund_record(
        self,
        refund: RefundRecordProtocol,
        *,
        source: ReconcileSource,
        respect_throttle: bool,
        audit_unchanged: bool = False,
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

        payment = await self._payments.get_payment(refund.payment_id)
        if payment is None or not payment.provider_payment_id:
            # Возврат уже создан в ЮKassa, а номера исходного платежа у нас
            # нет: сверить возврат не с чем, оставляем его на сверке.
            message = (
                f'Refund {refund.id} has provider refund id but payment '
                f'{refund.payment_id} has no provider payment id'
            )
            logger.error(message)
            sentry_sdk.capture_message(message, level='error')
            raise ProviderIntegrityMismatchError(message)

        snapshot = await self.call_for_operation(
            'refund',
            refund.id,
            self._providers.get(refund.provider_code).get_refund(
                refund.provider_refund_id,
                expected_refund_id=refund.id,
                expected_provider_payment_id=payment.provider_payment_id,
                expected_amount_kopeks=refund.amount_kopeks,
                expected_currency=refund.currency.strip(),
            ),
        )
        return await self._apply_refund_snapshot(
            refund, snapshot, source=source, audit_unchanged=audit_unchanged
        )

    async def _apply_refund_snapshot(
        self,
        refund: RefundRecordProtocol,
        snapshot: ProviderRefundSnapshot,
        *,
        source: ReconcileSource,
        audit_unchanged: bool,
    ) -> RefundReconcileOutcome:
        """Применяет проверенный снимок ЮKassa к возврату в обработке (инв. 6, 7)."""
        if snapshot.domain_status == 'processing':
            updated = await self._refunds.record_refund_checked(
                refund.id,
                provider_details=snapshot.provider_details,
                event_source=source if audit_unchanged else None,
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
