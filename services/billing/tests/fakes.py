from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import UUID, uuid4

from api.errors import BillingDomainError
from clients.auth import (
    AuthInvalidCredentialsError,
    AuthPermanentDeliveryError,
    AuthServiceUnavailableError,
    SubscriptionAccessSyncResult,
)
from clients.payment_provider import (
    CreateProviderPaymentRequest,
    CreateProviderRefundRequest,
    ProviderPaymentSnapshot,
    ProviderRefundSnapshot,
)
from db.sql.errors import DatabaseUnavailableError
from domain.subscription import (
    ReconcileSource,
    build_cancel_expiration_email_payload,
    build_payment_failed_email_payload,
    build_payment_succeeded_email_payload,
    build_refund_succeeded_email_payload,
    build_subscription_expiration_email_payload,
    compute_subscription_state,
)
from repositories.purchase import (
    DeliveryOutcome,
    InternalOperationEventItemData,
    InternalOutboxFilter,
    InternalOutboxTaskItemData,
    InternalPaymentCardData,
    InternalPaymentFilter,
    InternalPaymentItemData,
    InternalRefundItemData,
    InternalSubscriptionFilter,
    InternalSubscriptionItemData,
    LocalPaymentResultData,
    OutboxDeliverFn,
    UserBillingOverviewData,
    UserPaymentHistoryItemData,
    compute_outbox_backoff_seconds,
)


@dataclass
class FakePaymentRow:
    id: UUID
    user_id: UUID
    tariff_id: UUID
    provider_code: str
    amount_kopeks: int
    currency: str
    status: str
    idempotency_key: UUID
    provider_payment_id: str | None = None
    confirmation_url: str | None = None
    failure_reason: str | None = None
    provider_details: dict[str, Any] | None = None
    last_checked_at: datetime | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class FakeSubscriptionRow:
    id: UUID
    user_id: UUID
    tariff_id: UUID
    payment_id: UUID
    starts_at: datetime
    ends_at: datetime
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class FakeRefundRow:
    id: UUID
    payment_id: UUID
    subscription_id: UUID
    provider_code: str
    amount_kopeks: int
    status: str
    reason: str
    admin_user_id: UUID
    idempotency_key: UUID
    currency: str = 'RUB'
    provider_refund_id: str | None = None
    rejection_reason: str | None = None
    provider_details: dict[str, Any] | None = None
    last_checked_at: datetime | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class FakeAdminActionRow:
    id: UUID
    admin_user_id: UUID
    action_type: str
    target_type: str
    target_id: UUID
    reason: str
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class FakeOutboxTaskRow:
    id: UUID
    dedup_key: str
    target_service: str
    task_type: str
    payment_id: UUID
    user_id: UUID
    access_version: int | None
    payload: dict[str, Any]
    status: str = 'pending'
    attempts: int = 0
    last_error: str | None = None
    next_retry_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class InMemoryPurchaseRepository:
    """Единый in-memory репозиторий для поведенческих TDD-тестов T-10 и T-11."""

    def __init__(self) -> None:
        self.has_active_subscription: bool = False
        self.has_processing_refund: bool = False
        self.db_unavailable: bool = False
        self.payments: dict[UUID, FakePaymentRow] = {}
        self.subscriptions: dict[UUID, FakeSubscriptionRow] = {}
        self.refunds: dict[UUID, FakeRefundRow] = {}
        self.access_versions: dict[UUID, int] = {}
        self.outbox_tasks: dict[str, FakeOutboxTaskRow] = {}
        self.admin_actions: list[FakeAdminActionRow] = []
        self.events: list[
            tuple[str, UUID, str | None, str, str, dict[str, Any] | None]
        ] = []

    async def check_purchase_allowed(self, user_id: UUID) -> tuple[bool, str | None]:
        """Проверяет отсутствие активной подписки и возврата в обработке."""
        if self.has_active_subscription:
            return False, 'subscription_already_active'
        if self.has_processing_refund:
            return False, 'refund_in_processing'
        return True, None

    async def get_payment(self, payment_id: UUID) -> FakePaymentRow | None:
        """Возвращает платёж по внутреннему UUID."""
        return self.payments.get(payment_id)

    async def get_payment_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> FakePaymentRow | None:
        """Возвращает платёж пользователя по UUID или None для чужого/неизвестного."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        payment = self.payments.get(payment_id)
        if payment is None or payment.user_id != user_id:
            return None
        return payment

    async def get_local_payment_result_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> LocalPaymentResultData | None:
        """Читает локальный статус платежа, подписки и задачи Auth для Э3."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        payment = self.payments.get(payment_id)
        if payment is None or payment.user_id != user_id:
            return None
        sub = self.subscriptions.get(payment_id)
        sub_ends_at = sub.ends_at if sub is not None else None
        auth_task = self.outbox_tasks.get(f'payment_succeeded:{payment_id}:auth_access')
        auth_delivered = auth_task is not None and auth_task.status == 'completed'
        return LocalPaymentResultData(
            payment_id=payment.id,
            user_id=payment.user_id,
            tariff_title='Премиум',
            amount_kopeks=payment.amount_kopeks,
            currency=payment.currency,
            status=payment.status,
            failure_reason=payment.failure_reason,
            subscription_ends_at=sub_ends_at,
            auth_access_delivered=auth_delivered,
        )

    async def get_user_billing_overview(
        self,
        user_id: UUID,
    ) -> UserBillingOverviewData:
        """Читает сводные данные для экрана Э2 «Моя подписка» (T-23)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')

        user_subs = sorted(
            [sub for sub in self.subscriptions.values() if sub.user_id == user_id],
            key=lambda s: (s.ends_at, s.created_at),
            reverse=True,
        )
        latest_sub = user_subs[0] if user_subs else None
        latest_ends_at = latest_sub.ends_at if latest_sub is not None else None
        has_succeeded_refund = False
        has_processing_refund = False
        auth_delivered = False

        if latest_sub is not None:
            auth_task = self.outbox_tasks.get(
                f'payment_succeeded:{latest_sub.payment_id}:auth_access'
            )
            auth_delivered = auth_task is not None and auth_task.status == 'completed'
            for refund in self.refunds.values():
                if refund.payment_id == latest_sub.payment_id:
                    if refund.status == 'succeeded':
                        has_succeeded_refund = True
                    elif refund.status == 'processing':
                        has_processing_refund = True

        user_payments = sorted(
            [p for p in self.payments.values() if p.user_id == user_id],
            key=lambda p: p.created_at,
            reverse=True,
        )
        pending_pid: UUID | None = None
        history: list[UserPaymentHistoryItemData] = []
        for p in user_payments:
            if p.status == 'pending' and pending_pid is None:
                pending_pid = p.id
            active_refund: Literal['processing', 'succeeded'] | None = None
            for refund in self.refunds.values():
                if refund.payment_id == p.id and refund.status in (
                    'processing',
                    'succeeded',
                ):
                    active_refund = (
                        'succeeded' if refund.status == 'succeeded' else 'processing'
                    )
                    break
            history.append(
                UserPaymentHistoryItemData(
                    payment_id=p.id,
                    created_at=p.created_at,
                    tariff_title='Премиум',
                    amount_kopeks=p.amount_kopeks,
                    currency=p.currency,
                    status=p.status,
                    failure_reason=p.failure_reason,
                    refund_status=active_refund,
                )
            )
        return UserBillingOverviewData(
            pending_payment_id=pending_pid,
            latest_subscription_ends_at=latest_ends_at,
            latest_subscription_has_succeeded_refund=has_succeeded_refund,
            latest_subscription_has_processing_refund=has_processing_refund,
            latest_subscription_auth_delivered=auth_delivered,
            tariff_price_kopeks=29_900,
            payments=history,
        )

    async def get_payment_by_provider_id(
        self,
        provider_code: str,
        provider_payment_id: str,
    ) -> FakePaymentRow | None:
        """Возвращает платёж по коду провайдера и внешнему номеру."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        for payment in self.payments.values():
            if (
                payment.provider_code == provider_code
                and payment.provider_payment_id == provider_payment_id
            ):
                return payment
        return None

    async def list_due_pending_payments(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
        limit: int = 20,
    ) -> list[FakePaymentRow]:
        """Возвращает pending-платежи старше min_age_seconds с учётом лимита 5 с."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        now_utc = datetime.now(UTC)
        created_cutoff = now_utc - timedelta(seconds=min_age_seconds)
        checked_cutoff = now_utc - timedelta(seconds=throttle_seconds)
        candidates = [
            p
            for p in self.payments.values()
            if p.status == 'pending'
            and p.created_at <= created_cutoff
            and (p.last_checked_at is None or p.last_checked_at <= checked_cutoff)
        ]
        candidates.sort(
            key=lambda p: (
                p.last_checked_at is not None,
                p.last_checked_at or p.created_at,
                p.created_at,
            )
        )
        return candidates[:limit]

    async def claim_next_due_pending_payment(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
    ) -> FakePaymentRow | None:
        """Атомарно захватывает один pending-платёж на сверку."""
        due = await self.list_due_pending_payments(
            min_age_seconds=min_age_seconds,
            throttle_seconds=throttle_seconds,
            limit=1,
        )
        if not due:
            return None
        payment = due[0]
        now_utc = datetime.now(UTC)
        payment.last_checked_at = now_utc
        payment.updated_at = now_utc
        return payment

    async def record_payment_reconcile_error(
        self,
        payment_id: UUID,
        *,
        error_message: str,
    ) -> FakePaymentRow:
        """Фиксирует ошибку фоновой сверки с экспоненциальным backoff."""
        now_utc = datetime.now(UTC)
        payment = self.payments[payment_id]
        if payment.status == 'pending':
            details = dict(payment.provider_details or {})
            prev_errors = details.get('_reconcile_errors', 0)
            err_count = (
                int(prev_errors) + 1 if isinstance(prev_errors, (int, str)) else 1
            )
            backoff_sec = min(300, 5 * (2 ** min(err_count - 1, 6)))
            next_eligible_at = now_utc + timedelta(seconds=max(0, backoff_sec - 5))
            details['_reconcile_errors'] = err_count
            details['_last_reconcile_error'] = error_message[:250]
            payment.provider_details = details
            payment.last_checked_at = next_eligible_at
            payment.updated_at = now_utc
        return payment

    async def get_or_create_pending_payment(
        self,
        user_id: UUID,
    ) -> tuple[FakePaymentRow, bool]:
        """Возвращает существующий или создаёт новый pending-платёж."""
        allowed, block_code = await self.check_purchase_allowed(user_id)
        if not allowed:
            if block_code == 'subscription_already_active':
                raise BillingDomainError(
                    'Подписка уже активна',
                    error_code='subscription_already_active',
                    status_code=409,
                )
            raise BillingDomainError(
                'Нельзя оформить новую подписку, пока возврат находится в обработке',
                error_code='refund_in_processing',
                status_code=409,
            )
        for payment in self.payments.values():
            if payment.user_id == user_id and payment.status == 'pending':
                return payment, False
        pid = uuid4()
        row = FakePaymentRow(
            id=pid,
            user_id=user_id,
            tariff_id=UUID('00000000-0000-0000-0000-000000000001'),
            provider_code='yookassa',
            amount_kopeks=29_900,
            currency='RUB',
            status='pending',
            idempotency_key=pid,
        )
        self.payments[pid] = row
        self.events.append(
            ('payment', pid, None, 'pending', 'continue', {'created': True})
        )
        return row, True

    async def save_provider_payment_created(
        self,
        payment_id: UUID,
        *,
        provider_payment_id: str,
        confirmation_url: str | None,
        provider_details: dict[str, Any],
    ) -> FakePaymentRow:
        """Сохраняет номер платежа провайдера и confirmation_url."""
        row = self.payments[payment_id]
        if row.status == 'pending':
            row.provider_payment_id = provider_payment_id
            if confirmation_url is not None:
                row.confirmation_url = confirmation_url
            row.provider_details = provider_details
        return row

    async def record_payment_checked(
        self,
        payment_id: UUID,
        *,
        confirmation_url: str | None = None,
        provider_details: dict[str, Any] | None = None,
    ) -> FakePaymentRow:
        """Фиксирует время последней сверки GET для pending-платежа."""
        row = self.payments[payment_id]
        if row.status == 'pending':
            if confirmation_url:
                row.confirmation_url = confirmation_url
            if provider_details is not None:
                row.provider_details = provider_details
            row.last_checked_at = datetime.now(UTC)
            row.updated_at = row.last_checked_at
        return row

    async def finalize_payment_succeeded(
        self,
        payment_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[FakePaymentRow, bool]:
        """Атомарно переводит pending -> succeeded, создаёт подписку и outbox."""
        row = self.payments[payment_id]
        if row.status != 'pending':
            return row, False

        now_utc = datetime.now(UTC)
        old_status = row.status
        row.status = 'succeeded'
        if provider_details is not None:
            row.provider_details = provider_details
        row.last_checked_at = now_utc
        row.updated_at = now_utc

        starts_at = now_utc
        ends_at = starts_at + timedelta(days=30)
        sub_id = uuid4()
        self.subscriptions[payment_id] = FakeSubscriptionRow(
            id=sub_id,
            user_id=row.user_id,
            tariff_id=row.tariff_id,
            payment_id=payment_id,
            starts_at=starts_at,
            ends_at=ends_at,
        )

        next_version = self.access_versions.get(row.user_id, 0) + 1
        self.access_versions[row.user_id] = next_version

        self.events.append(
            (
                'payment',
                payment_id,
                old_status,
                'succeeded',
                source,
                provider_details or {'status': 'succeeded'},
            )
        )

        tasks = [
            FakeOutboxTaskRow(
                id=uuid4(),
                dedup_key=f'payment_succeeded:{payment_id}:auth_access',
                target_service='auth',
                task_type='sync_access',
                payment_id=payment_id,
                user_id=row.user_id,
                access_version=next_version,
                payload={
                    'user_id': str(row.user_id),
                    'version': next_version,
                    'expires_at': ends_at.isoformat(),
                },
            ),
            FakeOutboxTaskRow(
                id=uuid4(),
                dedup_key=f'payment_succeeded:{payment_id}:notify_paid',
                target_service='notifications',
                task_type='payment_succeeded_email',
                payment_id=payment_id,
                user_id=row.user_id,
                access_version=None,
                payload=build_payment_succeeded_email_payload(
                    payment_id=payment_id,
                    subscription_id=sub_id,
                    user_id=row.user_id,
                    ends_at=ends_at,
                    public_base_url=public_base_url,
                ),
            ),
            FakeOutboxTaskRow(
                id=uuid4(),
                dedup_key=f'subscription_expiration:{sub_id}:notify_expiring',
                target_service='notifications',
                task_type='subscription_expiration_email',
                payment_id=payment_id,
                user_id=row.user_id,
                access_version=None,
                payload=build_subscription_expiration_email_payload(
                    subscription_id=sub_id,
                    user_id=row.user_id,
                    ends_at=ends_at,
                    public_base_url=public_base_url,
                ),
            ),
        ]
        for task in tasks:
            self.outbox_tasks[task.dedup_key] = task

        return row, True

    async def finalize_payment_failed(
        self,
        payment_id: UUID,
        *,
        failure_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[FakePaymentRow, bool]:
        """Атомарно переводит pending -> failed и ставит задачу письма отказа."""
        row = self.payments[payment_id]
        if row.status != 'pending':
            return row, False

        now_utc = datetime.now(UTC)
        old_status = row.status
        row.status = 'failed'
        row.failure_reason = failure_reason
        if provider_details is not None:
            row.provider_details = provider_details
        row.last_checked_at = now_utc
        row.updated_at = now_utc

        event_details: dict[str, Any] = {
            **(provider_details or {}),
            'failure_reason': failure_reason,
        }
        self.events.append(
            ('payment', payment_id, old_status, 'failed', source, event_details)
        )
        dedup_key = f'payment_failed:{payment_id}:notify_failed'
        self.outbox_tasks[dedup_key] = FakeOutboxTaskRow(
            id=uuid4(),
            dedup_key=dedup_key,
            target_service='notifications',
            task_type='payment_failed_email',
            payment_id=payment_id,
            user_id=row.user_id,
            access_version=None,
            payload=build_payment_failed_email_payload(
                payment_id=payment_id,
                user_id=row.user_id,
                failure_reason=failure_reason,
                public_base_url=public_base_url,
            ),
        )
        return row, True

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
    ) -> int:
        """Обрабатывает готовые задачи outbox в in-memory репозитории (T-19)."""
        import logging

        import sentry_sdk

        logger = logging.getLogger(__name__)

        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        if not target_services:
            return 0
        processed = 0
        now_utc = datetime.now(UTC)
        candidates = sorted(
            [
                task
                for task in self.outbox_tasks.values()
                if task.target_service in target_services
                and task.status == 'pending'
                and task.next_retry_at <= now_utc
                and (payment_id is None or task.payment_id == payment_id)
                and (task_id is None or task.id == task_id)
            ],
            key=lambda t: t.next_retry_at,
        )
        for task in candidates[:limit]:
            if should_stop is not None and should_stop():
                break
            if task.target_service == 'auth' and task.access_version is not None:
                delivered_versions = [
                    int(t.access_version)
                    for t in self.outbox_tasks.values()
                    if t.user_id == task.user_id
                    and t.target_service == 'auth'
                    and t.status == 'completed'
                    and t.access_version is not None
                ]
                if (
                    delivered_versions
                    and max(delivered_versions) >= task.access_version
                ):
                    task.status = 'completed'
                    task.last_error = None
                    processed += 1
                    continue

            outcome: DeliveryOutcome
            try:
                outcome, error_msg = await deliver_fn(task)
            except Exception as exc:
                logger.exception(
                    'Unexpected error delivering outbox task %s',
                    task.id,
                )
                outcome = 'transient_error'
                error_msg = f'Unexpected delivery error: {exc}'

            now_after = datetime.now(UTC)
            if outcome == 'completed':
                task.status = 'completed'
                task.last_error = None
            elif outcome == 'permanent_error':
                task.attempts += 1
                task.status = 'requires_intervention'
                task.last_error = error_msg
                alert_msg = (
                    f'Outbox task {task.id} requires intervention '
                    f'(permanent error): {error_msg}'
                )
                logger.error(alert_msg)
                sentry_sdk.capture_message(alert_msg, level='error')
            else:
                task.attempts += 1
                task.last_error = error_msg
                if task.attempts >= max_attempts:
                    task.status = 'requires_intervention'
                    alert_msg = (
                        f'Outbox task {task.id} exhausted {max_attempts} '
                        f'attempts and requires intervention: {error_msg}'
                    )
                    logger.error(alert_msg)
                    sentry_sdk.capture_message(alert_msg, level='error')
                else:
                    backoff_sec = compute_outbox_backoff_seconds(task.attempts)
                    task.next_retry_at = now_after + timedelta(seconds=backoff_sec)
            processed += 1
        return processed

    async def process_due_auth_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        payment_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int:
        """Обрабатывает готовые задачи Auth в in-memory репозитории."""
        return await self.process_due_outbox_tasks(
            deliver_fn,
            target_services=('auth',),
            payment_id=payment_id,
            limit=limit,
            max_attempts=max_attempts,
            should_stop=should_stop,
        )

    async def list_internal_subscriptions(
        self,
        filters: InternalSubscriptionFilter,
        *,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalSubscriptionItemData]]:
        """Возвращает отфильтрованный список подписок для админки (T-26)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        now = now_utc or datetime.now(UTC)
        matched: list[InternalSubscriptionItemData] = []
        subs = sorted(
            self.subscriptions.values(),
            key=lambda s: (s.created_at, s.id),
            reverse=True,
        )
        for sub in subs:
            if filters.user_id is not None and sub.user_id != filters.user_id:
                continue
            if (
                filters.created_from is not None
                and sub.created_at < filters.created_from
            ):
                continue
            if filters.created_to is not None and sub.created_at > filters.created_to:
                continue
            has_succeeded_refund = any(
                r.subscription_id == sub.id and r.status == 'succeeded'
                for r in self.refunds.values()
            )
            state_enum = compute_subscription_state(
                ends_at=sub.ends_at,
                has_succeeded_refund=has_succeeded_refund,
                now_utc=now,
            )
            computed_status: Literal['active', 'expired', 'refunded'] = (
                'refunded'
                if state_enum == 'refunded'
                else 'active'
                if state_enum == 'active'
                else 'expired'
            )
            if filters.status is not None and computed_status != filters.status:
                continue
            pay = self.payments.get(sub.payment_id)
            amount_kopeks = pay.amount_kopeks if pay is not None else 29900
            currency = pay.currency.strip() if pay is not None else 'RUB'
            matched.append(
                InternalSubscriptionItemData(
                    id=sub.id,
                    user_id=sub.user_id,
                    payment_id=sub.payment_id,
                    tariff_code='premium',
                    tariff_name='Подписка «Премиум»',
                    starts_at=sub.starts_at,
                    ends_at=sub.ends_at,
                    status=computed_status,
                    amount_kopeks=amount_kopeks,
                    currency=currency,
                    created_at=sub.created_at,
                )
            )
        total = len(matched)
        return total, matched[filters.offset : filters.offset + filters.limit]

    async def list_internal_payments(
        self,
        filters: InternalPaymentFilter,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalPaymentItemData]]:
        """Возвращает отфильтрованный список платежей для админки (T-26)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        now = now_utc or datetime.now(UTC)
        stuck_cutoff = now - timedelta(seconds=stuck_threshold_seconds)
        matched: list[InternalPaymentItemData] = []
        payments = sorted(
            self.payments.values(),
            key=lambda p: (p.created_at, p.id),
            reverse=True,
        )
        for pay in payments:
            p_refunds = sorted(
                [r for r in self.refunds.values() if r.payment_id == pay.id],
                key=lambda r: (r.created_at, r.id),
                reverse=True,
            )
            has_ref = len(p_refunds) > 0
            is_stuck = (pay.status == 'pending' and pay.created_at <= stuck_cutoff) or (
                any(
                    r.status == 'processing' and r.created_at <= stuck_cutoff
                    for r in p_refunds
                )
            )
            if filters.status is not None and pay.status != filters.status:
                continue
            if filters.has_refund is True and not has_ref:
                continue
            if filters.has_refund is False and has_ref:
                continue
            if filters.stuck and not is_stuck:
                continue
            if filters.user_id is not None and pay.user_id != filters.user_id:
                continue
            if filters.payment_id is not None and pay.id != filters.payment_id:
                continue
            if (
                filters.provider_payment_id is not None
                and pay.provider_payment_id != filters.provider_payment_id
            ):
                continue
            if (
                filters.created_from is not None
                and pay.created_at < filters.created_from
            ):
                continue
            if filters.created_to is not None and pay.created_at > filters.created_to:
                continue

            active_or_latest = next(
                (r for r in p_refunds if r.status in ('processing', 'succeeded')),
                p_refunds[0] if p_refunds else None,
            )
            ref_status: Literal['processing', 'succeeded', 'rejected'] | None = None
            active_ref_id: UUID | None = None
            if active_or_latest is not None:
                if active_or_latest.status == 'succeeded':
                    ref_status = 'succeeded'
                    active_ref_id = active_or_latest.id
                elif active_or_latest.status == 'processing':
                    ref_status = 'processing'
                    active_ref_id = active_or_latest.id
                else:
                    ref_status = 'rejected'

            normalized_status: Literal['pending', 'succeeded', 'failed'] = (
                'succeeded'
                if pay.status == 'succeeded'
                else 'failed'
                if pay.status == 'failed'
                else 'pending'
            )
            matched.append(
                InternalPaymentItemData(
                    id=pay.id,
                    user_id=pay.user_id,
                    tariff_code='premium',
                    tariff_name='Подписка «Премиум»',
                    provider_code=pay.provider_code,
                    provider_payment_id=pay.provider_payment_id,
                    amount_kopeks=pay.amount_kopeks,
                    currency=pay.currency.strip(),
                    status=normalized_status,
                    failure_reason=pay.failure_reason,
                    has_refund=has_ref,
                    refund_status=ref_status,
                    active_refund_id=active_ref_id,
                    is_stuck=is_stuck,
                    created_at=pay.created_at,
                    updated_at=pay.updated_at,
                )
            )
        total = len(matched)
        return total, matched[filters.offset : filters.offset + filters.limit]

    async def get_internal_payment_card(
        self,
        payment_id: UUID,
        *,
        now_utc: datetime | None = None,
    ) -> InternalPaymentCardData | None:
        """Возвращает полную карточку платежа для админки (T-28, BIL-API-08)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        pay = self.payments.get(payment_id)
        if pay is None:
            return None
        now = now_utc or datetime.now(UTC)

        p_refunds = sorted(
            [r for r in self.refunds.values() if r.payment_id == payment_id],
            key=lambda r: (r.created_at, r.id),
            reverse=True,
        )
        has_succeeded_refund = any(r.status == 'succeeded' for r in p_refunds)
        has_active_or_succeeded = any(
            r.status in ('processing', 'succeeded') for r in p_refunds
        )

        sub_row = next(
            (s for s in self.subscriptions.values() if s.payment_id == payment_id),
            None,
        )
        sub_data: InternalSubscriptionItemData | None = None
        if sub_row is not None:
            state_enum = compute_subscription_state(
                ends_at=sub_row.ends_at,
                has_succeeded_refund=has_succeeded_refund,
                now_utc=now,
            )
            sub_status: Literal['active', 'expired', 'refunded'] = (
                'refunded'
                if state_enum == 'refunded'
                else 'active'
                if state_enum == 'active'
                else 'expired'
            )
            sub_data = InternalSubscriptionItemData(
                id=sub_row.id,
                user_id=sub_row.user_id,
                payment_id=sub_row.payment_id,
                tariff_code='premium',
                tariff_name='Подписка «Премиум»',
                starts_at=sub_row.starts_at,
                ends_at=sub_row.ends_at,
                status=sub_status,
                amount_kopeks=pay.amount_kopeks,
                currency=pay.currency.strip(),
                created_at=sub_row.created_at,
            )

        refund_ids = {r.id for r in p_refunds}
        event_items: list[InternalOperationEventItemData] = []
        for idx, (op_type, op_id, from_st, to_st, src, details) in enumerate(
            self.events
        ):
            if (op_type == 'payment' and op_id == payment_id) or (
                op_type == 'refund' and op_id in refund_ids
            ):
                event_items.append(
                    InternalOperationEventItemData(
                        id=uuid4(),
                        operation_type='refund' if op_type == 'refund' else 'payment',
                        operation_id=op_id,
                        from_status=from_st,
                        to_status=to_st,
                        source=src,
                        details=details,
                        created_at=pay.created_at + timedelta(seconds=idx),
                    )
                )

        refund_items: list[InternalRefundItemData] = []
        for r in p_refunds:
            r_status: Literal['processing', 'succeeded', 'rejected'] = (
                'succeeded'
                if r.status == 'succeeded'
                else 'processing'
                if r.status == 'processing'
                else 'rejected'
            )
            refund_items.append(
                InternalRefundItemData(
                    id=r.id,
                    payment_id=r.payment_id,
                    subscription_id=r.subscription_id,
                    provider_code=r.provider_code,
                    provider_refund_id=r.provider_refund_id,
                    amount_kopeks=r.amount_kopeks,
                    currency=r.currency.strip(),
                    status=r_status,
                    reason=r.reason,
                    rejection_reason=r.rejection_reason,
                    admin_user_id=r.admin_user_id,
                    created_at=r.created_at,
                    updated_at=r.created_at,
                    can_reconcile=(r_status == 'processing'),
                )
            )

        task_items: list[InternalOutboxTaskItemData] = []
        for t in self.outbox_tasks.values():
            if t.payment_id != payment_id:
                continue
            t_status: Literal[
                'pending', 'completed', 'requires_intervention', 'closed'
            ] = (
                'completed'
                if t.status == 'completed'
                else 'requires_intervention'
                if t.status == 'requires_intervention'
                else 'closed'
                if t.status == 'closed'
                else 'pending'
            )
            task_items.append(
                InternalOutboxTaskItemData(
                    id=t.id,
                    target_service=(
                        'notifications'
                        if t.target_service == 'notifications'
                        else 'auth'
                    ),
                    task_type=t.task_type,
                    access_version=t.access_version,
                    status=t_status,
                    attempts=t.attempts,
                    last_error=t.last_error,
                    created_at=t.created_at,
                    updated_at=t.updated_at,
                    can_retry_or_close=(t_status == 'requires_intervention'),
                    payment_id=t.payment_id,
                    user_id=t.user_id,
                )
            )

        normalized_status: Literal['pending', 'succeeded', 'failed'] = (
            'succeeded'
            if pay.status == 'succeeded'
            else 'failed'
            if pay.status == 'failed'
            else 'pending'
        )
        can_refund = (
            normalized_status == 'succeeded'
            and sub_data is not None
            and sub_data.status == 'active'
            and not has_active_or_succeeded
        )
        return InternalPaymentCardData(
            id=pay.id,
            user_id=pay.user_id,
            tariff_code='premium',
            tariff_name='Подписка «Премиум»',
            provider_code=pay.provider_code,
            provider_payment_id=pay.provider_payment_id,
            amount_kopeks=pay.amount_kopeks,
            currency=pay.currency.strip(),
            status=normalized_status,
            failure_reason=pay.failure_reason,
            created_at=pay.created_at,
            updated_at=pay.updated_at,
            can_refund=can_refund,
            can_reconcile_payment=(normalized_status == 'pending'),
            subscription=sub_data,
            refunds=refund_items,
            operation_events=event_items,
            outbox_tasks=task_items,
        )

    async def list_internal_outbox_tasks(
        self,
        filters: InternalOutboxFilter,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalOutboxTaskItemData]]:
        """Возвращает проблемные задачи outbox для админки (T-31, BIL-API-11)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        now = now_utc or datetime.now(UTC)
        stuck_cutoff = now - timedelta(seconds=stuck_threshold_seconds)
        matched_rows: list[FakeOutboxTaskRow] = []

        for t in self.outbox_tasks.values():
            if filters.status == 'requires_intervention':
                if t.status != 'requires_intervention':
                    continue
            elif filters.status == 'pending':
                if not (t.status == 'pending' and t.created_at <= stuck_cutoff):
                    continue
            else:
                if not (
                    t.status == 'requires_intervention'
                    or (t.status == 'pending' and t.created_at <= stuck_cutoff)
                ):
                    continue
            if (
                filters.target_service is not None
                and t.target_service != filters.target_service
            ):
                continue
            if filters.user_id is not None and t.user_id != filters.user_id:
                continue
            if filters.payment_id is not None and t.payment_id != filters.payment_id:
                continue
            if filters.created_from is not None and t.created_at < filters.created_from:
                continue
            if filters.created_to is not None and t.created_at > filters.created_to:
                continue
            matched_rows.append(t)

        matched_rows.sort(
            key=lambda r: (
                0 if r.status == 'requires_intervention' else 1,
                -r.created_at.timestamp(),
                r.id,
            )
        )
        items: list[InternalOutboxTaskItemData] = []
        for t in matched_rows:
            t_status: Literal[
                'pending', 'completed', 'requires_intervention', 'closed'
            ] = (
                'requires_intervention'
                if t.status == 'requires_intervention'
                else 'pending'
            )
            items.append(
                InternalOutboxTaskItemData(
                    id=t.id,
                    target_service=(
                        'notifications'
                        if t.target_service == 'notifications'
                        else 'auth'
                    ),
                    task_type=t.task_type,
                    access_version=t.access_version,
                    status=t_status,
                    attempts=t.attempts,
                    last_error=t.last_error,
                    created_at=t.created_at,
                    updated_at=t.updated_at,
                    can_retry_or_close=(t_status == 'requires_intervention'),
                    payment_id=t.payment_id,
                    user_id=t.user_id,
                )
            )
        total = len(items)
        return total, items[filters.offset : filters.offset + filters.limit]

    @staticmethod
    def _to_fake_outbox_item(t: FakeOutboxTaskRow) -> InternalOutboxTaskItemData:
        t_status: Literal['pending', 'completed', 'requires_intervention', 'closed'] = (
            'completed'
            if t.status == 'completed'
            else 'requires_intervention'
            if t.status == 'requires_intervention'
            else 'closed'
            if t.status == 'closed'
            else 'pending'
        )
        return InternalOutboxTaskItemData(
            id=t.id,
            target_service=(
                'notifications' if t.target_service == 'notifications' else 'auth'
            ),
            task_type=t.task_type,
            access_version=t.access_version,
            status=t_status,
            attempts=t.attempts,
            last_error=t.last_error,
            created_at=t.created_at,
            updated_at=t.updated_at,
            can_retry_or_close=(t_status == 'requires_intervention'),
            payment_id=t.payment_id,
            user_id=t.user_id,
        )

    async def get_internal_outbox_task(
        self,
        task_id: UUID,
    ) -> InternalOutboxTaskItemData | None:
        """Возвращает задачу outbox по ID (T-32)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        found = next((t for t in self.outbox_tasks.values() if t.id == task_id), None)
        if found is None:
            return None
        return self._to_fake_outbox_item(found)

    async def retry_outbox_task_by_admin(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> InternalOutboxTaskItemData:
        """Сбрасывает счётчик попыток и ставит задачу на повтор (T-32, BIL-API-12)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        found = next((t for t in self.outbox_tasks.values() if t.id == task_id), None)
        if found is None:
            raise BillingDomainError(
                'Outbox task not found',
                error_code='outbox_task_not_found',
                status_code=404,
            )
        if found.status in ('completed', 'closed'):
            raise BillingDomainError(
                'Задача уже завершена или закрыта',
                error_code='outbox_task_not_retryable',
                status_code=409,
            )
        now = now_utc or datetime.now(UTC)
        cleaned_reason = (
            reason.strip()
            if reason.strip()
            else 'Ручной повтор отправки администратором'
        )
        found.status = 'pending'
        found.attempts = 0
        found.next_retry_at = now
        found.updated_at = now
        self.admin_actions.append(
            FakeAdminActionRow(
                id=uuid4(),
                admin_user_id=admin_user_id,
                action_type='outbox_retried',
                target_type='outbox_task',
                target_id=found.id,
                reason=cleaned_reason,
                created_at=now,
            )
        )
        return self._to_fake_outbox_item(found)

    async def close_outbox_task_by_admin(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> InternalOutboxTaskItemData:
        """Закрывает задачу requires_intervention с причиной (T-32, BIL-API-13)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        found = next((t for t in self.outbox_tasks.values() if t.id == task_id), None)
        if found is None:
            raise BillingDomainError(
                'Outbox task not found',
                error_code='outbox_task_not_found',
                status_code=404,
            )
        if found.status != 'requires_intervention':
            raise BillingDomainError(
                'Закрытие доступно только для задачи в состоянии requires_intervention',
                error_code='outbox_task_not_requires_intervention',
                status_code=409,
            )
        now = now_utc or datetime.now(UTC)
        cleaned_reason = reason.strip()
        found.status = 'closed'
        found.updated_at = now
        self.admin_actions.append(
            FakeAdminActionRow(
                id=uuid4(),
                admin_user_id=admin_user_id,
                action_type='outbox_closed',
                target_type='outbox_task',
                target_id=found.id,
                reason=cleaned_reason,
                created_at=now,
            )
        )
        return self._to_fake_outbox_item(found)

    async def get_refund(self, refund_id: UUID) -> FakeRefundRow | None:
        """Возвращает возврат по ID (T-29)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        return self.refunds.get(refund_id)

    async def get_refund_by_provider_id(
        self,
        provider_code: str,
        provider_refund_id: str,
    ) -> FakeRefundRow | None:
        """Возвращает возврат по provider_refund_id (T-29)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        for r in self.refunds.values():
            if (
                r.provider_code == provider_code
                and r.provider_refund_id == provider_refund_id
            ):
                return r
        return None

    async def claim_next_due_processing_refund(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
    ) -> FakeRefundRow | None:
        """Выбирает возврат в статусе processing старше min_age_seconds (T-29)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        now_utc = datetime.now(UTC)
        age_cutoff = now_utc - timedelta(seconds=min_age_seconds)
        throttle_cutoff = now_utc - timedelta(seconds=throttle_seconds)
        candidates = sorted(
            [
                r
                for r in self.refunds.values()
                if r.status == 'processing'
                and r.created_at <= age_cutoff
                and (r.last_checked_at is None or r.last_checked_at <= throttle_cutoff)
            ],
            key=lambda r: (
                r.last_checked_at is not None,
                r.last_checked_at or r.created_at,
                r.created_at,
            ),
        )
        if not candidates:
            return None
        chosen = candidates[0]
        chosen.last_checked_at = now_utc
        chosen.updated_at = now_utc
        return chosen

    async def get_or_create_processing_refund(
        self,
        payment_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> tuple[FakeRefundRow, bool]:
        """Создаёт или возвращает существующий возврат в обработке (T-29, инв. 4, 5)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        pay = self.payments.get(payment_id)
        if pay is None:
            raise BillingDomainError(
                'Payment not found',
                error_code='payment_not_found',
                status_code=404,
            )
        now = now_utc or datetime.now(UTC)
        cleaned_reason = reason.strip()

        for existing in self.refunds.values():
            if existing.payment_id == payment_id and existing.status in (
                'processing',
                'succeeded',
            ):
                if existing.status == 'processing':
                    return existing, False
                raise BillingDomainError(
                    'Возврат по данному платежу уже выполнен',
                    error_code='refund_already_succeeded',
                    status_code=409,
                )

        if pay.status != 'succeeded':
            raise BillingDomainError(
                'Возврат возможен только для оплаченного платежа активной подписки',
                error_code='refund_not_allowed',
                status_code=409,
            )
        sub = next(
            (s for s in self.subscriptions.values() if s.payment_id == payment_id),
            None,
        )
        if (
            sub is None
            or compute_subscription_state(
                ends_at=sub.ends_at,
                has_succeeded_refund=False,
                now_utc=now,
            )
            != 'active'
        ):
            raise BillingDomainError(
                'Возврат возможен только для платежа текущей активной подписки',
                error_code='refund_not_allowed',
                status_code=409,
            )

        refund_id = uuid4()
        refund = FakeRefundRow(
            id=refund_id,
            payment_id=payment_id,
            subscription_id=sub.id,
            provider_code=pay.provider_code,
            amount_kopeks=pay.amount_kopeks,
            currency=pay.currency.strip(),
            status='processing',
            reason=cleaned_reason,
            admin_user_id=admin_user_id,
            idempotency_key=refund_id,
            created_at=now,
            updated_at=now,
        )
        self.refunds[refund_id] = refund
        self.events.append(
            (
                'refund',
                refund_id,
                None,
                'processing',
                'admin',
                {
                    'payment_id': str(payment_id),
                    'admin_user_id': str(admin_user_id),
                    'reason': cleaned_reason,
                },
            )
        )
        self.admin_actions.append(
            FakeAdminActionRow(
                id=uuid4(),
                admin_user_id=admin_user_id,
                action_type='refund_requested',
                target_type='refund',
                target_id=refund_id,
                reason=cleaned_reason,
                created_at=now,
            )
        )
        return refund, True

    async def save_provider_refund_created(
        self,
        refund_id: UUID,
        *,
        provider_refund_id: str,
        provider_details: dict[str, Any],
    ) -> FakeRefundRow:
        """Сохраняет номер возврата от провайдера (T-29)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        row = self.refunds[refund_id]
        if row.status == 'processing':
            row.provider_refund_id = provider_refund_id
            row.provider_details = provider_details
            row.updated_at = datetime.now(UTC)
        return row

    async def record_refund_checked(
        self,
        refund_id: UUID,
        *,
        provider_details: dict[str, Any] | None = None,
    ) -> FakeRefundRow:
        """Обновляет last_checked_at при сверке возврата в processing (T-29)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        now_utc = datetime.now(UTC)
        row = self.refunds[refund_id]
        if row.status == 'processing':
            row.last_checked_at = now_utc
            row.updated_at = now_utc
            if provider_details is not None:
                row.provider_details = provider_details
        return row

    async def record_refund_reconcile_error(
        self,
        refund_id: UUID,
        *,
        error_message: str,
    ) -> FakeRefundRow:
        """Фиксирует ошибку фоновой сверки возврата с backoff (T-29)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        now_utc = datetime.now(UTC)
        row = self.refunds[refund_id]
        if row.status == 'processing':
            details = dict(row.provider_details or {})
            prev_errors = details.get('_reconcile_errors', 0)
            err_count = (
                int(prev_errors) + 1 if isinstance(prev_errors, (int, str)) else 1
            )
            backoff_sec = min(300, 5 * (2 ** min(err_count - 1, 6)))
            details['_reconcile_errors'] = err_count
            details['_last_reconcile_error'] = error_message[:250]
            row.provider_details = details
            row.last_checked_at = now_utc + timedelta(seconds=max(0, backoff_sec - 5))
            row.updated_at = now_utc
        return row

    async def finalize_refund_succeeded(
        self,
        refund_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[FakeRefundRow, bool]:
        """Атомарно переводит возврат в succeeded и ставит задачи в outbox (T-29)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        row = self.refunds[refund_id]
        if row.status != 'processing':
            return row, False
        pay = self.payments[row.payment_id]
        now_utc = datetime.now(UTC)
        old_status = row.status
        row.status = 'succeeded'
        if provider_details is not None:
            row.provider_details = provider_details
        row.last_checked_at = now_utc
        row.updated_at = now_utc

        self.events.append(
            ('refund', refund_id, old_status, 'succeeded', source, provider_details)
        )
        new_version = self.access_versions.get(pay.user_id, 0) + 1
        self.access_versions[pay.user_id] = new_version

        k1 = f'refund_succeeded:{refund_id}:sync_access'
        self.outbox_tasks[k1] = FakeOutboxTaskRow(
            id=uuid4(),
            dedup_key=k1,
            target_service='auth',
            task_type='sync_access',
            payment_id=pay.id,
            user_id=pay.user_id,
            access_version=new_version,
            payload={'version': new_version, 'expires_at': None},
        )
        k2 = f'refund_succeeded:{refund_id}:cancel_expiration'
        self.outbox_tasks[k2] = FakeOutboxTaskRow(
            id=uuid4(),
            dedup_key=k2,
            target_service='notifications',
            task_type='cancel_expiration_email',
            payment_id=pay.id,
            user_id=pay.user_id,
            access_version=None,
            payload=build_cancel_expiration_email_payload(
                subscription_id=row.subscription_id,
                user_id=pay.user_id,
            ),
        )
        k3 = f'refund_succeeded:{refund_id}:notify_refund'
        self.outbox_tasks[k3] = FakeOutboxTaskRow(
            id=uuid4(),
            dedup_key=k3,
            target_service='notifications',
            task_type='refund_succeeded_email',
            payment_id=pay.id,
            user_id=pay.user_id,
            access_version=None,
            payload=build_refund_succeeded_email_payload(
                refund_id=refund_id,
                payment_id=pay.id,
                user_id=pay.user_id,
                amount_kopeks=row.amount_kopeks,
                currency=row.currency.strip(),
                public_base_url=public_base_url,
            ),
        )
        return row, True

    async def finalize_refund_rejected(
        self,
        refund_id: UUID,
        *,
        rejection_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
    ) -> tuple[FakeRefundRow, bool]:
        """Переводит возврат в rejected с сохранением причины отказа (T-29)."""
        if self.db_unavailable:
            raise DatabaseUnavailableError('billing-db is down')
        row = self.refunds[refund_id]
        if row.status != 'processing':
            return row, False
        now_utc = datetime.now(UTC)
        old_status = row.status
        row.status = 'rejected'
        row.rejection_reason = rejection_reason
        if provider_details is not None:
            row.provider_details = provider_details
        row.last_checked_at = now_utc
        row.updated_at = now_utc
        self.events.append(
            (
                'refund',
                refund_id,
                old_status,
                'rejected',
                source,
                {**(provider_details or {}), 'rejection_reason': rejection_reason},
            )
        )
        return row, True


class FakeProviderGateway:
    """Единый in-memory стаб платёжного провайдера для модульных TDD-тестов."""

    provider_code = 'yookassa'

    def __init__(self, *, id_prefix: str = 'yk-pay') -> None:
        self._id_prefix = id_prefix
        self.create_calls: list[CreateProviderPaymentRequest] = []
        self.get_calls: list[str] = []
        self.create_error: Exception | None = None
        self.get_error: Exception | None = None
        self.get_status: str = 'pending'
        self.next_provider_num: int = 1
        self.create_refund_calls: list[CreateProviderRefundRequest] = []
        self.get_refund_calls: list[str] = []
        self.create_refund_error: Exception | None = None
        self.get_refund_error: Exception | None = None
        self.find_refund_error: Exception | None = None
        self.next_get_refund_status: str = 'succeeded'
        self.next_get_refund_rejection_reason: str | None = None
        self.metadata_refunds: dict[UUID, str] = {}
        self.next_refund_num: int = 1

    async def create_payment(
        self,
        request: CreateProviderPaymentRequest,
    ) -> ProviderPaymentSnapshot:
        """Эмулирует создание платежа в ЮKassa."""
        self.create_calls.append(request)
        if self.create_error is not None:
            raise self.create_error
        provider_id = f'{self._id_prefix}-{self.next_provider_num}'
        self.next_provider_num += 1
        return ProviderPaymentSnapshot(
            provider_code='yookassa',
            provider_payment_id=provider_id,
            domain_status='pending',
            amount_kopeks=request.amount_kopeks,
            currency=request.currency,
            confirmation_url=(
                f'https://yoomoney.ru/checkout/payments/v2/contract?orderId={provider_id}'
            ),
            failure_reason=None,
            metadata_payment_id=str(request.payment_id),
            provider_details={'id': provider_id, 'status': 'pending'},
        )

    async def get_payment(
        self,
        provider_payment_id: str,
        *,
        expected_payment_id: UUID | None = None,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderPaymentSnapshot:
        """Эмулирует чтение статуса платежа в ЮKassa."""
        self.get_calls.append(provider_payment_id)
        if self.get_error is not None:
            raise self.get_error
        return ProviderPaymentSnapshot(
            provider_code='yookassa',
            provider_payment_id=provider_payment_id,
            domain_status=self.get_status,  # type: ignore[arg-type]
            amount_kopeks=expected_amount_kopeks or 29_900,
            currency=expected_currency or 'RUB',
            confirmation_url=(
                f'https://yoomoney.ru/checkout/payments/v2/contract?orderId={provider_payment_id}'
            ),
            failure_reason=(
                'expired_on_confirmation' if self.get_status == 'failed' else None
            ),
            metadata_payment_id=(
                str(expected_payment_id) if expected_payment_id else None
            ),
            provider_details={'id': provider_payment_id, 'status': self.get_status},
        )

    async def create_refund(
        self,
        request: CreateProviderRefundRequest,
    ) -> ProviderRefundSnapshot:
        """Эмулирует создание возврата в ЮKassa (T-29)."""
        self.create_refund_calls.append(request)
        if self.create_refund_error is not None:
            raise self.create_refund_error
        provider_refund_id = f'yk-ref-{self.next_refund_num}'
        self.next_refund_num += 1
        return ProviderRefundSnapshot(
            provider_code='yookassa',
            provider_refund_id=provider_refund_id,
            provider_payment_id=request.provider_payment_id,
            domain_status='processing',
            amount_kopeks=request.amount_kopeks,
            currency=request.currency,
            rejection_reason=None,
            metadata_refund_id=str(request.refund_id),
            provider_details={'id': provider_refund_id, 'status': 'pending'},
        )

    async def get_refund(
        self,
        provider_refund_id: str,
        *,
        expected_refund_id: UUID | None = None,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderRefundSnapshot:
        """Эмулирует чтение статуса возврата в ЮKassa (T-29)."""
        self.get_refund_calls.append(provider_refund_id)
        if self.get_refund_error is not None:
            raise self.get_refund_error
        return ProviderRefundSnapshot(
            provider_code='yookassa',
            provider_refund_id=provider_refund_id,
            provider_payment_id='yk-pay-1',
            domain_status=self.next_get_refund_status,  # type: ignore[arg-type]
            amount_kopeks=expected_amount_kopeks or 29_900,
            currency=expected_currency or 'RUB',
            rejection_reason=(
                self.next_get_refund_rejection_reason or 'canceled_by_provider'
                if self.next_get_refund_status == 'rejected'
                else None
            ),
            metadata_refund_id=(
                str(expected_refund_id) if expected_refund_id else None
            ),
            provider_details={
                'id': provider_refund_id,
                'status': self.next_get_refund_status,
            },
        )

    async def find_refund_by_metadata(
        self,
        provider_payment_id: str,
        *,
        refund_id: UUID,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderRefundSnapshot | None:
        """Эмулирует розыск возврата по metadata в списке ЮKassa (T-29, инв. 14)."""
        if self.find_refund_error is not None:
            raise self.find_refund_error
        prov_ref_id = self.metadata_refunds.get(refund_id)
        if prov_ref_id is None:
            return None
        return ProviderRefundSnapshot(
            provider_code='yookassa',
            provider_refund_id=prov_ref_id,
            provider_payment_id=provider_payment_id,
            domain_status=self.next_get_refund_status,  # type: ignore[arg-type]
            amount_kopeks=expected_amount_kopeks or 29_900,
            currency=expected_currency or 'RUB',
            rejection_reason=None,
            metadata_refund_id=str(refund_id),
            provider_details={'id': prov_ref_id, 'payment_id': provider_payment_id},
        )


class ConfigurableAuthGateway:
    """Единый стаб AuthGateway с поддержкой настройки часового пояса и отказа 503."""

    def __init__(
        self,
        user_id: UUID,
        *,
        login: str = 'viewer',
        tz: str = 'Europe/Moscow',
        unavailable: bool = False,
    ) -> None:
        self.user_id = user_id
        self.user_login = login
        self.tz = tz
        self.unavailable = unavailable
        self.invalid_token = False
        self.permanent_error: AuthPermanentDeliveryError | None = None
        self.synced_access_calls: list[tuple[UUID, int, str | None]] = []
        self.seen_request_ids: list[str] = []

    async def login(
        self, login: str, password: str, *, request_id: str
    ) -> tuple[str, str]:
        """Эмулирует вход."""
        if self.unavailable:
            raise AuthServiceUnavailableError('Auth is down')
        if self.invalid_token:
            raise AuthInvalidCredentialsError('Invalid credentials')
        return 'acc', 'ref'

    async def get_current_user(
        self, access_token: str, *, request_id: str
    ) -> tuple[UUID, str, str]:
        """Возвращает пользователя или имитирует отказ Auth."""
        if self.unavailable:
            raise AuthServiceUnavailableError('Auth is down')
        if self.invalid_token:
            raise AuthInvalidCredentialsError('Token expired or revoked')
        return self.user_id, self.user_login, self.tz

    async def logout(
        self, access_token: str, refresh_token: str, *, request_id: str
    ) -> None:
        """Эмулирует выход."""
        return None

    async def put_subscription_access(
        self,
        user_id: UUID,
        *,
        version: int,
        expires_at: str | None,
        request_id: str,
    ) -> SubscriptionAccessSyncResult:
        """Эмулирует запись состояния доступа в Auth."""
        self.seen_request_ids.append(request_id)
        if self.unavailable:
            raise AuthServiceUnavailableError('Auth is down')
        if self.permanent_error is not None:
            raise self.permanent_error
        self.synced_access_calls.append((user_id, version, expires_at))
        return SubscriptionAccessSyncResult(
            applied=True,
            current_version=version,
        )
