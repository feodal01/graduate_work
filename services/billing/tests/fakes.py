from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from api.errors import BillingDomainError
from db.sql.errors import DatabaseUnavailableError
from domain.subscription import ReconcileSource, deterministic_id, format_msk_datetime
from repositories.purchase import LocalPaymentResultData


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


class InMemoryPurchaseRepository:
    """Единый in-memory репозиторий для поведенческих TDD-тестов T-10 и T-11."""

    def __init__(self) -> None:
        self.has_active_subscription: bool = False
        self.has_processing_refund: bool = False
        self.db_unavailable: bool = False
        self.payments: dict[UUID, FakePaymentRow] = {}
        self.subscriptions: dict[UUID, FakeSubscriptionRow] = {}
        self.access_versions: dict[UUID, int] = {}
        self.outbox_tasks: dict[str, FakeOutboxTaskRow] = {}
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

    async def get_payment_by_provider_id(
        self,
        provider_code: str,
        provider_payment_id: str,
    ) -> FakePaymentRow | None:
        """Возвращает платёж по коду провайдера и внешнему номеру."""
        for payment in self.payments.values():
            if (
                payment.provider_code == provider_code
                and payment.provider_payment_id == provider_payment_id
            ):
                return payment
        return None

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

        base_url = public_base_url.rstrip('/')
        ends_msk = format_msk_datetime(ends_at)
        paid_campaign_id = deterministic_id('payment_succeeded_email', payment_id)
        paid_batch_id = deterministic_id('payment_succeeded_batch', payment_id)
        exp_campaign_id = deterministic_id('subscription_expiration', sub_id)
        exp_batch_id = deterministic_id('subscription_expiration_batch', sub_id)

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
                payload={
                    'campaign_id': str(paid_campaign_id),
                    'batch_id': str(paid_batch_id),
                    'user_id': str(row.user_id),
                    'template_id': 'billing_notification',
                    'subject': 'Подписка «Премиум» оплачена',
                    'text': (
                        f'Оплата подписки «Премиум» подтверждена. '
                        f'Доступ к новинкам открыт до {ends_msk}. '
                        f'Страница подписки: {base_url}/billing/'
                    ),
                    'available_at': None,
                    'subscription_id': str(sub_id),
                    'ends_at': ends_at.isoformat(),
                },
            ),
            FakeOutboxTaskRow(
                id=uuid4(),
                dedup_key=f'subscription_expiration:{sub_id}:notify_expiring',
                target_service='notifications',
                task_type='subscription_expiration_email',
                payment_id=payment_id,
                user_id=row.user_id,
                access_version=None,
                payload={
                    'campaign_id': str(exp_campaign_id),
                    'batch_id': str(exp_batch_id),
                    'user_id': str(row.user_id),
                    'template_id': 'billing_notification',
                    'subject': 'Срок подписки «Премиум» закончился',
                    'text': (
                        f'Срок действия вашей подписки «Премиум» истёк '
                        f'({ends_msk}). Вы можете оформить новую подписку '
                        f'на странице: {base_url}/billing/'
                    ),
                    'available_at': ends_at.isoformat(),
                    'subscription_id': str(sub_id),
                },
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
        failed_campaign_id = deterministic_id('payment_failed_email', payment_id)
        failed_batch_id = deterministic_id('payment_failed_batch', payment_id)
        self.outbox_tasks[dedup_key] = FakeOutboxTaskRow(
            id=uuid4(),
            dedup_key=dedup_key,
            target_service='notifications',
            task_type='payment_failed_email',
            payment_id=payment_id,
            user_id=row.user_id,
            access_version=None,
            payload={
                'campaign_id': str(failed_campaign_id),
                'batch_id': str(failed_batch_id),
                'user_id': str(row.user_id),
                'template_id': 'billing_notification',
                'subject': 'Оплата подписки «Премиум» не прошла',
                'text': (
                    f'Не удалось провести оплату подписки «Премиум» '
                    f'(причина: {failure_reason}). '
                    f'Попробовать снова можно на странице: '
                    f'{public_base_url.rstrip("/")}/billing/'
                ),
                'available_at': None,
                'failure_reason': failure_reason,
            },
        )
        return row, True
