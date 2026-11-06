import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

import sentry_sdk
from sqlalchemy import and_, case, exists, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from api.errors import BillingDomainError
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
from models import (
    DEFAULT_PROVIDER_CODE,
    DEFAULT_TARIFF_CODE,
    DEFAULT_TARIFF_PRICE_KOPEKS,
    AdminAction,
    OperationEvent,
    OutboxTask,
    Payment,
    PaymentProvider,
    Refund,
    Subscription,
    Tariff,
    UserAccessVersion,
)

logger = logging.getLogger(__name__)


class PaymentRecordProtocol(Protocol):
    id: UUID
    user_id: UUID
    tariff_id: UUID
    provider_code: str
    amount_kopeks: int
    currency: str
    status: str
    idempotency_key: UUID
    provider_payment_id: str | None
    confirmation_url: str | None
    failure_reason: str | None
    provider_details: dict[str, Any] | None
    last_checked_at: datetime | None
    created_at: datetime
    updated_at: datetime


class RefundRecordProtocol(Protocol):
    id: UUID
    payment_id: UUID
    subscription_id: UUID
    provider_code: str
    amount_kopeks: int
    currency: str
    status: str
    idempotency_key: UUID
    provider_refund_id: str | None
    rejection_reason: str | None
    provider_details: dict[str, Any] | None
    admin_user_id: UUID
    reason: str
    last_checked_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class LocalPaymentResultData:
    payment_id: UUID
    user_id: UUID
    tariff_title: str
    amount_kopeks: int
    currency: str
    status: str
    failure_reason: str | None
    subscription_ends_at: datetime | None
    auth_access_delivered: bool


@dataclass(frozen=True, slots=True)
class UserPaymentHistoryItemData:
    payment_id: UUID
    created_at: datetime
    tariff_title: str
    amount_kopeks: int
    currency: str
    status: str
    failure_reason: str | None
    refund_status: Literal['processing', 'succeeded'] | None


@dataclass(frozen=True, slots=True)
class UserBillingOverviewData:
    pending_payment_id: UUID | None
    latest_subscription_ends_at: datetime | None
    latest_subscription_has_succeeded_refund: bool
    latest_subscription_has_processing_refund: bool
    latest_subscription_auth_delivered: bool
    tariff_price_kopeks: int
    payments: list[UserPaymentHistoryItemData]


@dataclass(frozen=True, slots=True)
class InternalSubscriptionFilter:
    user_id: UUID | None = None
    status: Literal['active', 'expired', 'refunded'] | None = None
    created_from: datetime | None = None
    created_to: datetime | None = None
    limit: int = 50
    offset: int = 0


@dataclass(frozen=True, slots=True)
class InternalSubscriptionItemData:
    id: UUID
    user_id: UUID
    payment_id: UUID
    tariff_code: str
    tariff_name: str
    starts_at: datetime
    ends_at: datetime
    status: Literal['active', 'expired', 'refunded']
    amount_kopeks: int
    currency: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class InternalPaymentFilter:
    status: Literal['pending', 'succeeded', 'failed'] | None = None
    has_refund: bool | None = None
    stuck: bool = False
    user_id: UUID | None = None
    payment_id: UUID | None = None
    provider_payment_id: str | None = None
    created_from: datetime | None = None
    created_to: datetime | None = None
    limit: int = 50
    offset: int = 0


@dataclass(frozen=True, slots=True)
class InternalPaymentItemData:
    id: UUID
    user_id: UUID
    tariff_code: str
    tariff_name: str
    provider_code: str
    provider_payment_id: str | None
    amount_kopeks: int
    currency: str
    status: Literal['pending', 'succeeded', 'failed']
    failure_reason: str | None
    has_refund: bool
    refund_status: Literal['processing', 'succeeded', 'rejected'] | None
    active_refund_id: UUID | None
    is_stuck: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class InternalRefundItemData:
    id: UUID
    payment_id: UUID
    subscription_id: UUID
    provider_code: str
    provider_refund_id: str | None
    amount_kopeks: int
    currency: str
    status: Literal['processing', 'succeeded', 'rejected']
    reason: str
    rejection_reason: str | None
    admin_user_id: UUID
    created_at: datetime
    updated_at: datetime
    can_reconcile: bool


@dataclass(frozen=True, slots=True)
class InternalOperationEventItemData:
    id: UUID
    operation_type: Literal['payment', 'refund']
    operation_id: UUID
    from_status: str | None
    to_status: str
    source: str
    details: dict[str, Any] | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class InternalOutboxTaskItemData:
    id: UUID
    target_service: Literal['auth', 'notifications']
    task_type: str
    access_version: int | None
    status: Literal['pending', 'completed', 'requires_intervention', 'closed']
    attempts: int
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    can_retry_or_close: bool
    payment_id: UUID | None = None
    user_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class InternalOutboxFilter:
    target_service: Literal['auth', 'notifications'] | None = None
    status: Literal['pending', 'requires_intervention'] | None = None
    user_id: UUID | None = None
    payment_id: UUID | None = None
    created_from: datetime | None = None
    created_to: datetime | None = None
    limit: int = 50
    offset: int = 0


@dataclass(frozen=True, slots=True)
class InternalPaymentCardData:
    id: UUID
    user_id: UUID
    tariff_code: str
    tariff_name: str
    provider_code: str
    provider_payment_id: str | None
    amount_kopeks: int
    currency: str
    status: Literal['pending', 'succeeded', 'failed']
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime
    can_refund: bool
    can_reconcile_payment: bool
    subscription: InternalSubscriptionItemData | None
    refunds: list[InternalRefundItemData]
    operation_events: list[InternalOperationEventItemData]
    outbox_tasks: list[InternalOutboxTaskItemData]


class OutboxTaskRecordProtocol(Protocol):
    id: UUID
    dedup_key: str
    target_service: str
    task_type: str
    payment_id: UUID
    user_id: UUID
    access_version: int | None
    payload: dict[str, Any]
    status: str
    attempts: int
    last_error: str | None
    next_retry_at: datetime


DeliveryOutcome = Literal['completed', 'transient_error', 'permanent_error']
OutboxDeliverFn = Callable[
    [OutboxTaskRecordProtocol],
    Awaitable[tuple[DeliveryOutcome, str | None]],
]


def compute_outbox_backoff_seconds(attempts: int) -> int:
    """Вычисляет паузу экспоненциального backoff (2^attempt с потолком 300 с)."""
    normalized = max(1, attempts)
    return int(min(300, 2 ** min(normalized, 9)))


class PurchaseRepository(Protocol):
    async def check_purchase_allowed(
        self,
        user_id: UUID,
    ) -> tuple[bool, str | None]: ...

    async def get_payment(
        self,
        payment_id: UUID,
    ) -> PaymentRecordProtocol | None: ...

    async def get_payment_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> PaymentRecordProtocol | None: ...

    async def get_local_payment_result_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> LocalPaymentResultData | None: ...

    async def get_user_billing_overview(
        self,
        user_id: UUID,
    ) -> UserBillingOverviewData: ...

    async def list_internal_subscriptions(
        self,
        filters: InternalSubscriptionFilter,
        *,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalSubscriptionItemData]]: ...

    async def list_internal_payments(
        self,
        filters: InternalPaymentFilter,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalPaymentItemData]]: ...

    async def get_internal_payment_card(
        self,
        payment_id: UUID,
        *,
        now_utc: datetime | None = None,
    ) -> InternalPaymentCardData | None: ...

    async def list_internal_outbox_tasks(
        self,
        filters: InternalOutboxFilter,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalOutboxTaskItemData]]: ...

    async def get_internal_outbox_task(
        self,
        task_id: UUID,
    ) -> InternalOutboxTaskItemData | None: ...

    async def retry_outbox_task_by_admin(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> InternalOutboxTaskItemData: ...

    async def close_outbox_task_by_admin(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> InternalOutboxTaskItemData: ...

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

    async def get_or_create_pending_payment(
        self,
        user_id: UUID,
    ) -> tuple[PaymentRecordProtocol, bool]: ...

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

    async def get_refund(
        self,
        refund_id: UUID,
    ) -> RefundRecordProtocol | None: ...

    async def get_refund_by_provider_id(
        self,
        provider_code: str,
        provider_refund_id: str,
    ) -> RefundRecordProtocol | None: ...

    async def claim_next_due_processing_refund(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
    ) -> RefundRecordProtocol | None: ...

    async def get_or_create_processing_refund(
        self,
        payment_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> tuple[RefundRecordProtocol, bool]: ...

    async def save_provider_refund_created(
        self,
        refund_id: UUID,
        *,
        provider_refund_id: str,
        provider_details: dict[str, Any],
    ) -> RefundRecordProtocol: ...

    async def record_refund_checked(
        self,
        refund_id: UUID,
        *,
        provider_details: dict[str, Any] | None = None,
    ) -> RefundRecordProtocol: ...

    async def record_refund_reconcile_error(
        self,
        refund_id: UUID,
        *,
        error_message: str,
    ) -> RefundRecordProtocol: ...

    async def finalize_refund_succeeded(
        self,
        refund_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[RefundRecordProtocol, bool]: ...

    async def finalize_refund_rejected(
        self,
        refund_id: UUID,
        *,
        rejection_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
    ) -> tuple[RefundRecordProtocol, bool]: ...

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

    async def process_due_auth_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        payment_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int: ...


class SQLAlchemyPurchaseRepository:
    """SQLAlchemy-репозиторий создания и атомарной фиксации платежей (T-10, T-11)."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    @staticmethod
    async def _acquire_user_lock(session: AsyncSession, user_id: UUID) -> None:
        await session.execute(
            text('SELECT pg_advisory_xact_lock(hashtext(:uid))'),
            {'uid': str(user_id)},
        )

    @classmethod
    async def _lock_user_and_payment(
        cls,
        session: AsyncSession,
        payment_id: UUID,
    ) -> Payment:
        """Захватывает user advisory lock и FOR UPDATE на платеже в одном порядке."""
        user_id = await session.scalar(
            select(Payment.user_id).where(Payment.id == payment_id)
        )
        if user_id is None:
            raise LookupError(f'Payment {payment_id} not found')
        await cls._acquire_user_lock(session, user_id)
        payment = await session.get(
            Payment,
            payment_id,
            with_for_update=True,
        )
        if payment is None:
            raise LookupError(f'Payment {payment_id} not found')
        return payment

    @staticmethod
    async def _increment_access_version(session: AsyncSession, user_id: UUID) -> int:
        now_utc = datetime.now(UTC)
        stmt = (
            pg_insert(UserAccessVersion)
            .values(user_id=user_id, last_version=1, updated_at=now_utc)
            .on_conflict_do_update(
                index_elements=[UserAccessVersion.user_id],
                set_={
                    'last_version': UserAccessVersion.last_version + 1,
                    'updated_at': now_utc,
                },
            )
            .returning(UserAccessVersion.last_version)
        )
        version = await session.scalar(stmt)
        if version is None:
            raise RuntimeError(f'Failed to increment access_version for user {user_id}')
        return int(version)

    @staticmethod
    async def _check_allowed_in_session(
        session: AsyncSession,
        user_id: UUID,
    ) -> tuple[bool, str | None]:
        now_utc = datetime.now(UTC)
        succeeded_refund_exists = (
            select(Refund.id)
            .where(
                Refund.subscription_id == Subscription.id,
                Refund.status == 'succeeded',
            )
            .exists()
        )
        active_sub_stmt = select(
            exists().where(
                Subscription.user_id == user_id,
                Subscription.ends_at > now_utc,
                ~succeeded_refund_exists,
            )
        )
        if await session.scalar(active_sub_stmt):
            return False, 'subscription_already_active'

        processing_refund_stmt = select(
            exists().where(
                Refund.payment_id == Payment.id,
                Payment.user_id == user_id,
                Refund.status == 'processing',
            )
        )
        if await session.scalar(processing_refund_stmt):
            return False, 'refund_in_processing'

        return True, None

    async def check_purchase_allowed(self, user_id: UUID) -> tuple[bool, str | None]:
        """Проверяет отсутствие активной подписки и возврата в обработке (BIL-FR-03)."""
        try:
            async with self._session_factory() as session:
                return await self._check_allowed_in_session(session, user_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_payment(self, payment_id: UUID) -> Payment | None:
        """Возвращает платёж по его внутреннему UUID."""
        try:
            async with self._session_factory() as session:
                return await session.get(Payment, payment_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_payment_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> Payment | None:
        """Возвращает платёж пользователя по UUID или None (BIL-NFR-SEC-03)."""
        try:
            async with self._session_factory() as session:
                return await session.scalar(
                    select(Payment).where(
                        Payment.id == payment_id,
                        Payment.user_id == user_id,
                    )
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_local_payment_result_for_user(
        self,
        payment_id: UUID,
        user_id: UUID,
    ) -> LocalPaymentResultData | None:
        """Читает локальный статус платежа и подписки одним запросом (BIL-FR-04)."""
        try:
            async with self._session_factory() as session:
                stmt = (
                    select(
                        Payment.id,
                        Payment.user_id,
                        Payment.amount_kopeks,
                        Payment.currency,
                        Payment.status,
                        Payment.failure_reason,
                        Tariff.title,
                        Subscription.ends_at,
                        OutboxTask.status,
                    )
                    .join(Tariff, Tariff.id == Payment.tariff_id)
                    .outerjoin(
                        Subscription,
                        Subscription.payment_id == Payment.id,
                    )
                    .outerjoin(
                        OutboxTask,
                        and_(
                            OutboxTask.payment_id == Payment.id,
                            OutboxTask.target_service == 'auth',
                            OutboxTask.task_type == 'sync_access',
                        ),
                    )
                    .where(
                        Payment.id == payment_id,
                        Payment.user_id == user_id,
                    )
                )
                row = (await session.execute(stmt)).one_or_none()
                if row is None:
                    return None

                (
                    row_payment_id,
                    row_user_id,
                    amount_kopeks,
                    currency,
                    status,
                    failure_reason,
                    tariff_title,
                    sub_ends_at,
                    auth_task_status,
                ) = row

                return LocalPaymentResultData(
                    payment_id=row_payment_id,
                    user_id=row_user_id,
                    tariff_title=tariff_title,
                    amount_kopeks=amount_kopeks,
                    currency=currency.strip(),
                    status=status,
                    failure_reason=failure_reason,
                    subscription_ends_at=sub_ends_at,
                    auth_access_delivered=auth_task_status == 'completed',
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_user_billing_overview(
        self,
        user_id: UUID,
    ) -> UserBillingOverviewData:
        """Читает сводные данные для экрана Э2 «Моя подписка» (T-23, BIL-FR-09)."""
        try:
            async with self._session_factory() as session:
                tariff = await session.scalar(
                    select(Tariff)
                    .where(
                        Tariff.code == DEFAULT_TARIFF_CODE,
                        Tariff.is_active.is_(True),
                    )
                    .limit(1)
                )
                tariff_price_kopeks = (
                    tariff.price_kopeks
                    if tariff is not None
                    else DEFAULT_TARIFF_PRICE_KOPEKS
                )

                latest_sub_stmt = (
                    select(Subscription.payment_id, Subscription.ends_at)
                    .where(Subscription.user_id == user_id)
                    .order_by(
                        Subscription.ends_at.desc(), Subscription.created_at.desc()
                    )
                    .limit(1)
                )
                latest_sub_row = (await session.execute(latest_sub_stmt)).one_or_none()

                latest_ends_at: datetime | None = None
                has_succeeded_refund = False
                has_processing_refund = False
                auth_delivered = False

                if latest_sub_row is not None:
                    latest_sub_payment_id, latest_ends_at = latest_sub_row
                    auth_task_status = await session.scalar(
                        select(OutboxTask.status)
                        .where(
                            OutboxTask.payment_id == latest_sub_payment_id,
                            OutboxTask.target_service == 'auth',
                            OutboxTask.task_type == 'sync_access',
                        )
                        .limit(1)
                    )
                    auth_delivered = auth_task_status == 'completed'

                    active_refund_status = await session.scalar(
                        select(Refund.status)
                        .where(
                            Refund.payment_id == latest_sub_payment_id,
                            Refund.status.in_(('processing', 'succeeded')),
                        )
                        .limit(1)
                    )
                    has_succeeded_refund = active_refund_status == 'succeeded'
                    has_processing_refund = active_refund_status == 'processing'

                payments_stmt = (
                    select(
                        Payment.id,
                        Payment.created_at,
                        Tariff.title,
                        Payment.amount_kopeks,
                        Payment.currency,
                        Payment.status,
                        Payment.failure_reason,
                        Refund.status,
                    )
                    .join(Tariff, Tariff.id == Payment.tariff_id)
                    .outerjoin(
                        Refund,
                        and_(
                            Refund.payment_id == Payment.id,
                            Refund.status.in_(('processing', 'succeeded')),
                        ),
                    )
                    .where(Payment.user_id == user_id)
                    .order_by(Payment.created_at.desc(), Payment.id.desc())
                )
                payment_rows = (await session.execute(payments_stmt)).all()

                pending_payment_id: UUID | None = None
                history_items: list[UserPaymentHistoryItemData] = []
                for (
                    pid,
                    created_at,
                    t_title,
                    amount_kopeks,
                    currency,
                    p_status,
                    failure_reason,
                    r_status,
                ) in payment_rows:
                    if p_status == 'pending' and pending_payment_id is None:
                        pending_payment_id = pid
                    normalized_refund: Literal['processing', 'succeeded'] | None = (
                        'succeeded'
                        if r_status == 'succeeded'
                        else 'processing'
                        if r_status == 'processing'
                        else None
                    )
                    history_items.append(
                        UserPaymentHistoryItemData(
                            payment_id=pid,
                            created_at=created_at,
                            tariff_title=t_title,
                            amount_kopeks=amount_kopeks,
                            currency=currency.strip(),
                            status=p_status,
                            failure_reason=failure_reason,
                            refund_status=normalized_refund,
                        )
                    )

                return UserBillingOverviewData(
                    pending_payment_id=pending_payment_id,
                    latest_subscription_ends_at=latest_ends_at,
                    latest_subscription_has_succeeded_refund=has_succeeded_refund,
                    latest_subscription_has_processing_refund=has_processing_refund,
                    latest_subscription_auth_delivered=auth_delivered,
                    tariff_price_kopeks=tariff_price_kopeks,
                    payments=history_items,
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def list_internal_subscriptions(
        self,
        filters: InternalSubscriptionFilter,
        *,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalSubscriptionItemData]]:
        now = now_utc or datetime.now(UTC)
        succeeded_refund_exists = exists(
            select(1).where(
                Refund.subscription_id == Subscription.id,
                Refund.status == 'succeeded',
            )
        )
        conditions: list[Any] = []
        if filters.user_id is not None:
            conditions.append(Subscription.user_id == filters.user_id)
        if filters.created_from is not None:
            conditions.append(Subscription.created_at >= filters.created_from)
        if filters.created_to is not None:
            conditions.append(Subscription.created_at <= filters.created_to)
        if filters.status == 'refunded':
            conditions.append(succeeded_refund_exists)
        elif filters.status == 'active':
            conditions.append(~succeeded_refund_exists)
            conditions.append(Subscription.ends_at > now)
        elif filters.status == 'expired':
            conditions.append(~succeeded_refund_exists)
            conditions.append(Subscription.ends_at <= now)

        try:
            async with self._session_factory() as session:
                count_stmt = select(func.count(Subscription.id))
                if conditions:
                    count_stmt = count_stmt.where(*conditions)
                total = int(await session.scalar(count_stmt) or 0)

                stmt = (
                    select(
                        Subscription.id,
                        Subscription.user_id,
                        Subscription.payment_id,
                        Tariff.code,
                        Tariff.title,
                        Subscription.starts_at,
                        Subscription.ends_at,
                        Payment.amount_kopeks,
                        Payment.currency,
                        Subscription.created_at,
                        succeeded_refund_exists.label('has_succeeded_refund'),
                    )
                    .join(Payment, Payment.id == Subscription.payment_id)
                    .join(Tariff, Tariff.id == Subscription.tariff_id)
                )
                if conditions:
                    stmt = stmt.where(*conditions)
                stmt = (
                    stmt.order_by(
                        Subscription.created_at.desc(),
                        Subscription.id.desc(),
                    )
                    .limit(filters.limit)
                    .offset(filters.offset)
                )
                rows = (await session.execute(stmt)).all()
                items: list[InternalSubscriptionItemData] = []
                for (
                    sub_id,
                    uid,
                    pay_id,
                    t_code,
                    t_title,
                    starts_at,
                    ends_at,
                    amount_kopeks,
                    currency,
                    created_at,
                    has_succeeded_refund,
                ) in rows:
                    state_enum = compute_subscription_state(
                        ends_at=ends_at,
                        has_succeeded_refund=bool(has_succeeded_refund),
                        now_utc=now,
                    )
                    computed_status: Literal['active', 'expired', 'refunded'] = (
                        'refunded'
                        if state_enum == 'refunded'
                        else 'active'
                        if state_enum == 'active'
                        else 'expired'
                    )
                    items.append(
                        InternalSubscriptionItemData(
                            id=sub_id,
                            user_id=uid,
                            payment_id=pay_id,
                            tariff_code=t_code,
                            tariff_name=t_title,
                            starts_at=starts_at,
                            ends_at=ends_at,
                            status=computed_status,
                            amount_kopeks=amount_kopeks,
                            currency=currency.strip(),
                            created_at=created_at,
                        )
                    )
                return total, items
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def list_internal_payments(
        self,
        filters: InternalPaymentFilter,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalPaymentItemData]]:
        now = now_utc or datetime.now(UTC)
        stuck_cutoff = now - timedelta(seconds=stuck_threshold_seconds)
        any_refund_exists = exists(select(1).where(Refund.payment_id == Payment.id))
        stuck_refund_exists = exists(
            select(1).where(
                Refund.payment_id == Payment.id,
                Refund.status == 'processing',
                Refund.created_at <= stuck_cutoff,
            )
        )
        stuck_condition = or_(
            and_(Payment.status == 'pending', Payment.created_at <= stuck_cutoff),
            stuck_refund_exists,
        )

        conditions: list[Any] = []
        if filters.status is not None:
            conditions.append(Payment.status == filters.status)
        if filters.has_refund is True:
            conditions.append(any_refund_exists)
        elif filters.has_refund is False:
            conditions.append(~any_refund_exists)
        if filters.stuck:
            conditions.append(stuck_condition)
        if filters.user_id is not None:
            conditions.append(Payment.user_id == filters.user_id)
        if filters.payment_id is not None:
            conditions.append(Payment.id == filters.payment_id)
        if filters.provider_payment_id is not None:
            conditions.append(
                Payment.provider_payment_id == filters.provider_payment_id
            )
        if filters.created_from is not None:
            conditions.append(Payment.created_at >= filters.created_from)
        if filters.created_to is not None:
            conditions.append(Payment.created_at <= filters.created_to)

        try:
            async with self._session_factory() as session:
                count_stmt = select(func.count(Payment.id))
                if conditions:
                    count_stmt = count_stmt.where(*conditions)
                total = int(await session.scalar(count_stmt) or 0)

                stmt = select(
                    Payment.id,
                    Payment.user_id,
                    Tariff.code,
                    Tariff.title,
                    Payment.provider_code,
                    Payment.provider_payment_id,
                    Payment.amount_kopeks,
                    Payment.currency,
                    Payment.status,
                    Payment.failure_reason,
                    Payment.created_at,
                    Payment.updated_at,
                ).join(Tariff, Tariff.id == Payment.tariff_id)
                if conditions:
                    stmt = stmt.where(*conditions)
                stmt = (
                    stmt.order_by(Payment.created_at.desc(), Payment.id.desc())
                    .limit(filters.limit)
                    .offset(filters.offset)
                )
                rows = (await session.execute(stmt)).all()
                payment_ids = [row[0] for row in rows]
                refunds_by_payment: dict[UUID, list[Refund]] = {}
                if payment_ids:
                    refund_rows = list(
                        await session.scalars(
                            select(Refund)
                            .where(Refund.payment_id.in_(payment_ids))
                            .order_by(Refund.created_at.desc(), Refund.id.desc())
                        )
                    )
                    for ref in refund_rows:
                        refunds_by_payment.setdefault(ref.payment_id, []).append(ref)

                items: list[InternalPaymentItemData] = []
                for (
                    pid,
                    uid,
                    t_code,
                    t_title,
                    prov_code,
                    prov_pid,
                    amount_kopeks,
                    currency,
                    p_status,
                    failure_reason,
                    created_at,
                    updated_at,
                ) in rows:
                    p_refunds = refunds_by_payment.get(pid, [])
                    has_ref = len(p_refunds) > 0
                    active_or_latest = next(
                        (
                            r
                            for r in p_refunds
                            if r.status in ('processing', 'succeeded')
                        ),
                        p_refunds[0] if p_refunds else None,
                    )
                    ref_status: (
                        Literal['processing', 'succeeded', 'rejected'] | None
                    ) = None
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

                    is_payment_stuck = (
                        p_status == 'pending' and created_at <= stuck_cutoff
                    ) or any(
                        r.status == 'processing' and r.created_at <= stuck_cutoff
                        for r in p_refunds
                    )
                    normalized_status: Literal['pending', 'succeeded', 'failed'] = (
                        'succeeded'
                        if p_status == 'succeeded'
                        else 'failed'
                        if p_status == 'failed'
                        else 'pending'
                    )
                    items.append(
                        InternalPaymentItemData(
                            id=pid,
                            user_id=uid,
                            tariff_code=t_code,
                            tariff_name=t_title,
                            provider_code=prov_code,
                            provider_payment_id=prov_pid,
                            amount_kopeks=amount_kopeks,
                            currency=currency.strip(),
                            status=normalized_status,
                            failure_reason=failure_reason,
                            has_refund=has_ref,
                            refund_status=ref_status,
                            active_refund_id=active_ref_id,
                            is_stuck=is_payment_stuck,
                            created_at=created_at,
                            updated_at=updated_at,
                        )
                    )
                return total, items
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_internal_payment_card(
        self,
        payment_id: UUID,
        *,
        now_utc: datetime | None = None,
    ) -> InternalPaymentCardData | None:
        now = now_utc or datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                pay_row = (
                    await session.execute(
                        select(Payment, Tariff.code, Tariff.title)
                        .join(Tariff, Tariff.id == Payment.tariff_id)
                        .where(Payment.id == payment_id)
                    )
                ).one_or_none()
                if pay_row is None:
                    return None
                payment, t_code, t_title = pay_row

                sub_row = await session.scalar(
                    select(Subscription).where(Subscription.payment_id == payment_id)
                )
                refund_rows = list(
                    await session.scalars(
                        select(Refund)
                        .where(Refund.payment_id == payment_id)
                        .order_by(Refund.created_at.desc(), Refund.id.desc())
                    )
                )
                has_succeeded_refund = any(r.status == 'succeeded' for r in refund_rows)
                has_active_or_succeeded_refund = any(
                    r.status in ('processing', 'succeeded') for r in refund_rows
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
                        tariff_code=t_code,
                        tariff_name=t_title,
                        starts_at=sub_row.starts_at,
                        ends_at=sub_row.ends_at,
                        status=sub_status,
                        amount_kopeks=payment.amount_kopeks,
                        currency=payment.currency.strip(),
                        created_at=sub_row.created_at,
                    )

                refund_ids = [r.id for r in refund_rows]
                event_cond: Any = and_(
                    OperationEvent.operation_type == 'payment',
                    OperationEvent.operation_id == payment_id,
                )
                if refund_ids:
                    event_cond = or_(
                        event_cond,
                        and_(
                            OperationEvent.operation_type == 'refund',
                            OperationEvent.operation_id.in_(refund_ids),
                        ),
                    )
                event_rows = list(
                    await session.scalars(
                        select(OperationEvent)
                        .where(event_cond)
                        .order_by(
                            OperationEvent.created_at.asc(),
                            OperationEvent.id.asc(),
                        )
                    )
                )
                task_rows = list(
                    await session.scalars(
                        select(OutboxTask)
                        .where(OutboxTask.payment_id == payment_id)
                        .order_by(OutboxTask.created_at.asc(), OutboxTask.id.asc())
                    )
                )

                can_refund = (
                    payment.status == 'succeeded'
                    and sub_data is not None
                    and sub_data.status == 'active'
                    and not has_active_or_succeeded_refund
                )
                normalized_p_status: Literal['pending', 'succeeded', 'failed'] = (
                    'succeeded'
                    if payment.status == 'succeeded'
                    else 'failed'
                    if payment.status == 'failed'
                    else 'pending'
                )
                refund_items: list[InternalRefundItemData] = []
                for r in refund_rows:
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
                            updated_at=r.updated_at,
                            can_reconcile=(r_status == 'processing'),
                        )
                    )

                event_items: list[InternalOperationEventItemData] = [
                    InternalOperationEventItemData(
                        id=ev.id,
                        operation_type=(
                            'refund' if ev.operation_type == 'refund' else 'payment'
                        ),
                        operation_id=ev.operation_id,
                        from_status=ev.from_status,
                        to_status=ev.to_status,
                        source=ev.source,
                        details=ev.details,
                        created_at=ev.created_at,
                    )
                    for ev in event_rows
                ]

                task_items: list[InternalOutboxTaskItemData] = []
                for t in task_rows:
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

                return InternalPaymentCardData(
                    id=payment.id,
                    user_id=payment.user_id,
                    tariff_code=t_code,
                    tariff_name=t_title,
                    provider_code=payment.provider_code,
                    provider_payment_id=payment.provider_payment_id,
                    amount_kopeks=payment.amount_kopeks,
                    currency=payment.currency.strip(),
                    status=normalized_p_status,
                    failure_reason=payment.failure_reason,
                    created_at=payment.created_at,
                    updated_at=payment.updated_at,
                    can_refund=can_refund,
                    can_reconcile_payment=(normalized_p_status == 'pending'),
                    subscription=sub_data,
                    refunds=refund_items,
                    operation_events=event_items,
                    outbox_tasks=task_items,
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def list_internal_outbox_tasks(
        self,
        filters: InternalOutboxFilter,
        *,
        stuck_threshold_seconds: int = 600,
        now_utc: datetime | None = None,
    ) -> tuple[int, list[InternalOutboxTaskItemData]]:
        now = now_utc or datetime.now(UTC)
        stuck_cutoff = now - timedelta(seconds=stuck_threshold_seconds)

        conditions: list[Any] = []
        if filters.status == 'requires_intervention':
            conditions.append(OutboxTask.status == 'requires_intervention')
        elif filters.status == 'pending':
            conditions.append(
                and_(
                    OutboxTask.status == 'pending',
                    OutboxTask.created_at <= stuck_cutoff,
                )
            )
        else:
            conditions.append(
                or_(
                    OutboxTask.status == 'requires_intervention',
                    and_(
                        OutboxTask.status == 'pending',
                        OutboxTask.created_at <= stuck_cutoff,
                    ),
                )
            )
        if filters.target_service is not None:
            conditions.append(OutboxTask.target_service == filters.target_service)
        if filters.user_id is not None:
            conditions.append(OutboxTask.user_id == filters.user_id)
        if filters.payment_id is not None:
            conditions.append(OutboxTask.payment_id == filters.payment_id)
        if filters.created_from is not None:
            conditions.append(OutboxTask.created_at >= filters.created_from)
        if filters.created_to is not None:
            conditions.append(OutboxTask.created_at <= filters.created_to)

        try:
            async with self._session_factory() as session:
                count_stmt = select(func.count(OutboxTask.id)).where(*conditions)
                total = int(await session.scalar(count_stmt) or 0)

                priority_order = case(
                    (OutboxTask.status == 'requires_intervention', 0),
                    else_=1,
                )
                stmt = (
                    select(OutboxTask)
                    .where(*conditions)
                    .order_by(
                        priority_order.asc(),
                        OutboxTask.created_at.desc(),
                        OutboxTask.id.desc(),
                    )
                    .limit(filters.limit)
                    .offset(filters.offset)
                )
                rows = list(await session.scalars(stmt))
                items: list[InternalOutboxTaskItemData] = []
                for t in rows:
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
                return total, items
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    @staticmethod
    def _to_internal_outbox_item(t: OutboxTask) -> InternalOutboxTaskItemData:
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
        try:
            async with self._session_factory() as session:
                task = await session.get(OutboxTask, task_id)
                if task is None:
                    return None
                return self._to_internal_outbox_item(task)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def retry_outbox_task_by_admin(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> InternalOutboxTaskItemData:
        now = now_utc or datetime.now(UTC)
        cleaned_reason = (
            reason.strip()
            if reason.strip()
            else 'Ручной повтор отправки администратором'
        )
        try:
            async with self._session_factory() as session:
                task = await session.get(OutboxTask, task_id, with_for_update=True)
                if task is None:
                    raise BillingDomainError(
                        'Outbox task not found',
                        error_code='outbox_task_not_found',
                        status_code=404,
                    )
                if task.status in ('completed', 'closed'):
                    raise BillingDomainError(
                        'Задача уже завершена или закрыта',
                        error_code='outbox_task_not_retryable',
                        status_code=409,
                    )
                task.status = 'pending'
                task.attempts = 0
                task.next_retry_at = now
                task.updated_at = now
                session.add(
                    AdminAction(
                        admin_user_id=admin_user_id,
                        action_type='outbox_retried',
                        target_type='outbox_task',
                        target_id=task.id,
                        reason=cleaned_reason,
                    )
                )
                await session.commit()
                return self._to_internal_outbox_item(task)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def close_outbox_task_by_admin(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> InternalOutboxTaskItemData:
        now = now_utc or datetime.now(UTC)
        cleaned_reason = reason.strip()
        try:
            async with self._session_factory() as session:
                task = await session.get(OutboxTask, task_id, with_for_update=True)
                if task is None:
                    raise BillingDomainError(
                        'Outbox task not found',
                        error_code='outbox_task_not_found',
                        status_code=404,
                    )
                if task.status != 'requires_intervention':
                    raise BillingDomainError(
                        'Закрытие доступно только для задачи в состоянии '
                        'requires_intervention',
                        error_code='outbox_task_not_requires_intervention',
                        status_code=409,
                    )
                task.status = 'closed'
                task.updated_at = now
                session.add(
                    AdminAction(
                        admin_user_id=admin_user_id,
                        action_type='outbox_closed',
                        target_type='outbox_task',
                        target_id=task.id,
                        reason=cleaned_reason,
                    )
                )
                await session.commit()
                return self._to_internal_outbox_item(task)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_payment_by_provider_id(
        self,
        provider_code: str,
        provider_payment_id: str,
    ) -> Payment | None:
        """Возвращает платёж по коду провайдера и внешнему идентификатору."""
        try:
            async with self._session_factory() as session:
                return await session.scalar(
                    select(Payment).where(
                        Payment.provider_code == provider_code,
                        Payment.provider_payment_id == provider_payment_id,
                    )
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def list_due_pending_payments(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
        limit: int = 20,
    ) -> list[Payment]:
        """Возвращает pending-платежи старше min_age_seconds (BIL-FR-05)."""
        now_utc = datetime.now(UTC)
        created_cutoff = now_utc - timedelta(seconds=min_age_seconds)
        checked_cutoff = now_utc - timedelta(seconds=throttle_seconds)
        try:
            async with self._session_factory() as session:
                stmt = (
                    select(Payment)
                    .where(
                        Payment.status == 'pending',
                        Payment.created_at <= created_cutoff,
                        or_(
                            Payment.last_checked_at.is_(None),
                            Payment.last_checked_at <= checked_cutoff,
                        ),
                    )
                    .order_by(
                        Payment.last_checked_at.asc().nulls_first(),
                        Payment.created_at.asc(),
                    )
                    .limit(limit)
                )
                return list((await session.scalars(stmt)).all())
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def claim_next_due_pending_payment(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
    ) -> Payment | None:
        """Атомарно захватывает pending-платёж через SKIP LOCKED (BIL-NFR-S-01)."""
        now_utc = datetime.now(UTC)
        created_cutoff = now_utc - timedelta(seconds=min_age_seconds)
        checked_cutoff = now_utc - timedelta(seconds=throttle_seconds)
        try:
            async with self._session_factory() as session:
                stmt = (
                    select(Payment)
                    .where(
                        Payment.status == 'pending',
                        Payment.created_at <= created_cutoff,
                        or_(
                            Payment.last_checked_at.is_(None),
                            Payment.last_checked_at <= checked_cutoff,
                        ),
                    )
                    .order_by(
                        Payment.last_checked_at.asc().nulls_first(),
                        Payment.created_at.asc(),
                    )
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                payment = await session.scalar(stmt)
                if payment is None:
                    return None
                payment.last_checked_at = now_utc
                payment.updated_at = now_utc
                await session.commit()
                return payment
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def record_payment_reconcile_error(
        self,
        payment_id: UUID,
        *,
        error_message: str,
    ) -> Payment:
        """Фиксирует ошибку фоновой сверки с backoff (US-S-01, BIL-NFR-R-05)."""
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                payment = await self._lock_user_and_payment(session, payment_id)
                if payment.status == 'pending':
                    details = dict(payment.provider_details or {})
                    prev_errors = details.get('_reconcile_errors', 0)
                    err_count = (
                        int(prev_errors) + 1
                        if isinstance(prev_errors, (int, str))
                        else 1
                    )
                    backoff_sec = min(300, 5 * (2 ** min(err_count - 1, 6)))
                    next_eligible_at = now_utc + timedelta(
                        seconds=max(0, backoff_sec - 5)
                    )
                    details['_reconcile_errors'] = err_count
                    details['_last_reconcile_error'] = error_message[:250]
                    payment.provider_details = details
                    payment.last_checked_at = next_eligible_at
                    payment.updated_at = now_utc
                    await session.commit()
                return payment
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_or_create_pending_payment(
        self,
        user_id: UUID,
    ) -> tuple[Payment, bool]:
        """Возвращает существующий pending-платёж или создаёт новый (инвариант 1)."""
        try:
            async with self._session_factory() as session:
                await self._acquire_user_lock(session, user_id)
                allowed, block_code = await self._check_allowed_in_session(
                    session,
                    user_id,
                )
                if not allowed:
                    if block_code == 'subscription_already_active':
                        raise BillingDomainError(
                            'Подписка уже активна',
                            error_code='subscription_already_active',
                            status_code=409,
                        )
                    raise BillingDomainError(
                        'Нельзя оформить новую подписку, '
                        'пока возврат находится в обработке',
                        error_code='refund_in_processing',
                        status_code=409,
                    )

                existing = await session.scalar(
                    select(Payment).where(
                        Payment.user_id == user_id,
                        Payment.status == 'pending',
                    )
                )
                if existing is not None:
                    return existing, False

                tariff = await session.scalar(
                    select(Tariff).where(
                        Tariff.code == DEFAULT_TARIFF_CODE,
                        Tariff.is_active.is_(True),
                    )
                )
                if tariff is None:
                    raise RuntimeError(
                        'Active default tariff not found in tariffs table'
                    )

                provider = await session.scalar(
                    select(PaymentProvider).where(
                        PaymentProvider.is_active.is_(True),
                    )
                )
                provider_code = (
                    provider.code if provider is not None else DEFAULT_PROVIDER_CODE
                )

                payment_id = uuid4()
                payment = Payment(
                    id=payment_id,
                    user_id=user_id,
                    tariff_id=tariff.id,
                    provider_code=provider_code,
                    amount_kopeks=tariff.price_kopeks,
                    currency=tariff.currency.strip(),
                    status='pending',
                    idempotency_key=payment_id,
                )
                event = OperationEvent(
                    operation_type='payment',
                    operation_id=payment_id,
                    from_status=None,
                    to_status='pending',
                    source='continue',
                    details={'created': True},
                )
                session.add(payment)
                session.add(event)
                try:
                    await session.commit()
                    return payment, True
                except IntegrityError:
                    await session.rollback()

                concurrent = await session.scalar(
                    select(Payment).where(
                        Payment.user_id == user_id,
                        Payment.status == 'pending',
                    )
                )
                if concurrent is None:
                    raise RuntimeError(
                        'Failed to load concurrent pending payment after IntegrityError'
                    )
                return concurrent, False
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def save_provider_payment_created(
        self,
        payment_id: UUID,
        *,
        provider_payment_id: str,
        confirmation_url: str | None,
        provider_details: dict[str, Any],
    ) -> Payment:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                payment = await self._lock_user_and_payment(session, payment_id)
                if payment.status == 'pending':
                    payment.provider_payment_id = provider_payment_id
                    if confirmation_url is not None:
                        payment.confirmation_url = confirmation_url
                    payment.provider_details = provider_details
                    payment.updated_at = now_utc
                    await session.commit()
                return payment
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def record_payment_checked(
        self,
        payment_id: UUID,
        *,
        confirmation_url: str | None = None,
        provider_details: dict[str, Any] | None = None,
    ) -> Payment:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                payment = await self._lock_user_and_payment(session, payment_id)
                if payment.status == 'pending':
                    if confirmation_url:
                        payment.confirmation_url = confirmation_url
                    if provider_details is not None:
                        payment.provider_details = provider_details
                    payment.last_checked_at = now_utc
                    payment.updated_at = now_utc
                    await session.commit()
                return payment
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def finalize_payment_succeeded(
        self,
        payment_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[Payment, bool]:
        """Атомарно фиксирует оплату (BIL-FR-08, BIL-FR-11, BIL-FR-13, BIL-NFR-R-03)."""
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                payment = await self._lock_user_and_payment(session, payment_id)
                if payment.status != 'pending':
                    return payment, False

                tariff = await session.get(Tariff, payment.tariff_id)
                duration_days = tariff.duration_days if tariff is not None else 30

                old_status = payment.status
                payment.status = 'succeeded'
                if provider_details is not None:
                    payment.provider_details = provider_details
                payment.last_checked_at = now_utc
                payment.updated_at = now_utc

                starts_at = now_utc
                ends_at = starts_at + timedelta(days=duration_days)
                sub_id = uuid4()
                subscription = Subscription(
                    id=sub_id,
                    user_id=payment.user_id,
                    tariff_id=payment.tariff_id,
                    payment_id=payment.id,
                    starts_at=starts_at,
                    ends_at=ends_at,
                )
                session.add(subscription)

                access_version = await self._increment_access_version(
                    session,
                    payment.user_id,
                )

                session.add(
                    OperationEvent(
                        operation_type='payment',
                        operation_id=payment.id,
                        from_status=old_status,
                        to_status='succeeded',
                        source=source,
                        details=provider_details or {'status': 'succeeded'},
                    )
                )

                session.add_all(
                    [
                        OutboxTask(
                            dedup_key=f'payment_succeeded:{payment.id}:auth_access',
                            target_service='auth',
                            task_type='sync_access',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=access_version,
                            payload={
                                'user_id': str(payment.user_id),
                                'version': access_version,
                                'expires_at': ends_at.isoformat(),
                            },
                            next_retry_at=now_utc,
                        ),
                        OutboxTask(
                            dedup_key=f'payment_succeeded:{payment.id}:notify_paid',
                            target_service='notifications',
                            task_type='payment_succeeded_email',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=None,
                            payload=build_payment_succeeded_email_payload(
                                payment_id=payment.id,
                                subscription_id=sub_id,
                                user_id=payment.user_id,
                                ends_at=ends_at,
                                public_base_url=public_base_url,
                            ),
                            next_retry_at=now_utc,
                        ),
                        OutboxTask(
                            dedup_key=f'subscription_expiration:{sub_id}:notify_expiring',
                            target_service='notifications',
                            task_type='subscription_expiration_email',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=None,
                            payload=build_subscription_expiration_email_payload(
                                subscription_id=sub_id,
                                user_id=payment.user_id,
                                ends_at=ends_at,
                                public_base_url=public_base_url,
                            ),
                            next_retry_at=now_utc,
                        ),
                    ]
                )
                await session.commit()
                return payment, True
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def finalize_payment_failed(
        self,
        payment_id: UUID,
        *,
        failure_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[Payment, bool]:
        """Атомарно переводит платёж в failed и ставит письмо (BIL-FR-06, BIL-FR-11)."""
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                payment = await self._lock_user_and_payment(session, payment_id)
                if payment.status != 'pending':
                    return payment, False

                old_status = payment.status
                payment.status = 'failed'
                payment.failure_reason = failure_reason
                if provider_details is not None:
                    payment.provider_details = provider_details
                payment.last_checked_at = now_utc
                payment.updated_at = now_utc

                event_details: dict[str, Any] = {
                    **(provider_details or {}),
                    'failure_reason': failure_reason,
                }
                session.add(
                    OperationEvent(
                        operation_type='payment',
                        operation_id=payment.id,
                        from_status=old_status,
                        to_status='failed',
                        source=source,
                        details=event_details,
                    )
                )

                session.add(
                    OutboxTask(
                        dedup_key=f'payment_failed:{payment.id}:notify_failed',
                        target_service='notifications',
                        task_type='payment_failed_email',
                        payment_id=payment.id,
                        user_id=payment.user_id,
                        access_version=None,
                        payload=build_payment_failed_email_payload(
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            failure_reason=failure_reason,
                            public_base_url=public_base_url,
                        ),
                        next_retry_at=now_utc,
                    )
                )
                await session.commit()
                return payment, True
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

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
        """Выбирает и обрабатывает задачи outbox под SKIP LOCKED (T-19, BIL-FR-11)."""
        if not target_services:
            return 0
        processed = 0
        try:
            for _ in range(limit):
                if should_stop is not None and should_stop():
                    break
                async with self._session_factory() as session:
                    now_utc = datetime.now(UTC)
                    stmt = (
                        select(OutboxTask)
                        .where(
                            OutboxTask.target_service.in_(tuple(target_services)),
                            OutboxTask.status == 'pending',
                            OutboxTask.next_retry_at <= now_utc,
                        )
                        .order_by(
                            OutboxTask.next_retry_at.asc(),
                            OutboxTask.created_at.asc(),
                        )
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    if payment_id is not None:
                        stmt = stmt.where(OutboxTask.payment_id == payment_id)
                    if task_id is not None:
                        stmt = stmt.where(OutboxTask.id == task_id)

                    task = await session.scalar(stmt)
                    if task is None:
                        break

                    # US-S-02 шаг 3: если уже доставлена более новая версия,
                    # закрываем устаревшую задачу без вызова Auth.
                    if (
                        task.target_service == 'auth'
                        and task.access_version is not None
                    ):
                        max_delivered = await session.scalar(
                            select(func.max(OutboxTask.access_version)).where(
                                OutboxTask.user_id == task.user_id,
                                OutboxTask.target_service == 'auth',
                                OutboxTask.status == 'completed',
                            )
                        )
                        if (
                            max_delivered is not None
                            and int(max_delivered) >= task.access_version
                        ):
                            task.status = 'completed'
                            task.last_error = None
                            task.updated_at = now_utc
                            await session.commit()
                            processed += 1
                            continue

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
                        task.updated_at = now_after
                    elif outcome == 'permanent_error':
                        task.attempts += 1
                        task.status = 'requires_intervention'
                        task.last_error = error_msg
                        task.updated_at = now_after
                        alert_msg = (
                            f'Outbox task {task.id} requires intervention '
                            f'(permanent error): {error_msg}'
                        )
                        logger.error(alert_msg)
                        sentry_sdk.capture_message(alert_msg, level='error')
                    else:
                        task.attempts += 1
                        task.last_error = error_msg
                        task.updated_at = now_after
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
                            task.next_retry_at = now_after + timedelta(
                                seconds=backoff_sec
                            )
                    await session.commit()
                    processed += 1
            return processed
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def process_due_auth_outbox_tasks(
        self,
        deliver_fn: OutboxDeliverFn,
        *,
        payment_id: UUID | None = None,
        limit: int = 20,
        max_attempts: int = 20,
        should_stop: Callable[[], bool] | None = None,
    ) -> int:
        """Выбирает и обрабатывает задачи Auth под SKIP LOCKED (BIL-FR-11)."""
        return await self.process_due_outbox_tasks(
            deliver_fn,
            target_services=('auth',),
            payment_id=payment_id,
            limit=limit,
            max_attempts=max_attempts,
            should_stop=should_stop,
        )

    async def _lock_user_and_refund(
        self,
        session: AsyncSession,
        refund_id: UUID,
    ) -> tuple[Refund, Payment]:
        ref_row = (
            await session.execute(
                select(Refund.id, Payment.user_id)
                .join(Payment, Payment.id == Refund.payment_id)
                .where(Refund.id == refund_id)
            )
        ).one_or_none()
        if ref_row is None:
            raise LookupError(f'Refund {refund_id} not found')
        _, user_id = ref_row
        await self._acquire_user_lock(session, user_id)

        refund = await session.scalar(
            select(Refund).where(Refund.id == refund_id).with_for_update()
        )
        if refund is None:
            raise LookupError(f'Refund {refund_id} not found')
        payment = await session.scalar(
            select(Payment).where(Payment.id == refund.payment_id).with_for_update()
        )
        if payment is None:
            raise LookupError(f'Payment {refund.payment_id} not found')
        return refund, payment

    async def get_refund(self, refund_id: UUID) -> Refund | None:
        try:
            async with self._session_factory() as session:
                return await session.get(Refund, refund_id)
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_refund_by_provider_id(
        self,
        provider_code: str,
        provider_refund_id: str,
    ) -> Refund | None:
        try:
            async with self._session_factory() as session:
                return await session.scalar(
                    select(Refund).where(
                        Refund.provider_code == provider_code,
                        Refund.provider_refund_id == provider_refund_id,
                    )
                )
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def claim_next_due_processing_refund(
        self,
        *,
        min_age_seconds: float,
        throttle_seconds: float = 5.0,
    ) -> Refund | None:
        now_utc = datetime.now(UTC)
        age_cutoff = now_utc - timedelta(seconds=min_age_seconds)
        throttle_cutoff = now_utc - timedelta(seconds=throttle_seconds)
        try:
            async with self._session_factory() as session:
                stmt = (
                    select(Refund)
                    .where(
                        Refund.status == 'processing',
                        Refund.created_at <= age_cutoff,
                        or_(
                            Refund.last_checked_at.is_(None),
                            Refund.last_checked_at <= throttle_cutoff,
                        ),
                    )
                    .order_by(
                        Refund.last_checked_at.asc().nulls_first(),
                        Refund.created_at.asc(),
                    )
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                refund = await session.scalar(stmt)
                if refund is None:
                    return None
                refund.last_checked_at = now_utc
                refund.updated_at = now_utc
                await session.commit()
                return refund
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def get_or_create_processing_refund(
        self,
        payment_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        now_utc: datetime | None = None,
    ) -> tuple[Refund, bool]:
        now = now_utc or datetime.now(UTC)
        cleaned_reason = reason.strip()
        try:
            async with self._session_factory() as session:
                payment = await self._lock_user_and_payment(session, payment_id)
                existing_active = await session.scalar(
                    select(Refund).where(
                        Refund.payment_id == payment_id,
                        Refund.status.in_(('processing', 'succeeded')),
                    )
                )
                if existing_active is not None:
                    if existing_active.status == 'processing':
                        return existing_active, False
                    raise BillingDomainError(
                        'Возврат по данному платежу уже выполнен',
                        error_code='refund_already_succeeded',
                        status_code=409,
                    )

                if payment.status != 'succeeded':
                    raise BillingDomainError(
                        'Возврат возможен только для оплаченного платежа '
                        'активной подписки',
                        error_code='refund_not_allowed',
                        status_code=409,
                    )

                sub = await session.scalar(
                    select(Subscription).where(Subscription.payment_id == payment_id)
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
                refund = Refund(
                    id=refund_id,
                    payment_id=payment.id,
                    subscription_id=sub.id,
                    provider_code=payment.provider_code,
                    amount_kopeks=payment.amount_kopeks,
                    currency=payment.currency.strip(),
                    status='processing',
                    idempotency_key=refund_id,
                    admin_user_id=admin_user_id,
                    reason=cleaned_reason,
                )
                event = OperationEvent(
                    operation_type='refund',
                    operation_id=refund_id,
                    from_status=None,
                    to_status='processing',
                    source='admin',
                    details={
                        'payment_id': str(payment.id),
                        'admin_user_id': str(admin_user_id),
                        'reason': cleaned_reason,
                    },
                )
                action = AdminAction(
                    admin_user_id=admin_user_id,
                    action_type='refund_requested',
                    target_type='refund',
                    target_id=refund_id,
                    reason=cleaned_reason,
                )
                session.add_all([refund, event, action])
                try:
                    await session.commit()
                    return refund, True
                except IntegrityError:
                    await session.rollback()

                concurrent = await session.scalar(
                    select(Refund).where(
                        Refund.payment_id == payment_id,
                        Refund.status == 'processing',
                    )
                )
                if concurrent is None:
                    raise BillingDomainError(
                        'Возврат по данному платежу уже оформлен',
                        error_code='refund_already_succeeded',
                        status_code=409,
                    )
                return concurrent, False
        except LookupError as exc:
            raise BillingDomainError(
                'Payment not found',
                error_code='payment_not_found',
                status_code=404,
            ) from exc
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def save_provider_refund_created(
        self,
        refund_id: UUID,
        *,
        provider_refund_id: str,
        provider_details: dict[str, Any],
    ) -> Refund:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                refund, _ = await self._lock_user_and_refund(session, refund_id)
                if refund.status == 'processing':
                    refund.provider_refund_id = provider_refund_id
                    refund.provider_details = provider_details
                    refund.updated_at = now_utc
                    await session.commit()
                return refund
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def record_refund_checked(
        self,
        refund_id: UUID,
        *,
        provider_details: dict[str, Any] | None = None,
    ) -> Refund:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                refund, _ = await self._lock_user_and_refund(session, refund_id)
                if refund.status == 'processing':
                    refund.last_checked_at = now_utc
                    refund.updated_at = now_utc
                    if provider_details is not None:
                        refund.provider_details = provider_details
                    await session.commit()
                return refund
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def record_refund_reconcile_error(
        self,
        refund_id: UUID,
        *,
        error_message: str,
    ) -> Refund:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                refund, _ = await self._lock_user_and_refund(session, refund_id)
                if refund.status == 'processing':
                    details = dict(refund.provider_details or {})
                    prev_errors = details.get('_reconcile_errors', 0)
                    err_count = (
                        int(prev_errors) + 1
                        if isinstance(prev_errors, (int, str))
                        else 1
                    )
                    backoff_sec = min(300, 5 * (2 ** min(err_count - 1, 6)))
                    next_eligible_at = now_utc + timedelta(
                        seconds=max(0, backoff_sec - 5)
                    )
                    details['_reconcile_errors'] = err_count
                    details['_last_reconcile_error'] = error_message[:250]
                    refund.provider_details = details
                    refund.last_checked_at = next_eligible_at
                    refund.updated_at = now_utc
                    await session.commit()
                return refund
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def finalize_refund_succeeded(
        self,
        refund_id: UUID,
        *,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
        public_base_url: str = 'http://localhost',
    ) -> tuple[Refund, bool]:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                refund, payment = await self._lock_user_and_refund(session, refund_id)
                if refund.status != 'processing':
                    return refund, False

                old_status = refund.status
                refund.status = 'succeeded'
                if provider_details is not None:
                    refund.provider_details = provider_details
                refund.last_checked_at = now_utc
                refund.updated_at = now_utc

                session.add(
                    OperationEvent(
                        operation_type='refund',
                        operation_id=refund.id,
                        from_status=old_status,
                        to_status='succeeded',
                        source=source,
                        details=provider_details,
                    )
                )

                version_stmt = (
                    pg_insert(UserAccessVersion)
                    .values(
                        user_id=payment.user_id,
                        last_version=1,
                        updated_at=now_utc,
                    )
                    .on_conflict_do_update(
                        index_elements=[UserAccessVersion.user_id],
                        set_={
                            'last_version': UserAccessVersion.last_version + 1,
                            'updated_at': now_utc,
                        },
                    )
                    .returning(UserAccessVersion.last_version)
                )
                new_version = int((await session.execute(version_stmt)).scalar_one())

                session.add_all(
                    [
                        OutboxTask(
                            dedup_key=f'refund_succeeded:{refund.id}:sync_access',
                            target_service='auth',
                            task_type='sync_access',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=new_version,
                            payload={
                                'version': new_version,
                                'expires_at': None,
                            },
                            next_retry_at=now_utc,
                        ),
                        OutboxTask(
                            dedup_key=f'refund_succeeded:{refund.id}:cancel_expiration',
                            target_service='notifications',
                            task_type='cancel_expiration_email',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=None,
                            payload=build_cancel_expiration_email_payload(
                                subscription_id=refund.subscription_id,
                                user_id=payment.user_id,
                            ),
                            next_retry_at=now_utc,
                        ),
                        OutboxTask(
                            dedup_key=f'refund_succeeded:{refund.id}:notify_refund',
                            target_service='notifications',
                            task_type='refund_succeeded_email',
                            payment_id=payment.id,
                            user_id=payment.user_id,
                            access_version=None,
                            payload=build_refund_succeeded_email_payload(
                                refund_id=refund.id,
                                payment_id=payment.id,
                                user_id=payment.user_id,
                                amount_kopeks=refund.amount_kopeks,
                                currency=refund.currency.strip(),
                                public_base_url=public_base_url,
                            ),
                            next_retry_at=now_utc,
                        ),
                    ]
                )
                await session.commit()
                return refund, True
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc

    async def finalize_refund_rejected(
        self,
        refund_id: UUID,
        *,
        rejection_reason: str,
        source: ReconcileSource,
        provider_details: dict[str, Any] | None = None,
    ) -> tuple[Refund, bool]:
        now_utc = datetime.now(UTC)
        try:
            async with self._session_factory() as session:
                refund, _ = await self._lock_user_and_refund(session, refund_id)
                if refund.status != 'processing':
                    return refund, False

                old_status = refund.status
                refund.status = 'rejected'
                refund.rejection_reason = rejection_reason
                if provider_details is not None:
                    refund.provider_details = provider_details
                refund.last_checked_at = now_utc
                refund.updated_at = now_utc

                event_details: dict[str, Any] = {
                    **(provider_details or {}),
                    'rejection_reason': rejection_reason,
                }
                session.add(
                    OperationEvent(
                        operation_type='refund',
                        operation_id=refund.id,
                        from_status=old_status,
                        to_status='rejected',
                        source=source,
                        details=event_details,
                    )
                )
                await session.commit()
                return refund, True
        except (SQLAlchemyError, OSError) as exc:
            raise DatabaseUnavailableError() from exc
