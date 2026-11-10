from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
from uuid import UUID


@dataclass(frozen=True, slots=True)
class TariffData:
    """Условия продажи из таблицы `tariffs`: единственный источник цены и срока."""

    id: UUID
    code: str
    title: str
    price_kopeks: int
    currency: str
    duration_days: int


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
    # None — активного тарифа нет, покупка недоступна.
    tariff_price_kopeks: int | None
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
class InternalAdminActionFilter:
    admin_user_id: UUID | None = None
    action_type: (
        Literal['refund_requested', 'outbox_retried', 'outbox_closed'] | None
    ) = None
    created_from: datetime | None = None
    created_to: datetime | None = None
    limit: int = 50
    offset: int = 0


@dataclass(frozen=True, slots=True)
class InternalAdminActionItemData:
    id: UUID
    admin_user_id: UUID
    action_type: Literal['refund_requested', 'outbox_retried', 'outbox_closed']
    target_type: Literal['refund', 'outbox_task']
    target_id: UUID
    payment_id: UUID | None
    target_status: str | None
    reason: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class OperationalMetricsSnapshot:
    stuck_payments_count: int
    stuck_refunds_count: int
    stuck_operations_count: int
    outbox_pending_count: int
    outbox_stuck_pending_count: int
    outbox_requires_intervention_count: int
    undelivered_outbox_count: int


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
