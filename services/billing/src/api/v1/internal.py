from datetime import UTC, date, datetime, time
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from api.errors import BillingDomainError
from core.config import settings
from dependencies import get_purchase_repository, require_internal_service
from repositories.purchase import (
    InternalPaymentFilter,
    InternalSubscriptionFilter,
    PurchaseRepository,
)

router = APIRouter(
    tags=['internal-billing'],
    dependencies=[Depends(require_internal_service)],
)


class InternalSubscriptionItemResponse(BaseModel):
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


class InternalSubscriptionsListResponse(BaseModel):
    total: int
    items: list[InternalSubscriptionItemResponse]


class InternalPaymentItemResponse(BaseModel):
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


class InternalPaymentsListResponse(BaseModel):
    total: int
    items: list[InternalPaymentItemResponse]


class InternalRefundItemResponse(BaseModel):
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


class InternalOperationEventItemResponse(BaseModel):
    id: UUID
    operation_type: Literal['payment', 'refund']
    operation_id: UUID
    from_status: str | None
    to_status: str
    source: str
    details: dict[str, object] | None
    created_at: datetime


class InternalOutboxTaskItemResponse(BaseModel):
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


class InternalPaymentCardResponse(BaseModel):
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
    subscription: InternalSubscriptionItemResponse | None
    refunds: list[InternalRefundItemResponse]
    operation_events: list[InternalOperationEventItemResponse]
    outbox_tasks: list[InternalOutboxTaskItemResponse]


def _normalize_bound(
    dt_value: datetime | None,
    date_value: date | None,
    *,
    is_upper: bool,
) -> datetime | None:
    if dt_value is not None:
        if dt_value.tzinfo is None:
            return dt_value.replace(tzinfo=UTC)
        return dt_value.astimezone(UTC)
    if date_value is not None:
        bound_time = time.max if is_upper else time.min
        return datetime.combine(date_value, bound_time, tzinfo=UTC)
    return None


def _validate_date_range(
    lower: datetime | None,
    upper: datetime | None,
) -> None:
    if lower is not None and upper is not None and lower > upper:
        raise BillingDomainError(
            'created_from must be less than or equal to created_to',
            error_code='validation_error',
            status_code=422,
        )


@router.get(
    '/subscriptions',
    response_model=InternalSubscriptionsListResponse,
    responses={
        401: {'description': 'Invalid or missing service API key'},
        422: {'description': 'Invalid filter parameters'},
        503: {'description': 'Database is unavailable'},
    },
)
async def get_internal_subscriptions(
    repo: Annotated[PurchaseRepository, Depends(get_purchase_repository)],
    user_id: UUID | None = None,
    status: Literal['active', 'expired', 'refunded'] | None = None,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> InternalSubscriptionsListResponse:
    lower = _normalize_bound(created_from, date_from, is_upper=False)
    upper = _normalize_bound(created_to, date_to, is_upper=True)
    _validate_date_range(lower, upper)

    total, rows = await repo.list_internal_subscriptions(
        InternalSubscriptionFilter(
            user_id=user_id,
            status=status,
            created_from=lower,
            created_to=upper,
            limit=limit,
            offset=offset,
        )
    )
    return InternalSubscriptionsListResponse(
        total=total,
        items=[
            InternalSubscriptionItemResponse(
                id=r.id,
                user_id=r.user_id,
                payment_id=r.payment_id,
                tariff_code=r.tariff_code,
                tariff_name=r.tariff_name,
                starts_at=r.starts_at,
                ends_at=r.ends_at,
                status=r.status,
                amount_kopeks=r.amount_kopeks,
                currency=r.currency,
                created_at=r.created_at,
            )
            for r in rows
        ],
    )


@router.get(
    '/payments',
    response_model=InternalPaymentsListResponse,
    responses={
        401: {'description': 'Invalid or missing service API key'},
        422: {'description': 'Invalid filter parameters'},
        503: {'description': 'Database is unavailable'},
    },
)
async def get_internal_payments(
    repo: Annotated[PurchaseRepository, Depends(get_purchase_repository)],
    status: Literal['pending', 'succeeded', 'failed'] | None = None,
    has_refund: bool | None = None,
    stuck: bool = False,
    user_id: UUID | None = None,
    id: UUID | None = None,
    payment_id: UUID | None = None,
    provider_payment_id: str | None = None,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> InternalPaymentsListResponse:
    lower = _normalize_bound(created_from, date_from, is_upper=False)
    upper = _normalize_bound(created_to, date_to, is_upper=True)
    _validate_date_range(lower, upper)
    effective_payment_id = id if id is not None else payment_id
    normalized_provider_id = (
        provider_payment_id.strip()
        if provider_payment_id is not None and provider_payment_id.strip()
        else None
    )

    total, rows = await repo.list_internal_payments(
        InternalPaymentFilter(
            status=status,
            has_refund=has_refund,
            stuck=stuck,
            user_id=user_id,
            payment_id=effective_payment_id,
            provider_payment_id=normalized_provider_id,
            created_from=lower,
            created_to=upper,
            limit=limit,
            offset=offset,
        ),
        stuck_threshold_seconds=settings.worker.billing_stuck_threshold_seconds,
    )
    return InternalPaymentsListResponse(
        total=total,
        items=[
            InternalPaymentItemResponse(
                id=r.id,
                user_id=r.user_id,
                tariff_code=r.tariff_code,
                tariff_name=r.tariff_name,
                provider_code=r.provider_code,
                provider_payment_id=r.provider_payment_id,
                amount_kopeks=r.amount_kopeks,
                currency=r.currency,
                status=r.status,
                failure_reason=r.failure_reason,
                has_refund=r.has_refund,
                refund_status=r.refund_status,
                active_refund_id=r.active_refund_id,
                is_stuck=r.is_stuck,
                created_at=r.created_at,
                updated_at=r.updated_at,
            )
            for r in rows
        ],
    )


@router.get(
    '/payments/{payment_id}',
    response_model=InternalPaymentCardResponse,
    responses={
        401: {'description': 'Invalid or missing service API key'},
        404: {'description': 'Payment not found'},
        422: {'description': 'Invalid payment UUID'},
        503: {'description': 'Database is unavailable'},
    },
)
async def get_internal_payment_card(
    payment_id: UUID,
    repo: Annotated[PurchaseRepository, Depends(get_purchase_repository)],
) -> InternalPaymentCardResponse:
    card = await repo.get_internal_payment_card(payment_id)
    if card is None:
        raise BillingDomainError(
            'Payment not found',
            error_code='payment_not_found',
            status_code=404,
        )
    sub_resp = (
        InternalSubscriptionItemResponse(
            id=card.subscription.id,
            user_id=card.subscription.user_id,
            payment_id=card.subscription.payment_id,
            tariff_code=card.subscription.tariff_code,
            tariff_name=card.subscription.tariff_name,
            starts_at=card.subscription.starts_at,
            ends_at=card.subscription.ends_at,
            status=card.subscription.status,
            amount_kopeks=card.subscription.amount_kopeks,
            currency=card.subscription.currency,
            created_at=card.subscription.created_at,
        )
        if card.subscription is not None
        else None
    )
    return InternalPaymentCardResponse(
        id=card.id,
        user_id=card.user_id,
        tariff_code=card.tariff_code,
        tariff_name=card.tariff_name,
        provider_code=card.provider_code,
        provider_payment_id=card.provider_payment_id,
        amount_kopeks=card.amount_kopeks,
        currency=card.currency,
        status=card.status,
        failure_reason=card.failure_reason,
        created_at=card.created_at,
        updated_at=card.updated_at,
        can_refund=card.can_refund,
        can_reconcile_payment=card.can_reconcile_payment,
        subscription=sub_resp,
        refunds=[
            InternalRefundItemResponse(
                id=r.id,
                payment_id=r.payment_id,
                subscription_id=r.subscription_id,
                provider_code=r.provider_code,
                provider_refund_id=r.provider_refund_id,
                amount_kopeks=r.amount_kopeks,
                currency=r.currency,
                status=r.status,
                reason=r.reason,
                rejection_reason=r.rejection_reason,
                admin_user_id=r.admin_user_id,
                created_at=r.created_at,
                updated_at=r.updated_at,
                can_reconcile=r.can_reconcile,
            )
            for r in card.refunds
        ],
        operation_events=[
            InternalOperationEventItemResponse(
                id=ev.id,
                operation_type=ev.operation_type,
                operation_id=ev.operation_id,
                from_status=ev.from_status,
                to_status=ev.to_status,
                source=ev.source,
                details=ev.details,
                created_at=ev.created_at,
            )
            for ev in card.operation_events
        ],
        outbox_tasks=[
            InternalOutboxTaskItemResponse(
                id=t.id,
                target_service=t.target_service,
                task_type=t.task_type,
                access_version=t.access_version,
                status=t.status,
                attempts=t.attempts,
                last_error=t.last_error,
                created_at=t.created_at,
                updated_at=t.updated_at,
                can_retry_or_close=t.can_retry_or_close,
            )
            for t in card.outbox_tasks
        ],
    )
