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
