from datetime import UTC, date, datetime, time
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, field_validator

from api.errors import BillingDomainError
from clients.payment_provider import PaymentProviderError
from core.config import settings
from dependencies import (
    get_admin_queries,
    get_metrics_repository,
    get_outbox_admin_repository,
    get_outbox_dispatcher,
    get_payment_reconciler,
    require_internal_service,
)
from repositories.admin_queries import AdminQueries
from repositories.metrics import OperationalMetricsRepository
from repositories.outbox import OutboxAdminRepository
from repositories.read_models import (
    InternalAdminActionFilter,
    InternalOutboxFilter,
    InternalOutboxTaskItemData,
    InternalPaymentFilter,
    InternalSubscriptionFilter,
)
from services.outbox_dispatcher import OutboxDispatcher
from services.reconciler import PaymentReconciler

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
    payment_id: UUID | None = None
    user_id: UUID | None = None


class InternalOutboxTasksListResponse(BaseModel):
    total: int
    items: list[InternalOutboxTaskItemResponse]


class InternalAdminActionItemResponse(BaseModel):
    id: UUID
    admin_user_id: UUID
    action_type: Literal['refund_requested', 'outbox_retried', 'outbox_closed']
    target_type: Literal['refund', 'outbox_task']
    target_id: UUID
    payment_id: UUID | None = None
    target_status: str | None = None
    reason: str
    created_at: datetime


class InternalAdminActionsListResponse(BaseModel):
    total: int
    items: list[InternalAdminActionItemResponse]


class ManualReconcileResponse(BaseModel):
    operation_type: Literal['payment', 'refund']
    operation_id: UUID
    payment_id: UUID
    status: str
    transitioned: bool


class OperationalMetricsResponse(BaseModel):
    stuck_payments_count: int
    stuck_refunds_count: int
    stuck_operations_count: int
    outbox_pending_count: int
    outbox_stuck_pending_count: int
    outbox_requires_intervention_count: int
    undelivered_outbox_count: int


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


class CreateRefundRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    admin_user_id: UUID
    reason: str

    @field_validator('reason')
    @classmethod
    def validate_reason(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError('reason must not be empty')
        return cleaned


class RetryOutboxTaskRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    admin_user_id: UUID
    reason: str | None = None


class CloseOutboxTaskRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    admin_user_id: UUID
    reason: str

    @field_validator('reason')
    @classmethod
    def validate_reason(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError('reason must not be empty')
        return cleaned


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
    repo: Annotated[AdminQueries, Depends(get_admin_queries)],
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
    repo: Annotated[AdminQueries, Depends(get_admin_queries)],
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
    repo: Annotated[AdminQueries, Depends(get_admin_queries)],
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
                payment_id=t.payment_id,
                user_id=t.user_id,
            )
            for t in card.outbox_tasks
        ],
    )


@router.get(
    '/outbox-tasks',
    response_model=InternalOutboxTasksListResponse,
    responses={
        401: {'description': 'Invalid or missing service API key'},
        422: {'description': 'Invalid filter parameters'},
        503: {'description': 'Database is unavailable'},
    },
)
async def get_internal_outbox_tasks(
    repo: Annotated[OutboxAdminRepository, Depends(get_outbox_admin_repository)],
    target_service: Literal['auth', 'notifications'] | None = None,
    status: Literal['pending', 'requires_intervention'] | None = None,
    user_id: UUID | None = None,
    payment_id: UUID | None = None,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> InternalOutboxTasksListResponse:
    lower = _normalize_bound(created_from, date_from, is_upper=False)
    upper = _normalize_bound(created_to, date_to, is_upper=True)
    _validate_date_range(lower, upper)

    total, rows = await repo.list_internal_outbox_tasks(
        InternalOutboxFilter(
            target_service=target_service,
            status=status,
            user_id=user_id,
            payment_id=payment_id,
            created_from=lower,
            created_to=upper,
            limit=limit,
            offset=offset,
        ),
        stuck_threshold_seconds=settings.worker.billing_stuck_threshold_seconds,
    )
    return InternalOutboxTasksListResponse(
        total=total,
        items=[
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
                payment_id=t.payment_id,
                user_id=t.user_id,
            )
            for t in rows
        ],
    )


@router.post(
    '/payments/{payment_id}/refunds',
    response_model=InternalRefundItemResponse,
    responses={
        401: {'description': 'Invalid or missing service API key'},
        404: {'description': 'Payment not found'},
        409: {'description': 'Refund is not allowed by domain rules'},
        422: {'description': 'Empty reason or invalid payload'},
        503: {'description': 'Database is unavailable'},
    },
)
async def post_internal_payment_refund(
    payment_id: UUID,
    payload: CreateRefundRequest,
    reconciler: Annotated[PaymentReconciler, Depends(get_payment_reconciler)],
) -> InternalRefundItemResponse:
    outcome = await reconciler.create_or_reconcile_refund(
        payment_id,
        admin_user_id=payload.admin_user_id,
        reason=payload.reason,
    )
    r_status: Literal['processing', 'succeeded', 'rejected'] = (
        'succeeded'
        if outcome.status == 'succeeded'
        else 'processing'
        if outcome.status == 'processing'
        else 'rejected'
    )
    return InternalRefundItemResponse(
        id=outcome.refund_id,
        payment_id=outcome.payment_id,
        subscription_id=outcome.subscription_id,
        provider_code=outcome.provider_code,
        provider_refund_id=outcome.provider_refund_id,
        amount_kopeks=outcome.amount_kopeks,
        currency=outcome.currency,
        status=r_status,
        reason=outcome.reason,
        rejection_reason=outcome.rejection_reason,
        admin_user_id=outcome.admin_user_id,
        created_at=outcome.created_at,
        updated_at=outcome.updated_at,
        can_reconcile=(r_status == 'processing'),
    )


def _to_outbox_task_response(
    t: InternalOutboxTaskItemData,
) -> InternalOutboxTaskItemResponse:
    return InternalOutboxTaskItemResponse(
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
        payment_id=t.payment_id,
        user_id=t.user_id,
    )


@router.post(
    '/outbox-tasks/{task_id}/retry',
    response_model=InternalOutboxTaskItemResponse,
    responses={
        401: {'description': 'Invalid or missing service API key'},
        404: {'description': 'Outbox task not found'},
        409: {'description': 'Outbox task is already completed or closed'},
        422: {'description': 'Invalid request payload'},
        503: {'description': 'Database is unavailable'},
    },
)
async def post_internal_outbox_task_retry(
    task_id: UUID,
    payload: RetryOutboxTaskRequest,
    repo: Annotated[OutboxAdminRepository, Depends(get_outbox_admin_repository)],
    dispatcher: Annotated[OutboxDispatcher, Depends(get_outbox_dispatcher)],
) -> InternalOutboxTaskItemResponse:
    updated = await repo.retry_outbox_task_by_admin(
        task_id,
        admin_user_id=payload.admin_user_id,
        reason=payload.reason or '',
    )
    await dispatcher.try_deliver_task(task_id)
    after_delivery = await repo.get_internal_outbox_task(task_id)
    return _to_outbox_task_response(after_delivery or updated)


@router.post(
    '/outbox-tasks/{task_id}/close',
    response_model=InternalOutboxTaskItemResponse,
    responses={
        401: {'description': 'Invalid or missing service API key'},
        404: {'description': 'Outbox task not found'},
        409: {'description': 'Outbox task is not in requires_intervention'},
        422: {'description': 'Empty reason or invalid payload'},
        503: {'description': 'Database is unavailable'},
    },
)
async def post_internal_outbox_task_close(
    task_id: UUID,
    payload: CloseOutboxTaskRequest,
    repo: Annotated[OutboxAdminRepository, Depends(get_outbox_admin_repository)],
) -> InternalOutboxTaskItemResponse:
    closed = await repo.close_outbox_task_by_admin(
        task_id,
        admin_user_id=payload.admin_user_id,
        reason=payload.reason,
    )
    return _to_outbox_task_response(closed)


@router.get(
    '/admin-actions',
    response_model=InternalAdminActionsListResponse,
    responses={
        401: {'description': 'Invalid or missing service API key'},
        422: {'description': 'Invalid filter parameters'},
        503: {'description': 'Database is unavailable'},
    },
)
async def get_internal_admin_actions(
    repo: Annotated[AdminQueries, Depends(get_admin_queries)],
    admin_user_id: UUID | None = None,
    action_type: Literal['refund_requested', 'outbox_retried', 'outbox_closed']
    | None = None,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> InternalAdminActionsListResponse:
    lower = _normalize_bound(created_from, date_from, is_upper=False)
    upper = _normalize_bound(created_to, date_to, is_upper=True)
    _validate_date_range(lower, upper)

    total, rows = await repo.list_internal_admin_actions(
        InternalAdminActionFilter(
            admin_user_id=admin_user_id,
            action_type=action_type,
            created_from=lower,
            created_to=upper,
            limit=limit,
            offset=offset,
        )
    )
    return InternalAdminActionsListResponse(
        total=total,
        items=[
            InternalAdminActionItemResponse(
                id=r.id,
                admin_user_id=r.admin_user_id,
                action_type=r.action_type,
                target_type=r.target_type,
                target_id=r.target_id,
                payment_id=r.payment_id,
                target_status=r.target_status,
                reason=r.reason,
                created_at=r.created_at,
            )
            for r in rows
        ],
    )


@router.post(
    '/operations/{operation_type}/{operation_id}/reconcile',
    response_model=ManualReconcileResponse,
    responses={
        401: {'description': 'Invalid or missing service API key'},
        404: {'description': 'Operation not found'},
        422: {'description': 'Invalid operation type or UUID'},
        503: {'description': 'YooKassa or database is unavailable'},
    },
)
async def post_internal_operation_reconcile(
    operation_type: Literal['payment', 'refund'],
    operation_id: UUID,
    reconciler: Annotated[PaymentReconciler, Depends(get_payment_reconciler)],
) -> ManualReconcileResponse:
    try:
        (
            payment_id,
            status_val,
            transitioned,
        ) = await reconciler.reconcile_operation_by_admin(
            operation_type,
            operation_id,
        )
    except LookupError as exc:
        raise BillingDomainError(
            'Operation not found',
            error_code='operation_not_found',
            status_code=404,
        ) from exc
    except PaymentProviderError as exc:
        raise BillingDomainError(
            'Не удалось связаться с платёжной системой ЮKassa',
            error_code='payment_provider_unavailable',
            status_code=503,
        ) from exc

    return ManualReconcileResponse(
        operation_type=operation_type,
        operation_id=operation_id,
        payment_id=payment_id,
        status=status_val,
        transitioned=transitioned,
    )


@router.get(
    '/metrics',
    response_model=OperationalMetricsResponse,
    responses={
        401: {'description': 'Invalid or missing service API key'},
        503: {'description': 'Database is unavailable'},
    },
)
async def get_internal_operational_metrics(
    repo: Annotated[OperationalMetricsRepository, Depends(get_metrics_repository)],
) -> OperationalMetricsResponse:
    snapshot = await repo.get_operational_metrics(
        stuck_threshold_seconds=settings.worker.billing_stuck_threshold_seconds,
    )
    return OperationalMetricsResponse(
        stuck_payments_count=snapshot.stuck_payments_count,
        stuck_refunds_count=snapshot.stuck_refunds_count,
        stuck_operations_count=snapshot.stuck_operations_count,
        outbox_pending_count=snapshot.outbox_pending_count,
        outbox_stuck_pending_count=snapshot.outbox_stuck_pending_count,
        outbox_requires_intervention_count=(
            snapshot.outbox_requires_intervention_count
        ),
        undelivered_outbox_count=snapshot.undelivered_outbox_count,
    )
