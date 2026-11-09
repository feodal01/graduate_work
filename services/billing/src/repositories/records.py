from datetime import datetime
from typing import Any, Protocol
from uuid import UUID


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
    next_reconcile_at: datetime | None
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
