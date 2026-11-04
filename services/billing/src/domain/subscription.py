from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo

MSK_TZ = ZoneInfo('Europe/Moscow')
BILLING_NAMESPACE = UUID('b1111600-0000-4000-8000-000000000001')

ReconcileSource = Literal['webhook', 'reconcile', 'continue', 'admin']


class SubscriptionState(StrEnum):
    """Вычисляемое состояние подписки на лету (3.4, BIL-FR-08)."""

    ACTIVE = 'active'
    EXPIRED = 'expired'
    REFUNDED = 'refunded'


def compute_subscription_state(
    *,
    ends_at: datetime,
    has_succeeded_refund: bool,
    now_utc: datetime | None = None,
) -> SubscriptionState:
    """Вычисляет состояние подписки по ends_at и наличию выполненного возврата."""
    if has_succeeded_refund:
        return SubscriptionState.REFUNDED
    current = now_utc if now_utc is not None else datetime.now(UTC)
    if ends_at > current:
        return SubscriptionState.ACTIVE
    return SubscriptionState.EXPIRED


def format_msk_datetime(dt: datetime) -> str:
    """Форматирует время по Москве с пометкой МСК для писем (2.2 п. 12, 4.4)."""
    msk = dt.astimezone(MSK_TZ)
    return f'{msk.strftime("%d.%m.%Y %H:%M")} МСК'


def deterministic_id(kind: str, operation_id: UUID) -> UUID:
    """Генерирует детерминированный UUID (v5) для campaign_id и batch_id (инв. 12)."""
    return uuid5(BILLING_NAMESPACE, f'{kind}:{operation_id}')
