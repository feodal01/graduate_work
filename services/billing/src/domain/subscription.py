from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

MSK_TZ = ZoneInfo('Europe/Moscow')
UTC_TZ = ZoneInfo('UTC')
BILLING_NAMESPACE = UUID('b1111600-0000-4000-8000-000000000001')

ReconcileSource = Literal['webhook', 'reconcile', 'continue', 'admin']
PaymentResultBlock = Literal[
    'succeeded_ready',
    'succeeded_activating',
    'failed',
    'pending',
]

_FAILURE_REASON_TRANSLATIONS: dict[str, str] = {
    'insufficient_funds': 'Недостаточно средств на карте',
    'card_expired': 'Истёк срок действия банковской карты',
    'expired_on_confirmation': 'Время на подтверждение оплаты истекло',
    'expired_on_capture': 'Истекло время подтверждения списания',
    'canceled_by_user': 'Оплата была отменена пользователем',
    'canceled_by_provider': 'Платёж отменён платёжной системой',
    'canceled_by_merchant': 'Платёж отменён магазином',
    'general_decline': 'Операция отклонена банком-эмитентом',
    '3d_secure_failed': 'Не пройдена проверка 3-D Secure',
    'payment_method_restricted': 'Операции по данной карте ограничены',
    'payment_method_limit_exceeded': 'Превышен лимит операций по карте',
    'permission_revoked': 'Разрешение на проведение платежа отозвано',
    'fraud_suspected': 'Операция отклонена системой безопасности',
    'call_issuer': 'Обратитесь в банк, выпустивший карту',
    'invalid_card_number': 'Указан неверный номер карты',
    'invalid_csc': 'Указан неверный код CVV/CVC',
    'issuer_unavailable': 'Банк-эмитент карты временно недоступен',
    'country_forbidden': 'Оплата картами данной страны не поддерживается',
    'deal_expired': 'Истёк срок действия операции',
    'identification_required': 'Требуется идентификация кошелька',
    'internal_timeout': 'Время ожидания ответа платёжной системы истекло',
    'unsupported_mobile_operator': 'Оператор сотовой связи не поддерживается',
}


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
    normalized_end = (
        ends_at if ends_at.tzinfo is not None else ends_at.replace(tzinfo=UTC)
    )
    if normalized_end > current:
        return SubscriptionState.ACTIVE
    return SubscriptionState.EXPIRED


def resolve_user_zoneinfo(tz_name: str | None) -> ZoneInfo:
    """Безопасно возвращает ZoneInfo пользователя (при ошибке — UTC)."""
    if not tz_name:
        return UTC_TZ
    cleaned = tz_name.strip()
    if not cleaned:
        return UTC_TZ
    try:
        return ZoneInfo(cleaned)
    except (ZoneInfoNotFoundError, KeyError, ValueError, OSError):
        return UTC_TZ


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def format_user_datetime(dt: datetime, tz_name: str | None) -> str:
    """Форматирует дату в поясе пользователя для экранов (BIL-NFR-U-01, NFR-U-03)."""
    zone = resolve_user_zoneinfo(tz_name)
    localized = _ensure_utc(dt).astimezone(zone)
    return localized.strftime('%d.%m.%Y %H:%M')


def format_msk_datetime(dt: datetime) -> str:
    """Форматирует время по Москве с пометкой МСК для писем (2.2 п. 12, 4.4)."""
    msk = _ensure_utc(dt).astimezone(MSK_TZ)
    return f'{msk.strftime("%d.%m.%Y %H:%M")} МСК'


def format_amount_rubles(amount_kopeks: int) -> str:
    """Форматирует сумму в копейках в рубли без потери копеек (BIL-NFR-R-07)."""
    rubles, kopeks = divmod(amount_kopeks, 100)
    if kopeks == 0:
        return f'{rubles} ₽'
    return f'{rubles},{kopeks:02d} ₽'


def humanize_failure_reason(reason: str | None) -> str:
    """Переводит причину отказа в человекочитаемый текст по allowlist (BIL-NFR-U-01)."""
    if not reason:
        return 'Платёж был отменён платёжной системой'
    cleaned = reason.strip()
    return _FAILURE_REASON_TRANSLATIONS.get(
        cleaned,
        'Платёж отклонён платёжной системой',
    )


def deterministic_id(kind: str, operation_id: UUID) -> UUID:
    """Генерирует детерминированный UUID (v5) для campaign_id и batch_id (инв. 12)."""
    return uuid5(BILLING_NAMESPACE, f'{kind}:{operation_id}')
