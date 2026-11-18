from datetime import UTC, datetime
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


# Служебные отметки Биллинга в `provider_details` (ЮKassa не присылает ключей
# с «_», поэтому они не пересекаются с её полями).
# Инв. 15: был POST /v3/refunds с неизвестным исходом — 400/401/403 на повтор
# уже не доказывает, что возврата нет.
POST_OUTCOME_UNKNOWN_KEY = '_post_outcome_unknown'
# Инв. 15: после POST с неизвестным исходом повтор получил 400/401/403 —
# POST по этой попытке больше не шлётся, итог решает розыск после 24 ч.
POST_STOPPED_KEY = '_post_stopped'
# Инв. 14: розыск возврата по списку ЮKassa ничего не нашёл ({'count', 'last_at'}).
LOOKUP_NOT_FOUND_KEY = '_lookup_not_found'
# Инв. 7: сигнал о расхождении итога с ЮKassa уже поднят (один на операцию).
DIVERGENCE_SIGNALED_KEY = '_divergence_signaled'
# Инв. 14, 16 и 6.4: виды аномалий ЮKassa, о которых по операции уже поднят
# сигнал ({kind: True}). Отметка вида `status` у ожидающего платежа ещё и
# запрещает уводить пользователя на страницу оплаты. Нормальный ответ ЮKassa
# заменяет `provider_details` и снимает её.
ANOMALIES_KEY = '_anomalies'

_STICKY_DETAIL_KEYS = (
    POST_OUTCOME_UNKNOWN_KEY,
    POST_STOPPED_KEY,
    LOOKUP_NOT_FOUND_KEY,
)


def keep_detail_markers(
    old: dict[str, Any] | None,
    new: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Новый снимок ЮKassa с перенесёнными служебными отметками попытки.

    Снимок провайдера целиком заменяет `provider_details`; без переноса
    отметки попытки возврата (инв. 14, 15) пропали бы при первом же
    сохранении ответа ЮKassa.
    """
    if new is None or not old:
        return new
    kept = {k: old[k] for k in _STICKY_DETAIL_KEYS if k in old and k not in new}
    return {**new, **kept} if kept else new


def set_signal_marker(
    record: PaymentRecordProtocol | RefundRecordProtocol,
    key: str,
) -> bool:
    """Отметка «сигнал уже поднят»; True — только в первый раз."""
    details = dict(record.provider_details or {})
    if details.get(key):
        return False
    details[key] = True
    record.provider_details = details
    return True


def set_anomaly_marker(
    record: PaymentRecordProtocol | RefundRecordProtocol,
    kind: str,
) -> bool:
    """Отметка «по операции уже поднят сигнал об аномалии вида kind»."""
    details = dict(record.provider_details or {})
    anomalies = details.get(ANOMALIES_KEY)
    seen = dict(anomalies) if isinstance(anomalies, dict) else {}
    if seen.get(kind):
        return False
    seen[kind] = True
    details[ANOMALIES_KEY] = seen
    record.provider_details = details
    return True


def has_anomaly_marker(details: dict[str, Any] | None) -> bool:
    """Есть ли у операции отметка хоть одной аномалии ЮKassa."""
    anomalies = (details or {}).get(ANOMALIES_KEY)
    return isinstance(anomalies, dict) and any(anomalies.values())


def set_divergence_marker(record: PaymentRecordProtocol | RefundRecordProtocol) -> bool:
    """Отметка расхождения с ЮKassa на завершённой операции (инв. 7)."""
    if record.status in ('pending', 'processing'):
        return False
    return set_signal_marker(record, DIVERGENCE_SIGNALED_KEY)


# Вид сигнала «расхождение итога с ЮKassa» (инв. 7); остальные виды —
# аномалии ответа ЮKassa (`ProviderOperationAnomalyError.kind`).
DIVERGENCE_SIGNAL_KIND = 'divergence'


def set_signal_kind_marker(
    record: PaymentRecordProtocol | RefundRecordProtocol,
    kind: str,
) -> bool:
    """Отметка «сигнал вида kind по операции уже поднят»; True — в первый раз."""
    if kind == DIVERGENCE_SIGNAL_KIND:
        return set_divergence_marker(record)
    return set_anomaly_marker(record, kind)


def set_post_stopped_marker(record: RefundRecordProtocol) -> bool:
    """Отметка «POST по попытке больше не шлём» (инв. 15); True — в первый раз."""
    if record.status != 'processing':
        return False
    return set_signal_marker(record, POST_STOPPED_KEY)


def lookup_not_found_state(
    details: dict[str, Any] | None,
) -> tuple[int, datetime | None]:
    """Сколько раз розыск по списку ничего не нашёл и когда последний раз (инв. 14)."""
    marker = (details or {}).get(LOOKUP_NOT_FOUND_KEY)
    if not isinstance(marker, dict):
        return 0, None
    count = marker.get('count')
    last_at = marker.get('last_at')
    try:
        parsed = datetime.fromisoformat(last_at) if isinstance(last_at, str) else None
    except ValueError:
        parsed = None
    if parsed is not None and parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return (count if isinstance(count, int) else 0), parsed


def lookup_not_found_marker(previous: Any, now_utc: datetime) -> dict[str, Any]:
    """Следующее значение отметки после ещё одного пустого розыска (инв. 14)."""
    count, _ = lookup_not_found_state({LOOKUP_NOT_FOUND_KEY: previous})
    return {'count': count + 1, 'last_at': now_utc.isoformat()}
