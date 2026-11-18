import logging
import re
from dataclasses import dataclass
from decimal import Decimal, Inexact, InvalidOperation
from typing import Any, Literal, Protocol
from urllib.parse import quote, urlencode
from uuid import UUID

import httpx
import sentry_sdk

logger = logging.getLogger(__name__)

DomainPaymentStatus = Literal['pending', 'succeeded', 'failed']
DomainRefundStatus = Literal['processing', 'succeeded', 'rejected']
_SAFE_PROVIDER_ID_RE = re.compile(r'^[A-Za-z0-9._-]{1,128}$')


class PaymentProviderError(Exception):
    """Базовое исключение антикоррупционного слоя платёжного провайдера."""


class ProviderPermanentError(PaymentProviderError):
    """Постоянная ошибка на создание операции (POST 400, 401, 403 — инв. 15 п. 1)."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(message)


class ProviderTransientError(PaymentProviderError):
    """Временная ошибка с неизвестным исходом (тайм-аут, 429, 5xx — инв. 10)."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(message)


class ProviderReconciliationError(ProviderTransientError):
    """Ошибка 4xx (400, 401, 403, 404) при сверке GET созданной операции (инв. 15 п. 2).

    Операция остаётся незавершённой, но поднимается сигнал в мониторинг.
    """

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message, status_code=status_code)


class ProviderProtocolAnomalyError(PaymentProviderError):
    """Аномалия протокола провайдера (например, waiting_for_capture — инв. 16)."""


class ProviderOperationAnomalyError(ProviderProtocolAnomalyError):
    """Аномалия, которая повторяется на каждом чтении одной и той же операции.

    Адаптер не знает, читали ли эту операцию раньше, поэтому только пишет
    WARNING. Сигнал в мониторинг поднимает сверка — владелец операции —
    один раз на операцию (6.4). `kind` различает виды аномалий.
    """

    kind: str = 'anomaly'


class ProviderStatusAnomalyError(ProviderOperationAnomalyError):
    """`waiting_for_capture` или неизвестный статус операции (инв. 16)."""

    kind = 'status'

    def __init__(self, message: str, *, provider_status: str) -> None:
        self.provider_status = provider_status
        super().__init__(message)


class RefundListIncompleteError(ProviderOperationAnomalyError):
    """Обход списка возвратов не дошёл до последней страницы (инв. 14)."""

    kind = 'refund_list_incomplete'


class DuplicateRefundsError(ProviderOperationAnomalyError):
    """В списке возвратов несколько объектов с UUID одной попытки (инв. 14)."""

    kind = 'duplicate_refunds'

    def __init__(self, message: str, *, provider_refund_ids: list[str]) -> None:
        self.provider_refund_ids = provider_refund_ids
        super().__init__(message)


class ProviderIntegrityMismatchError(PaymentProviderError):
    """Несовпадение суммы, валюты или UUID в metadata ответа провайдера (инв. 17)."""


@dataclass(frozen=True, slots=True)
class CreateProviderPaymentRequest:
    payment_id: UUID
    idempotency_key: UUID
    amount_kopeks: int
    currency: str
    description: str
    return_url: str


@dataclass(frozen=True, slots=True)
class ProviderPaymentSnapshot:
    provider_code: str
    provider_payment_id: str
    domain_status: DomainPaymentStatus
    amount_kopeks: int
    currency: str
    confirmation_url: str | None
    failure_reason: str | None
    metadata_payment_id: str | None
    provider_details: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CreateProviderRefundRequest:
    refund_id: UUID
    idempotency_key: UUID
    provider_payment_id: str
    amount_kopeks: int
    currency: str
    description: str


@dataclass(frozen=True, slots=True)
class ProviderRefundSnapshot:
    provider_code: str
    provider_refund_id: str
    provider_payment_id: str | None
    domain_status: DomainRefundStatus
    amount_kopeks: int
    currency: str
    rejection_reason: str | None
    metadata_refund_id: str | None
    provider_details: dict[str, Any]


class PaymentProviderGateway(Protocol):
    provider_code: str

    async def create_payment(
        self,
        request: CreateProviderPaymentRequest,
    ) -> ProviderPaymentSnapshot: ...

    async def get_payment(
        self,
        provider_payment_id: str,
        *,
        expected_payment_id: UUID | None = None,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderPaymentSnapshot: ...

    async def create_refund(
        self,
        request: CreateProviderRefundRequest,
    ) -> ProviderRefundSnapshot: ...

    async def get_refund(
        self,
        provider_refund_id: str,
        *,
        expected_refund_id: UUID | None = None,
        expected_provider_payment_id: str | None = None,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderRefundSnapshot: ...

    async def find_refund_by_metadata(
        self,
        provider_payment_id: str,
        *,
        refund_id: UUID,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderRefundSnapshot | None: ...


def _raise_protocol_anomaly(message: str) -> None:
    logger.error(message)
    sentry_sdk.capture_message(message, level='error')
    raise ProviderProtocolAnomalyError(message)


def _raise_operation_anomaly(exc: ProviderOperationAnomalyError) -> None:
    # Сигнал — за владельцем операции (сверкой), здесь только след в логе.
    logger.warning('%s', exc)
    raise exc


def _raise_integrity_mismatch(message: str) -> None:
    logger.error(message)
    sentry_sdk.capture_message(message, level='error')
    raise ProviderIntegrityMismatchError(message)


def format_kopeks_as_decimal(amount_kopeks: int) -> str:
    if amount_kopeks <= 0:
        raise ValueError('amount_kopeks must be positive')
    rubles, kopeks = divmod(amount_kopeks, 100)
    return f'{rubles}.{kopeks:02d}'


def parse_decimal_to_kopeks(value: str) -> int:
    try:
        dec = Decimal(value)
        if not dec.is_finite():
            raise InvalidOperation('Non-finite Decimal')
        scaled = dec * 100
        integral = scaled.to_integral_exact()
        if integral != scaled:
            raise Inexact('Fractional kopeks are not allowed')
        kopeks = int(integral)
    except (InvalidOperation, Inexact, ValueError, TypeError, OverflowError) as exc:
        msg = f'Invalid amount value from provider: {value!r}'
        logger.error(msg)
        sentry_sdk.capture_message(msg, level='error')
        raise ProviderProtocolAnomalyError(msg) from exc
    if kopeks <= 0:
        _raise_protocol_anomaly(f'Non-positive amount from provider: {value!r}')
    return kopeks


def is_safe_provider_operation_id(provider_id: str) -> bool:
    """Безопасен ли номер для подстановки в путь запроса (без сигнала)."""
    cleaned = provider_id.strip()
    return bool(cleaned) and _SAFE_PROVIDER_ID_RE.fullmatch(cleaned) is not None


def validate_provider_operation_id(provider_id: str) -> str:
    cleaned = provider_id.strip()
    if not is_safe_provider_operation_id(cleaned):
        _raise_protocol_anomaly(
            f'Unsafe or empty provider operation id: {provider_id!r}'
        )
    return quote(cleaned, safe='')


class YooKassaProviderAdapter:
    """Антикоррупционный слой (ACL) для ЮKassa API v3 (3.6, BIL-NFR-S-02)."""

    provider_code: str = 'yookassa'

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        shop_id: str,
        secret_key: str,
        base_url: str = 'https://api.yookassa.ru/v3',
        refund_list_page_size: int = 100,
        refund_list_max_pages: int = 10,
    ) -> None:
        self._http = http_client
        self._auth = httpx.BasicAuth(username=shop_id, password=secret_key)
        self._base_url = base_url.rstrip('/')
        # Размер страницы и предел обхода списка возвратов (инвариант 14).
        self._refund_list_page_size = refund_list_page_size
        self._refund_list_max_pages = refund_list_max_pages

    async def create_payment(
        self,
        request: CreateProviderPaymentRequest,
    ) -> ProviderPaymentSnapshot:
        payload = {
            'amount': {
                'value': format_kopeks_as_decimal(request.amount_kopeks),
                'currency': request.currency,
            },
            'capture': True,
            'confirmation': {
                'type': 'redirect',
                'return_url': request.return_url,
            },
            'description': request.description,
            'metadata': {
                'payment_id': str(request.payment_id),
            },
        }
        headers = {
            'Idempotence-Key': str(request.idempotency_key),
        }
        data = await self._request(
            'POST',
            '/payments',
            json_body=payload,
            headers=headers,
            is_creation=True,
        )
        snapshot = self._to_snapshot(
            data,
            expected_payment_id=request.payment_id,
            expected_amount_kopeks=request.amount_kopeks,
            expected_currency=request.currency,
        )
        if snapshot.domain_status == 'pending' and not snapshot.confirmation_url:
            _raise_protocol_anomaly(
                f'YooKassa POST /payments returned pending payment '
                f'{snapshot.provider_payment_id} without confirmation_url (inv. 13)'
            )
        return snapshot

    async def get_payment(
        self,
        provider_payment_id: str,
        *,
        expected_payment_id: UUID | None = None,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderPaymentSnapshot:
        safe_id = validate_provider_operation_id(provider_payment_id)
        data = await self._request(
            'GET',
            f'/payments/{safe_id}',
            is_creation=False,
        )
        snapshot = self._to_snapshot(
            data,
            expected_payment_id=expected_payment_id,
            expected_amount_kopeks=expected_amount_kopeks,
            expected_currency=expected_currency,
        )
        if snapshot.provider_payment_id != provider_payment_id.strip():
            _raise_integrity_mismatch(
                f'Provider payment id mismatch: requested {provider_payment_id!r}, '
                f'got {snapshot.provider_payment_id!r}'
            )
        return snapshot

    async def create_refund(
        self,
        request: CreateProviderRefundRequest,
    ) -> ProviderRefundSnapshot:
        safe_pay_id = validate_provider_operation_id(request.provider_payment_id)
        payload = {
            'payment_id': safe_pay_id,
            'amount': {
                'value': format_kopeks_as_decimal(request.amount_kopeks),
                'currency': request.currency,
            },
            'description': request.description,
            'metadata': {
                'refund_id': str(request.refund_id),
            },
        }
        headers = {
            'Idempotence-Key': str(request.idempotency_key),
        }
        data = await self._request(
            'POST',
            '/refunds',
            json_body=payload,
            headers=headers,
            is_creation=True,
        )
        return self._to_refund_snapshot(
            data,
            expected_refund_id=request.refund_id,
            expected_provider_payment_id=safe_pay_id,
            expected_amount_kopeks=request.amount_kopeks,
            expected_currency=request.currency,
        )

    async def get_refund(
        self,
        provider_refund_id: str,
        *,
        expected_refund_id: UUID | None = None,
        expected_provider_payment_id: str | None = None,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderRefundSnapshot:
        safe_id = validate_provider_operation_id(provider_refund_id)
        data = await self._request(
            'GET',
            f'/refunds/{safe_id}',
            is_creation=False,
        )
        snapshot = self._to_refund_snapshot(
            data,
            expected_refund_id=expected_refund_id,
            expected_provider_payment_id=expected_provider_payment_id,
            expected_amount_kopeks=expected_amount_kopeks,
            expected_currency=expected_currency,
        )
        if snapshot.provider_refund_id != provider_refund_id.strip():
            _raise_integrity_mismatch(
                f'Provider refund id mismatch: requested {provider_refund_id!r}, '
                f'got {snapshot.provider_refund_id!r}'
            )
        return snapshot

    async def find_refund_by_metadata(
        self,
        provider_payment_id: str,
        *,
        refund_id: UUID,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderRefundSnapshot | None:
        """Ищет возврат платежа по нашему номеру попытки в `metadata`.

        `None` означает, что список просмотрен до последней страницы и возврата
        в нём нет: по этому ответу попытка получает окончательный отказ. Поэтому
        незавершённый обход — ошибка, а не «не найдено».
        """
        safe_pay_id = validate_provider_operation_id(provider_payment_id)
        target_id = str(refund_id)
        seen_cursors: set[str] = set()
        cursor: str | None = None
        # Просматриваем все страницы: два возврата с UUID одной попытки —
        # двойной возврат у провайдера, привязка «первого» его скрыла бы.
        matches: list[dict[str, Any]] = []
        for _ in range(self._refund_list_max_pages):
            query: dict[str, str | int] = {
                'payment_id': safe_pay_id,
                'limit': self._refund_list_page_size,
            }
            if cursor is not None:
                query['cursor'] = cursor
            data = await self._request(
                'GET',
                f'/refunds?{urlencode(query)}',
                is_creation=False,
            )
            items = data.get('items')
            if not isinstance(items, list):
                _raise_protocol_anomaly('Missing items list in YooKassa GET /refunds')
            assert isinstance(items, list)
            matches.extend(
                item
                for item in items
                if isinstance(item, dict)
                and isinstance(item.get('metadata'), dict)
                and str(item['metadata'].get('refund_id') or '').strip() == target_id
            )
            next_cursor = data.get('next_cursor')
            if next_cursor is None:
                # Последняя страница: ЮKassa не прислала `next_cursor`.
                return self._single_refund_match(
                    matches,
                    refund_id=refund_id,
                    provider_payment_id=safe_pay_id,
                    expected_amount_kopeks=expected_amount_kopeks,
                    expected_currency=expected_currency,
                )
            if not isinstance(next_cursor, str) or not next_cursor.strip():
                _raise_protocol_anomaly('Invalid next_cursor in YooKassa GET /refunds')
            assert isinstance(next_cursor, str)
            if next_cursor in seen_cursors:
                _raise_operation_anomaly(
                    RefundListIncompleteError(
                        f'Repeated next_cursor in YooKassa GET /refunds '
                        f'for {safe_pay_id}; search is incomplete'
                    )
                )
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        _raise_operation_anomaly(
            RefundListIncompleteError(
                f'Refund list for {safe_pay_id} exceeds '
                f'{self._refund_list_max_pages} pages; search is incomplete'
            )
        )
        return None

    def _single_refund_match(
        self,
        matches: list[dict[str, Any]],
        *,
        refund_id: UUID,
        provider_payment_id: str,
        expected_amount_kopeks: int | None,
        expected_currency: str | None,
    ) -> ProviderRefundSnapshot | None:
        if not matches:
            return None
        if len(matches) > 1:
            ids = [str(item.get('id') or '?') for item in matches]
            _raise_operation_anomaly(
                DuplicateRefundsError(
                    f'Refund list for {provider_payment_id} has {len(ids)} refunds '
                    f'with attempt {refund_id}: {", ".join(ids)}',
                    provider_refund_ids=ids,
                )
            )
        return self._to_refund_snapshot(
            matches[0],
            expected_refund_id=refund_id,
            expected_provider_payment_id=provider_payment_id,
            expected_amount_kopeks=expected_amount_kopeks,
            expected_currency=expected_currency,
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        is_creation: bool,
    ) -> dict[str, Any]:
        url = f'{self._base_url}{path}'
        try:
            response = await self._http.request(
                method,
                url,
                json=json_body,
                headers=headers,
                auth=self._auth,
            )
        except httpx.HTTPError as exc:
            logger.warning(
                'YooKassa network/timeout error on %s %s: %s',
                method,
                path,
                exc,
            )
            raise ProviderTransientError(
                f'YooKassa transport failure on {method} {path}'
            ) from exc

        if response.status_code == 429 or response.status_code >= 500:
            description = self._extract_error_description(response)
            logger.warning(
                'YooKassa transient HTTP %s on %s %s: %s',
                response.status_code,
                method,
                path,
                description,
            )
            raise ProviderTransientError(
                description,
                status_code=response.status_code,
            )

        if is_creation and response.status_code in (400, 401, 403):
            description = self._extract_error_description(response)
            msg = (
                f'YooKassa permanent HTTP {response.status_code} '
                f'on {method} {path}: {description}'
            )
            logger.error(msg)
            sentry_sdk.capture_message(msg, level='error')
            raise ProviderPermanentError(response.status_code, description)

        if not is_creation and 400 <= response.status_code < 500:
            description = self._extract_error_description(response)
            # Сигнал поднимает владелец операции (сверка): чтение незнакомого
            # объекта по вебхуку с 404 сигнала не стоит.
            logger.warning(
                'YooKassa reconciliation HTTP %s on %s %s: %s',
                response.status_code,
                method,
                path,
                description,
            )
            raise ProviderReconciliationError(response.status_code, description)

        if response.status_code != 200:
            description = self._extract_error_description(response)
            logger.error(
                'Unexpected YooKassa HTTP %s on %s %s: %s',
                response.status_code,
                method,
                path,
                description,
            )
            raise ProviderTransientError(
                description,
                status_code=response.status_code,
            )

        try:
            parsed = response.json()
            if not isinstance(parsed, dict):
                raise ValueError('Response JSON is not an object')
            return parsed
        except ValueError as exc:
            _raise_protocol_anomaly('Invalid JSON payload from YooKassa')
            raise AssertionError from exc  # unreachable

    @staticmethod
    def _extract_error_description(response: httpx.Response) -> str:
        try:
            data = response.json()
            if isinstance(data, dict):
                code = data.get('code') or ''
                desc = data.get('description') or ''
                if code and desc:
                    return f'{code}: {desc}'
                if desc or code:
                    return str(desc or code)
        except ValueError:
            pass
        return f'HTTP {response.status_code}'

    def _to_snapshot(
        self,
        data: dict[str, Any],
        *,
        expected_payment_id: UUID | None,
        expected_amount_kopeks: int | None,
        expected_currency: str | None,
    ) -> ProviderPaymentSnapshot:
        provider_payment_id = str(data.get('id') or '').strip()
        raw_status = str(data.get('status') or '').strip()
        if not provider_payment_id or not raw_status:
            _raise_protocol_anomaly('Missing payment id or status in YooKassa response')

        if raw_status == 'waiting_for_capture':
            _raise_operation_anomaly(
                ProviderStatusAnomalyError(
                    f'YooKassa returned anomalous status waiting_for_capture '
                    f'for payment {provider_payment_id} (invariant 16)',
                    provider_status=raw_status,
                )
            )

        status_map: dict[str, DomainPaymentStatus] = {
            'pending': 'pending',
            'succeeded': 'succeeded',
            'canceled': 'failed',
        }
        if raw_status not in status_map:
            _raise_operation_anomaly(
                ProviderStatusAnomalyError(
                    f'Unknown YooKassa payment status {raw_status!r} '
                    f'for {provider_payment_id}',
                    provider_status=raw_status,
                )
            )
        domain_status = status_map[raw_status]

        amount_obj = data.get('amount')
        if not isinstance(amount_obj, dict):
            _raise_protocol_anomaly('Missing amount object in YooKassa response')
        assert isinstance(amount_obj, dict)
        amount_kopeks = parse_decimal_to_kopeks(str(amount_obj.get('value') or ''))
        currency = str(amount_obj.get('currency') or '').strip()

        metadata_obj = data.get('metadata')
        metadata_payment_id: str | None = None
        if (
            isinstance(metadata_obj, dict)
            and metadata_obj.get('payment_id') is not None
        ):
            metadata_payment_id = str(metadata_obj['payment_id']).strip()

        # Инвариант 17 и BIL-NFR-SEC-04: сверка целостности данных провайдера
        if (
            expected_amount_kopeks is not None
            and amount_kopeks != expected_amount_kopeks
        ):
            _raise_integrity_mismatch(
                f'Amount mismatch for {provider_payment_id}: '
                f'expected {expected_amount_kopeks}, got {amount_kopeks}'
            )
        if expected_currency is not None and currency != expected_currency.strip():
            _raise_integrity_mismatch(
                f'Currency mismatch for {provider_payment_id}: '
                f'expected {expected_currency!r}, got {currency!r}'
            )
        if expected_payment_id is not None and metadata_payment_id != str(
            expected_payment_id
        ):
            _raise_integrity_mismatch(
                f'Metadata payment_id mismatch for {provider_payment_id}: '
                f'expected {expected_payment_id}, got {metadata_payment_id!r}'
            )

        confirmation_url: str | None = None
        confirmation_obj = data.get('confirmation')
        if isinstance(confirmation_obj, dict):
            raw_url = confirmation_obj.get('confirmation_url')
            if isinstance(raw_url, str) and raw_url.strip():
                confirmation_url = raw_url.strip()

        failure_reason: str | None = None
        cancellation_obj = data.get('cancellation_details')
        if isinstance(cancellation_obj, dict):
            raw_reason = cancellation_obj.get('reason')
            if isinstance(raw_reason, str) and raw_reason.strip():
                failure_reason = raw_reason.strip()
        if domain_status == 'failed' and not failure_reason:
            failure_reason = 'canceled_by_provider'

        return ProviderPaymentSnapshot(
            provider_code=self.provider_code,
            provider_payment_id=provider_payment_id,
            domain_status=domain_status,
            amount_kopeks=amount_kopeks,
            currency=currency,
            confirmation_url=confirmation_url,
            failure_reason=failure_reason,
            metadata_payment_id=metadata_payment_id,
            provider_details=data,
        )

    def _to_refund_snapshot(
        self,
        data: dict[str, Any],
        *,
        expected_refund_id: UUID | None,
        expected_provider_payment_id: str | None,
        expected_amount_kopeks: int | None,
        expected_currency: str | None,
    ) -> ProviderRefundSnapshot:
        provider_refund_id = str(data.get('id') or '').strip()
        raw_status = str(data.get('status') or '').strip()
        if not provider_refund_id or not raw_status:
            _raise_protocol_anomaly('Missing refund id or status in YooKassa response')

        status_map: dict[str, DomainRefundStatus] = {
            'pending': 'processing',
            'succeeded': 'succeeded',
            'canceled': 'rejected',
        }
        if raw_status not in status_map:
            _raise_operation_anomaly(
                ProviderStatusAnomalyError(
                    f'Unknown YooKassa refund status {raw_status!r} '
                    f'for {provider_refund_id}',
                    provider_status=raw_status,
                )
            )
        domain_status = status_map[raw_status]

        amount_obj = data.get('amount')
        if not isinstance(amount_obj, dict):
            _raise_protocol_anomaly('Missing amount object in YooKassa refund response')
        assert isinstance(amount_obj, dict)
        amount_kopeks = parse_decimal_to_kopeks(str(amount_obj.get('value') or ''))
        currency = str(amount_obj.get('currency') or '').strip()

        metadata_obj = data.get('metadata')
        metadata_refund_id: str | None = None
        if isinstance(metadata_obj, dict) and metadata_obj.get('refund_id') is not None:
            metadata_refund_id = str(metadata_obj['refund_id']).strip()

        if (
            expected_amount_kopeks is not None
            and amount_kopeks != expected_amount_kopeks
        ):
            _raise_integrity_mismatch(
                f'Refund amount mismatch for {provider_refund_id}: '
                f'expected {expected_amount_kopeks}, got {amount_kopeks}'
            )
        if expected_currency is not None and currency != expected_currency.strip():
            _raise_integrity_mismatch(
                f'Refund currency mismatch for {provider_refund_id}: '
                f'expected {expected_currency!r}, got {currency!r}'
            )
        if expected_refund_id is not None and metadata_refund_id != str(
            expected_refund_id
        ):
            _raise_integrity_mismatch(
                f'Metadata refund_id mismatch for {provider_refund_id}: '
                f'expected {expected_refund_id}, got {metadata_refund_id!r}'
            )

        provider_payment_id: str | None = None
        if data.get('payment_id') is not None:
            provider_payment_id = str(data['payment_id']).strip() or None
        # Доступ отзывается по нашей записи платежа, поэтому возврат обязан
        # относиться к тому же платежу ЮKassa. Ответ без `payment_id` — тоже
        # расхождение: подтвердить исходный платёж нечем.
        if (
            expected_provider_payment_id is not None
            and provider_payment_id != expected_provider_payment_id.strip()
        ):
            _raise_integrity_mismatch(
                f'Refund payment_id mismatch for {provider_refund_id}: '
                f'expected {expected_provider_payment_id!r}, '
                f'got {provider_payment_id!r}'
            )

        rejection_reason: str | None = None
        cancellation_obj = data.get('cancellation_details')
        if isinstance(cancellation_obj, dict):
            raw_reason = cancellation_obj.get('reason')
            if isinstance(raw_reason, str) and raw_reason.strip():
                rejection_reason = raw_reason.strip()
        if domain_status == 'rejected' and not rejection_reason:
            rejection_reason = 'canceled_by_provider'

        return ProviderRefundSnapshot(
            provider_code=self.provider_code,
            provider_refund_id=provider_refund_id,
            provider_payment_id=provider_payment_id,
            domain_status=domain_status,
            amount_kopeks=amount_kopeks,
            currency=currency,
            rejection_reason=rejection_reason,
            metadata_refund_id=metadata_refund_id,
            provider_details=data,
        )
