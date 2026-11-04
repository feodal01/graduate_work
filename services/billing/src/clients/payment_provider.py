import logging
import re
from dataclasses import dataclass
from decimal import Decimal, Inexact, InvalidOperation
from typing import Any, Literal, Protocol
from urllib.parse import quote
from uuid import UUID

import httpx
import sentry_sdk

logger = logging.getLogger(__name__)

DomainPaymentStatus = Literal['pending', 'succeeded', 'failed']
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


def _raise_protocol_anomaly(message: str) -> None:
    logger.error(message)
    sentry_sdk.capture_message(message, level='error')
    raise ProviderProtocolAnomalyError(message)


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


def validate_provider_operation_id(provider_id: str) -> str:
    cleaned = provider_id.strip()
    if not cleaned or not _SAFE_PROVIDER_ID_RE.fullmatch(cleaned):
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
    ) -> None:
        self._http = http_client
        self._auth = httpx.BasicAuth(username=shop_id, password=secret_key)
        self._base_url = base_url.rstrip('/')

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
            msg = (
                f'YooKassa reconciliation HTTP {response.status_code} '
                f'on {method} {path}: {description}'
            )
            logger.error(msg)
            sentry_sdk.capture_message(msg, level='error')
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
            _raise_protocol_anomaly(
                f'YooKassa returned anomalous status waiting_for_capture '
                f'for payment {provider_payment_id} (invariant 16)'
            )

        status_map: dict[str, DomainPaymentStatus] = {
            'pending': 'pending',
            'succeeded': 'succeeded',
            'canceled': 'failed',
        }
        if raw_status not in status_map:
            _raise_protocol_anomaly(
                f'Unknown YooKassa payment status {raw_status!r} '
                f'for {provider_payment_id}'
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
