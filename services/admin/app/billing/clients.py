from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Protocol
from uuid import UUID

import httpx
from django.conf import settings


class HTTPClient(Protocol):
    def request(self, method: str, url: str, **kwargs: Any) -> Any: ...


class BillingUnavailableError(Exception):
    """Сервис Billing временно недоступен (ADM-NFR-R-02)."""


class BillingContractError(BillingUnavailableError):
    """Ответ Billing не соответствует внутреннему контракту."""


class AuthLookupUnavailableError(Exception):
    """Внутренний поиск пользователей в Auth временно недоступен (ADM-NFR-R-01)."""


@dataclass(frozen=True, slots=True)
class UserLookupProfile:
    user_id: UUID
    login: str
    tz: str


@dataclass(frozen=True, slots=True)
class BillingSubscriptionItem:
    id: UUID
    user_id: UUID
    payment_id: UUID
    tariff_code: str
    tariff_name: str
    starts_at: datetime
    ends_at: datetime
    status: str
    amount_kopeks: int
    currency: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class BillingSubscriptionsPage:
    total: int
    items: tuple[BillingSubscriptionItem, ...]


@dataclass(frozen=True, slots=True)
class BillingPaymentItem:
    id: UUID
    user_id: UUID
    tariff_code: str
    tariff_name: str
    provider_code: str
    provider_payment_id: str | None
    amount_kopeks: int
    currency: str
    status: str
    failure_reason: str | None
    has_refund: bool
    refund_status: str | None
    active_refund_id: UUID | None
    is_stuck: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class BillingPaymentsPage:
    total: int
    items: tuple[BillingPaymentItem, ...]


def _parse_iso_datetime(value: Any) -> datetime:
    if not isinstance(value, str):
        raise BillingContractError('Expected ISO-8601 datetime string')
    normalized = value.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise BillingContractError('Invalid ISO-8601 datetime string') from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_uuid(value: Any) -> UUID:
    if not isinstance(value, str):
        raise BillingContractError('Expected UUID string')
    try:
        return UUID(value)
    except ValueError as exc:
        raise BillingContractError('Invalid UUID string') from exc


class AuthUserLookupClient:
    """Синхронный клиент внутреннего эндпоинта AUTH-API-02 (/api/v1/internal/users/lookup)."""

    def __init__(
        self,
        *,
        base_url: str,
        service_api_key: str,
        http_client: HTTPClient,
    ) -> None:
        self._base_url = base_url.rstrip('/')
        self._service_api_key = service_api_key
        self._http_client = http_client

    def find_by_login(
        self,
        login: str,
        *,
        request_id: str,
    ) -> UserLookupProfile | None:
        cleaned = login.strip()
        if not cleaned:
            return None
        items = self._lookup({'login': cleaned}, request_id=request_id)
        return items[0] if items else None

    def get_by_ids(
        self,
        user_ids: list[UUID],
        *,
        request_id: str,
    ) -> dict[UUID, UserLookupProfile]:
        unique_ids = list(dict.fromkeys(user_ids))
        if not unique_ids:
            return {}
        items = self._lookup(
            {'user_ids': [str(uid) for uid in unique_ids[:100]]},
            request_id=request_id,
        )
        return {item.user_id: item for item in items}

    def _lookup(
        self,
        payload: dict[str, Any],
        *,
        request_id: str,
    ) -> list[UserLookupProfile]:
        try:
            response = self._http_client.request(
                'POST',
                f'{self._base_url}/users/lookup',
                headers={
                    'X-Service-Api-Key': self._service_api_key,
                    'X-Request-Id': request_id,
                },
                json=payload,
            )
        except httpx.HTTPError as exc:
            raise AuthLookupUnavailableError('Auth lookup request failed') from exc

        if response.status_code != 200:
            raise AuthLookupUnavailableError(f'Auth lookup returned status {response.status_code}')
        try:
            body = response.json()
            raw_items = body['items']
            if not isinstance(raw_items, list):
                raise TypeError('items must be a list')
            result: list[UserLookupProfile] = []
            for entry in raw_items:
                result.append(
                    UserLookupProfile(
                        user_id=UUID(str(entry['user_id'])),
                        login=str(entry['login']),
                        tz=str(entry['tz']),
                    )
                )
            return result
        except (ValueError, KeyError, TypeError) as exc:
            raise AuthLookupUnavailableError('Invalid Auth lookup response') from exc


class BillingAdminClient:
    """Синхронный клиент внутреннего API сервиса Billing (BIL-API-06..14)."""

    def __init__(
        self,
        *,
        base_url: str,
        service_api_key: str,
        http_client: HTTPClient,
    ) -> None:
        self._base_url = base_url.rstrip('/')
        self._service_api_key = service_api_key
        self._http_client = http_client

    def list_subscriptions(
        self,
        *,
        request_id: str,
        user_id: UUID | None = None,
        status: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> BillingSubscriptionsPage:
        params: dict[str, Any] = {'limit': limit, 'offset': offset}
        if user_id is not None:
            params['user_id'] = str(user_id)
        if status:
            params['status'] = status
        if date_from:
            params['date_from'] = date_from
        if date_to:
            params['date_to'] = date_to

        body = self._get_json('/subscriptions', params=params, request_id=request_id)
        try:
            total = int(body['total'])
            raw_items = body['items']
            if not isinstance(raw_items, list):
                raise TypeError('items must be a list')
            items = tuple(
                BillingSubscriptionItem(
                    id=_parse_uuid(item['id']),
                    user_id=_parse_uuid(item['user_id']),
                    payment_id=_parse_uuid(item['payment_id']),
                    tariff_code=str(item['tariff_code']),
                    tariff_name=str(item['tariff_name']),
                    starts_at=_parse_iso_datetime(item['starts_at']),
                    ends_at=_parse_iso_datetime(item['ends_at']),
                    status=str(item['status']),
                    amount_kopeks=int(item['amount_kopeks']),
                    currency=str(item['currency']),
                    created_at=_parse_iso_datetime(item['created_at']),
                )
                for item in raw_items
            )
            return BillingSubscriptionsPage(total=total, items=items)
        except (KeyError, TypeError, ValueError) as exc:
            raise BillingContractError('Invalid subscriptions payload') from exc

    def list_payments(
        self,
        *,
        request_id: str,
        status: str | None = None,
        has_refund: bool | None = None,
        stuck: bool = False,
        user_id: UUID | None = None,
        payment_id: UUID | None = None,
        provider_payment_id: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> BillingPaymentsPage:
        params: dict[str, Any] = {'limit': limit, 'offset': offset}
        if status:
            params['status'] = status
        if has_refund is not None:
            params['has_refund'] = 'true' if has_refund else 'false'
        if stuck:
            params['stuck'] = 'true'
        if user_id is not None:
            params['user_id'] = str(user_id)
        if payment_id is not None:
            params['id'] = str(payment_id)
        if provider_payment_id:
            params['provider_payment_id'] = provider_payment_id
        if date_from:
            params['date_from'] = date_from
        if date_to:
            params['date_to'] = date_to

        body = self._get_json('/payments', params=params, request_id=request_id)
        try:
            total = int(body['total'])
            raw_items = body['items']
            if not isinstance(raw_items, list):
                raise TypeError('items must be a list')
            items = tuple(
                BillingPaymentItem(
                    id=_parse_uuid(item['id']),
                    user_id=_parse_uuid(item['user_id']),
                    tariff_code=str(item['tariff_code']),
                    tariff_name=str(item['tariff_name']),
                    provider_code=str(item['provider_code']),
                    provider_payment_id=(
                        str(item['provider_payment_id'])
                        if item.get('provider_payment_id') is not None
                        else None
                    ),
                    amount_kopeks=int(item['amount_kopeks']),
                    currency=str(item['currency']),
                    status=str(item['status']),
                    failure_reason=(
                        str(item['failure_reason'])
                        if item.get('failure_reason') is not None
                        else None
                    ),
                    has_refund=bool(item.get('has_refund', False)),
                    refund_status=(
                        str(item['refund_status'])
                        if item.get('refund_status') is not None
                        else None
                    ),
                    active_refund_id=(
                        _parse_uuid(item['active_refund_id'])
                        if item.get('active_refund_id') is not None
                        else None
                    ),
                    is_stuck=bool(item.get('is_stuck', False)),
                    created_at=_parse_iso_datetime(item['created_at']),
                    updated_at=_parse_iso_datetime(item['updated_at']),
                )
                for item in raw_items
            )
            return BillingPaymentsPage(total=total, items=items)
        except (KeyError, TypeError, ValueError) as exc:
            raise BillingContractError('Invalid payments payload') from exc

    def _get_json(
        self,
        path: str,
        *,
        params: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        try:
            response = self._http_client.request(
                'GET',
                f'{self._base_url}{path}',
                params=params,
                headers={
                    'X-Service-Api-Key': self._service_api_key,
                    'X-Request-Id': request_id,
                },
            )
        except httpx.HTTPError as exc:
            raise BillingUnavailableError('Billing service request failed') from exc

        if response.status_code != 200:
            raise BillingUnavailableError(f'Billing service returned status {response.status_code}')
        try:
            data = response.json()
        except ValueError as exc:
            raise BillingContractError('Billing response is not valid JSON') from exc
        if not isinstance(data, dict):
            raise BillingContractError('Billing response must be a JSON object')
        return data


@lru_cache(maxsize=1)
def get_billing_client() -> BillingAdminClient:
    timeout = httpx.Timeout(
        getattr(settings, 'BILLING_CLIENT_TIMEOUT_SECONDS', 3.0),
        connect=getattr(settings, 'BILLING_CLIENT_CONNECT_TIMEOUT_SECONDS', 0.5),
    )
    return BillingAdminClient(
        base_url=settings.BILLING_INTERNAL_URL,
        service_api_key=settings.SERVICE_API_KEY,
        http_client=httpx.Client(timeout=timeout),
    )


@lru_cache(maxsize=1)
def get_auth_lookup_client() -> AuthUserLookupClient:
    timeout = httpx.Timeout(
        getattr(settings, 'AUTH_CLIENT_REQUEST_TIMEOUT_SECONDS', 1.0),
        connect=getattr(settings, 'AUTH_CLIENT_CONNECT_TIMEOUT_SECONDS', 0.5),
    )
    return AuthUserLookupClient(
        base_url=settings.AUTH_INTERNAL_URL,
        service_api_key=settings.SERVICE_API_KEY,
        http_client=httpx.Client(timeout=timeout),
    )
