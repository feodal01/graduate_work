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


class BillingNotFoundError(Exception):
    """Запрошенная операция в Billing не найдена (404)."""


class BillingConflictError(Exception):
    """Операция отклонена доменными правилами Billing (409)."""

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(detail)


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


@dataclass(frozen=True, slots=True)
class BillingRefundItem:
    id: UUID
    payment_id: UUID
    subscription_id: UUID
    provider_code: str
    provider_refund_id: str | None
    amount_kopeks: int
    currency: str
    status: str
    reason: str
    rejection_reason: str | None
    admin_user_id: UUID
    created_at: datetime
    updated_at: datetime
    can_reconcile: bool


@dataclass(frozen=True, slots=True)
class BillingOperationEventItem:
    id: UUID
    operation_type: str
    operation_id: UUID
    from_status: str | None
    to_status: str
    source: str
    details: dict[str, Any] | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class BillingOutboxTaskItem:
    id: UUID
    target_service: str
    task_type: str
    access_version: int | None
    status: str
    attempts: int
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    can_retry_or_close: bool
    payment_id: UUID | None = None
    user_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class BillingOutboxTasksPage:
    total: int
    items: tuple[BillingOutboxTaskItem, ...]


@dataclass(frozen=True, slots=True)
class BillingAdminActionItem:
    id: UUID
    admin_user_id: UUID
    action_type: str
    target_type: str
    target_id: UUID
    payment_id: UUID | None
    target_status: str | None
    reason: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class BillingAdminActionsPage:
    total: int
    items: tuple[BillingAdminActionItem, ...]


@dataclass(frozen=True, slots=True)
class BillingReconcileResult:
    operation_type: str
    operation_id: UUID
    payment_id: UUID
    status: str
    transitioned: bool


@dataclass(frozen=True, slots=True)
class BillingPaymentCardData:
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
    created_at: datetime
    updated_at: datetime
    can_refund: bool
    can_reconcile_payment: bool
    subscription: BillingSubscriptionItem | None
    refunds: tuple[BillingRefundItem, ...]
    operation_events: tuple[BillingOperationEventItem, ...]
    outbox_tasks: tuple[BillingOutboxTaskItem, ...]


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

    def get_payment_card(
        self,
        payment_id: UUID,
        *,
        request_id: str,
    ) -> BillingPaymentCardData:
        body = self._get_json(
            f'/payments/{payment_id}',
            params={},
            request_id=request_id,
            allow_not_found=True,
        )
        try:
            raw_sub = body.get('subscription')
            sub_item: BillingSubscriptionItem | None = None
            if isinstance(raw_sub, dict):
                sub_item = BillingSubscriptionItem(
                    id=_parse_uuid(raw_sub['id']),
                    user_id=_parse_uuid(raw_sub['user_id']),
                    payment_id=_parse_uuid(raw_sub['payment_id']),
                    tariff_code=str(raw_sub['tariff_code']),
                    tariff_name=str(raw_sub['tariff_name']),
                    starts_at=_parse_iso_datetime(raw_sub['starts_at']),
                    ends_at=_parse_iso_datetime(raw_sub['ends_at']),
                    status=str(raw_sub['status']),
                    amount_kopeks=int(raw_sub['amount_kopeks']),
                    currency=str(raw_sub['currency']),
                    created_at=_parse_iso_datetime(raw_sub['created_at']),
                )

            raw_refunds = body.get('refunds', [])
            if not isinstance(raw_refunds, list):
                raise TypeError('refunds must be a list')
            refund_items = tuple(
                BillingRefundItem(
                    id=_parse_uuid(r['id']),
                    payment_id=_parse_uuid(r['payment_id']),
                    subscription_id=_parse_uuid(r['subscription_id']),
                    provider_code=str(r['provider_code']),
                    provider_refund_id=(
                        str(r['provider_refund_id'])
                        if r.get('provider_refund_id') is not None
                        else None
                    ),
                    amount_kopeks=int(r['amount_kopeks']),
                    currency=str(r['currency']),
                    status=str(r['status']),
                    reason=str(r['reason']),
                    rejection_reason=(
                        str(r['rejection_reason'])
                        if r.get('rejection_reason') is not None
                        else None
                    ),
                    admin_user_id=_parse_uuid(r['admin_user_id']),
                    created_at=_parse_iso_datetime(r['created_at']),
                    updated_at=_parse_iso_datetime(r['updated_at']),
                    can_reconcile=bool(r.get('can_reconcile', False)),
                )
                for r in raw_refunds
            )

            raw_events = body.get('operation_events', [])
            if not isinstance(raw_events, list):
                raise TypeError('operation_events must be a list')
            event_items = tuple(
                BillingOperationEventItem(
                    id=_parse_uuid(ev['id']),
                    operation_type=str(ev['operation_type']),
                    operation_id=_parse_uuid(ev['operation_id']),
                    from_status=(
                        str(ev['from_status']) if ev.get('from_status') is not None else None
                    ),
                    to_status=str(ev['to_status']),
                    source=str(ev['source']),
                    details=(dict(ev['details']) if isinstance(ev.get('details'), dict) else None),
                    created_at=_parse_iso_datetime(ev['created_at']),
                )
                for ev in raw_events
            )

            raw_tasks = body.get('outbox_tasks', [])
            if not isinstance(raw_tasks, list):
                raise TypeError('outbox_tasks must be a list')
            task_items = tuple(
                BillingOutboxTaskItem(
                    id=_parse_uuid(t['id']),
                    target_service=str(t['target_service']),
                    task_type=str(t['task_type']),
                    access_version=(
                        int(t['access_version']) if t.get('access_version') is not None else None
                    ),
                    status=str(t['status']),
                    attempts=int(t['attempts']),
                    last_error=(str(t['last_error']) if t.get('last_error') is not None else None),
                    created_at=_parse_iso_datetime(t['created_at']),
                    updated_at=_parse_iso_datetime(t['updated_at']),
                    can_retry_or_close=bool(t.get('can_retry_or_close', False)),
                    payment_id=(
                        _parse_uuid(t['payment_id']) if t.get('payment_id') is not None else None
                    ),
                    user_id=_parse_uuid(t['user_id']) if t.get('user_id') is not None else None,
                )
                for t in raw_tasks
            )

            return BillingPaymentCardData(
                id=_parse_uuid(body['id']),
                user_id=_parse_uuid(body['user_id']),
                tariff_code=str(body['tariff_code']),
                tariff_name=str(body['tariff_name']),
                provider_code=str(body['provider_code']),
                provider_payment_id=(
                    str(body['provider_payment_id'])
                    if body.get('provider_payment_id') is not None
                    else None
                ),
                amount_kopeks=int(body['amount_kopeks']),
                currency=str(body['currency']),
                status=str(body['status']),
                failure_reason=(
                    str(body['failure_reason']) if body.get('failure_reason') is not None else None
                ),
                created_at=_parse_iso_datetime(body['created_at']),
                updated_at=_parse_iso_datetime(body['updated_at']),
                can_refund=bool(body.get('can_refund', False)),
                can_reconcile_payment=bool(body.get('can_reconcile_payment', False)),
                subscription=sub_item,
                refunds=refund_items,
                operation_events=event_items,
                outbox_tasks=task_items,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise BillingContractError('Invalid payment card payload') from exc

    def list_outbox_tasks(
        self,
        *,
        request_id: str,
        target_service: str | None = None,
        status: str | None = None,
        user_id: UUID | None = None,
        payment_id: UUID | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> BillingOutboxTasksPage:
        params: dict[str, Any] = {'limit': limit, 'offset': offset}
        if target_service:
            params['target_service'] = target_service
        if status:
            params['status'] = status
        if user_id is not None:
            params['user_id'] = str(user_id)
        if payment_id is not None:
            params['payment_id'] = str(payment_id)
        if date_from:
            params['date_from'] = date_from
        if date_to:
            params['date_to'] = date_to

        body = self._get_json('/outbox-tasks', params=params, request_id=request_id)
        try:
            total = int(body['total'])
            raw_items = body['items']
            if not isinstance(raw_items, list):
                raise TypeError('items must be a list')
            items = tuple(
                BillingOutboxTaskItem(
                    id=_parse_uuid(t['id']),
                    target_service=str(t['target_service']),
                    task_type=str(t['task_type']),
                    access_version=(
                        int(t['access_version']) if t.get('access_version') is not None else None
                    ),
                    status=str(t['status']),
                    attempts=int(t['attempts']),
                    last_error=(str(t['last_error']) if t.get('last_error') is not None else None),
                    created_at=_parse_iso_datetime(t['created_at']),
                    updated_at=_parse_iso_datetime(t['updated_at']),
                    can_retry_or_close=bool(t.get('can_retry_or_close', False)),
                    payment_id=(
                        _parse_uuid(t['payment_id']) if t.get('payment_id') is not None else None
                    ),
                    user_id=_parse_uuid(t['user_id']) if t.get('user_id') is not None else None,
                )
                for t in raw_items
            )
            return BillingOutboxTasksPage(total=total, items=items)
        except (KeyError, TypeError, ValueError) as exc:
            raise BillingContractError('Invalid outbox tasks payload') from exc

    def create_refund(
        self,
        payment_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        request_id: str,
        refund_id: UUID | None = None,
    ) -> BillingRefundItem:
        json_body = {
            'admin_user_id': str(admin_user_id),
            'reason': reason,
        }
        if refund_id is not None:
            # UUID попытки из формы: повтор той же формы не создаёт вторую
            # попытку возврата (BIL-NFR-R-02).
            json_body['refund_id'] = str(refund_id)
        body = self._post_json(
            f'/payments/{payment_id}/refunds',
            json_body=json_body,
            request_id=request_id,
        )
        try:
            return BillingRefundItem(
                id=_parse_uuid(body['id']),
                payment_id=_parse_uuid(body['payment_id']),
                subscription_id=_parse_uuid(body['subscription_id']),
                provider_code=str(body['provider_code']),
                provider_refund_id=(
                    str(body['provider_refund_id'])
                    if body.get('provider_refund_id') is not None
                    else None
                ),
                amount_kopeks=int(body['amount_kopeks']),
                currency=str(body['currency']),
                status=str(body['status']),
                reason=str(body['reason']),
                rejection_reason=(
                    str(body['rejection_reason'])
                    if body.get('rejection_reason') is not None
                    else None
                ),
                admin_user_id=_parse_uuid(body['admin_user_id']),
                created_at=_parse_iso_datetime(body['created_at']),
                updated_at=_parse_iso_datetime(body['updated_at']),
                can_reconcile=bool(body.get('can_reconcile', False)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise BillingContractError('Invalid refund payload') from exc

    def retry_outbox_task(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        request_id: str,
        reason: str | None = None,
    ) -> BillingOutboxTaskItem:
        json_body: dict[str, Any] = {'admin_user_id': str(admin_user_id)}
        if reason:
            json_body['reason'] = reason
        body = self._post_json(
            f'/outbox-tasks/{task_id}/retry',
            json_body=json_body,
            request_id=request_id,
        )
        return self._parse_outbox_task_dict(body)

    def close_outbox_task(
        self,
        task_id: UUID,
        *,
        admin_user_id: UUID,
        reason: str,
        request_id: str,
    ) -> BillingOutboxTaskItem:
        body = self._post_json(
            f'/outbox-tasks/{task_id}/close',
            json_body={
                'admin_user_id': str(admin_user_id),
                'reason': reason,
            },
            request_id=request_id,
        )
        return self._parse_outbox_task_dict(body)

    @staticmethod
    def _parse_outbox_task_dict(t: dict[str, Any]) -> BillingOutboxTaskItem:
        try:
            return BillingOutboxTaskItem(
                id=_parse_uuid(t['id']),
                target_service=str(t['target_service']),
                task_type=str(t['task_type']),
                access_version=(
                    int(t['access_version']) if t.get('access_version') is not None else None
                ),
                status=str(t['status']),
                attempts=int(t['attempts']),
                last_error=(str(t['last_error']) if t.get('last_error') is not None else None),
                created_at=_parse_iso_datetime(t['created_at']),
                updated_at=_parse_iso_datetime(t['updated_at']),
                can_retry_or_close=bool(t.get('can_retry_or_close', False)),
                payment_id=(
                    _parse_uuid(t['payment_id']) if t.get('payment_id') is not None else None
                ),
                user_id=_parse_uuid(t['user_id']) if t.get('user_id') is not None else None,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise BillingContractError('Invalid outbox task payload') from exc

    def list_admin_actions(
        self,
        *,
        request_id: str,
        admin_user_id: UUID | None = None,
        action_type: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> BillingAdminActionsPage:
        params: dict[str, Any] = {'limit': limit, 'offset': offset}
        if admin_user_id is not None:
            params['admin_user_id'] = str(admin_user_id)
        if action_type:
            params['action_type'] = action_type
        if date_from:
            params['date_from'] = date_from
        if date_to:
            params['date_to'] = date_to

        body = self._get_json('/admin-actions', params=params, request_id=request_id)
        try:
            total = int(body['total'])
            raw_items = body['items']
            if not isinstance(raw_items, list):
                raise TypeError('items must be a list')
            items = tuple(
                BillingAdminActionItem(
                    id=_parse_uuid(item['id']),
                    admin_user_id=_parse_uuid(item['admin_user_id']),
                    action_type=str(item['action_type']),
                    target_type=str(item['target_type']),
                    target_id=_parse_uuid(item['target_id']),
                    payment_id=(
                        _parse_uuid(item['payment_id'])
                        if item.get('payment_id') is not None
                        else None
                    ),
                    target_status=(
                        str(item['target_status'])
                        if item.get('target_status') is not None
                        else None
                    ),
                    reason=str(item['reason']),
                    created_at=_parse_iso_datetime(item['created_at']),
                )
                for item in raw_items
            )
            return BillingAdminActionsPage(total=total, items=items)
        except (KeyError, TypeError, ValueError) as exc:
            raise BillingContractError('Invalid admin actions payload') from exc

    def reconcile_operation(
        self,
        operation_type: str,
        operation_id: UUID,
        *,
        request_id: str,
    ) -> BillingReconcileResult:
        body = self._post_json(
            f'/operations/{operation_type}/{operation_id}/reconcile',
            json_body={},
            request_id=request_id,
        )
        try:
            return BillingReconcileResult(
                operation_type=str(body['operation_type']),
                operation_id=_parse_uuid(body['operation_id']),
                payment_id=_parse_uuid(body['payment_id']),
                status=str(body['status']),
                transitioned=bool(body.get('transitioned', False)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise BillingContractError('Invalid reconcile payload') from exc

    def _post_json(
        self,
        path: str,
        *,
        json_body: dict[str, Any],
        request_id: str,
    ) -> dict[str, Any]:
        try:
            response = self._http_client.request(
                'POST',
                f'{self._base_url}{path}',
                json=json_body,
                headers={
                    'X-Service-Api-Key': self._service_api_key,
                    'X-Request-Id': request_id,
                },
            )
        except httpx.HTTPError as exc:
            raise BillingUnavailableError('Billing service request failed') from exc

        if response.status_code == 404:
            raise BillingNotFoundError('Resource not found in Billing')
        if response.status_code == 409:
            detail = 'Операция запрещена правилами биллинга'
            try:
                err_body = response.json()
                if isinstance(err_body, dict) and err_body.get('detail'):
                    detail = str(err_body['detail'])
            except ValueError:
                pass
            raise BillingConflictError(detail)
        if response.status_code not in (200, 201):
            raise BillingUnavailableError(f'Billing service returned status {response.status_code}')
        try:
            data = response.json()
        except ValueError as exc:
            raise BillingContractError('Billing response is not valid JSON') from exc
        if not isinstance(data, dict):
            raise BillingContractError('Billing response must be a JSON object')
        return data

    def _get_json(
        self,
        path: str,
        *,
        params: dict[str, Any],
        request_id: str,
        allow_not_found: bool = False,
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

        if allow_not_found and response.status_code == 404:
            raise BillingNotFoundError('Resource not found in Billing')
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
