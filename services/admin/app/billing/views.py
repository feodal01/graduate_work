from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from users.request_id import get_request_id

from billing.clients import (
    AuthLookupUnavailableError,
    BillingPaymentItem,
    BillingSubscriptionItem,
    BillingUnavailableError,
    UserLookupProfile,
    get_auth_lookup_client,
    get_billing_client,
)

SUBSCRIPTION_STATUS_LABELS: dict[str, str] = {
    'active': 'Активна',
    'expired': 'Завершена',
    'refunded': 'Возвращена',
}

PAYMENT_STATUS_LABELS: dict[str, str] = {
    'pending': 'Ожидает оплаты',
    'succeeded': 'Оплачен',
    'failed': 'Не оплачен',
}

REFUND_STATUS_LABELS: dict[str, str] = {
    'processing': 'Возврат в обработке',
    'succeeded': 'Возвращён',
    'rejected': 'Возврат отклонён',
}

DEFAULT_PAGE_LIMIT = 50


@dataclass(frozen=True, slots=True)
class SubscriptionRowView:
    id: UUID
    user_id: UUID
    user_login: str | None
    user_tz: str | None
    payment_id: UUID
    tariff_name: str
    starts_utc_display: str
    ends_utc_display: str
    starts_local_display: str
    ends_local_display: str
    status_code: str
    status_label: str
    amount_display: str


@dataclass(frozen=True, slots=True)
class PaymentRowView:
    id: UUID
    user_id: UUID
    user_login: str | None
    user_tz: str | None
    tariff_name: str
    provider_payment_id: str
    amount_display: str
    status_code: str
    status_label: str
    refund_label: str
    is_stuck: bool
    created_utc_display: str
    created_local_display: str


def format_amount_rub(amount_kopeks: int, currency: str = 'RUB') -> str:
    rubles = amount_kopeks // 100
    kopeks = amount_kopeks % 100
    suffix = '₽' if currency.strip().upper() == 'RUB' else currency.strip()
    if kopeks == 0:
        return f'{rubles} {suffix}'
    return f'{rubles},{kopeks:02d} {suffix}'


def format_utc_datetime(dt: datetime) -> str:
    utc_dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    formatted = utc_dt.strftime('%d.%m.%Y %H:%M')
    return f'{formatted} UTC'


def format_user_tz_datetime(dt: datetime, tz_name: str | None) -> str:
    utc_dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    utc_formatted = utc_dt.strftime('%d.%m.%Y %H:%M')
    if not tz_name:
        return f'{utc_formatted} UTC (пояс недоступен)'
    cleaned_tz = tz_name.strip()
    try:
        zone = ZoneInfo(cleaned_tz)
    except (ZoneInfoNotFoundError, KeyError, ValueError, OSError):
        return f'{utc_formatted} UTC'
    localized = utc_dt.astimezone(zone)
    local_formatted = localized.strftime('%d.%m.%Y %H:%M')
    return f'{local_formatted} ({cleaned_tz})'


def _try_parse_uuid(raw: str) -> UUID | None:
    cleaned = raw.strip()
    if not cleaned:
        return None
    try:
        return UUID(cleaned)
    except ValueError:
        return None


def _parse_pagination(request: HttpRequest) -> tuple[int, int]:
    raw_limit = (request.GET.get('limit') or '').strip()
    raw_offset = (request.GET.get('offset') or '').strip()
    try:
        limit = int(raw_limit) if raw_limit else DEFAULT_PAGE_LIMIT
    except ValueError:
        limit = DEFAULT_PAGE_LIMIT
    limit = max(1, min(100, limit))
    try:
        offset = int(raw_offset) if raw_offset else 0
    except ValueError:
        offset = 0
    offset = max(0, offset)
    return limit, offset


def _validate_date_filters(
    raw_from: str,
    raw_to: str,
) -> tuple[str | None, str | None, str | None]:
    cleaned_from = raw_from.strip()
    cleaned_to = raw_to.strip()
    d_from: date | None = None
    d_to: date | None = None
    if cleaned_from:
        try:
            d_from = date.fromisoformat(cleaned_from)
        except ValueError:
            return None, None, 'Некорректный формат начальной даты фильтра'
    if cleaned_to:
        try:
            d_to = date.fromisoformat(cleaned_to)
        except ValueError:
            return None, None, 'Некорректный формат конечной даты фильтра'
    if d_from is not None and d_to is not None and d_from > d_to:
        return (
            None,
            None,
            'Дата начала периода не может быть позже даты окончания',
        )
    return (
        d_from.isoformat() if d_from else None,
        d_to.isoformat() if d_to else None,
        None,
    )


def _enrich_profiles(
    user_ids: list[UUID],
    *,
    request_id: str,
    known_profiles: dict[UUID, UserLookupProfile] | None = None,
) -> tuple[dict[UUID, UserLookupProfile], bool]:
    profiles: dict[UUID, UserLookupProfile] = dict(known_profiles or {})
    missing = [uid for uid in dict.fromkeys(user_ids) if uid not in profiles]
    if not missing:
        return profiles, False
    try:
        fetched = get_auth_lookup_client().get_by_ids(missing, request_id=request_id)
        profiles.update(fetched)
        return profiles, False
    except AuthLookupUnavailableError:
        return profiles, True


def subscriptions_list(request: HttpRequest) -> HttpResponse:
    request_id = get_request_id(request)
    query = (request.GET.get('q') or '').strip()
    raw_status = (request.GET.get('status') or '').strip()
    status_filter = raw_status if raw_status in SUBSCRIPTION_STATUS_LABELS else None
    raw_date_from = (request.GET.get('date_from') or '').strip()
    raw_date_to = (request.GET.get('date_to') or '').strip()
    limit, offset = _parse_pagination(request)

    date_from, date_to, filter_error = _validate_date_filters(
        raw_date_from,
        raw_date_to,
    )
    if filter_error is not None:
        return render(
            request,
            'admin/billing/subscriptions_list.html',
            {
                'rows': (),
                'total': 0,
                'q': query,
                'status_filter': status_filter or '',
                'date_from': raw_date_from,
                'date_to': raw_date_to,
                'limit': limit,
                'offset': offset,
                'prev_offset': None,
                'next_offset': None,
                'auth_warning': None,
                'filter_error': filter_error,
            },
            status=400,
        )

    known_profiles: dict[UUID, UserLookupProfile] = {}
    auth_warning: str | None = None
    target_user_id: UUID | None = None

    if query:
        parsed_uuid = _try_parse_uuid(query)
        if parsed_uuid is not None:
            target_user_id = parsed_uuid
        else:
            try:
                found = get_auth_lookup_client().find_by_login(
                    query,
                    request_id=request_id,
                )
            except AuthLookupUnavailableError:
                auth_warning = 'Поиск по логину временно недоступен, используйте UUID'
                return render(
                    request,
                    'admin/billing/subscriptions_list.html',
                    {
                        'rows': (),
                        'total': 0,
                        'q': query,
                        'status_filter': status_filter or '',
                        'date_from': date_from or '',
                        'date_to': date_to or '',
                        'limit': limit,
                        'offset': offset,
                        'prev_offset': None,
                        'next_offset': None,
                        'auth_warning': auth_warning,
                        'filter_error': None,
                    },
                )
            if found is None:
                return render(
                    request,
                    'admin/billing/subscriptions_list.html',
                    {
                        'rows': (),
                        'total': 0,
                        'q': query,
                        'status_filter': status_filter or '',
                        'date_from': date_from or '',
                        'date_to': date_to or '',
                        'limit': limit,
                        'offset': offset,
                        'prev_offset': None,
                        'next_offset': None,
                        'auth_warning': None,
                        'filter_error': None,
                    },
                )
            target_user_id = found.user_id
            known_profiles[found.user_id] = found

    try:
        page = get_billing_client().list_subscriptions(
            request_id=request_id,
            user_id=target_user_id,
            status=status_filter,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )
    except BillingUnavailableError:
        return render(
            request,
            'admin/billing/unavailable.html',
            status=503,
        )

    profiles, enrich_degraded = _enrich_profiles(
        [item.user_id for item in page.items],
        request_id=request_id,
        known_profiles=known_profiles,
    )
    if enrich_degraded and auth_warning is None:
        auth_warning = 'Сервис Auth временно недоступен: показаны UUID пользователей и даты в UTC.'

    rows = tuple(_build_subscription_row(item, profiles.get(item.user_id)) for item in page.items)
    prev_offset = max(0, offset - limit) if offset > 0 else None
    next_offset = offset + limit if offset + limit < page.total else None
    return render(
        request,
        'admin/billing/subscriptions_list.html',
        {
            'rows': rows,
            'total': page.total,
            'q': query,
            'status_filter': status_filter or '',
            'date_from': date_from or '',
            'date_to': date_to or '',
            'limit': limit,
            'offset': offset,
            'prev_offset': prev_offset,
            'next_offset': next_offset,
            'auth_warning': auth_warning,
            'filter_error': None,
        },
    )


def payments_list(request: HttpRequest) -> HttpResponse:
    request_id = get_request_id(request)
    query = (request.GET.get('q') or '').strip()
    raw_search_by = (request.GET.get('search_by') or '').strip()
    tab = (request.GET.get('tab') or '').strip()
    raw_status = (request.GET.get('status') or '').strip()
    has_refund_param = (request.GET.get('has_refund') or '').strip().lower()
    stuck_param = (request.GET.get('stuck') or '').strip().lower()
    raw_date_from = (request.GET.get('date_from') or '').strip()
    raw_date_to = (request.GET.get('date_to') or '').strip()
    limit, offset = _parse_pagination(request)

    status_filter: str | None = None
    has_refund: bool | None = None
    stuck = False

    if tab == 'all':
        active_tab = 'all'
    elif tab == 'pending' or (not tab and raw_status == 'pending'):
        status_filter = 'pending'
        active_tab = 'pending'
    elif tab == 'succeeded' or (not tab and raw_status == 'succeeded'):
        status_filter = 'succeeded'
        active_tab = 'succeeded'
    elif tab == 'failed' or (not tab and raw_status == 'failed'):
        status_filter = 'failed'
        active_tab = 'failed'
    elif tab == 'has_refund' or (not tab and has_refund_param == 'true'):
        has_refund = True
        active_tab = 'has_refund'
    elif tab == 'stuck' or (not tab and stuck_param == 'true'):
        stuck = True
        active_tab = 'stuck'
    else:
        active_tab = 'all'

    search_by = (
        raw_search_by
        if raw_search_by in {'user', 'payment_id', 'provider_payment_id'}
        else ('provider_payment_id' if query.startswith('yk_') else 'user')
    )

    date_from, date_to, filter_error = _validate_date_filters(
        raw_date_from,
        raw_date_to,
    )
    if filter_error is not None:
        return render(
            request,
            'admin/billing/payments_list.html',
            {
                'rows': (),
                'total': 0,
                'q': query,
                'search_by': search_by,
                'active_tab': active_tab,
                'date_from': raw_date_from,
                'date_to': raw_date_to,
                'limit': limit,
                'offset': offset,
                'prev_offset': None,
                'next_offset': None,
                'auth_warning': None,
                'filter_error': filter_error,
            },
            status=400,
        )

    known_profiles: dict[UUID, UserLookupProfile] = {}
    auth_warning: str | None = None
    user_id_filter = _try_parse_uuid(request.GET.get('user_id') or '')
    payment_id_filter = _try_parse_uuid(request.GET.get('payment_id') or '')
    provider_id_filter = (request.GET.get('provider_payment_id') or '').strip() or None

    if (
        query
        and user_id_filter is None
        and payment_id_filter is None
        and provider_id_filter is None
    ):
        if search_by == 'payment_id':
            parsed_pid = _try_parse_uuid(query)
            if parsed_pid is None:
                return render(
                    request,
                    'admin/billing/payments_list.html',
                    {
                        'rows': (),
                        'total': 0,
                        'q': query,
                        'search_by': search_by,
                        'active_tab': active_tab,
                        'date_from': date_from or '',
                        'date_to': date_to or '',
                        'limit': limit,
                        'offset': offset,
                        'prev_offset': None,
                        'next_offset': None,
                        'auth_warning': None,
                        'filter_error': 'Некорректный формат UUID платежа',
                    },
                    status=400,
                )
            payment_id_filter = parsed_pid
        elif search_by == 'provider_payment_id':
            provider_id_filter = query
        else:
            parsed_uid = _try_parse_uuid(query)
            if parsed_uid is not None:
                user_id_filter = parsed_uid
            else:
                try:
                    found = get_auth_lookup_client().find_by_login(
                        query,
                        request_id=request_id,
                    )
                except AuthLookupUnavailableError:
                    auth_warning = 'Поиск по логину временно недоступен, используйте UUID'
                    return render(
                        request,
                        'admin/billing/payments_list.html',
                        {
                            'rows': (),
                            'total': 0,
                            'q': query,
                            'search_by': search_by,
                            'active_tab': active_tab,
                            'date_from': date_from or '',
                            'date_to': date_to or '',
                            'limit': limit,
                            'offset': offset,
                            'prev_offset': None,
                            'next_offset': None,
                            'auth_warning': auth_warning,
                            'filter_error': None,
                        },
                    )
                if found is None:
                    return render(
                        request,
                        'admin/billing/payments_list.html',
                        {
                            'rows': (),
                            'total': 0,
                            'q': query,
                            'search_by': search_by,
                            'active_tab': active_tab,
                            'date_from': date_from or '',
                            'date_to': date_to or '',
                            'limit': limit,
                            'offset': offset,
                            'prev_offset': None,
                            'next_offset': None,
                            'auth_warning': None,
                            'filter_error': None,
                        },
                    )
                user_id_filter = found.user_id
                known_profiles[found.user_id] = found

    try:
        billing_client = get_billing_client()
        page = billing_client.list_payments(
            request_id=request_id,
            status=status_filter,
            has_refund=has_refund,
            stuck=stuck,
            user_id=user_id_filter,
            payment_id=payment_id_filter,
            provider_payment_id=provider_id_filter,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )
    except BillingUnavailableError:
        return render(
            request,
            'admin/billing/unavailable.html',
            status=503,
        )

    profiles, enrich_degraded = _enrich_profiles(
        [item.user_id for item in page.items],
        request_id=request_id,
        known_profiles=known_profiles,
    )
    if enrich_degraded and auth_warning is None:
        auth_warning = 'Сервис Auth временно недоступен: показаны UUID пользователей и даты в UTC.'

    rows = tuple(_build_payment_row(item, profiles.get(item.user_id)) for item in page.items)
    prev_offset = max(0, offset - limit) if offset > 0 else None
    next_offset = offset + limit if offset + limit < page.total else None
    return render(
        request,
        'admin/billing/payments_list.html',
        {
            'rows': rows,
            'total': page.total,
            'q': query,
            'search_by': search_by,
            'active_tab': active_tab,
            'date_from': date_from or '',
            'date_to': date_to or '',
            'limit': limit,
            'offset': offset,
            'prev_offset': prev_offset,
            'next_offset': next_offset,
            'auth_warning': auth_warning,
            'filter_error': None,
        },
    )


def payment_detail(request: HttpRequest, payment_id: UUID) -> HttpResponse:
    return render(
        request,
        'admin/billing/payment_detail.html',
        {'payment_id': payment_id},
    )


def _build_subscription_row(
    item: BillingSubscriptionItem,
    profile: UserLookupProfile | None,
) -> SubscriptionRowView:
    tz_name = profile.tz if profile is not None else None
    return SubscriptionRowView(
        id=item.id,
        user_id=item.user_id,
        user_login=profile.login if profile is not None else None,
        user_tz=tz_name,
        payment_id=item.payment_id,
        tariff_name=item.tariff_name,
        starts_utc_display=format_utc_datetime(item.starts_at),
        ends_utc_display=format_utc_datetime(item.ends_at),
        starts_local_display=format_user_tz_datetime(item.starts_at, tz_name),
        ends_local_display=format_user_tz_datetime(item.ends_at, tz_name),
        status_code=item.status,
        status_label=SUBSCRIPTION_STATUS_LABELS.get(item.status, item.status),
        amount_display=format_amount_rub(item.amount_kopeks, item.currency),
    )


def _build_payment_row(
    item: BillingPaymentItem,
    profile: UserLookupProfile | None,
) -> PaymentRowView:
    tz_name = profile.tz if profile is not None else None
    refund_label = (
        REFUND_STATUS_LABELS.get(item.refund_status or '', item.refund_status or '—')
        if item.has_refund
        else '—'
    )
    return PaymentRowView(
        id=item.id,
        user_id=item.user_id,
        user_login=profile.login if profile is not None else None,
        user_tz=tz_name,
        tariff_name=item.tariff_name,
        provider_payment_id=item.provider_payment_id or '—',
        amount_display=format_amount_rub(item.amount_kopeks, item.currency),
        status_code=item.status,
        status_label=PAYMENT_STATUS_LABELS.get(item.status, item.status),
        refund_label=refund_label,
        is_stuck=item.is_stuck,
        created_utc_display=format_utc_datetime(item.created_at),
        created_local_display=format_user_tz_datetime(item.created_at, tz_name),
    )
