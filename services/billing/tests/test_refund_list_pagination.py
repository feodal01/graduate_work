"""Розыск возврата по `metadata` просматривает все страницы списка (инв. 14)."""

from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import ValidationError

from clients.payment_provider import (
    ProviderProtocolAnomalyError,
    YooKassaProviderAdapter,
)
from clients.payment_provider_registry import (
    PaymentProviderRegistry,
    build_payment_provider_registry,
)
from core.config import PaymentProvidersSettings, YooKassaSettings
from services.reconciler import PaymentReconciler
from tests.fakes import (
    FakePaymentRow,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
)

PAYMENT = 'yk_pay_ours'
OUR_PROVIDER_REFUND = 'yk_ref_ours'

Page = dict[str, Any]


def _item(provider_refund_id: str, refund_id: UUID) -> dict[str, Any]:
    return {
        'id': provider_refund_id,
        'status': 'succeeded',
        'payment_id': PAYMENT,
        'amount': {'value': '299.00', 'currency': 'RUB'},
        'metadata': {'refund_id': str(refund_id)},
    }


def _foreign_page(next_cursor: str | None) -> Page:
    """Полная страница возвратов того же платежа, созданных мимо Биллинга."""
    page: Page = {
        'type': 'list',
        'items': [_item(f'yk_ref_other_{n}', uuid4()) for n in range(100)],
    }
    if next_cursor is not None:
        page['next_cursor'] = next_cursor
    return page


def _adapter(
    page_for_cursor: Callable[[str | None], Page],
    queries: list[dict[str, str]],
    *,
    refund_id: UUID | None = None,
    page_size: int = 100,
    max_pages: int = 10,
) -> YooKassaProviderAdapter:
    """Адаптер поверх подставной ЮKassa; `queries` копит параметры списка."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == 'POST':
            # Ответ на создание потерян: номер возврата ЮKassa неизвестен.
            return httpx.Response(503, json={'type': 'error'})
        if request.url.path == '/v3/refunds':
            params = dict(request.url.params)
            queries.append(params)
            return httpx.Response(200, json=page_for_cursor(params.get('cursor')))
        assert refund_id is not None
        return httpx.Response(200, json=_item(OUR_PROVIDER_REFUND, refund_id))

    return YooKassaProviderAdapter(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        shop_id='shop-1',
        secret_key='secret-1',
        refund_list_page_size=page_size,
        refund_list_max_pages=max_pages,
    )


def _two_pages(refund_id: UUID) -> Callable[[str | None], Page]:
    """Первая страница чужая, нужный возврат приходит на второй."""

    def page_for_cursor(cursor: str | None) -> Page:
        if cursor == 'page-2':
            return {'type': 'list', 'items': [_item(OUR_PROVIDER_REFUND, refund_id)]}
        return _foreign_page('page-2')

    return page_for_cursor


async def test_refund_on_second_page_is_found() -> None:
    """Возврат со второй страницы находится: поиск идёт по `next_cursor`."""
    refund_id = uuid4()
    queries: list[dict[str, str]] = []
    adapter = _adapter(_two_pages(refund_id), queries)

    found = await adapter.find_refund_by_metadata(PAYMENT, refund_id=refund_id)

    assert found is not None
    assert found.provider_refund_id == OUR_PROVIDER_REFUND
    assert queries == [
        {'payment_id': PAYMENT, 'limit': '100'},
        {'payment_id': PAYMENT, 'limit': '100', 'cursor': 'page-2'},
    ]


async def test_refund_is_absent_only_after_last_page() -> None:
    """«Не найдено» возвращается, когда просмотрена страница без `next_cursor`."""
    queries: list[dict[str, str]] = []

    def page_for_cursor(cursor: str | None) -> Page:
        return _foreign_page('page-2' if cursor is None else None)

    adapter = _adapter(page_for_cursor, queries)

    found = await adapter.find_refund_by_metadata(PAYMENT, refund_id=uuid4())

    assert found is None
    assert [query.get('cursor') for query in queries] == [None, 'page-2']


@pytest.mark.parametrize(
    ('next_cursor_for', 'expected_requests'),
    [
        # ЮKassa зациклилась: вернула курсор, по которому мы уже ходили.
        (lambda cursor: 'same', 2),
        # Список длиннее предела обхода: 10 страниц по 100 записей.
        (lambda cursor: f'after-{cursor}', 10),
        # Курсор не строка.
        (lambda cursor: 42, 1),
    ],
)
async def test_unfinished_listing_is_an_error_not_absence(
    next_cursor_for: Callable[[str | None], Any],
    expected_requests: int,
) -> None:
    """Обход, не дошедший до последней страницы, не выдаётся за «не найдено»."""
    queries: list[dict[str, str]] = []

    def page_for_cursor(cursor: str | None) -> Page:
        return {'type': 'list', 'items': [], 'next_cursor': next_cursor_for(cursor)}

    adapter = _adapter(page_for_cursor, queries)

    with pytest.raises(ProviderProtocolAnomalyError):
        await adapter.find_refund_by_metadata(PAYMENT, refund_id=uuid4())
    assert len(queries) == expected_requests


async def test_page_size_and_page_cap_come_from_adapter_settings() -> None:
    """Размер страницы и предел обхода берутся из параметров адаптера."""
    queries: list[dict[str, str]] = []

    def page_for_cursor(cursor: str | None) -> Page:
        return {'type': 'list', 'items': [], 'next_cursor': f'after-{cursor}'}

    adapter = _adapter(
        page_for_cursor,
        queries,
        page_size=25,
        max_pages=3,
    )

    with pytest.raises(ProviderProtocolAnomalyError, match='exceeds 3 pages'):
        await adapter.find_refund_by_metadata(PAYMENT, refund_id=uuid4())
    assert [query['limit'] for query in queries] == ['25', '25', '25']


async def test_registry_passes_refund_list_settings_to_adapter() -> None:
    """Настройки `YOOKASSA_REFUND_LIST_*` доходят до адаптера ЮKassa."""
    registry = build_payment_provider_registry(
        PaymentProvidersSettings(
            yookassa=YooKassaSettings(refund_list_page_size=50, refund_list_max_pages=4)
        )
    )
    try:
        adapter = registry.get('yookassa')
        assert isinstance(adapter, YooKassaProviderAdapter)
        assert adapter._refund_list_page_size == 50
        assert adapter._refund_list_max_pages == 4
    finally:
        await registry.aclose()


@pytest.mark.parametrize(
    ('page_size', 'max_pages'),
    [(101, 10), (0, 10), (100, 0)],
)
def test_refund_list_settings_are_validated(page_size: int, max_pages: int) -> None:
    """ЮKassa отдаёт до 100 записей за запрос; пустой обход не допускается."""
    with pytest.raises(ValidationError):
        YooKassaSettings(
            refund_list_page_size=page_size, refund_list_max_pages=max_pages
        )


def _reconciler_with_lost_refund(
    adapter: YooKassaProviderAdapter,
) -> tuple[PaymentReconciler, InMemoryPurchaseRepository, UUID, UUID]:
    repo = InMemoryPurchaseRepository()
    now_utc = datetime.now(UTC)
    user_id, tariff_id, pay_id, sub_id = uuid4(), uuid4(), uuid4(), uuid4()
    repo.payments[pay_id] = FakePaymentRow(
        id=pay_id,
        user_id=user_id,
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='succeeded',
        idempotency_key=pay_id,
        provider_payment_id=PAYMENT,
        created_at=now_utc - timedelta(days=3),
        updated_at=now_utc - timedelta(days=3),
    )
    repo.subscriptions[sub_id] = FakeSubscriptionRow(
        id=sub_id,
        user_id=user_id,
        tariff_id=tariff_id,
        payment_id=pay_id,
        starts_at=now_utc - timedelta(days=3),
        ends_at=now_utc + timedelta(days=27),
        created_at=now_utc - timedelta(days=3),
    )
    repo.access_versions[user_id] = 1
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(adapter),
        public_base_url='http://localhost',
    )
    return reconciler, repo, pay_id, user_id


async def _lose_refund_and_expire_key(
    reconciler: PaymentReconciler,
    repo: InMemoryPurchaseRepository,
    pay_id: UUID,
    refund_id: UUID,
) -> None:
    """Создание возврата сорвалось, а 24 часа ключа идемпотентности прошли."""
    outcome = await reconciler.create_or_reconcile_refund(
        pay_id, admin_user_id=uuid4(), reason='Возврат', refund_id=refund_id
    )
    assert outcome.status == 'processing'
    assert outcome.provider_refund_id is None
    repo.refunds[refund_id].created_at = datetime.now(UTC) - timedelta(hours=25)
    repo.refunds[refund_id].last_checked_at = None


async def test_reconciler_attaches_refund_from_second_page() -> None:
    """Возврат со второй страницы привязывается к попытке, а не отклоняется."""
    refund_id = uuid4()
    adapter = _adapter(_two_pages(refund_id), [], refund_id=refund_id)
    reconciler, repo, pay_id, user_id = _reconciler_with_lost_refund(adapter)
    await _lose_refund_and_expire_key(reconciler, repo, pay_id, refund_id)

    await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)

    refund = repo.refunds[refund_id]
    assert refund.provider_refund_id == OUR_PROVIDER_REFUND
    assert refund.status == 'succeeded'
    assert refund.rejection_reason is None
    assert repo.access_versions[user_id] == 2


async def test_reconciler_keeps_refund_processing_when_listing_is_unfinished() -> None:
    """Зациклившийся список не даёт отказа: попытка остаётся на сверке."""
    refund_id = uuid4()

    def page_for_cursor(cursor: str | None) -> Page:
        return _foreign_page('same')

    adapter = _adapter(page_for_cursor, [])
    reconciler, repo, pay_id, user_id = _reconciler_with_lost_refund(adapter)
    await _lose_refund_and_expire_key(reconciler, repo, pay_id, refund_id)

    await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)

    refund = repo.refunds[refund_id]
    assert refund.status == 'processing'
    assert refund.rejection_reason is None
    assert repo.access_versions[user_id] == 1
