"""История платежей на «Моей подписке» читается страницами (BIL-FR-09)."""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from api.auth_cookies import ACCESS_TOKEN_COOKIE
from core.config import settings
from tests.fakes import (
    ConfigurableAuthGateway,
    FakePaymentRow,
    FakeProviderGateway,
    InMemoryPurchaseRepository,
)
from tests.test_my_subscription_t23 import _build_t23_app

TARIFF_ID = UUID('00000000-0000-0000-0000-000000000001')
NEWEST = datetime(2026, 11, 1, 12, 0, tzinfo=UTC)


def _add_payment(
    repo: InMemoryPurchaseRepository,
    user_id: UUID,
    *,
    age_days: int,
    status: str = 'failed',
) -> FakePaymentRow:
    pid = uuid4()
    row = FakePaymentRow(
        id=pid,
        user_id=user_id,
        tariff_id=TARIFF_ID,
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status=status,
        idempotency_key=pid,
        created_at=NEWEST - timedelta(days=age_days),
    )
    repo.payments[pid] = row
    return row


def _client_with_history(
    count: int,
) -> tuple[TestClient, InMemoryPurchaseRepository, UUID, list[FakePaymentRow]]:
    """Клиент пользователя с `count` неуспешными платежами, от новых к старым."""
    user_id = uuid4()
    repo = InMemoryPurchaseRepository()
    auth = ConfigurableAuthGateway(user_id=user_id, tz='UTC')
    app = _build_t23_app(repo, FakeProviderGateway(), auth)
    rows = [_add_payment(repo, user_id, age_days=n) for n in range(count)]
    client = TestClient(app)
    client.cookies.set(ACCESS_TOKEN_COOKIE, 'valid-user-token')
    return client, repo, user_id, rows


def test_summary_returns_one_page_of_history() -> None:
    """JSON-сводка отдаёт страницу истории, общее число и параметры страницы."""
    client, _, _, rows = _client_with_history(7)
    headers = {
        'X-Request-Id': 'history-page',
        'Authorization': 'Bearer valid-user-token',
    }

    first = client.get(
        '/api/v1/billing/me/summary', params={'limit': 3}, headers=headers
    ).json()
    last = client.get(
        '/api/v1/billing/me/summary',
        params={'limit': 3, 'offset': 6},
        headers=headers,
    ).json()

    assert [item['payment_id'] for item in first['payments']] == [
        str(row.id) for row in rows[:3]
    ]
    assert (first['payments_total'], first['limit'], first['offset']) == (7, 3, 0)
    assert [item['payment_id'] for item in last['payments']] == [str(rows[6].id)]
    assert (last['payments_total'], last['limit'], last['offset']) == (7, 3, 6)


def test_summary_uses_default_page_size_and_rejects_oversized_page() -> None:
    """Без параметров берётся размер страницы из настроек; больше максимума — 422."""
    count = settings.pagination.default_page_size + 5
    client, _, _, _ = _client_with_history(count)
    headers = {
        'X-Request-Id': 'history-default',
        'Authorization': 'Bearer valid-user-token',
    }

    default = client.get('/api/v1/billing/me/summary', headers=headers).json()
    oversized = client.get(
        '/api/v1/billing/me/summary',
        params={'limit': settings.pagination.max_page_size + 1},
        headers=headers,
    )

    assert len(default['payments']) == settings.pagination.default_page_size
    assert default['payments_total'] == count
    assert oversized.status_code == 422


def test_pending_payment_is_found_on_any_history_page() -> None:
    """Кнопка «Продолжить оплату» не зависит от страницы истории.

    Незавершённый платёж ищется отдельным запросом: на странице, куда он не
    попал, статус и кнопка остаются прежними.
    """
    client, repo, user_id, _ = _client_with_history(5)
    pending = _add_payment(repo, user_id, age_days=0, status='pending')
    pending.created_at = NEWEST + timedelta(days=1)

    for offset in (0, 2, 4):
        page = client.get('/billing/', params={'limit': 2, 'offset': offset})
        assert page.status_code == 200
        assert 'Ожидает оплаты' in page.text
        assert (
            f'<form method="post" action="/billing/payments/{pending.id}/continue"'
            in page.text
        )
        # Сам платёж виден только на первой странице истории.
        in_history = f'href="/billing/payments/{pending.id}"' in page.text
        assert in_history is (offset == 0)


def test_history_page_links_lead_to_neighbour_pages() -> None:
    """Экран показывает положение в истории и ссылки на соседние страницы."""
    client, _, _, rows = _client_with_history(5)

    first = client.get('/billing/', params={'limit': 2}).text
    middle = client.get('/billing/', params={'limit': 2, 'offset': 2}).text
    last = client.get('/billing/', params={'limit': 2, 'offset': 4}).text

    assert 'Платежи 1–2 из 5' in first
    assert 'href="/billing/?limit=2&amp;offset=2"' in first
    assert 'Более новые' not in first

    assert 'Платежи 3–4 из 5' in middle
    assert 'href="/billing/?limit=2&amp;offset=0"' in middle
    assert 'href="/billing/?limit=2&amp;offset=4"' in middle
    assert str(rows[2].id) in middle
    assert str(rows[0].id) not in middle

    assert 'Платежи 5–5 из 5' in last
    assert 'Более старые' not in last


def test_short_history_has_no_page_links() -> None:
    """Если история помещается на одну страницу, навигации нет."""
    client, _, _, _ = _client_with_history(2)

    page = client.get('/billing/').text

    assert 'Страницы истории платежей' not in page
    assert 'Платежей пока не было' not in page


def test_page_past_the_end_says_so_and_links_back() -> None:
    """Смещение за концом истории: страница пуста, но ведёт обратно."""
    client, _, _, _ = _client_with_history(3)

    page = client.get('/billing/', params={'limit': 2, 'offset': 10}).text

    assert 'На этой странице платежей нет' in page
    assert 'Платежей пока не было' not in page
    # Ссылка ведёт на последнюю непустую страницу, а не на соседнюю пустую.
    assert 'href="/billing/?limit=2&amp;offset=2"' in page
    assert 'из 3' not in page
