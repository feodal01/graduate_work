from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI

from clients.payment_provider import (
    CreateProviderPaymentRequest,
    ProviderPaymentSnapshot,
)
from dependencies import (
    get_auth_gateway,
    get_payment_provider,
    get_purchase_service,
)
from main import create_app
from services.purchase import PurchaseService
from tests.fakes import (
    ConfigurableAuthGateway,
    FakeOutboxTaskRow,
    FakePaymentRow,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
)


class ForbiddenProviderGateway:
    """Адаптер провайдера, падающий при любом вызове (0 запросов в ЮKassa)."""

    provider_code = 'yookassa'

    def __init__(self) -> None:
        self.calls: int = 0

    async def create_payment(
        self,
        request: CreateProviderPaymentRequest,
    ) -> ProviderPaymentSnapshot:
        """Выбрасывает ошибку: экран результата не имеет права вызывать ЮKassa."""
        self.calls += 1
        raise AssertionError('YooKassa create_payment must not be called on GET result')

    async def get_payment(
        self,
        provider_payment_id: str,
        *,
        expected_payment_id: UUID | None = None,
        expected_amount_kopeks: int | None = None,
        expected_currency: str | None = None,
    ) -> ProviderPaymentSnapshot:
        """Выбрасывает ошибку: экран результата не имеет права вызывать ЮKassa."""
        self.calls += 1
        raise AssertionError('YooKassa get_payment must not be called on GET result')


@pytest.fixture
async def payment_result_env() -> AsyncGenerator[
    tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        ForbiddenProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
    None,
]:
    """Создаёт тестовое окружение для экрана Э3 «Результат оплаты» (T-12)."""
    user_id = uuid4()
    repo = InMemoryPurchaseRepository()
    provider = ForbiddenProviderGateway()
    auth = ConfigurableAuthGateway(user_id=user_id, tz='Asia/Yekaterinburg')
    service = PurchaseService(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_auth_gateway] = lambda: auth
    app.dependency_overrides[get_payment_provider] = lambda: provider
    app.dependency_overrides[get_purchase_service] = lambda: service

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={
            'X-Request-Id': 'result-screen-test',
            'Authorization': 'Bearer valid-token',
        },
        follow_redirects=False,
    ) as client:
        yield client, app, repo, provider, auth, user_id


def _create_payment_row(
    repo: InMemoryPurchaseRepository,
    *,
    user_id: UUID,
    status: str,
    failure_reason: str | None = None,
) -> FakePaymentRow:
    pid = uuid4()
    row = FakePaymentRow(
        id=pid,
        user_id=user_id,
        tariff_id=UUID('00000000-0000-0000-0000-000000000001'),
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status=status,
        idempotency_key=pid,
        provider_payment_id=f'yk-{pid.hex[:8]}',
        confirmation_url='https://yoomoney.ru/checkout/test',
        failure_reason=failure_reason,
    )
    repo.payments[pid] = row
    return row


async def test_result_screen_pending_shows_checking_block_and_zero_yookassa_calls(
    payment_result_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        ForbiddenProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """BIL-FR-04, BIL-API-04, BIL-NFR-R-06: 10 открытий экрана делают 0 вызовов."""
    client, _, repo, provider, _, user_id = payment_result_env
    payment = _create_payment_row(repo, user_id=user_id, status='pending')

    for _ in range(10):
        resp = await client.get(f'/billing/payments/{payment.id}')
        assert resp.status_code == 200
        body = resp.text
        assert 'Проверяем оплату…' in body
        assert 'meta http-equiv="refresh" content="5"' in body
        assert f'/billing/payments/{payment.id}/continue' in body
        assert 'href="/billing/"' in body
        # Однозначность состояния (BIL-NFR-U-01): других блоков на странице нет
        assert 'Оплата прошла, доступ открыт до' not in body
        assert 'Оплачено, доступ активируется' not in body
        assert 'Оплата не прошла' not in body

    assert provider.calls == 0


async def test_result_screen_succeeded_and_delivered_shows_date_in_user_tz(
    payment_result_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        ForbiddenProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """BIL-FR-04, BIL-NFR-U-01, NFR-U-03: блок «Оплата прошла…» в поясе пользователя."""
    client, _, repo, provider, _, user_id = payment_result_env
    payment = _create_payment_row(repo, user_id=user_id, status='succeeded')
    starts_at = datetime(2026, 11, 4, 12, 0, tzinfo=UTC)
    ends_at = datetime(2026, 12, 4, 12, 0, tzinfo=UTC)
    sub_id = uuid4()
    repo.subscriptions[payment.id] = FakeSubscriptionRow(
        id=sub_id,
        user_id=user_id,
        tariff_id=payment.tariff_id,
        payment_id=payment.id,
        starts_at=starts_at,
        ends_at=ends_at,
    )
    dedup_key = f'payment_succeeded:{payment.id}:auth_access'
    repo.outbox_tasks[dedup_key] = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=dedup_key,
        target_service='auth',
        task_type='sync_access',
        payment_id=payment.id,
        user_id=user_id,
        access_version=1,
        payload={'expires_at': ends_at.isoformat()},
        status='completed',
    )

    resp = await client.get(f'/billing/payments/{payment.id}')
    assert resp.status_code == 200
    body = resp.text
    # 12:00 UTC -> 17:00 в Asia/Yekaterinburg (UTC+5)
    assert 'Оплата прошла, доступ открыт до 04.12.2026 17:00' in body
    assert 'meta http-equiv="refresh"' not in body
    assert 'Оплачено, доступ активируется' not in body
    assert 'Оплата не прошла' not in body
    assert 'Проверяем оплату…' not in body
    assert 'href="/billing/"' in body
    assert provider.calls == 0


async def test_result_screen_succeeded_while_auth_outbox_pending_shows_activating_block(
    payment_result_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        ForbiddenProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """BIL-FR-04, 4.4: пока sync_access pending -> «Оплачено, доступ активируется»."""
    client, _, repo, provider, _, user_id = payment_result_env
    payment = _create_payment_row(repo, user_id=user_id, status='succeeded')
    now_utc = datetime.now(UTC)
    repo.subscriptions[payment.id] = FakeSubscriptionRow(
        id=uuid4(),
        user_id=user_id,
        tariff_id=payment.tariff_id,
        payment_id=payment.id,
        starts_at=now_utc,
        ends_at=now_utc + timedelta(days=30),
    )
    dedup_key = f'payment_succeeded:{payment.id}:auth_access'
    repo.outbox_tasks[dedup_key] = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=dedup_key,
        target_service='auth',
        task_type='sync_access',
        payment_id=payment.id,
        user_id=user_id,
        access_version=1,
        payload={},
        status='pending',
    )

    resp = await client.get(f'/billing/payments/{payment.id}')
    assert resp.status_code == 200
    body = resp.text
    assert 'Оплачено, доступ активируется' in body
    assert 'meta http-equiv="refresh" content="5"' in body
    assert 'Оплата прошла, доступ открыт до' not in body
    assert 'Оплата не прошла' not in body
    assert 'Проверяем оплату…' not in body
    assert provider.calls == 0


async def test_result_screen_failed_shows_human_reason_and_retry_button(
    payment_result_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        ForbiddenProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """BIL-FR-04, US-U-03, BIL-NFR-U-01: при failed показывает причину и кнопку."""
    client, _, repo, provider, _, user_id = payment_result_env
    payment = _create_payment_row(
        repo,
        user_id=user_id,
        status='failed',
        failure_reason='insufficient_funds',
    )

    resp = await client.get(f'/billing/payments/{payment.id}')
    assert resp.status_code == 200
    body = resp.text
    assert 'Оплата не прошла' in body
    assert 'Недостаточно средств на карте' in body
    assert 'insufficient_funds' not in body
    assert 'action="/billing/subscribe"' in body
    assert 'meta http-equiv="refresh"' not in body
    assert 'Оплата прошла, доступ открыт до' not in body
    assert 'Оплачено, доступ активируется' not in body
    assert 'Проверяем оплату…' not in body
    assert provider.calls == 0


async def test_result_screen_unhappy_foreign_unknown_unauth_and_auth_down(
    payment_result_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        ForbiddenProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """BIL-API-04, BIL-NFR-SEC-03, NFR-U-02: чужой -> 404, Auth down -> 503."""
    client, _, repo, provider, auth, _ = payment_result_env
    foreign_payment = _create_payment_row(
        repo,
        user_id=uuid4(),
        status='succeeded',
    )

    # 1. Чужой платёж -> 404 экран ошибки Э4
    resp_foreign = await client.get(f'/billing/payments/{foreign_payment.id}')
    assert resp_foreign.status_code == 404
    assert 'Платёж не найден' in resp_foreign.text

    # 2. Несуществующий UUID -> 404 экран ошибки Э4
    resp_missing = await client.get(f'/billing/payments/{uuid4()}')
    assert resp_missing.status_code == 404
    assert 'Платёж не найден' in resp_missing.text

    # 3. Невалидный UUID в URL на HTML-маршруте -> 404 экран ошибки Э4
    resp_bad_uuid = await client.get('/billing/payments/not-a-valid-uuid')
    assert resp_bad_uuid.status_code == 404
    assert 'text/html' in resp_bad_uuid.headers['content-type']
    assert 'Платёж не найден' in resp_bad_uuid.text

    # 4. Запрос без токена -> 303 редирект на /billing/login
    resp_unauth = await client.get(
        f'/billing/payments/{foreign_payment.id}',
        headers={'X-Request-Id': 'unauth-req', 'Authorization': ''},
    )
    assert resp_unauth.status_code == 303
    assert resp_unauth.headers['location'] == '/billing/login'

    # 5. Auth недоступен -> 503 экран ошибки Э4 без раскрытия данных платежа
    auth.unavailable = True
    resp_auth_down = await client.get(f'/billing/payments/{foreign_payment.id}')
    assert resp_auth_down.status_code == 503
    assert 'Сервис авторизации временно недоступен' in resp_auth_down.text
    assert '299' not in resp_auth_down.text
    auth.unavailable = False

    # 6. БД недоступна -> 503 экран ошибки Э4
    repo.db_unavailable = True
    resp_db_down = await client.get(f'/billing/payments/{foreign_payment.id}')
    assert resp_db_down.status_code == 503
    assert 'Сервис биллинга временно недоступен' in resp_db_down.text
    assert provider.calls == 0


@pytest.mark.parametrize(
    ('raw_reason', 'expected_substring'),
    [
        (None, 'Платёж был отменён платёжной системой'),
        ('insufficient_funds', 'Недостаточно средств на карте'),
        ('invalid_csc', 'Указан неверный код CVV/CVC'),
        ('Invalid shopId or key', 'Платёж отклонён платёжной системой'),
        ('HTTP 401', 'Платёж отклонён платёжной системой'),
        ('forbidden: Secret key is expired', 'Платёж отклонён платёжной системой'),
        ('unknown_future_code', 'Платёж отклонён платёжной системой'),
    ],
)
async def test_result_screen_edge_cases_technical_reasons_fractional_kopeks_and_tz(
    payment_result_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        ForbiddenProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
    raw_reason: str | None,
    expected_substring: str,
) -> None:
    """BIL-NFR-U-01, BIL-NFR-R-07, NFR-U-03: скрытие тех. ошибок, копейки и TZ."""
    from domain.subscription import format_user_datetime, resolve_user_zoneinfo

    client, _, repo, provider, auth, user_id = payment_result_env
    payment = _create_payment_row(
        repo,
        user_id=user_id,
        status='failed',
        failure_reason=raw_reason,
    )
    payment.amount_kopeks = 29_950
    auth.tz = 'Invalid/Timezone'

    resp = await client.get(f'/billing/payments/{payment.id}')
    assert resp.status_code == 200
    body = resp.text
    assert '299,50 ₽' in body
    assert expected_substring in body
    if raw_reason:
        assert raw_reason not in body

    # Проверка resolve_user_zoneinfo и naive datetime на краевых входах ('', '/', None)
    assert resolve_user_zoneinfo('').key == 'UTC'
    assert resolve_user_zoneinfo('/').key == 'UTC'
    naive_dt = datetime(2026, 11, 4, 12, 0)
    assert format_user_datetime(naive_dt, 'Invalid/Zone') == '04.11.2026 12:00'
    assert provider.calls == 0
