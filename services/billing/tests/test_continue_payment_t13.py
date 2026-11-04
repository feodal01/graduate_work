from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI

from clients.payment_provider import ProviderTransientError
from dependencies import (
    get_auth_gateway,
    get_payment_provider,
    get_purchase_service,
)
from main import create_app
from services.purchase import PurchaseService
from tests.fakes import (
    ConfigurableAuthGateway,
    FakePaymentRow,
    FakeProviderGateway,
    InMemoryPurchaseRepository,
)


@pytest.fixture
async def continue_test_env() -> AsyncGenerator[
    tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
    None,
]:
    """Создаёт окружение для проверки /billing/payments/{id}/continue (T-13)."""
    user_id = uuid4()
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway(id_prefix='yk-cont')
    auth = ConfigurableAuthGateway(user_id=user_id)
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
            'X-Request-Id': 'continue-test-req',
            'Authorization': 'Bearer valid-token',
        },
        follow_redirects=False,
    ) as client:
        yield client, app, repo, provider, auth, user_id


def _seed_payment(
    repo: InMemoryPurchaseRepository,
    *,
    user_id: UUID,
    status: str = 'pending',
    provider_payment_id: str | None = 'yk-existing-1',
    confirmation_url: str | None = 'https://yoomoney.ru/checkout/existing-1',
    last_checked_at: datetime | None = None,
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
        provider_payment_id=provider_payment_id,
        confirmation_url=confirmation_url,
        last_checked_at=last_checked_at,
    )
    repo.payments[pid] = row
    return row


async def test_continue_local_succeeded_redirects_to_result_without_calling_yookassa(
    continue_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """US-U-02, BIL-FR-04: локально succeeded -> 303 на результат без ЮKassa."""
    client, _, repo, provider, _, user_id = continue_test_env
    payment = _seed_payment(repo, user_id=user_id, status='succeeded')

    resp = await client.get(f'/billing/payments/{payment.id}/continue')
    assert resp.status_code == 303
    assert resp.headers['location'] == f'/billing/payments/{payment.id}'
    assert len(provider.get_calls) == 0
    assert len(provider.create_calls) == 0


async def test_continue_local_failed_creates_new_payment_and_redirects_to_new_url(
    continue_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """US-U-02, US-U-03, BIL-FR-04: локально failed -> создаёт новый платёж."""
    client, _, repo, provider, _, user_id = continue_test_env
    failed_payment = _seed_payment(repo, user_id=user_id, status='failed')

    resp = await client.get(f'/billing/payments/{failed_payment.id}/continue')
    assert resp.status_code == 303
    assert (
        resp.headers['location']
        == 'https://yoomoney.ru/checkout/payments/v2/contract?orderId=yk-cont-1'
    )
    assert len(provider.get_calls) == 0
    assert len(provider.create_calls) == 1
    assert len(repo.payments) == 2


async def test_continue_pending_throttles_10_calls_to_1_yookassa_get_call(
    continue_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """BIL-NFR-R-06: 10 вызовов /continue за 2 с делают ровно 1 запрос GET в ЮKassa."""
    client, _, repo, provider, _, user_id = continue_test_env
    payment = _seed_payment(repo, user_id=user_id, status='pending')

    for _ in range(10):
        resp = await client.get(f'/billing/payments/{payment.id}/continue')
        assert resp.status_code == 303
        assert (
            resp.headers['location']
            == 'https://yoomoney.ru/checkout/payments/v2/contract?orderId=yk-existing-1'
        )

    assert len(provider.get_calls) == 1
    assert len(provider.create_calls) == 0


async def test_continue_pending_throttled_without_confirmation_url_returns_503_e4(
    continue_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """Краевой случай: троттлинг 5 с при confirmation_url=None отдаёт 503 Э4 без GET."""
    client, _, repo, provider, _, user_id = continue_test_env
    payment = _seed_payment(
        repo,
        user_id=user_id,
        status='pending',
        provider_payment_id='yk-no-url-1',
        confirmation_url=None,
        last_checked_at=datetime.now(UTC) - timedelta(seconds=1),
    )

    resp = await client.get(f'/billing/payments/{payment.id}/continue')
    assert resp.status_code == 503
    assert 'Не удалось связаться с платёжной системой' in resp.text
    assert len(provider.get_calls) == 0


async def test_continue_pending_when_canceled_in_yookassa_marks_failed_and_creates_new(
    continue_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """BIL-FR-02, BIL-FR-04, BIL-FR-06: canceled в ЮKassa -> failed + новый платёж."""
    client, _, repo, provider, _, user_id = continue_test_env
    payment = _seed_payment(repo, user_id=user_id, status='pending')
    provider.get_status = 'failed'

    resp = await client.get(f'/billing/payments/{payment.id}/continue')
    assert resp.status_code == 303
    assert (
        resp.headers['location']
        == 'https://yoomoney.ru/checkout/payments/v2/contract?orderId=yk-cont-1'
    )
    assert repo.payments[payment.id].status == 'failed'
    assert repo.payments[payment.id].failure_reason == 'expired_on_confirmation'
    assert f'payment_failed:{payment.id}:notify_failed' in repo.outbox_tasks
    assert len(provider.get_calls) == 1
    assert len(provider.create_calls) == 1
    assert len(repo.payments) == 2


async def test_continue_pending_when_succeeded_in_yookassa_finalizes_and_redirects(
    continue_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """US-U-02, BIL-FR-04: succeeded в ЮKassa -> проводит и редиректит на Э3."""
    client, _, repo, provider, _, user_id = continue_test_env
    payment = _seed_payment(repo, user_id=user_id, status='pending')
    provider.get_status = 'succeeded'

    resp = await client.get(f'/billing/payments/{payment.id}/continue')
    assert resp.status_code == 303
    assert resp.headers['location'] == f'/billing/payments/{payment.id}'
    assert repo.payments[payment.id].status == 'succeeded'
    assert payment.id in repo.subscriptions
    assert repo.access_versions[user_id] == 1
    assert len(repo.outbox_tasks) == 3


async def test_continue_pending_without_provider_id_recovers_by_same_idempotency_key(
    continue_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """Инв. 13: без provider_payment_id вызывает POST с тем же ключом."""
    client, _, repo, provider, _, user_id = continue_test_env
    payment = _seed_payment(
        repo,
        user_id=user_id,
        status='pending',
        provider_payment_id=None,
        confirmation_url=None,
    )

    resp = await client.get(f'/billing/payments/{payment.id}/continue')
    assert resp.status_code == 303
    assert (
        resp.headers['location']
        == 'https://yoomoney.ru/checkout/payments/v2/contract?orderId=yk-cont-1'
    )
    assert len(provider.create_calls) == 1
    assert provider.create_calls[0].idempotency_key == payment.idempotency_key
    assert repo.payments[payment.id].provider_payment_id == 'yk-cont-1'


async def test_continue_unhappy_foreign_unknown_invalid_uuid_provider_down_and_409(
    continue_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
        UUID,
    ],
) -> None:
    """BIL-API-04, BIL-NFR-SEC-03, BIL-NFR-R-05: 404, 503 ЮKassa и 409 подписка."""
    client, _, repo, provider, auth, user_id = continue_test_env
    foreign_payment = _seed_payment(repo, user_id=uuid4(), status='pending')
    own_pending = _seed_payment(repo, user_id=user_id, status='pending')

    # 1. Чужой платёж -> 404 Э4
    resp_foreign = await client.get(f'/billing/payments/{foreign_payment.id}/continue')
    assert resp_foreign.status_code == 404
    assert 'Платёж не найден' in resp_foreign.text

    # 2. Неизвестный UUID -> 404 Э4
    resp_missing = await client.get(f'/billing/payments/{uuid4()}/continue')
    assert resp_missing.status_code == 404
    assert 'Платёж не найден' in resp_missing.text

    # 3. Невалидный UUID -> 404 Э4
    resp_invalid = await client.get('/billing/payments/invalid-uuid/continue')
    assert resp_invalid.status_code == 404
    assert 'Платёж не найден' in resp_invalid.text

    # 4. ЮKassa недоступна при GET -> 503 Э4, платёж остаётся pending
    provider.get_error = ProviderTransientError('YooKassa timeout')
    resp_yk_down = await client.get(f'/billing/payments/{own_pending.id}/continue')
    assert resp_yk_down.status_code == 503
    assert 'Не удалось связаться с платёжной системой' in resp_yk_down.text
    assert repo.payments[own_pending.id].status == 'pending'
    provider.get_error = None

    # 5. Если платёж failed, но подписка уже активна -> 409 Э4
    failed_own = _seed_payment(repo, user_id=user_id, status='failed')
    repo.has_active_subscription = True
    resp_409 = await client.get(f'/billing/payments/{failed_own.id}/continue')
    assert resp_409.status_code == 409
    assert 'Подписка уже активна' in resp_409.text
    repo.has_active_subscription = False

    # 6. Auth недоступен -> 503 Э4
    auth.unavailable = True
    resp_auth = await client.get(f'/billing/payments/{own_pending.id}/continue')
    assert resp_auth.status_code == 503
    assert 'Сервис авторизации временно недоступен' in resp_auth.text
