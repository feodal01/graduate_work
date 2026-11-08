from collections.abc import AsyncGenerator
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI

from clients.payment_provider import (
    CreateProviderPaymentRequest,
    ProviderPaymentSnapshot,
    ProviderPermanentError,
    ProviderTransientError,
)
from clients.payment_provider_registry import PaymentProviderRegistry
from dependencies import AuthenticatedUser, get_auth_gateway, get_payment_providers
from main import create_app
from tests.fakes import (
    ConfigurableAuthGateway,
    FakeProviderGateway,
    InMemoryPurchaseRepository,
)


@pytest.fixture
async def purchase_test_env() -> AsyncGenerator[
    tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
    None,
]:
    """Создаёт тестовое окружение для сценариев покупки T-10."""
    from dependencies import get_purchase_service
    from services.purchase import PurchaseService

    user_id = uuid4()
    user = AuthenticatedUser(
        user_id=user_id,
        login='viewer',
        tz='Europe/Moscow',
        access_token='valid-token',
    )
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    service = PurchaseService(
        repository=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        public_base_url='http://localhost',
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_auth_gateway] = lambda: ConfigurableAuthGateway(
        user_id
    )
    app.dependency_overrides[get_payment_providers] = lambda: (
        PaymentProviderRegistry.single(provider)
    )
    app.dependency_overrides[get_purchase_service] = lambda: service

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={
            'X-Request-Id': 'purchase-test-req',
            'Authorization': 'Bearer valid-token',
        },
        follow_redirects=False,
    ) as client:
        yield client, app, repo, provider, user


@pytest.mark.asyncio
async def test_create_purchase_creates_pending_payment_calls_yookassa_and_redirects(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-FR-01, BIL-API-03: POST /billing/subscribe создаёт платёж и редиректит."""
    client, _, repo, provider, _ = purchase_test_env

    response = await client.post('/billing/subscribe')

    assert response.status_code == 303
    assert (
        response.headers['location']
        == 'https://yoomoney.ru/checkout/payments/v2/contract?orderId=yk-pay-1'
    )
    assert len(provider.create_calls) == 1
    assert len(repo.payments) == 1
    saved = next(iter(repo.payments.values()))
    assert saved.status == 'pending'
    assert saved.provider_payment_id == 'yk-pay-1'
    assert saved.idempotency_key == saved.id


@pytest.mark.asyncio
async def test_repeat_purchase_checks_yookassa_and_reuses_live_pending_payment(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-FR-02: повтор при живом pending в ЮKassa возвращает тот же платёж."""
    client, _, repo, provider, _ = purchase_test_env

    first = await client.post('/api/v1/billing/payments')
    assert first.status_code == 200
    first_data = first.json()

    second = await client.post('/api/v1/billing/payments')
    assert second.status_code == 200
    second_data = second.json()

    assert first_data['payment_id'] == second_data['payment_id']
    assert first_data['confirmation_url'] == second_data['confirmation_url']
    assert len(provider.create_calls) == 1
    assert provider.get_calls == ['yk-pay-1']
    assert len(repo.payments) == 1


@pytest.mark.asyncio
async def test_repeat_purchase_when_pending_was_canceled_creates_new_payment(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-FR-02: если pending отменён в ЮKassa, он закрывается и создаётся новый."""
    client, _, repo, provider, _ = purchase_test_env

    first = await client.post('/api/v1/billing/payments')
    first_id = UUID(first.json()['payment_id'])

    # ЮKassa отменила первый платёж по тайм-ауту
    provider.get_status = 'failed'

    second = await client.post('/billing/subscribe')
    assert second.status_code == 303
    assert (
        second.headers['location']
        == 'https://yoomoney.ru/checkout/payments/v2/contract?orderId=yk-pay-2'
    )
    assert repo.payments[first_id].status == 'failed'
    assert f'payment_failed:{first_id}:notify_failed' in repo.outbox_tasks
    assert len(repo.payments) == 2
    assert len(provider.create_calls) == 2


@pytest.mark.asyncio
async def test_repeat_purchase_recovers_payment_without_provider_id_by_same_key(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-FR-02, инв. 13: повтор после сбоя до номера вызывает POST с тем же ключом."""
    client, _, repo, provider, _ = purchase_test_env
    provider.create_error = ProviderTransientError('Timeout on first attempt')

    failed_ui = await client.post('/billing/subscribe')
    assert failed_ui.status_code == 503
    assert 'Не удалось связаться с платёжной системой' in failed_ui.text
    assert len(repo.payments) == 1
    pending_row = next(iter(repo.payments.values()))
    assert pending_row.provider_payment_id is None

    # Вторая попытка проходит успешно с тем же ключом идемпотентности
    provider.create_error = None
    recovered = await client.post('/billing/subscribe')
    assert recovered.status_code == 303
    assert len(provider.create_calls) == 2
    assert (
        provider.create_calls[0].idempotency_key
        == provider.create_calls[1].idempotency_key
    )
    assert pending_row.provider_payment_id == 'yk-pay-1'


@pytest.mark.asyncio
@pytest.mark.parametrize('block_reason', ['active_subscription', 'processing_refund'])
async def test_purchase_rejected_with_409_when_active_subscription_or_processing_refund(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
    block_reason: str,
) -> None:
    """BIL-FR-03: покупка отклоняется с 409 при активной подписке или возврате."""
    client, _, repo, provider, _ = purchase_test_env
    if block_reason == 'active_subscription':
        repo.has_active_subscription = True
    else:
        repo.has_processing_refund = True

    api_resp = await client.post('/api/v1/billing/payments')
    assert api_resp.status_code == 409
    assert api_resp.json()['error_code'] in (
        'subscription_already_active',
        'refund_in_processing',
    )

    ui_resp = await client.post('/billing/subscribe')
    assert ui_resp.status_code == 409
    assert len(provider.create_calls) == 0


@pytest.mark.asyncio
async def test_purchase_marks_payment_failed_when_yookassa_returns_400_401_403(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-FR-06, инв. 15: при 400/401/403 от ЮKassa платёж переводится в failed."""
    client, _, repo, provider, _ = purchase_test_env
    provider.create_error = ProviderPermanentError(401, 'Invalid shopId or key')

    resp = await client.post('/api/v1/billing/payments')
    assert resp.status_code == 503
    saved = next(iter(repo.payments.values()))
    assert saved.status == 'failed'
    assert saved.failure_reason is not None
    assert f'payment_failed:{saved.id}:notify_failed' in repo.outbox_tasks


@pytest.mark.asyncio
async def test_repeat_purchase_throttles_yookassa_get_calls_to_once_per_5_seconds(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """BIL-NFR-R-06: 10 повторных вызовов за 2 секунды делают ровно 1 GET в ЮKassa."""
    client, _, repo, provider, _ = purchase_test_env

    # Первое создание платежа (POST в ЮKassa)
    first = await client.post('/api/v1/billing/payments')
    assert first.status_code == 200
    assert len(provider.create_calls) == 1
    assert len(provider.get_calls) == 0

    # 10 повторных запросов подряд в пределах 5 секунд
    for _ in range(10):
        repeat = await client.post('/api/v1/billing/payments')
        assert repeat.status_code == 200
        assert repeat.json()['payment_id'] == first.json()['payment_id']

    # Первый из 10 повторов сделал GET и записал last_checked_at, остальные 9 взяли кэш
    assert len(provider.get_calls) == 1
    assert len(repo.payments) == 1


@pytest.mark.asyncio
async def test_repeat_purchase_redirects_to_result_when_yookassa_returns_succeeded(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """US-U-01: при succeeded в ЮKassa создаётся подписка и редирект на результат."""
    client, _, repo, provider, user = purchase_test_env

    first = await client.post('/api/v1/billing/payments')
    payment_id = UUID(first.json()['payment_id'])

    provider.get_status = 'succeeded'
    second = await client.post('/billing/subscribe')
    assert second.status_code == 303
    assert second.headers['location'] == f'/billing/payments/{payment_id}'
    assert payment_id in repo.subscriptions
    assert repo.access_versions[user.user_id] == 1
    assert len(repo.outbox_tasks) == 3


@pytest.mark.asyncio
async def test_idempotent_post_replay_verifies_via_get_when_non_pending(
    purchase_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        AuthenticatedUser,
    ],
) -> None:
    """Инвариант 6: при canceled на повторе POST статус сверяется через GET."""
    client, _, repo, provider, user = purchase_test_env

    # В БД есть pending-платёж без сохранённого provider_payment_id (после обрыва связи)
    existing, _ = await repo.get_or_create_pending_payment(user.user_id)
    assert existing.provider_payment_id is None

    # ЮKassa на идемпотентный POST возвращает canceled, смена статуса только после GET
    provider.get_status = 'failed'

    orig_create = provider.create_payment

    async def create_returning_canceled_first(
        request: CreateProviderPaymentRequest,
    ) -> ProviderPaymentSnapshot:
        if len(provider.create_calls) == 0:
            provider.create_calls.append(request)
            return ProviderPaymentSnapshot(
                provider_code='yookassa',
                provider_payment_id='yk-old-canceled',
                domain_status='failed',
                amount_kopeks=request.amount_kopeks,
                currency=request.currency,
                confirmation_url=None,
                failure_reason='expired_on_confirmation',
                metadata_payment_id=str(request.payment_id),
                provider_details={'id': 'yk-old-canceled', 'status': 'canceled'},
            )
        return await orig_create(request)

    provider.create_payment = create_returning_canceled_first  # type: ignore[method-assign]

    resp = await client.post('/billing/subscribe')
    assert resp.status_code == 303
    assert 'yk-pay-1' in resp.headers['location']
    assert 'yk-old-canceled' in provider.get_calls
    assert repo.payments[existing.id].status == 'failed'
    assert len(repo.payments) == 2
