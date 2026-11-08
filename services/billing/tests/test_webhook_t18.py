from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI

from clients.payment_provider import (
    ProviderIntegrityMismatchError,
    ProviderProtocolAnomalyError,
    ProviderReconciliationError,
    ProviderTransientError,
)
from clients.payment_provider_registry import PaymentProviderRegistry
from dependencies import (
    get_auth_gateway,
    get_outbox_dispatcher,
    get_payment_providers,
    get_payment_reconciler,
)
from main import create_app
from services.outbox_dispatcher import OutboxDispatcher
from services.reconciler import PaymentReconciler
from tests.fakes import (
    ConfigurableAuthGateway,
    FakePaymentRow,
    FakeProviderGateway,
    InMemoryPurchaseRepository,
)


@pytest.fixture
async def webhook_test_env() -> AsyncGenerator[
    tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
    ],
    None,
]:
    """Создаёт тестовое окружение для обработчика вебхуков ЮKassa (T-18)."""
    user_id = uuid4()
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    auth = ConfigurableAuthGateway(user_id=user_id)
    dispatcher = OutboxDispatcher(repository=repo, auth_gateway=auth)
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        public_base_url='http://localhost',
        outbox_dispatcher=dispatcher,
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_auth_gateway] = lambda: auth
    app.dependency_overrides[get_payment_providers] = lambda: (
        PaymentProviderRegistry.single(provider)
    )
    app.dependency_overrides[get_outbox_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url='http://testserver',
        headers={'X-Request-Id': 'webhook-test-req'},
    ) as client:
        yield client, app, repo, provider, auth


def _seed_pending_payment(
    repo: InMemoryPurchaseRepository,
    *,
    provider_payment_id: str = 'yk-wh-101',
    last_checked_at: datetime | None = None,
) -> FakePaymentRow:
    pid = uuid4()
    row = FakePaymentRow(
        id=pid,
        user_id=uuid4(),
        tariff_id=UUID('00000000-0000-0000-0000-000000000001'),
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status='pending',
        idempotency_key=pid,
        provider_payment_id=provider_payment_id,
        confirmation_url='https://yoomoney.ru/checkout/wh-101',
        last_checked_at=last_checked_at,
    )
    repo.payments[pid] = row
    return row


@pytest.mark.asyncio
async def test_webhook_happy_path_bypasses_throttle_and_reconciles_via_get(
    webhook_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
    ],
) -> None:
    """BIL-FR-07, BIL-API-05, US-S-01: вебхук обходит 5 с лимит и сверяет через GET."""
    client, _, repo, provider, auth = webhook_test_env
    # Даже если last_checked_at был секунду назад, вебхук не ждёт 5 с
    payment = _seed_pending_payment(
        repo,
        provider_payment_id='yk-wh-101',
        last_checked_at=datetime.now(UTC) - timedelta(seconds=1),
    )
    provider.get_status = 'succeeded'

    resp = await client.post(
        '/api/v1/billing/webhooks/yookassa',
        json={
            'type': 'notification',
            'event': 'payment.succeeded',
            'object': {
                'id': 'yk-wh-101',
                'status': 'succeeded',
            },
        },
    )

    assert resp.status_code == 200
    assert resp.json()['status'] == 'ok'
    assert provider.get_calls == ['yk-wh-101']
    assert repo.payments[payment.id].status == 'succeeded'
    assert payment.id in repo.subscriptions
    assert repo.events[-1][:5] == (
        'payment',
        payment.id,
        'pending',
        'succeeded',
        'webhook',
    )
    assert len(auth.synced_access_calls) == 1

    # Повторная доставка того же вебхука идемпотентна (200 OK, 0 новых вызовов GET)
    resp_dup = await client.post(
        '/api/v1/billing/webhooks/yookassa',
        json={
            'type': 'notification',
            'event': 'payment.succeeded',
            'object': {'id': 'yk-wh-101', 'status': 'succeeded'},
        },
    )
    assert resp_dup.status_code == 200
    assert len(provider.get_calls) == 1


@pytest.mark.asyncio
async def test_forged_webhook_claiming_succeeded_does_not_change_pending_state_in_db(
    webhook_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
    ],
) -> None:
    """BIL-FR-07, BIL-NFR-SEC-04, инв. 6: поддельный вебхук не меняет состояние в БД."""
    client, _, repo, provider, auth = webhook_test_env
    payment = _seed_pending_payment(repo, provider_payment_id='yk-forged-1')
    # В самой ЮKassa платёж всё ещё pending!
    provider.get_status = 'pending'

    resp = await client.post(
        '/api/v1/billing/webhooks/yookassa',
        json={
            'type': 'notification',
            'event': 'payment.succeeded',
            'object': {
                'id': 'yk-forged-1',
                'status': 'succeeded',
                'paid': True,
                'amount': {'value': '299.00', 'currency': 'RUB'},
            },
        },
    )

    assert resp.status_code == 200
    assert provider.get_calls == ['yk-forged-1']
    assert repo.payments[payment.id].status == 'pending'
    assert payment.id not in repo.subscriptions
    assert len(repo.outbox_tasks) == 0
    assert len(auth.synced_access_calls) == 0


@pytest.mark.asyncio
async def test_webhook_unhappy_malformed_payload_outages_and_unknown_id(
    webhook_test_env: tuple[
        httpx.AsyncClient,
        FastAPI,
        InMemoryPurchaseRepository,
        FakeProviderGateway,
        ConfigurableAuthGateway,
    ],
) -> None:
    """BIL-API-05, US-S-01: 422 на невалидный JSON, 200 на unknown, 503 при сбое."""
    client, _, repo, provider, _ = webhook_test_env
    payment = _seed_pending_payment(repo, provider_payment_id='yk-err-1')

    # 1. Невалидные тела вебхука (включая небезопасные id) -> 422 validation_error
    invalid_bodies: list[dict[str, object]] = [
        {},
        {'object': {}},
        {'object': {'id': ''}},
        {'object': {'id': '   '}},
        {'object': {'id': '../refunds'}},
        {'object': {'id': 'yk-1?foo=bar'}},
        {'object': {'id': 'yk 1'}},
        {'object': {'id': 12345}},
    ]
    for invalid_body in invalid_bodies:
        r_422 = await client.post(
            '/api/v1/billing/webhooks/yookassa',
            json=invalid_body,
        )
        assert r_422.status_code == 422
        assert r_422.json()['error_code'] == 'validation_error'

    # 2. Неизвестный provider_payment_id -> 200 OK (безопасный игнор без ретраев)
    r_unknown = await client.post(
        '/api/v1/billing/webhooks/yookassa',
        json={'event': 'payment.succeeded', 'object': {'id': 'yk-unknown-999'}},
    )
    assert r_unknown.status_code == 200
    assert len(provider.get_calls) == 0

    # 3. Временный сбой ЮKassa или 401 на GET -> 503 (чтобы ЮKassa повторила вебхук)
    for transient_err in (
        ProviderTransientError('YooKassa 503 on GET'),
        ProviderReconciliationError(401, 'Unauthorized on GET'),
    ):
        provider.get_error = transient_err
        r_yk_down = await client.post(
            '/api/v1/billing/webhooks/yookassa',
            json={'event': 'payment.succeeded', 'object': {'id': 'yk-err-1'}},
        )
        assert r_yk_down.status_code == 503
        assert repo.payments[payment.id].status == 'pending'
    provider.get_error = None

    # 4. Несовпадение суммы или waiting_for_capture -> статус pending, ответ 200 OK
    for anomaly_err in (
        ProviderIntegrityMismatchError('Amount mismatch'),
        ProviderProtocolAnomalyError('waiting_for_capture anomaly'),
    ):
        provider.get_error = anomaly_err
        r_anomaly = await client.post(
            '/api/v1/billing/webhooks/yookassa',
            json={'event': 'payment.succeeded', 'object': {'id': 'yk-err-1'}},
        )
        assert r_anomaly.status_code == 200
        assert repo.payments[payment.id].status == 'pending'
    provider.get_error = None

    # 5. Сбой БД Биллинга -> 503 (чтобы ЮKassa повторила вебхук)
    repo.db_unavailable = True
    r_db_down = await client.post(
        '/api/v1/billing/webhooks/yookassa',
        json={'event': 'payment.succeeded', 'object': {'id': 'yk-err-1'}},
    )
    assert r_db_down.status_code == 503
    repo.db_unavailable = False


@pytest.mark.asyncio
async def test_webhook_emulator_payload_builder_cli_and_sender() -> None:
    """T-18: эмулятор вебхука формирует payload, парсит CLI и отправляет запрос."""
    from emulate_webhook import (
        build_yookassa_webhook_payload,
        parse_args,
        send_emulated_webhook,
    )

    payload = build_yookassa_webhook_payload(
        provider_object_id='2e001001-000f-5000-8000-18db351245c7',
        event='payment.succeeded',
    )
    assert payload == {
        'type': 'notification',
        'event': 'payment.succeeded',
        'object': {
            'id': '2e001001-000f-5000-8000-18db351245c7',
            'status': 'succeeded',
        },
    }

    with pytest.raises(ValueError):
        build_yookassa_webhook_payload(provider_object_id='../unsafe')

    args = parse_args(
        [
            '--id',
            '2e001001-000f-5000-8000-18db351245c7',
            '--event',
            'payment.canceled',
            '--base-url',
            'http://127.0.0.1:8000',
        ]
    )
    assert args.provider_object_id == '2e001001-000f-5000-8000-18db351245c7'
    assert args.event == 'payment.canceled'
    assert args.base_url == 'http://127.0.0.1:8000'

    # Проверяем send_emulated_webhook на JSON 200 и не-JSON 502 HTML от nginx
    return_html_502 = False

    def mock_handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == '/api/v1/billing/webhooks/yookassa'
        assert request.headers.get('X-Request-Id', '').startswith('emulated-')
        if return_html_502:
            return httpx.Response(502, text='<html>502 Bad Gateway</html>')
        return httpx.Response(200, json={'status': 'ok'})

    transport = httpx.MockTransport(mock_handler)
    code_ok, body_ok = await send_emulated_webhook(
        provider_object_id='yk-emu-1',
        transport=transport,
    )
    assert code_ok == 200
    assert body_ok == {'status': 'ok'}

    return_html_502 = True
    code_502, body_502 = await send_emulated_webhook(
        provider_object_id='yk-emu-1',
        transport=transport,
    )
    assert code_502 == 502
    assert '502 Bad Gateway' in str(body_502.get('raw_body'))
