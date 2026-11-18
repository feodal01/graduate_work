"""Вебхук по завершённой операции: сверка расхождений с ЮKassa (инв. 7, 6.4).

Итог операции не меняется. Если ЮKassa противоречит нашему итогу, поднимается
ровно один сигнал на операцию (отметка хранится в `provider_details`);
совпадающий статус сигнала не даёт; сбой ЮKassa не превращает вебхук в 5xx.
"""

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
import sentry_sdk

from clients.payment_provider import ProviderTransientError
from clients.payment_provider_registry import PaymentProviderRegistry
from dependencies import get_payment_reconciler
from main import create_app
from repositories.records import DIVERGENCE_SIGNALED_KEY
from services.reconciler import PaymentReconciler
from tests.fakes import (
    FakePaymentRow,
    FakeProviderGateway,
    FakeRefundRow,
    InMemoryPurchaseRepository,
)

Env = tuple[httpx.AsyncClient, InMemoryPurchaseRepository, FakeProviderGateway]
WEBHOOK_PATH = '/api/v1/billing/webhooks/yookassa'


@pytest.fixture
def signals(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Сообщения, отправленные в GlitchTip через sentry_sdk.capture_message."""
    captured: list[str] = []

    def fake_capture_message(message: str, level: str = 'info', **_: object) -> str:
        if level == 'error':
            captured.append(message)
        return 'evt-id'

    monkeypatch.setattr(sentry_sdk, 'capture_message', fake_capture_message)
    return captured


@pytest.fixture
async def env() -> AsyncGenerator[Env, None]:
    """HTTP-клиент обработчика вебхуков с фейковыми репозиторием и ЮKassa."""
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
    )
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://testserver',
        headers={'X-Request-Id': 'divergence-test'},
    ) as client:
        yield client, repo, provider


def _final_payment(
    repo: InMemoryPurchaseRepository, status: str, provider_payment_id: str
) -> FakePaymentRow:
    pid = uuid4()
    row = FakePaymentRow(
        id=pid,
        user_id=uuid4(),
        tariff_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status=status,
        idempotency_key=pid,
        provider_payment_id=provider_payment_id,
        failure_reason='expired_on_confirmation' if status == 'failed' else None,
        provider_details={'id': provider_payment_id},
        created_at=datetime.now(UTC) - timedelta(days=1),
    )
    repo.payments[pid] = row
    return row


def _final_refund(
    repo: InMemoryPurchaseRepository, status: str, provider_refund_id: str
) -> FakeRefundRow:
    payment = _final_payment(repo, 'succeeded', f'pay-of-{provider_refund_id}')
    rid = uuid4()
    row = FakeRefundRow(
        id=rid,
        payment_id=payment.id,
        subscription_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29_900,
        status=status,
        reason='Возврат',
        admin_user_id=uuid4(),
        idempotency_key=rid,
        provider_refund_id=provider_refund_id,
        rejection_reason='canceled_by_provider' if status == 'rejected' else None,
        provider_details={'id': provider_refund_id},
    )
    repo.refunds[rid] = row
    return row


async def _webhook(client: httpx.AsyncClient, object_id: str) -> httpx.Response:
    return await client.post(
        WEBHOOK_PATH,
        json={'type': 'notification', 'event': 'x', 'object': {'id': object_id}},
    )


@pytest.mark.parametrize(
    ('local', 'provider_status'),
    [('failed', 'succeeded'), ('succeeded', 'failed')],
)
async def test_payment_divergence_raises_one_signal_and_keeps_status(
    env: Env, signals: list[str], local: str, provider_status: str
) -> None:
    """Платёж: один сигнал на расхождение, итог и последствия не меняются."""
    client, repo, provider = env
    payment = _final_payment(repo, local, 'yk-div-pay')
    provider.get_status = provider_status
    events_before = list(repo.events)

    first = await _webhook(client, 'yk-div-pay')
    second = await _webhook(client, 'yk-div-pay')

    assert (first.status_code, second.status_code) == (200, 200)
    assert payment.status == local
    assert repo.events == events_before
    assert repo.outbox_tasks == {}
    matching = [m for m in signals if str(payment.id) in m]
    assert len(matching) == 1
    assert 'yk-div-pay' in matching[0]
    assert repr(local) in matching[0] and repr(provider_status) in matching[0]
    assert (payment.provider_details or {})[DIVERGENCE_SIGNALED_KEY] is True
    # Повторный вебхук после сигнала в ЮKassa уже не ходит.
    assert provider.get_calls == ['yk-div-pay']


@pytest.mark.parametrize(
    ('local', 'provider_status'),
    [('rejected', 'succeeded'), ('succeeded', 'rejected')],
)
async def test_refund_divergence_raises_one_signal_and_keeps_status(
    env: Env, signals: list[str], local: str, provider_status: str
) -> None:
    """Возврат: один сигнал на расхождение, итог не меняется."""
    client, repo, provider = env
    refund = _final_refund(repo, local, 'yk-div-ref')
    provider.next_get_refund_status = provider_status

    first = await _webhook(client, 'yk-div-ref')
    second = await _webhook(client, 'yk-div-ref')

    assert (first.status_code, second.status_code) == (200, 200)
    assert refund.status == local
    assert repo.events == []
    matching = [m for m in signals if str(refund.id) in m]
    assert len(matching) == 1
    assert 'yk-div-ref' in matching[0] and repr(provider_status) in matching[0]
    assert provider.get_refund_calls == ['yk-div-ref']


@pytest.mark.parametrize(
    ('local', 'provider_status'),
    [('succeeded', 'succeeded'), ('failed', 'failed'), ('failed', 'pending')],
)
async def test_matching_payment_status_gives_no_signal(
    env: Env, signals: list[str], local: str, provider_status: str
) -> None:
    """Совпадающий (или не противоречащий) статус ЮKassa сигнала не даёт."""
    client, repo, provider = env
    payment = _final_payment(repo, local, 'yk-same-pay')
    provider.get_status = provider_status

    resp = await _webhook(client, 'yk-same-pay')

    assert resp.status_code == 200
    assert provider.get_calls == ['yk-same-pay']
    assert signals == []
    assert DIVERGENCE_SIGNALED_KEY not in (payment.provider_details or {})


async def test_matching_refund_status_gives_no_signal(
    env: Env, signals: list[str]
) -> None:
    """Совпадающий статус возврата сигнала не даёт."""
    client, repo, provider = env
    _final_refund(repo, 'succeeded', 'yk-same-ref')
    provider.next_get_refund_status = 'succeeded'

    resp = await _webhook(client, 'yk-same-ref')

    assert resp.status_code == 200
    assert provider.get_refund_calls == ['yk-same-ref']
    assert signals == []


async def test_provider_error_during_divergence_check_answers_200(
    env: Env, signals: list[str], caplog: pytest.LogCaptureFixture
) -> None:
    """Сбой ЮKassa при проверке завершённой операции: 200 и WARNING-лог."""
    client, repo, provider = env
    payment = _final_payment(repo, 'failed', 'yk-err-pay')
    provider.get_error = ProviderTransientError('YooKassa 503', status_code=503)

    with caplog.at_level('WARNING', logger='services.reconciler'):
        resp = await _webhook(client, 'yk-err-pay')

    assert resp.status_code == 200
    assert payment.status == 'failed'
    assert signals == []
    assert any(
        r.levelname == 'WARNING' and str(payment.id) in r.getMessage()
        for r in caplog.records
    )


async def test_background_reconcile_of_final_payment_does_not_query_provider(
    env: Env,
) -> None:
    """Проверка расхождений — только по вебхуку; фоновая сверка как раньше."""
    _, repo, provider = env
    payment = _final_payment(repo, 'failed', 'yk-bg-pay')
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
    )

    await reconciler.reconcile_operation_by_admin('payment', payment.id)
    await reconciler.reconcile_payment(payment.id, source='reconcile')

    assert provider.get_calls == []
