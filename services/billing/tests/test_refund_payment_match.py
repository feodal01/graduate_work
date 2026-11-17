"""Возврат из ЮKassa обязан относиться к исходному платежу (инвариант 17)."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from clients.payment_provider import (
    CreateProviderRefundRequest,
    ProviderIntegrityMismatchError,
    YooKassaProviderAdapter,
)
from clients.payment_provider_registry import PaymentProviderRegistry
from services.outbox_dispatcher import OutboxDispatcher
from services.reconciler import PaymentReconciler
from tests.fakes import (
    FakePaymentRow,
    FakeProviderGateway,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
)
from tests.test_refunds_t29 import (
    _RecordingAuthGateway,
    _RecordingNotificationsGateway,
)

OUR_PAYMENT = 'yk_pay_ours'
FOREIGN_PAYMENT = 'yk_pay_foreign'
PROVIDER_REFUND = 'yk_ref_001'


def _refund_body(
    refund_id: UUID,
    *,
    payment_id: str | None,
    status: str = 'succeeded',
) -> dict[str, Any]:
    body: dict[str, Any] = {
        'id': PROVIDER_REFUND,
        'status': status,
        'amount': {'value': '299.00', 'currency': 'RUB'},
        'metadata': {'refund_id': str(refund_id)},
    }
    if payment_id is not None:
        body['payment_id'] = payment_id
    return body


def _adapter(
    refund_id: UUID,
    *,
    post_payment_id: str | None,
    get_payment_id: str | None,
) -> YooKassaProviderAdapter:
    """Адаптер поверх подставной ЮKassa с заданным `payment_id` в ответах."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == 'POST':
            return httpx.Response(
                200,
                json=_refund_body(
                    refund_id, payment_id=post_payment_id, status='pending'
                ),
            )
        if request.url.path == '/v3/refunds':
            return httpx.Response(
                200,
                json={
                    'type': 'list',
                    'items': [_refund_body(refund_id, payment_id=get_payment_id)],
                },
            )
        return httpx.Response(
            200, json=_refund_body(refund_id, payment_id=get_payment_id)
        )

    return YooKassaProviderAdapter(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        shop_id='shop-1',
        secret_key='secret-1',
    )


def _create_request(refund_id: UUID) -> CreateProviderRefundRequest:
    return CreateProviderRefundRequest(
        refund_id=refund_id,
        idempotency_key=refund_id,
        provider_payment_id=OUR_PAYMENT,
        amount_kopeks=29900,
        currency='RUB',
        description='Возврат',
    )


@pytest.mark.parametrize('answered_payment_id', [FOREIGN_PAYMENT, None])
async def test_adapter_rejects_refund_of_another_payment(
    answered_payment_id: str | None,
) -> None:
    """Чужой или отсутствующий `payment_id` — расхождение во всех трёх методах."""
    refund_id = uuid4()
    adapter = _adapter(
        refund_id,
        post_payment_id=answered_payment_id,
        get_payment_id=answered_payment_id,
    )
    with pytest.raises(ProviderIntegrityMismatchError, match='payment_id mismatch'):
        await adapter.create_refund(_create_request(refund_id))
    with pytest.raises(ProviderIntegrityMismatchError, match='payment_id mismatch'):
        await adapter.get_refund(
            PROVIDER_REFUND,
            expected_refund_id=refund_id,
            expected_provider_payment_id=OUR_PAYMENT,
            expected_amount_kopeks=29900,
            expected_currency='RUB',
        )
    with pytest.raises(ProviderIntegrityMismatchError, match='payment_id mismatch'):
        await adapter.find_refund_by_metadata(
            OUR_PAYMENT,
            refund_id=refund_id,
            expected_amount_kopeks=29900,
            expected_currency='RUB',
        )


async def test_adapter_accepts_refund_of_expected_payment() -> None:
    """Возврат по ожидаемому платежу проходит проверку во всех трёх методах."""
    refund_id = uuid4()
    adapter = _adapter(
        refund_id, post_payment_id=OUR_PAYMENT, get_payment_id=OUR_PAYMENT
    )

    created = await adapter.create_refund(_create_request(refund_id))
    fetched = await adapter.get_refund(
        PROVIDER_REFUND,
        expected_refund_id=refund_id,
        expected_provider_payment_id=OUR_PAYMENT,
    )
    found = await adapter.find_refund_by_metadata(OUR_PAYMENT, refund_id=refund_id)

    assert created.provider_payment_id == OUR_PAYMENT
    assert fetched.domain_status == 'succeeded'
    assert found is not None


def _repo_with_paid_subscription() -> tuple[InMemoryPurchaseRepository, UUID, UUID]:
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
        provider_payment_id=OUR_PAYMENT,
        created_at=now_utc - timedelta(days=1),
        updated_at=now_utc - timedelta(days=1),
    )
    repo.subscriptions[sub_id] = FakeSubscriptionRow(
        id=sub_id,
        user_id=user_id,
        tariff_id=tariff_id,
        payment_id=pay_id,
        starts_at=now_utc - timedelta(days=1),
        ends_at=now_utc + timedelta(days=29),
        created_at=now_utc - timedelta(days=1),
    )
    repo.access_versions[user_id] = 1
    return repo, pay_id, user_id


async def test_refund_of_another_payment_stays_on_reconciliation() -> None:
    """ЮKassa подтвердила возврат чужого платежа: доступ не отзывается."""
    repo, pay_id, user_id = _repo_with_paid_subscription()
    refund_id = uuid4()
    auth_gw = _RecordingAuthGateway()
    ntf_gw = _RecordingNotificationsGateway()
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(
            _adapter(
                refund_id,
                post_payment_id=OUR_PAYMENT,
                get_payment_id=FOREIGN_PAYMENT,
            )
        ),
        public_base_url='http://localhost',
        outbox_dispatcher=OutboxDispatcher(
            repository=repo,
            auth_gateway=auth_gw,
            notifications_gateway=ntf_gw,
        ),
    )

    outcome = await reconciler.create_or_reconcile_refund(
        pay_id,
        admin_user_id=uuid4(),
        reason='Проверка расхождения',
        refund_id=refund_id,
    )

    assert outcome.status == 'processing'
    assert outcome.transitioned is False
    assert outcome.provider_refund_id == PROVIDER_REFUND
    assert repo.access_versions[user_id] == 1
    assert auth_gw.calls == []
    assert ntf_gw.batches == []

    # Повторная сверка (воркер, вебхук, кнопка) тоже не закрывает возврат.
    with pytest.raises(ProviderIntegrityMismatchError):
        await reconciler.reconcile_refund(refund_id, source='reconcile')
    assert repo.access_versions[user_id] == 1
    assert auth_gw.calls == []


async def test_reconciler_passes_payment_provider_id_to_refund_check() -> None:
    """Сверка передаёт в проверку номер платежа ЮKassa из записи платежа."""
    repo, pay_id, _ = _repo_with_paid_subscription()
    provider = FakeProviderGateway()
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        public_base_url='http://localhost',
    )

    await reconciler.create_or_reconcile_refund(
        pay_id, admin_user_id=uuid4(), reason='Возврат'
    )

    assert provider.get_refund_expected_payment_ids == [OUR_PAYMENT]
