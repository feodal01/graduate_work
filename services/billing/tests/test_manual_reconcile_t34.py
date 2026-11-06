from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from clients.payment_provider import ProviderTransientError
from dependencies import (
    get_payment_reconciler,
    get_purchase_repository,
    get_service_credentials,
)
from main import create_app
from security.service_credentials import ServiceCredentials
from services.reconciler import PaymentReconciler
from tests.fakes import (
    FakePaymentRow,
    FakeProviderGateway,
    FakeRefundRow,
    InMemoryPurchaseRepository,
)

SERVICE_KEY = 'billing-internal-manual-reconcile-key-long-enough'


@pytest.mark.parametrize(
    ('op_type', 'header_name', 'header_value', 'expected_status'),
    [
        ('payment', 'X-Service-Api-Key', SERVICE_KEY, 404),
        ('payment', 'X-Service-Key', SERVICE_KEY, 404),
        ('payment', 'X-Service-Api-Key', 'wrong-key', 401),
        ('payment', None, None, 401),
        ('refund', 'X-Service-Api-Key', SERVICE_KEY, 404),
        ('refund', 'X-Service-Api-Key', 'wrong-key', 401),
    ],
)
async def test_manual_reconcile_auth_and_not_found_step(
    op_type: str,
    header_name: str | None,
    header_value: str | None,
    expected_status: int,
) -> None:
    """T-34 (BIL-API-10, BIL-NFR-SEC-03): проверка сервисного ключа и 404."""
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    reconciler = PaymentReconciler(repository=repo, payment_provider=provider)

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    headers: dict[str, str] = {'X-Request-Id': 't34-auth'}
    if header_name is not None and header_value is not None:
        headers[header_name] = header_value

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers=headers,
    ) as client:
        resp = await client.post(
            f'/api/v1/internal/billing/operations/{op_type}/{uuid4()}/reconcile'
        )
        assert resp.status_code == expected_status
        if expected_status == 404:
            assert resp.json()['error_code'] == 'operation_not_found'


async def test_manual_reconcile_payment_and_refund_bypass_throttle() -> None:
    """T-34 (BIL-FR-15, BIL-API-10, US-A-06): сверка без лимита 5 с и источник admin."""
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    reconciler = PaymentReconciler(repository=repo, payment_provider=provider)

    now_utc = datetime.now(UTC)
    user_id = uuid4()
    tariff_id = uuid4()
    pay_id = uuid4()

    # Свежий платёж (создан 1 с назад и проверен 1 с назад): фоновая сверка и /continue
    # пропустили бы его из-за лимита 5 с и отступа воркера, но кнопка админа — НЕТ!
    repo.payments[pay_id] = FakePaymentRow(
        id=pay_id,
        user_id=user_id,
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='pending',
        idempotency_key=pay_id,
        provider_payment_id='yk_manual_pay_01',
        last_checked_at=now_utc - timedelta(seconds=1),
        created_at=now_utc - timedelta(seconds=1),
        updated_at=now_utc - timedelta(seconds=1),
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={
            'X-Request-Id': 't34-reconcile',
            'X-Service-Api-Key': SERVICE_KEY,
        },
    ) as client:
        # 1. Unhappy: ЮKassa недоступна (503) -> возвращает 503, статус не меняется
        provider.get_error = ProviderTransientError('YooKassa timeout', status_code=504)
        r_yk_down = await client.post(
            f'/api/v1/internal/billing/operations/payment/{pay_id}/reconcile'
        )
        assert r_yk_down.status_code == 503
        assert r_yk_down.json()['error_code'] == 'payment_provider_unavailable'
        assert repo.payments[pay_id].status == 'pending'
        provider.get_error = None

        # 2. Happy: принудительная сверка платежа без лимита 5 с -> succeeded,
        #    источник перехода в operation_events = 'admin'
        provider.get_status = 'succeeded'
        r_pay_ok = await client.post(
            f'/api/v1/internal/billing/operations/payment/{pay_id}/reconcile'
        )
        assert r_pay_ok.status_code == 200
        pay_data = r_pay_ok.json()
        assert pay_data == {
            'operation_type': 'payment',
            'operation_id': str(pay_id),
            'payment_id': str(pay_id),
            'status': 'succeeded',
            'transitioned': True,
        }
        assert repo.payments[pay_id].status == 'succeeded'
        assert any(
            ev[0] == 'payment'
            and ev[1] == pay_id
            and ev[3] == 'succeeded'
            and ev[4] == 'admin'
            for ev in repo.events
        )

        # 3. Happy: принудительная сверка возврата в статусе processing (1 с назад)
        sub_row = next(s for s in repo.subscriptions.values() if s.payment_id == pay_id)
        ref_id = uuid4()
        repo.refunds[ref_id] = FakeRefundRow(
            id=ref_id,
            payment_id=pay_id,
            subscription_id=sub_row.id,
            provider_code='yookassa',
            amount_kopeks=29900,
            status='processing',
            reason='Ручная проверка возврата',
            admin_user_id=uuid4(),
            idempotency_key=ref_id,
            provider_refund_id='yk_manual_ref_01',
            last_checked_at=now_utc - timedelta(seconds=1),
            created_at=now_utc - timedelta(seconds=1),
        )
        provider.next_get_refund_status = 'succeeded'
        r_ref_ok = await client.post(
            f'/api/v1/internal/billing/operations/refund/{ref_id}/reconcile'
        )
        assert r_ref_ok.status_code == 200
        ref_data = r_ref_ok.json()
        assert ref_data == {
            'operation_type': 'refund',
            'operation_id': str(ref_id),
            'payment_id': str(pay_id),
            'status': 'succeeded',
            'transitioned': True,
        }
        assert repo.refunds[ref_id].status == 'succeeded'
        assert any(
            ev[0] == 'refund'
            and ev[1] == ref_id
            and ev[3] == 'succeeded'
            and ev[4] == 'admin'
            for ev in repo.events
        )

        # 4. Unhappy: невалидный тип операции или UUID -> 422, сбой БД -> 503
        r_422 = await client.post(
            f'/api/v1/internal/billing/operations/subscription/{pay_id}/reconcile'
        )
        assert r_422.status_code == 422

        repo.db_unavailable = True
        r_db_503 = await client.post(
            f'/api/v1/internal/billing/operations/payment/{pay_id}/reconcile'
        )
        assert r_db_503.status_code == 503
        assert r_db_503.json()['error_code'] == 'database_unavailable'
