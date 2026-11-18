from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from clients.payment_provider import ProviderTransientError
from clients.payment_provider_registry import PaymentProviderRegistry
from dependencies import (
    get_payment_reconciler,
    get_repositories,
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
    in_memory_repositories,
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
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_repositories] = lambda: in_memory_repositories(repo)
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
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
    )

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
    app.dependency_overrides[get_repositories] = lambda: in_memory_repositories(repo)
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


def _unchanged_admin_events(
    repo: InMemoryPurchaseRepository,
) -> list[tuple[str, object, str | None, str, str]]:
    return [e[:5] for e in repo.events if e[2] == e[3]]


def _seed_check_targets(
    repo: InMemoryPurchaseRepository,
) -> tuple[FakePaymentRow, FakeRefundRow]:
    now_utc = datetime.now(UTC)
    pay_id = uuid4()
    payment = FakePaymentRow(
        id=pay_id,
        user_id=uuid4(),
        tariff_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='pending',
        idempotency_key=pay_id,
        provider_payment_id='yk_p30_pay',
        created_at=now_utc - timedelta(minutes=1),
    )
    repo.payments[pay_id] = payment
    paid_id = uuid4()
    repo.payments[paid_id] = FakePaymentRow(
        id=paid_id,
        user_id=uuid4(),
        tariff_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='succeeded',
        idempotency_key=paid_id,
        provider_payment_id='yk_p30_paid',
    )
    ref_id = uuid4()
    refund = FakeRefundRow(
        id=ref_id,
        payment_id=paid_id,
        subscription_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29900,
        status='processing',
        reason='Возврат',
        admin_user_id=uuid4(),
        idempotency_key=ref_id,
        provider_refund_id='yk_p30_ref',
    )
    repo.refunds[ref_id] = refund
    return payment, refund


async def test_admin_check_without_status_change_writes_admin_event() -> None:
    """US-A-06, BIL-FR-13: проверка без смены статуса — запись X -> X от admin."""
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    provider.get_status = 'pending'
    provider.next_get_refund_status = 'processing'
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
    )
    payment, refund = _seed_check_targets(repo)

    await reconciler.reconcile_operation_by_admin('payment', payment.id)
    await reconciler.reconcile_operation_by_admin('refund', refund.id)

    assert _unchanged_admin_events(repo) == [
        ('payment', payment.id, 'pending', 'pending', 'admin'),
        ('refund', refund.id, 'processing', 'processing', 'admin'),
    ]
    assert repo.events[0][5] == {'check': 'status_unchanged'}


async def test_admin_first_empty_lookup_after_key_expiry_writes_admin_event() -> None:
    """Розыск по списку ЮKassa — тоже запрос к провайдеру: запись X -> X."""
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
    )
    _, refund = _seed_check_targets(repo)
    refund.provider_refund_id = None
    refund.created_at = datetime.now(UTC) - timedelta(hours=25)

    await reconciler.reconcile_operation_by_admin('refund', refund.id)

    assert refund.status == 'processing'
    assert _unchanged_admin_events(repo) == [
        ('refund', refund.id, 'processing', 'processing', 'admin')
    ]


async def test_checks_without_provider_query_or_not_by_admin_write_no_event() -> None:
    """Без запроса к ЮKassa или не по кнопке администратора записи X -> X нет."""
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    provider.get_status = 'pending'
    provider.next_get_refund_status = 'processing'
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
    )
    payment, refund = _seed_check_targets(repo)

    # Фоновая сверка, вебхук и /continue пишут журнал только при переходе.
    await reconciler.reconcile_payment(payment.id, source='reconcile')
    await reconciler.reconcile_by_provider_operation_id(
        'yk_p30_ref', provider_code='yookassa'
    )
    await reconciler.reconcile_payment_record(
        payment, source='continue', respect_throttle=False
    )
    # Попытка возврата в последний час ключа ждёт розыска без запросов (инв. 14).
    refund.provider_refund_id = None
    refund.created_at = datetime.now(UTC) - timedelta(hours=23, minutes=30)
    await reconciler.reconcile_operation_by_admin('refund', refund.id)
    # Завершённый платёж кнопка не сверяет (инв. 7).
    payment.status = 'failed'
    await reconciler.reconcile_operation_by_admin('payment', payment.id)
    # Сбой ЮKassa: администратор видит ошибку, записи нет.
    payment.status = 'pending'
    provider.get_error = ProviderTransientError('YooKassa 503', status_code=503)
    with pytest.raises(ProviderTransientError):
        await reconciler.reconcile_operation_by_admin('payment', payment.id)

    assert _unchanged_admin_events(repo) == []


async def test_admin_check_with_permanent_error_after_unknown_post_writes_event() -> (
    None
):
    """Инв. 15, US-A-06: 401 на повтор POST после тайм-аута — статус тот же."""
    from clients.payment_provider import ProviderPermanentError
    from repositories.records import POST_OUTCOME_UNKNOWN_KEY

    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    provider.create_refund_error = ProviderPermanentError(401, 'invalid credentials')
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
    )
    _, refund = _seed_check_targets(repo)
    refund.provider_refund_id = None
    refund.provider_details = {POST_OUTCOME_UNKNOWN_KEY: True}

    _, status, transitioned = await reconciler.reconcile_operation_by_admin(
        'refund', refund.id
    )

    assert (status, transitioned) == ('processing', False)
    assert len(provider.create_refund_calls) == 1
    assert _unchanged_admin_events(repo) == [
        ('refund', refund.id, 'processing', 'processing', 'admin')
    ]
