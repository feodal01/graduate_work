from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from dependencies import get_purchase_repository, get_service_credentials
from main import create_app
from security.service_credentials import ServiceCredentials
from tests.fakes import (
    FakeOutboxTaskRow,
    FakePaymentRow,
    FakeRefundRow,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
)

SERVICE_KEY = 'billing-internal-card-service-key-long-enough'


@pytest.mark.parametrize(
    ('header_name', 'header_value', 'expected_status'),
    [
        ('X-Service-Api-Key', SERVICE_KEY, 404),
        ('X-Service-Key', SERVICE_KEY, 404),
        ('X-Service-Api-Key', 'wrong-key', 401),
        (None, None, 401),
    ],
)
async def test_payment_card_auth_and_not_found_step(
    header_name: str | None,
    header_value: str | None,
    expected_status: int,
) -> None:
    """T-28 (BIL-API-08, BIL-NFR-SEC-03): проверка ключа и 404 на неизвестный платёж."""
    repo = InMemoryPurchaseRepository()
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    headers: dict[str, str] = {'X-Request-Id': 't28-auth-step'}
    if header_name is not None and header_value is not None:
        headers[header_name] = header_value

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers=headers,
    ) as client:
        resp = await client.get(f'/api/v1/internal/billing/payments/{uuid4()}')
        assert resp.status_code == expected_status
        if expected_status == 404:
            assert resp.json()['error_code'] == 'payment_not_found'


async def test_payment_card_full_chronology_can_refund_rules_and_unhappy_paths() -> (
    None
):
    """T-28 (BIL-API-08, BIL-FR-13): карточка платежа и правила can_refund."""
    repo = InMemoryPurchaseRepository()
    now_utc = datetime.now(UTC)
    user_id = uuid4()
    admin_id = uuid4()
    tariff_id = uuid4()
    pay_id = uuid4()
    sub_id = uuid4()
    rejected_ref_id = uuid4()

    repo.payments[pay_id] = FakePaymentRow(
        id=pay_id,
        user_id=user_id,
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='succeeded',
        idempotency_key=pay_id,
        provider_payment_id='yk_card_test_01',
        created_at=now_utc - timedelta(hours=2),
        updated_at=now_utc - timedelta(hours=1),
    )
    repo.subscriptions[sub_id] = FakeSubscriptionRow(
        id=sub_id,
        user_id=user_id,
        tariff_id=tariff_id,
        payment_id=pay_id,
        starts_at=now_utc - timedelta(hours=1),
        ends_at=now_utc + timedelta(days=29),
        created_at=now_utc - timedelta(hours=1),
    )
    repo.refunds[rejected_ref_id] = FakeRefundRow(
        id=rejected_ref_id,
        payment_id=pay_id,
        subscription_id=sub_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        status='rejected',
        reason='Первая тестовая попытка возврата',
        admin_user_id=admin_id,
        idempotency_key=rejected_ref_id,
        provider_refund_id='yk_ref_rej_01',
        rejection_reason='rejected_by_provider',
        created_at=now_utc - timedelta(minutes=30),
    )
    repo.events.extend(
        [
            ('payment', pay_id, None, 'pending', 'continue', {'created': True}),
            (
                'payment',
                pay_id,
                'pending',
                'succeeded',
                'webhook',
                {'id': 'yk_card_test_01'},
            ),
            (
                'refund',
                rejected_ref_id,
                'processing',
                'rejected',
                'reconcile',
                {'rejection_reason': 'rejected_by_provider'},
            ),
        ]
    )
    task_auth_id = uuid4()
    task_ntf_id = uuid4()
    repo.outbox_tasks[f'sync:{pay_id}'] = FakeOutboxTaskRow(
        id=task_auth_id,
        dedup_key=f'sync:{pay_id}',
        target_service='auth',
        task_type='sync_access',
        payment_id=pay_id,
        user_id=user_id,
        access_version=1,
        payload={'version': 1},
        status='completed',
        attempts=1,
        last_error=None,
    )
    repo.outbox_tasks[f'ntf:{pay_id}'] = FakeOutboxTaskRow(
        id=task_ntf_id,
        dedup_key=f'ntf:{pay_id}',
        target_service='notifications',
        task_type='payment_succeeded_email',
        payment_id=pay_id,
        user_id=user_id,
        access_version=None,
        payload={'subject': 'Оплачено'},
        status='requires_intervention',
        attempts=20,
        last_error='Notifications HTTP 500',
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={
            'X-Request-Id': 't28-card',
            'X-Service-Api-Key': SERVICE_KEY,
        },
    ) as client:
        # 1. Полная карточка: подписка активна, возврат rejected -> can_refund=True
        resp = await client.get(f'/api/v1/internal/billing/payments/{pay_id}')
        assert resp.status_code == 200
        card = resp.json()
        assert card['id'] == str(pay_id)
        assert card['user_id'] == str(user_id)
        assert card['provider_payment_id'] == 'yk_card_test_01'
        assert card['status'] == 'succeeded'
        assert card['can_refund'] is True
        assert card['can_reconcile_payment'] is False

        assert card['subscription'] is not None
        assert card['subscription']['id'] == str(sub_id)
        assert card['subscription']['status'] == 'active'

        assert len(card['refunds']) == 1
        ref_item = card['refunds'][0]
        assert ref_item['id'] == str(rejected_ref_id)
        assert ref_item['status'] == 'rejected'
        assert ref_item['admin_user_id'] == str(admin_id)
        assert ref_item['reason'] == 'Первая тестовая попытка возврата'
        assert ref_item['rejection_reason'] == 'rejected_by_provider'
        assert ref_item['can_reconcile'] is False

        assert len(card['operation_events']) == 3
        sources = [e['source'] for e in card['operation_events']]
        assert sources == ['continue', 'webhook', 'reconcile']

        assert len(card['outbox_tasks']) == 2
        ntf_task = next(
            t for t in card['outbox_tasks'] if t['target_service'] == 'notifications'
        )
        assert ntf_task['status'] == 'requires_intervention'
        assert ntf_task['attempts'] == 20
        assert ntf_task['last_error'] == 'Notifications HTTP 500'

        # 2. При возврате processing -> can_refund=False, can_reconcile=True
        proc_ref_id = uuid4()
        repo.refunds[proc_ref_id] = FakeRefundRow(
            id=proc_ref_id,
            payment_id=pay_id,
            subscription_id=sub_id,
            provider_code='yookassa',
            amount_kopeks=29900,
            status='processing',
            reason='Вторая попытка возврата',
            admin_user_id=admin_id,
            idempotency_key=proc_ref_id,
            created_at=now_utc - timedelta(minutes=5),
        )
        resp_proc = await client.get(f'/api/v1/internal/billing/payments/{pay_id}')
        assert resp_proc.status_code == 200
        assert resp_proc.json()['can_refund'] is False
        proc_item = next(
            r for r in resp_proc.json()['refunds'] if r['id'] == str(proc_ref_id)
        )
        assert proc_item['can_reconcile'] is True

        # 3. Если подписка завершена (ends_at в прошлом) -> can_refund=False
        del repo.refunds[proc_ref_id]
        repo.subscriptions[sub_id].ends_at = now_utc - timedelta(minutes=1)
        resp_exp = await client.get(f'/api/v1/internal/billing/payments/{pay_id}')
        assert resp_exp.status_code == 200
        assert resp_exp.json()['subscription']['status'] == 'expired'
        assert resp_exp.json()['can_refund'] is False

        # 4. Unhappy paths: невалидный UUID -> 422, отказ БД -> 503
        bad_uuid = await client.get('/api/v1/internal/billing/payments/not-a-uuid')
        assert bad_uuid.status_code == 422
        assert bad_uuid.json()['error_code'] == 'validation_error'

        repo.db_unavailable = True
        db_err = await client.get(f'/api/v1/internal/billing/payments/{pay_id}')
        assert db_err.status_code == 503
        assert db_err.json()['error_code'] == 'database_unavailable'
