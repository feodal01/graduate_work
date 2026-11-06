from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from core.config import settings
from dependencies import get_purchase_repository, get_service_credentials
from main import create_app
from security.service_credentials import ServiceCredentials
from tests.fakes import (
    FakePaymentRow,
    FakeRefundRow,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
)

SERVICE_KEY = 'billing-internal-admin-service-key-long-enough'
SUBSCRIPTIONS_PATH = '/api/v1/internal/billing/subscriptions'
PAYMENTS_PATH = '/api/v1/internal/billing/payments'


def _seed_subscriptions_and_payments(
    repo: InMemoryPurchaseRepository,
    now_utc: datetime,
) -> dict[str, object]:
    tariff_id = uuid4()
    user_active = uuid4()
    user_expired = uuid4()
    user_refunded = uuid4()
    user_stuck = uuid4()

    # 1. Активная подписка (создана 2 дня назад, действует ещё 28 дней)
    pay_active_id = uuid4()
    sub_active_id = uuid4()
    repo.payments[pay_active_id] = FakePaymentRow(
        id=pay_active_id,
        user_id=user_active,
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='succeeded',
        idempotency_key=pay_active_id,
        provider_payment_id='yk_pay_active_01',
        created_at=now_utc - timedelta(days=2),
        updated_at=now_utc - timedelta(days=2),
    )
    repo.subscriptions[sub_active_id] = FakeSubscriptionRow(
        id=sub_active_id,
        user_id=user_active,
        tariff_id=tariff_id,
        payment_id=pay_active_id,
        starts_at=now_utc - timedelta(days=2),
        ends_at=now_utc + timedelta(days=28),
        created_at=now_utc - timedelta(days=2),
    )

    # 2. Завершённая подписка (закончилась 5 дней назад)
    pay_expired_id = uuid4()
    sub_expired_id = uuid4()
    repo.payments[pay_expired_id] = FakePaymentRow(
        id=pay_expired_id,
        user_id=user_expired,
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='succeeded',
        idempotency_key=pay_expired_id,
        provider_payment_id='yk_pay_expired_02',
        created_at=now_utc - timedelta(days=35),
        updated_at=now_utc - timedelta(days=35),
    )
    repo.subscriptions[sub_expired_id] = FakeSubscriptionRow(
        id=sub_expired_id,
        user_id=user_expired,
        tariff_id=tariff_id,
        payment_id=pay_expired_id,
        starts_at=now_utc - timedelta(days=35),
        ends_at=now_utc - timedelta(days=5),
        created_at=now_utc - timedelta(days=35),
    )

    # 3. Возвращённая подписка (ends_at в будущем, но есть возврат succeeded)
    pay_refunded_id = uuid4()
    sub_refunded_id = uuid4()
    ref_succeeded_id = uuid4()
    repo.payments[pay_refunded_id] = FakePaymentRow(
        id=pay_refunded_id,
        user_id=user_refunded,
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='succeeded',
        idempotency_key=pay_refunded_id,
        provider_payment_id='yk_pay_refunded_03',
        created_at=now_utc - timedelta(days=10),
        updated_at=now_utc - timedelta(days=9),
    )
    repo.subscriptions[sub_refunded_id] = FakeSubscriptionRow(
        id=sub_refunded_id,
        user_id=user_refunded,
        tariff_id=tariff_id,
        payment_id=pay_refunded_id,
        starts_at=now_utc - timedelta(days=10),
        ends_at=now_utc + timedelta(days=20),
        created_at=now_utc - timedelta(days=10),
    )
    repo.refunds[ref_succeeded_id] = FakeRefundRow(
        id=ref_succeeded_id,
        payment_id=pay_refunded_id,
        subscription_id=sub_refunded_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        status='succeeded',
        reason='Запрос клиента',
        admin_user_id=uuid4(),
        idempotency_key=ref_succeeded_id,
        created_at=now_utc - timedelta(days=9),
    )

    # 4. Зависший pending-платёж (старше 10 минут: 15 минут назад)
    pay_stuck_pending_id = uuid4()
    repo.payments[pay_stuck_pending_id] = FakePaymentRow(
        id=pay_stuck_pending_id,
        user_id=user_stuck,
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='pending',
        idempotency_key=pay_stuck_pending_id,
        provider_payment_id='yk_pay_stuck_04',
        created_at=now_utc - timedelta(minutes=15),
        updated_at=now_utc - timedelta(minutes=15),
    )

    # 5. Свежий pending-платёж (младше 10 минут: 2 минуты назад — НЕ зависший)
    pay_fresh_pending_id = uuid4()
    repo.payments[pay_fresh_pending_id] = FakePaymentRow(
        id=pay_fresh_pending_id,
        user_id=uuid4(),
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='pending',
        idempotency_key=pay_fresh_pending_id,
        provider_payment_id='yk_pay_fresh_05',
        created_at=now_utc - timedelta(minutes=2),
        updated_at=now_utc - timedelta(minutes=2),
    )

    # 6. Оплаченный платёж с зависшим возвратом processing (старше 10 минут)
    pay_stuck_refund_id = uuid4()
    sub_stuck_refund_id = uuid4()
    ref_stuck_id = uuid4()
    repo.payments[pay_stuck_refund_id] = FakePaymentRow(
        id=pay_stuck_refund_id,
        user_id=uuid4(),
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='succeeded',
        idempotency_key=pay_stuck_refund_id,
        provider_payment_id='yk_pay_stuck_ref_06',
        created_at=now_utc - timedelta(days=1),
        updated_at=now_utc - timedelta(minutes=20),
    )
    repo.subscriptions[sub_stuck_refund_id] = FakeSubscriptionRow(
        id=sub_stuck_refund_id,
        user_id=repo.payments[pay_stuck_refund_id].user_id,
        tariff_id=tariff_id,
        payment_id=pay_stuck_refund_id,
        starts_at=now_utc - timedelta(days=1),
        ends_at=now_utc + timedelta(days=29),
        created_at=now_utc - timedelta(days=1),
    )
    repo.refunds[ref_stuck_id] = FakeRefundRow(
        id=ref_stuck_id,
        payment_id=pay_stuck_refund_id,
        subscription_id=sub_stuck_refund_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        status='processing',
        reason='Зависший возврат',
        admin_user_id=uuid4(),
        idempotency_key=ref_stuck_id,
        created_at=now_utc - timedelta(minutes=20),
    )

    return {
        'user_active': user_active,
        'user_expired': user_expired,
        'user_refunded': user_refunded,
        'user_stuck': user_stuck,
        'sub_active_id': sub_active_id,
        'sub_expired_id': sub_expired_id,
        'sub_refunded_id': sub_refunded_id,
        'sub_stuck_refund_id': sub_stuck_refund_id,
        'pay_active_id': pay_active_id,
        'pay_expired_id': pay_expired_id,
        'pay_refunded_id': pay_refunded_id,
        'pay_stuck_pending_id': pay_stuck_pending_id,
        'pay_fresh_pending_id': pay_fresh_pending_id,
        'pay_stuck_refund_id': pay_stuck_refund_id,
    }


@pytest.mark.parametrize(
    ('path', 'header_name', 'header_value', 'expected_status'),
    [
        (SUBSCRIPTIONS_PATH, 'X-Service-Api-Key', SERVICE_KEY, 200),
        (SUBSCRIPTIONS_PATH, 'X-Service-Key', SERVICE_KEY, 200),
        (SUBSCRIPTIONS_PATH, 'X-Service-Api-Key', 'wrong-key', 401),
        (SUBSCRIPTIONS_PATH, None, None, 401),
        (PAYMENTS_PATH, 'X-Service-Api-Key', SERVICE_KEY, 200),
        (PAYMENTS_PATH, 'X-Service-Key', SERVICE_KEY, 200),
        (PAYMENTS_PATH, 'X-Service-Api-Key', 'wrong-key', 401),
        (PAYMENTS_PATH, None, None, 401),
    ],
)
async def test_internal_lists_require_service_api_key_step(
    path: str,
    header_name: str | None,
    header_value: str | None,
    expected_status: int,
) -> None:
    """T-26 (BIL-NFR-SEC-03, NFR-SEC-01): проверка сервисного ключа."""
    repo = InMemoryPurchaseRepository()
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    headers: dict[str, str] = {'X-Request-Id': 't26-auth-step'}
    if header_name is not None and header_value is not None:
        headers[header_name] = header_value

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers=headers,
    ) as client:
        resp = await client.get(path)
        assert resp.status_code == expected_status
        if expected_status == 401:
            assert resp.json()['error_code'] in (
                'unauthorized',
                'invalid_service_credentials',
            )
        else:
            assert resp.json() == {'total': 0, 'items': []}


async def test_internal_subscriptions_filters_and_unhappy_paths() -> None:
    """T-26 (BIL-API-06): фильтрация подписок и unhappy paths."""
    repo = InMemoryPurchaseRepository()
    now_utc = datetime.now(UTC)
    ids = _seed_subscriptions_and_payments(repo, now_utc)

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={
            'X-Request-Id': 't26-subs',
            'X-Service-Api-Key': SERVICE_KEY,
        },
    ) as client:
        # 1. Полный список подписок (4 подписки, отсортированы по created_at DESC)
        all_resp = await client.get(SUBSCRIPTIONS_PATH)
        assert all_resp.status_code == 200
        all_data = all_resp.json()
        assert all_data['total'] == 4
        assert [item['id'] for item in all_data['items']] == [
            str(ids['sub_stuck_refund_id']),
            str(ids['sub_active_id']),
            str(ids['sub_refunded_id']),
            str(ids['sub_expired_id']),
        ]

        # 2. Фильтр по user_id
        user_resp = await client.get(
            SUBSCRIPTIONS_PATH,
            params={'user_id': str(ids['user_active'])},
        )
        assert user_resp.status_code == 200
        assert user_resp.json()['total'] == 1
        item = user_resp.json()['items'][0]
        assert item['id'] == str(ids['sub_active_id'])
        assert item['payment_id'] == str(ids['pay_active_id'])
        assert item['status'] == 'active'
        assert item['amount_kopeks'] == 29900
        assert item['currency'] == 'RUB'
        assert item['tariff_code'] == 'premium'

        # 3. Фильтр по вычисленному статусу: active, expired, refunded
        active_resp = await client.get(SUBSCRIPTIONS_PATH, params={'status': 'active'})
        assert active_resp.status_code == 200
        assert {i['id'] for i in active_resp.json()['items']} == {
            str(ids['sub_active_id']),
            str(ids['sub_stuck_refund_id']),
        }

        expired_resp = await client.get(
            SUBSCRIPTIONS_PATH, params={'status': 'expired'}
        )
        assert expired_resp.status_code == 200
        assert [i['id'] for i in expired_resp.json()['items']] == [
            str(ids['sub_expired_id'])
        ]

        refunded_resp = await client.get(
            SUBSCRIPTIONS_PATH, params={'status': 'refunded'}
        )
        assert refunded_resp.status_code == 200
        assert [i['id'] for i in refunded_resp.json()['items']] == [
            str(ids['sub_refunded_id'])
        ]

        # 4. Фильтр по дате создания и пагинация
        date_resp = await client.get(
            SUBSCRIPTIONS_PATH,
            params={
                'created_from': (now_utc - timedelta(days=15)).isoformat(),
                'created_to': (now_utc - timedelta(days=5)).isoformat(),
            },
        )
        assert date_resp.status_code == 200
        assert [i['id'] for i in date_resp.json()['items']] == [
            str(ids['sub_refunded_id'])
        ]

        page_resp = await client.get(
            SUBSCRIPTIONS_PATH,
            params={'limit': 2, 'offset': 1},
        )
        assert page_resp.status_code == 200
        assert page_resp.json()['total'] == 4
        assert [i['id'] for i in page_resp.json()['items']] == [
            str(ids['sub_active_id']),
            str(ids['sub_refunded_id']),
        ]

        # 5. Unhappy path: пользователь без подписок -> пустой список
        empty_resp = await client.get(
            SUBSCRIPTIONS_PATH,
            params={'user_id': str(uuid4())},
        )
        assert empty_resp.status_code == 200
        assert empty_resp.json() == {'total': 0, 'items': []}

        # 6. Unhappy paths: невалидные query-параметры -> 422
        invalid_sub_params: list[dict[str, str | int]] = [
            {'status': 'unknown'},
            {'user_id': 'not-a-uuid'},
            {'limit': 0},
            {'limit': 101},
            {'offset': -1},
            {
                'created_from': now_utc.isoformat(),
                'created_to': (now_utc - timedelta(days=1)).isoformat(),
            },
        ]
        for bad_params in invalid_sub_params:
            bad_res = await client.get(SUBSCRIPTIONS_PATH, params=bad_params)
            assert bad_res.status_code == 422, f'Expected 422 for {bad_params}'
            assert bad_res.json()['error_code'] == 'validation_error'

        # 7. Unhappy path: БД недоступна -> 503
        repo.db_unavailable = True
        db_err = await client.get(SUBSCRIPTIONS_PATH)
        assert db_err.status_code == 503
        assert db_err.json()['error_code'] == 'database_unavailable'


async def test_internal_payments_filters_stuck_and_unhappy_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-26 (BIL-API-07, US-A-02): фильтры платежей, stuck=true и unhappy paths."""
    monkeypatch.setattr(settings.worker, 'billing_stuck_threshold_seconds', 600)
    repo = InMemoryPurchaseRepository()
    now_utc = datetime.now(UTC)
    ids = _seed_subscriptions_and_payments(repo, now_utc)

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={
            'X-Request-Id': 't26-payments',
            'X-Service-Api-Key': SERVICE_KEY,
        },
    ) as client:
        # 1. Фильтр по status='pending' -> 2 платежа (свежий и зависший)
        pending_resp = await client.get(PAYMENTS_PATH, params={'status': 'pending'})
        assert pending_resp.status_code == 200
        assert {i['id'] for i in pending_resp.json()['items']} == {
            str(ids['pay_stuck_pending_id']),
            str(ids['pay_fresh_pending_id']),
        }

        # 2. Фильтр has_refund=true -> 2 платежа (с succeeded и с processing возвратом)
        has_ref_resp = await client.get(PAYMENTS_PATH, params={'has_refund': 'true'})
        assert has_ref_resp.status_code == 200
        assert {i['id'] for i in has_ref_resp.json()['items']} == {
            str(ids['pay_refunded_id']),
            str(ids['pay_stuck_refund_id']),
        }

        no_ref_resp = await client.get(PAYMENTS_PATH, params={'has_refund': 'false'})
        assert no_ref_resp.status_code == 200
        assert no_ref_resp.json()['total'] == 4

        # 3. Поиск по id и по provider_payment_id
        by_id_resp = await client.get(
            PAYMENTS_PATH,
            params={'id': str(ids['pay_active_id'])},
        )
        assert by_id_resp.status_code == 200
        assert [i['id'] for i in by_id_resp.json()['items']] == [
            str(ids['pay_active_id'])
        ]

        by_prov_resp = await client.get(
            PAYMENTS_PATH,
            params={'provider_payment_id': 'yk_pay_refunded_03'},
        )
        assert by_prov_resp.status_code == 200
        assert [i['id'] for i in by_prov_resp.json()['items']] == [
            str(ids['pay_refunded_id'])
        ]
        assert by_prov_resp.json()['items'][0]['refund_status'] == 'succeeded'

        # 4. Фильтр «Зависшие операции» (stuck=true):
        # включает pending старше 10 мин и платёж с processing-возвратом старше 10 мин,
        # но НЕ включает свежий pending-платёж младше 10 мин!
        stuck_resp = await client.get(PAYMENTS_PATH, params={'stuck': 'true'})
        assert stuck_resp.status_code == 200
        assert {i['id'] for i in stuck_resp.json()['items']} == {
            str(ids['pay_stuck_pending_id']),
            str(ids['pay_stuck_refund_id']),
        }
        assert all(i['is_stuck'] is True for i in stuck_resp.json()['items'])

        # 5. Unhappy paths: невалидные параметры -> 422
        invalid_pay_params: list[dict[str, str | int]] = [
            {'status': 'invalid_status'},
            {'id': 'not-a-uuid'},
            {'user_id': 'not-a-uuid'},
            {'limit': 0},
            {'limit': 101},
            {'offset': -1},
            {
                'created_from': now_utc.isoformat(),
                'created_to': (now_utc - timedelta(days=1)).isoformat(),
            },
        ]
        for bad_params in invalid_pay_params:
            bad_res = await client.get(PAYMENTS_PATH, params=bad_params)
            assert bad_res.status_code == 422, f'Expected 422 for {bad_params}'
            assert bad_res.json()['error_code'] == 'validation_error'

        # 6. Unhappy path: БД недоступна -> 503
        repo.db_unavailable = True
        db_err = await client.get(PAYMENTS_PATH)
        assert db_err.status_code == 503
        assert db_err.json()['error_code'] == 'database_unavailable'
