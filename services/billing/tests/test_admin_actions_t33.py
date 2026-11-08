from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from dependencies import get_repositories, get_service_credentials
from main import create_app
from security.service_credentials import ServiceCredentials
from tests.fakes import (
    FakeAdminActionRow,
    FakeOutboxTaskRow,
    FakeRefundRow,
    InMemoryPurchaseRepository,
    in_memory_repositories,
)

SERVICE_KEY = 'billing-internal-admin-actions-key-long-enough'
ACTIONS_PATH = '/api/v1/internal/billing/admin-actions'


@pytest.mark.parametrize(
    ('header_name', 'header_value', 'expected_status'),
    [
        ('X-Service-Api-Key', SERVICE_KEY, 200),
        ('X-Service-Key', SERVICE_KEY, 200),
        ('X-Service-Api-Key', 'wrong-key', 401),
        (None, None, 401),
    ],
)
async def test_admin_actions_list_requires_service_key_step(
    header_name: str | None,
    header_value: str | None,
    expected_status: int,
) -> None:
    """T-33 (BIL-API-14, BIL-NFR-SEC-03): проверка сервисного ключа."""
    repo = InMemoryPurchaseRepository()
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_repositories] = lambda: in_memory_repositories(repo)
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    headers: dict[str, str] = {'X-Request-Id': 't33-auth'}
    if header_name is not None and header_value is not None:
        headers[header_name] = header_value

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers=headers,
    ) as client:
        resp = await client.get(ACTIONS_PATH)
        assert resp.status_code == expected_status
        if expected_status == 200:
            assert resp.json() == {'total': 0, 'items': []}


async def test_admin_actions_list_filters_target_resolution_and_unhappy_paths() -> None:
    """T-33 (BIL-FR-13, BIL-API-14, US-A-05): журнал действий администраторов."""
    repo = InMemoryPurchaseRepository()
    now_utc = datetime.now(UTC)

    admin_1 = uuid4()
    admin_2 = uuid4()
    pay_1 = uuid4()
    pay_2 = uuid4()
    sub_1 = uuid4()
    ref_1 = uuid4()
    task_1 = uuid4()

    repo.refunds[ref_1] = FakeRefundRow(
        id=ref_1,
        payment_id=pay_1,
        subscription_id=sub_1,
        provider_code='yookassa',
        amount_kopeks=29900,
        status='succeeded',
        reason='Возврат по обращению #1',
        admin_user_id=admin_1,
        idempotency_key=ref_1,
        created_at=now_utc - timedelta(days=2),
    )
    repo.outbox_tasks['k_task_1'] = FakeOutboxTaskRow(
        id=task_1,
        dedup_key='k_task_1',
        target_service='notifications',
        task_type='payment_succeeded_email',
        payment_id=pay_2,
        user_id=uuid4(),
        access_version=None,
        payload={},
        status='closed',
        attempts=20,
        last_error='Invalid email',
        created_at=now_utc - timedelta(days=1),
    )

    act_refund_id = uuid4()
    act_close_id = uuid4()
    repo.admin_actions.extend(
        [
            FakeAdminActionRow(
                id=act_refund_id,
                admin_user_id=admin_1,
                action_type='refund_requested',
                target_type='refund',
                target_id=ref_1,
                reason='Возврат по обращению #1',
                created_at=now_utc - timedelta(days=2),
            ),
            FakeAdminActionRow(
                id=act_close_id,
                admin_user_id=admin_2,
                action_type='outbox_closed',
                target_type='outbox_task',
                target_id=task_1,
                reason='Закрыто из-за некорректного email',
                created_at=now_utc - timedelta(days=1),
            ),
        ]
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_repositories] = lambda: in_memory_repositories(repo)
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={
            'X-Request-Id': 't33-actions',
            'X-Service-Api-Key': SERVICE_KEY,
        },
    ) as client:
        # 1. Список отсортирован по created_at DESC и включает payment_id и статус
        all_resp = await client.get(ACTIONS_PATH)
        assert all_resp.status_code == 200
        all_data = all_resp.json()
        assert all_data['total'] == 2
        assert [i['id'] for i in all_data['items']] == [
            str(act_close_id),
            str(act_refund_id),
        ]
        assert all_data['items'][0]['payment_id'] == str(pay_2)
        assert all_data['items'][0]['target_status'] == 'closed'
        assert all_data['items'][1]['payment_id'] == str(pay_1)
        assert all_data['items'][1]['target_status'] == 'succeeded'

        # 2. Фильтр по admin_user_id и action_type
        by_adm = await client.get(ACTIONS_PATH, params={'admin_user_id': str(admin_1)})
        assert by_adm.status_code == 200
        assert [i['id'] for i in by_adm.json()['items']] == [str(act_refund_id)]

        by_type = await client.get(
            ACTIONS_PATH, params={'action_type': 'outbox_closed'}
        )
        assert by_type.status_code == 200
        assert [i['id'] for i in by_type.json()['items']] == [str(act_close_id)]

        # 3. Unhappy paths: невалидные параметры -> 422
        invalid_params: list[dict[str, str | int]] = [
            {'admin_user_id': 'not-a-uuid'},
            {'action_type': 'invalid_type'},
            {'limit': 0},
            {'limit': 101},
            {'offset': -1},
            {
                'created_from': now_utc.isoformat(),
                'created_to': (now_utc - timedelta(days=1)).isoformat(),
            },
        ]
        for bad in invalid_params:
            bad_res = await client.get(ACTIONS_PATH, params=bad)
            assert bad_res.status_code == 422, f'Expected 422 for {bad}'

        # 4. Unhappy: БД недоступна -> 503
        repo.db_unavailable = True
        db_err = await client.get(ACTIONS_PATH)
        assert db_err.status_code == 503
        assert db_err.json()['error_code'] == 'database_unavailable'
