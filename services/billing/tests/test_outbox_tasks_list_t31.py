from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from core.config import settings
from dependencies import get_purchase_repository, get_service_credentials
from main import create_app
from security.service_credentials import ServiceCredentials
from tests.fakes import FakeOutboxTaskRow, InMemoryPurchaseRepository

SERVICE_KEY = 'billing-internal-outbox-list-key-long-enough'
OUTBOX_PATH = '/api/v1/internal/billing/outbox-tasks'


@pytest.mark.parametrize(
    ('header_name', 'header_value', 'expected_status'),
    [
        ('X-Service-Api-Key', SERVICE_KEY, 200),
        ('X-Service-Key', SERVICE_KEY, 200),
        ('X-Service-Api-Key', 'wrong-key', 401),
        (None, None, 401),
    ],
)
async def test_outbox_tasks_list_requires_service_key_step(
    header_name: str | None,
    header_value: str | None,
    expected_status: int,
) -> None:
    """T-31 (BIL-API-11, BIL-NFR-SEC-03): проверка сервисного ключа."""
    repo = InMemoryPurchaseRepository()
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    headers: dict[str, str] = {'X-Request-Id': 't31-auth'}
    if header_name is not None and header_value is not None:
        headers[header_name] = header_value

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers=headers,
    ) as client:
        resp = await client.get(OUTBOX_PATH)
        assert resp.status_code == expected_status
        if expected_status == 200:
            assert resp.json() == {'total': 0, 'items': []}


async def test_outbox_tasks_list_threshold_filters_and_unhappy_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-31 (BIL-FR-14, BIL-API-11, US-A-02): выборка проблемных задач outbox."""
    monkeypatch.setattr(settings.worker, 'billing_stuck_threshold_seconds', 600)
    repo = InMemoryPurchaseRepository()
    now_utc = datetime.now(UTC)

    user_1 = uuid4()
    user_2 = uuid4()
    pay_1 = uuid4()
    pay_2 = uuid4()

    # 1. Задача в requires_intervention (создана только что — видна СРАЗУ без порога!)
    t_intervene_id = uuid4()
    repo.outbox_tasks['k_intervene'] = FakeOutboxTaskRow(
        id=t_intervene_id,
        dedup_key='k_intervene',
        target_service='auth',
        task_type='sync_access',
        payment_id=pay_1,
        user_id=user_1,
        access_version=1,
        payload={'version': 1},
        status='requires_intervention',
        attempts=20,
        last_error='Auth HTTP 404 user not found',
        created_at=now_utc - timedelta(seconds=15),
        updated_at=now_utc - timedelta(seconds=15),
    )

    # 2. Задача pending старше 10 минут (15 минут назад — видна в фильтре!)
    t_stuck_pending_id = uuid4()
    repo.outbox_tasks['k_stuck_pending'] = FakeOutboxTaskRow(
        id=t_stuck_pending_id,
        dedup_key='k_stuck_pending',
        target_service='notifications',
        task_type='payment_succeeded_email',
        payment_id=pay_2,
        user_id=user_2,
        access_version=None,
        payload={'subject': 'Оплачено'},
        status='pending',
        attempts=5,
        last_error='Notifications HTTP 503',
        created_at=now_utc - timedelta(minutes=15),
        updated_at=now_utc - timedelta(minutes=1),
    )

    # 3. Свежая задача pending младше 10 минут (1 минута назад — НЕ должна попадать)
    repo.outbox_tasks['k_fresh_pending'] = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key='k_fresh_pending',
        target_service='auth',
        task_type='sync_access',
        payment_id=uuid4(),
        user_id=uuid4(),
        access_version=1,
        payload={'version': 1},
        status='pending',
        attempts=0,
        last_error=None,
        created_at=now_utc - timedelta(minutes=1),
        updated_at=now_utc - timedelta(minutes=1),
    )

    # 4. Завершённая и закрытая задачи (completed, closed — НЕ должны попадать)
    repo.outbox_tasks['k_completed'] = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key='k_completed',
        target_service='auth',
        task_type='sync_access',
        payment_id=pay_1,
        user_id=user_1,
        access_version=1,
        payload={'version': 1},
        status='completed',
        attempts=1,
        created_at=now_utc - timedelta(hours=1),
    )
    repo.outbox_tasks['k_closed'] = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key='k_closed',
        target_service='notifications',
        task_type='payment_failed_email',
        payment_id=pay_1,
        user_id=user_1,
        access_version=None,
        payload={},
        status='closed',
        attempts=20,
        created_at=now_utc - timedelta(hours=1),
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
            'X-Request-Id': 't31-outbox',
            'X-Service-Api-Key': SERVICE_KEY,
        },
    ) as client:
        # 1. По умолчанию: возвращает только requires_intervention и старую pending
        resp = await client.get(OUTBOX_PATH)
        assert resp.status_code == 200
        data = resp.json()
        assert data['total'] == 2
        assert [i['id'] for i in data['items']] == [
            str(t_intervene_id),
            str(t_stuck_pending_id),
        ]
        assert data['items'][0]['can_retry_or_close'] is True
        assert data['items'][1]['can_retry_or_close'] is False

        # 2. Фильтры по target_service, status, user_id и payment_id
        by_srv = await client.get(
            OUTBOX_PATH, params={'target_service': 'notifications'}
        )
        assert by_srv.status_code == 200
        assert [i['id'] for i in by_srv.json()['items']] == [str(t_stuck_pending_id)]

        by_status = await client.get(
            OUTBOX_PATH, params={'status': 'requires_intervention'}
        )
        assert by_status.status_code == 200
        assert [i['id'] for i in by_status.json()['items']] == [str(t_intervene_id)]

        by_user = await client.get(OUTBOX_PATH, params={'user_id': str(user_1)})
        assert by_user.status_code == 200
        assert [i['id'] for i in by_user.json()['items']] == [str(t_intervene_id)]

        by_pay = await client.get(OUTBOX_PATH, params={'payment_id': str(pay_2)})
        assert by_pay.status_code == 200
        assert [i['id'] for i in by_pay.json()['items']] == [str(t_stuck_pending_id)]

        # 3. Unhappy paths: невалидные query-параметры -> 422
        invalid_params: list[dict[str, str | int]] = [
            {'target_service': 'unknown'},
            {'status': 'completed'},
            {'user_id': 'not-a-uuid'},
            {'payment_id': 'not-a-uuid'},
            {'limit': 0},
            {'limit': 101},
            {'offset': -1},
            {
                'created_from': now_utc.isoformat(),
                'created_to': (now_utc - timedelta(days=1)).isoformat(),
            },
        ]
        for bad in invalid_params:
            bad_res = await client.get(OUTBOX_PATH, params=bad)
            assert bad_res.status_code == 422, f'Expected 422 for {bad}'

        # 4. Unhappy path: БД недоступна -> 503
        repo.db_unavailable = True
        db_err = await client.get(OUTBOX_PATH)
        assert db_err.status_code == 503
        assert db_err.json()['error_code'] == 'database_unavailable'
