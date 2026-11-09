from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from clients.auth import (
    AuthServiceUnavailableError,
    LogoutOutcome,
    SubscriptionAccessSyncResult,
)
from dependencies import (
    get_outbox_dispatcher,
    get_repositories,
    get_service_credentials,
)
from main import create_app
from security.service_credentials import ServiceCredentials
from services.outbox_dispatcher import OutboxDispatcher
from tests.fakes import (
    FakeOutboxTaskRow,
    FakePaymentRow,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
    in_memory_repositories,
)

SERVICE_KEY = 'billing-internal-outbox-actions-key-long-enough'


class _StubAuthGateway:
    def __init__(self) -> None:
        self.fail_transient = False
        self.calls: list[tuple[UUID, int, str | None]] = []

    async def login(self, login: str, password: str, *, request_id: str) -> Any:
        raise NotImplementedError

    async def logout(
        self, access_token: str, refresh_token: str, *, request_id: str
    ) -> LogoutOutcome:
        raise NotImplementedError

    async def get_current_user(
        self, access_token: str, *, request_id: str
    ) -> tuple[UUID, str, str]:
        raise NotImplementedError

    async def put_subscription_access(
        self,
        user_id: UUID,
        *,
        version: int,
        expires_at: str | None,
        request_id: str,
    ) -> SubscriptionAccessSyncResult:
        if self.fail_transient:
            raise AuthServiceUnavailableError('Auth 503')
        self.calls.append((user_id, version, expires_at))
        return SubscriptionAccessSyncResult(applied=True, current_version=version)


@pytest.mark.parametrize(
    ('action', 'header_name', 'header_value', 'expected_status'),
    [
        ('retry', 'X-Service-Api-Key', SERVICE_KEY, 404),
        ('retry', 'X-Service-Key', SERVICE_KEY, 404),
        ('retry', 'X-Service-Api-Key', 'wrong-key', 401),
        ('retry', None, None, 401),
        ('close', 'X-Service-Api-Key', SERVICE_KEY, 404),
        ('close', 'X-Service-Key', SERVICE_KEY, 404),
        ('close', 'X-Service-Api-Key', 'wrong-key', 401),
        ('close', None, None, 401),
    ],
)
async def test_outbox_actions_require_service_key_and_404_step(
    action: str,
    header_name: str | None,
    header_value: str | None,
    expected_status: int,
) -> None:
    """T-32 (BIL-API-12, BIL-API-13, BIL-NFR-SEC-03): проверка ключа и 404."""
    repo = InMemoryPurchaseRepository()
    auth_gw = _StubAuthGateway()
    dispatcher = OutboxDispatcher(repository=repo, auth_gateway=auth_gw)

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_repositories] = lambda: in_memory_repositories(repo)
    app.dependency_overrides[get_outbox_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    headers: dict[str, str] = {'X-Request-Id': 't32-auth'}
    if header_name is not None and header_value is not None:
        headers[header_name] = header_value

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers=headers,
    ) as client:
        resp = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{uuid4()}/{action}',
            json={'admin_user_id': str(uuid4()), 'reason': 'Тестовая причина'},
        )
        assert resp.status_code == expected_status
        if expected_status == 404:
            assert resp.json()['error_code'] == 'outbox_task_not_found'


async def test_outbox_retry_and_close_happy_and_unhappy_paths() -> None:
    """T-32 (BIL-FR-14, BIL-API-12, BIL-API-13, US-A-02, инв. 9): retry и close."""
    repo = InMemoryPurchaseRepository()
    auth_gw = _StubAuthGateway()
    dispatcher = OutboxDispatcher(repository=repo, auth_gateway=auth_gw)

    now_utc = datetime.now(UTC)
    user_id = uuid4()
    admin_id = uuid4()
    pay_id = uuid4()
    sub_id = uuid4()

    repo.payments[pay_id] = FakePaymentRow(
        id=pay_id,
        user_id=user_id,
        tariff_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='succeeded',
        idempotency_key=pay_id,
        provider_payment_id='yk_t32_01',
        created_at=now_utc - timedelta(hours=1),
    )
    repo.subscriptions[sub_id] = FakeSubscriptionRow(
        id=sub_id,
        user_id=user_id,
        tariff_id=repo.payments[pay_id].tariff_id,
        payment_id=pay_id,
        starts_at=now_utc - timedelta(hours=1),
        ends_at=now_utc + timedelta(days=29),
        created_at=now_utc - timedelta(hours=1),
    )
    repo.access_versions[user_id] = 1

    t_retry_id = uuid4()
    t_close_id = uuid4()
    t_pending_id = uuid4()
    expires_iso = (now_utc + timedelta(days=29)).isoformat()

    repo.outbox_tasks['k_retry'] = FakeOutboxTaskRow(
        id=t_retry_id,
        dedup_key='k_retry',
        target_service='auth',
        task_type='sync_access',
        payment_id=pay_id,
        user_id=user_id,
        access_version=1,
        payload={'version': 1, 'expires_at': expires_iso},
        status='requires_intervention',
        attempts=20,
        last_error='Auth HTTP 503 after 20 attempts',
    )
    repo.outbox_tasks['k_close'] = FakeOutboxTaskRow(
        id=t_close_id,
        dedup_key='k_close',
        target_service='notifications',
        task_type='payment_succeeded_email',
        payment_id=pay_id,
        user_id=user_id,
        access_version=None,
        payload={'subject': 'Оплачено'},
        status='requires_intervention',
        attempts=20,
        last_error='Notifications HTTP 422 invalid recipient',
    )
    repo.outbox_tasks['k_pending'] = FakeOutboxTaskRow(
        id=t_pending_id,
        dedup_key='k_pending',
        target_service='notifications',
        task_type='payment_failed_email',
        payment_id=pay_id,
        user_id=user_id,
        access_version=None,
        payload={'subject': 'Отказ'},
        status='pending',
        attempts=2,
        last_error='Transient error',
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_repositories] = lambda: in_memory_repositories(repo)
    app.dependency_overrides[get_outbox_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={
            'X-Request-Id': 't32-actions',
            'X-Service-Api-Key': SERVICE_KEY,
        },
    ) as client:
        # 1. Retry при временно недоступном Auth: сбрасывает attempts в 0, пишет в
        #    admin_actions и запускает отправку (attempts=1, status=pending)
        auth_gw.fail_transient = True
        r_retry_transient = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{t_retry_id}/retry',
            json={'admin_user_id': str(admin_id)},
        )
        assert r_retry_transient.status_code == 200
        assert r_retry_transient.json()['status'] == 'pending'
        assert r_retry_transient.json()['attempts'] == 1
        assert len(repo.admin_actions) == 1
        assert repo.admin_actions[0].action_type == 'outbox_retried'
        assert repo.admin_actions[0].target_type == 'outbox_task'
        assert repo.admin_actions[0].target_id == t_retry_id

        # 2. Retry при восстановившемся Auth: задача доставляется -> completed!
        repo.outbox_tasks['k_retry'].status = 'requires_intervention'
        repo.outbox_tasks['k_retry'].attempts = 20
        auth_gw.fail_transient = False
        r_retry_ok = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{t_retry_id}/retry',
            json={
                'admin_user_id': str(admin_id),
                'reason': 'Auth восстановлен, повторяем синхронизацию',
            },
        )
        assert r_retry_ok.status_code == 200
        assert r_retry_ok.json()['status'] == 'completed'
        assert r_retry_ok.json()['can_retry_or_close'] is False
        assert auth_gw.calls == [(user_id, 1, expires_iso)]
        assert len(repo.admin_actions) == 2

        # 3. Unhappy: повтор уже выполненной задачи completed -> 409
        r_retry_done = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{t_retry_id}/retry',
            json={'admin_user_id': str(admin_id)},
        )
        assert r_retry_done.status_code == 409
        assert r_retry_done.json()['error_code'] == 'outbox_task_not_retryable'

        # 4. Unhappy: закрытие без причины -> 422 (инвариант 9, BIL-API-13)
        for bad_reason in ('', '   '):
            r_close_422 = await client.post(
                f'/api/v1/internal/billing/outbox-tasks/{t_close_id}/close',
                json={'admin_user_id': str(admin_id), 'reason': bad_reason},
            )
            assert r_close_422.status_code == 422

        # 5. Unhappy: закрытие задачи не в requires_intervention -> 409
        for non_intervene_id in (t_pending_id, t_retry_id):
            r_close_409 = await client.post(
                f'/api/v1/internal/billing/outbox-tasks/{non_intervene_id}/close',
                json={'admin_user_id': str(admin_id), 'reason': 'Причина'},
            )
            assert r_close_409.status_code == 409
            assert (
                r_close_409.json()['error_code']
                == 'outbox_task_not_requires_intervention'
            )

        # 6. Happy: закрытие задачи requires_intervention с причиной -> status='closed',
        #    запись в admin_actions, а платёж и подписка НЕ меняются!
        r_close_ok = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{t_close_id}/close',
            json={
                'admin_user_id': str(admin_id),
                'reason': 'Некорректный email профиля, клиент уведомлён в чате',
            },
        )
        assert r_close_ok.status_code == 200
        assert r_close_ok.json()['status'] == 'closed'
        assert r_close_ok.json()['can_retry_or_close'] is False
        assert repo.payments[pay_id].status == 'succeeded'
        assert len(repo.subscriptions) == 1
        assert repo.admin_actions[-1].action_type == 'outbox_closed'
        assert repo.admin_actions[-1].target_id == t_close_id
        assert (
            repo.admin_actions[-1].reason
            == 'Некорректный email профиля, клиент уведомлён в чате'
        )

        # Повторное закрытие или повтор закрытой задачи -> 409
        r_close_again = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{t_close_id}/close',
            json={'admin_user_id': str(admin_id), 'reason': 'Повторное закрытие'},
        )
        assert r_close_again.status_code == 409

        r_retry_closed = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{t_close_id}/retry',
            json={'admin_user_id': str(admin_id)},
        )
        assert r_retry_closed.status_code == 409

        # 7. Unhappy: сбой БД -> 503
        repo.db_unavailable = True
        r_db_err = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{t_close_id}/close',
            json={'admin_user_id': str(admin_id), 'reason': 'Причина'},
        )
        assert r_db_err.status_code == 503
        assert r_db_err.json()['error_code'] == 'database_unavailable'


@pytest.mark.parametrize('status', ['pending', 'completed', 'closed'])
async def test_retry_is_only_for_requires_intervention(status: str) -> None:
    """«Повторить» доступен только в «Требует вмешательства» (US-A-02, BIL-API-12).

    Регрессия: retry pending-задачи обнулял attempts и next_retry_at, обходя
    backoff и лимит попыток, и писал лишнее действие в журнал.
    """
    from domain.errors import RuleConflictError

    repo = InMemoryPurchaseRepository()
    payment_id = uuid4()
    task = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'retry_guard:{payment_id}',
        target_service='notifications',
        task_type='payment_succeeded_email',
        payment_id=payment_id,
        user_id=uuid4(),
        access_version=None,
        payload={},
        status=status,
        attempts=3,
    )
    repo.outbox_tasks[task.dedup_key] = task

    with pytest.raises(RuleConflictError) as exc_info:
        await repo.retry_outbox_task_by_admin(
            task.id, admin_user_id=uuid4(), reason='Повтор'
        )
    assert exc_info.value.error_code == 'outbox_task_not_retryable'
    assert task.attempts == 3


@pytest.mark.parametrize('task_type', ['sync_access', 'cancel_expiration_email'])
async def test_retry_and_close_work_for_endless_task_past_the_limit(
    task_type: str,
) -> None:
    """Бессрочная задача после лимита доступна для «Повторить» и «Закрыть».

    Такая задача не уходит в requires_intervention и висит в pending. Без этого
    правила администратор не мог ни ускорить повтор, ни закрыть задачу,
    которую получатель отвергает всегда (матрица 6.3, BIL-API-12/13).
    """
    repo = InMemoryPurchaseRepository()

    def _task(attempts: int) -> FakeOutboxTaskRow:
        payment_id = uuid4()
        row = FakeOutboxTaskRow(
            id=uuid4(),
            dedup_key=f'endless:{payment_id}',
            target_service='auth' if task_type == 'sync_access' else 'notifications',
            task_type=task_type,
            payment_id=payment_id,
            user_id=uuid4(),
            access_version=1 if task_type == 'sync_access' else None,
            payload={},
            status='pending',
            attempts=attempts,
        )
        repo.outbox_tasks[row.dedup_key] = row
        return row

    from core.config import settings
    from domain.errors import RuleConflictError

    limit = settings.worker.billing_outbox_max_attempts
    young = _task(limit - 1)
    stuck_retry = _task(limit + 3)
    stuck_close = _task(limit)

    with pytest.raises(RuleConflictError):
        await repo.close_outbox_task_by_admin(
            young.id, admin_user_id=uuid4(), reason='Рано'
        )
    retried = await repo.retry_outbox_task_by_admin(
        stuck_retry.id, admin_user_id=uuid4(), reason='Ключ починили'
    )
    closed = await repo.close_outbox_task_by_admin(
        stuck_close.id, admin_user_id=uuid4(), reason='Пользователь удалён в Auth'
    )

    assert (retried.status, stuck_retry.attempts) == ('pending', 0)
    assert closed.status == 'closed'
