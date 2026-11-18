"""Задача доступа, экраны пользователя и «зависшие» операции (US-U-02, US-U-05, 6.4).

- Задачу доступа с последней версией пользователя нельзя закрыть, только
  повторить; устаревшую — можно (BIL-API-13).
- Пока задача доступа текущей подписки ждёт администратора или закрыта, экраны
  не обещают «доступ активируется» и не обновляются сами.
- Доставлена новая версия — старая задача доступа в ТВ выполнена без Auth.
- Платёж с номером ЮKassa ждёт пользователя и «зависает» позже (70 минут).
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from api.auth_cookies import ACCESS_TOKEN_COOKIE
from clients.payment_provider_registry import PaymentProviderRegistry
from core.config import WorkerSettings, settings
from dependencies import (
    get_auth_gateway,
    get_outbox_dispatcher,
    get_purchase_service,
    get_repositories,
    get_service_credentials,
)
from main import create_app
from repositories.outbox import DeliveryOutcome
from repositories.records import OutboxTaskRecordProtocol
from security.service_credentials import ServiceCredentials
from services.monitoring import OperationalMonitor
from services.outbox_dispatcher import OutboxDispatcher
from services.purchase import PurchaseService
from services.reconciler import PaymentReconciler
from tests.fakes import (
    ConfigurableAuthGateway,
    FakeOutboxTaskRow,
    FakePaymentRow,
    FakeProviderGateway,
    FakeRefundRow,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
    in_memory_repositories,
)

SERVICE_KEY = 'billing-internal-access-task-key-long-enough'


def _paid_user(
    repo: InMemoryPurchaseRepository,
    *,
    user_id: UUID | None = None,
    access_task_status: str = 'requires_intervention',
) -> tuple[FakePaymentRow, FakeOutboxTaskRow]:
    user = user_id or uuid4()
    pid = uuid4()
    payment = FakePaymentRow(
        id=pid,
        user_id=user,
        tariff_id=repo.tariff.id,
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status='succeeded',
        idempotency_key=pid,
        provider_payment_id=f'yk-{pid.hex[:8]}',
    )
    repo.payments[pid] = payment
    now = datetime.now(UTC)
    repo.subscriptions[pid] = FakeSubscriptionRow(
        id=uuid4(),
        user_id=user,
        tariff_id=repo.tariff.id,
        payment_id=pid,
        starts_at=now - timedelta(days=1),
        ends_at=now + timedelta(days=29),
    )
    repo.access_versions[user] = 1
    key = f'payment_succeeded:{pid}:auth_access'
    task = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=key,
        target_service='auth',
        task_type='sync_access',
        payment_id=pid,
        user_id=user,
        access_version=1,
        payload={'version': 1},
        status=access_task_status,
        attempts=3,
        last_error='Auth HTTP 404',
    )
    repo.outbox_tasks[key] = task
    return payment, task


def _internal_client(repo: InMemoryPurchaseRepository) -> httpx.AsyncClient:
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_repositories] = lambda: in_memory_repositories(repo)
    # «Повторить» сразу пробует доставить задачу: Auth здесь отвечает успехом.
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=ConfigurableAuthGateway(user_id=uuid4()),
    )
    app.dependency_overrides[get_outbox_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={'X-Service-Api-Key': SERVICE_KEY, 'X-Request-Id': 'access-rules'},
    )


# --- Закрытие задачи доступа администратором --------------------------------


async def test_latest_access_task_cannot_be_closed_but_can_be_retried() -> None:
    """BIL-API-13: 409 с понятным кодом; «Повторить» остаётся доступным."""
    repo = InMemoryPurchaseRepository()
    payment, task = _paid_user(repo)

    async with _internal_client(repo) as client:
        listed = await client.get(
            '/api/v1/internal/billing/outbox-tasks',
            params={'status': 'requires_intervention'},
        )
        closed = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{task.id}/close',
            json={'admin_user_id': str(uuid4()), 'reason': 'Не доходит до Auth'},
        )
        retried = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{task.id}/retry',
            json={'admin_user_id': str(uuid4()), 'reason': 'Повтор'},
        )

    item = next(i for i in listed.json()['items'] if i['id'] == str(task.id))
    assert (item['can_retry_or_close'], item['can_close']) == (True, False)
    assert closed.status_code == 409
    assert closed.json()['error_code'] == 'outbox_task_latest_access_version'
    assert 'Повторить' in closed.json()['detail']
    assert retried.status_code == 200
    # Повтор сразу доставлен в Auth (немедленная попытка после «Повторить»).
    assert task.status == 'completed'


async def test_superseded_access_task_can_still_be_closed() -> None:
    """Устаревшую задачу (выдана версия новее) закрыть можно."""
    repo = InMemoryPurchaseRepository()
    payment, task = _paid_user(repo)
    repo.access_versions[payment.user_id] = 2

    async with _internal_client(repo) as client:
        card = await client.get(f'/api/v1/internal/billing/payments/{payment.id}')
        closed = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{task.id}/close',
            json={'admin_user_id': str(uuid4()), 'reason': 'Устарела'},
        )

    card_task = next(t for t in card.json()['outbox_tasks'] if t['id'] == str(task.id))
    assert card_task['can_close'] is True
    assert closed.status_code == 200
    assert task.status == 'closed'


# --- Экраны пользователя при недоставленном доступе --------------------------


@pytest.mark.parametrize('task_status', ['requires_intervention', 'closed'])
async def test_result_screen_asks_to_contact_support_without_refresh(
    task_status: str,
) -> None:
    """US-U-02: не «доступ активируется» и без автообновления."""
    repo = InMemoryPurchaseRepository()
    payment, _ = _paid_user(repo, access_task_status=task_status)
    provider = FakeProviderGateway()
    service = PurchaseService(
        payments=repo,
        user_views=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        public_base_url='http://localhost',
        reconciler=PaymentReconciler(
            payments=repo,
            refunds=repo,
            payment_providers=PaymentProviderRegistry.single(provider),
        ),
    )
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_service] = lambda: service
    app.dependency_overrides[get_auth_gateway] = lambda: ConfigurableAuthGateway(
        user_id=payment.user_id
    )

    with TestClient(app) as client:
        client.cookies.set(ACCESS_TOKEN_COOKIE, 'valid-token')
        result = client.get(f'/billing/payments/{payment.id}')
        screen = client.get('/billing/')
        summary = client.get(
            '/api/v1/billing/me/summary',
            headers={'X-Request-Id': 'p07-summary', 'Authorization': 'Bearer t'},
        )

    assert result.status_code == 200
    assert 'Оплачено, но доступ не открылся' in result.text
    assert 'обратитесь в поддержку' in result.text.lower()
    assert 'доступ активируется' not in result.text
    assert 'http-equiv="refresh"' not in result.text
    assert 'Оплачено, но доступ не открылся — обратитесь в поддержку' in screen.text
    assert 'доступ активируется' not in screen.text
    assert summary.json()['status_key'] == 'access_problem'
    assert summary.json()['rule_number'] == 2


# --- Устаревшая задача доступа в ТВ уходит из «Не доставлено» -----------------


async def test_newer_delivered_version_completes_old_access_task_without_auth() -> None:
    """US-S-02: доставлена версия 2 — задача версии 1 в ТВ выполнена без Auth."""
    repo = InMemoryPurchaseRepository()
    payment, old_task = _paid_user(repo)
    newer_key = f'refund_succeeded:{uuid4()}:sync_access'
    newer = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=newer_key,
        target_service='auth',
        task_type='sync_access',
        payment_id=payment.id,
        user_id=payment.user_id,
        access_version=2,
        payload={'version': 2},
    )
    repo.outbox_tasks[newer_key] = newer
    repo.access_versions[payment.user_id] = 2
    delivered: list[int | None] = []

    async def deliver(
        task: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        delivered.append(task.access_version)
        return 'completed', None

    await repo.process_due_outbox_tasks(deliver)

    assert delivered == [2]
    assert newer.status == 'completed'
    assert old_task.status == 'completed'
    assert 'superseded by access version 2' in (old_task.last_error or '')
    metrics = await repo.get_operational_metrics()
    assert metrics.outbox_requires_intervention_count == 0


async def test_older_delivery_does_not_complete_newer_task_in_intervention() -> None:
    """Обратное направление не действует: доставка старой версии новую не трогает."""
    repo = InMemoryPurchaseRepository()
    payment, newer_task = _paid_user(repo)
    newer_task.access_version = 3
    old_key = f'payment_succeeded:{uuid4()}:auth_access'
    old = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=old_key,
        target_service='auth',
        task_type='sync_access',
        payment_id=payment.id,
        user_id=payment.user_id,
        access_version=2,
        payload={'version': 2},
    )
    repo.outbox_tasks[old_key] = old

    async def deliver(
        task: OutboxTaskRecordProtocol,
    ) -> tuple[DeliveryOutcome, str | None]:
        return 'completed', None

    await repo.process_due_outbox_tasks(deliver)

    assert old.status == 'completed'
    assert newer_task.status == 'requires_intervention'


# --- «Зависшие операции» --------------------------------------------------------


def _pending(
    repo: InMemoryPurchaseRepository,
    *,
    provider_payment_id: str | None,
    age: timedelta,
) -> FakePaymentRow:
    pid = uuid4()
    row = FakePaymentRow(
        id=pid,
        user_id=uuid4(),
        tariff_id=repo.tariff.id,
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status='pending',
        idempotency_key=pid,
        provider_payment_id=provider_payment_id,
        created_at=datetime.now(UTC) - age,
    )
    repo.payments[pid] = row
    return row


async def test_waiting_payment_becomes_stuck_only_after_longer_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """6.4, US-A-02: метрика монитора и фильтр админки считают одинаково."""
    monkeypatch.setattr(settings.worker, 'billing_stuck_threshold_seconds', 600)
    monkeypatch.setattr(
        settings.worker, 'billing_stuck_waiting_payment_threshold_seconds', 4200
    )
    repo = InMemoryPurchaseRepository()
    waiting = _pending(repo, provider_payment_id='yk-wait', age=timedelta(minutes=15))
    late = _pending(repo, provider_payment_id='yk-late', age=timedelta(minutes=71))
    no_number = _pending(repo, provider_payment_id=None, age=timedelta(minutes=15))
    pay_with_refund = _pending(
        repo, provider_payment_id='yk-old', age=timedelta(minutes=1)
    )
    pay_with_refund.status = 'succeeded'
    rid = uuid4()
    repo.refunds[rid] = FakeRefundRow(
        id=rid,
        payment_id=pay_with_refund.id,
        subscription_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29_900,
        status='processing',
        reason='Возврат',
        admin_user_id=uuid4(),
        idempotency_key=rid,
        created_at=datetime.now(UTC) - timedelta(minutes=15),
    )

    monitor = OperationalMonitor(
        repository=repo,
        stuck_threshold_seconds=600,
        waiting_payment_threshold_seconds=4200,
    )
    snapshot = await monitor.check_and_emit_signals()
    async with _internal_client(repo) as client:
        stuck = await client.get(
            '/api/v1/internal/billing/payments', params={'stuck': 'true'}
        )
        metrics = await client.get('/api/v1/internal/billing/metrics')

    stuck_ids = {i['id'] for i in stuck.json()['items']}
    assert stuck_ids == {str(late.id), str(no_number.id), str(pay_with_refund.id)}
    assert str(waiting.id) not in stuck_ids
    assert (snapshot.stuck_payments_count, snapshot.stuck_refunds_count) == (2, 1)
    assert metrics.json()['stuck_payments_count'] == 2


def test_waiting_payment_threshold_must_not_be_shorter_than_general() -> None:
    """Порог для платежа с номером не меньше общего; умолчание — 4200 с."""
    fields = WorkerSettings.model_fields
    assert fields['billing_stuck_waiting_payment_threshold_seconds'].default == 4200
    with pytest.raises(ValidationError):
        WorkerSettings.model_validate(
            {
                'billing_stuck_threshold_seconds': 600,
                'billing_stuck_waiting_payment_threshold_seconds': 599,
            }
        )
    with pytest.raises(ValidationError):
        WorkerSettings.model_validate(
            {'billing_stuck_waiting_payment_threshold_seconds': 0}
        )
