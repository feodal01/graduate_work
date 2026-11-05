from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest

from clients.auth import (
    AuthPermanentDeliveryError,
    AuthServiceUnavailableError,
    HttpAuthGateway,
    SubscriptionAccessSyncResult,
)
from services.outbox_dispatcher import OutboxDispatcher
from services.reconciler import PaymentReconciler
from tests.fakes import (
    ConfigurableAuthGateway,
    FakeOutboxTaskRow,
    FakePaymentRow,
    FakeProviderGateway,
    InMemoryPurchaseRepository,
)


@pytest.mark.asyncio
async def test_http_auth_gateway_put_subscription_access_mapping() -> None:
    """T-16 (BIL-FR-11, AUTH-API-01): put_subscription_access и перевод ошибок."""
    user_id = uuid4()
    seen_headers: dict[str, str] = {}
    seen_json: list[dict[str, object]] = []
    next_status: int = 200
    next_body: dict[str, object] = {'applied': True, 'current_version': 3}

    def handler(request: httpx.Request) -> httpx.Response:
        assert (
            request.url.path == f'/api/v1/internal/users/{user_id}/subscription-access'
        )
        assert request.method == 'PUT'
        seen_headers['X-Request-Id'] = request.headers.get('X-Request-Id', '')
        seen_headers['X-Service-Api-Key'] = request.headers.get('X-Service-Api-Key', '')
        import json

        seen_json.append(json.loads(request.read().decode('utf-8')))
        return httpx.Response(next_status, json=next_body)

    transport = httpx.MockTransport(handler)
    expires = datetime(2026, 12, 31, 15, 0, tzinfo=UTC)
    async with httpx.AsyncClient(transport=transport) as http_client:
        gateway = HttpAuthGateway(
            http_client=http_client,
            base_url='http://auth:8000',
            service_api_key='secret-service-key-123',
        )

        # 1. Happy path: applied=True
        res: SubscriptionAccessSyncResult = await gateway.put_subscription_access(
            user_id,
            version=3,
            expires_at=expires.isoformat(),
            request_id='req-sync-1',
        )
        assert res.applied is True
        assert res.current_version == 3
        assert seen_headers == {
            'X-Request-Id': 'req-sync-1',
            'X-Service-Api-Key': 'secret-service-key-123',
        }
        assert seen_json[-1] == {'version': 3, 'expires_at': expires.isoformat()}

        # 2. Идемпотентный ответ applied=False -> тоже успех
        next_body = {'applied': False, 'current_version': 5}
        res_stale = await gateway.put_subscription_access(
            user_id,
            version=2,
            expires_at=None,
            request_id='req-sync-2',
        )
        assert res_stale.applied is False
        assert res_stale.current_version == 5

        # 3. Временные ошибки (429, 502, 503) -> AuthServiceUnavailableError
        for status_code in (429, 502, 503):
            next_status = status_code
            next_body = {'detail': 'Temporary error'}
            with pytest.raises(AuthServiceUnavailableError):
                await gateway.put_subscription_access(
                    user_id,
                    version=4,
                    expires_at=None,
                    request_id='req-transient',
                )

        # 4. Постоянные ошибки клиента 4xx (401, 404, 422) -> AuthPermanentDeliveryError
        for status_code in (401, 404, 422):
            next_status = status_code
            next_body = {'detail': f'Client error {status_code}'}
            with pytest.raises(AuthPermanentDeliveryError) as exc_info:
                await gateway.put_subscription_access(
                    user_id,
                    version=4,
                    expires_at=None,
                    request_id='req-permanent',
                )
            assert exc_info.value.status_code == status_code


@pytest.mark.asyncio
async def test_reconciler_immediately_dispatches_auth_access() -> None:
    """T-16 (BIL-FR-11, BIL-NFR-R-03): сразу после оплаты доступ уходит в Auth."""
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    user_id = uuid4()
    auth = ConfigurableAuthGateway(user_id=user_id)
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth,
        max_attempts=20,
    )
    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
        outbox_dispatcher=dispatcher,
    )

    pid = uuid4()
    repo.payments[pid] = FakePaymentRow(
        id=pid,
        user_id=user_id,
        tariff_id=UUID('00000000-0000-0000-0000-000000000001'),
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status='pending',
        idempotency_key=pid,
        provider_payment_id='yk-imm-1',
    )
    provider.get_status = 'succeeded'

    outcome = await reconciler.reconcile_payment(
        pid,
        source='webhook',
        respect_throttle=False,
    )
    assert outcome.status == 'succeeded'
    assert outcome.transitioned is True

    # Проверяем, что задача sync_access немедленно доставлена и переведена в completed
    auth_task = repo.outbox_tasks[f'payment_succeeded:{pid}:auth_access']
    assert auth_task.status == 'completed'
    assert len(auth.synced_access_calls) == 1
    assert auth.seen_request_ids == [f'outbox-{auth_task.id}']
    called_uid, called_ver, called_exp = auth.synced_access_calls[0]
    assert called_uid == user_id
    assert called_ver == 1
    assert called_exp is not None


@pytest.mark.asyncio
async def test_immediate_dispatch_handles_auth_outage_and_4xx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-16 (BIL-FR-11, BIL-FR-14): при отказе Auth оплата не падает."""
    import sentry_sdk

    captured_alerts: list[str] = []

    def fake_capture(msg: str, *, level: str = 'info') -> None:
        if level == 'error':
            captured_alerts.append(msg)

    monkeypatch.setattr(sentry_sdk, 'capture_message', fake_capture)

    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    user_id = uuid4()
    auth = ConfigurableAuthGateway(user_id=user_id, unavailable=True)
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth,
        max_attempts=2,
    )
    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
        outbox_dispatcher=dispatcher,
    )

    pid = uuid4()
    repo.payments[pid] = FakePaymentRow(
        id=pid,
        user_id=user_id,
        tariff_id=UUID('00000000-0000-0000-0000-000000000001'),
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status='pending',
        idempotency_key=pid,
        provider_payment_id='yk-imm-down',
    )
    provider.get_status = 'succeeded'

    # 1. Auth недоступен (503) -> сверка succeeded, задача pending с attempts=1
    outcome = await reconciler.reconcile_payment(
        pid,
        source='webhook',
        respect_throttle=False,
    )
    assert outcome.status == 'succeeded'
    assert outcome.transitioned is True
    auth_task = repo.outbox_tasks[f'payment_succeeded:{pid}:auth_access']
    assert auth_task.status == 'pending'
    assert auth_task.attempts == 1
    assert auth_task.last_error is not None

    # 2. Если уже доставлена более новая версия V=5, старая V=1 закрывается (US-S-02)
    newer_task = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'manual_newer:{pid}:auth_access',
        target_service='auth',
        task_type='sync_access',
        payment_id=pid,
        user_id=user_id,
        access_version=5,
        payload={'user_id': str(user_id), 'version': 5, 'expires_at': None},
        status='completed',
    )
    repo.outbox_tasks[newer_task.dedup_key] = newer_task
    auth_task.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)
    await dispatcher.try_deliver_payment_tasks(pid)
    assert auth_task.status == 'completed'
    assert len(auth.synced_access_calls) == 0

    # 3. При постоянной ошибке 404 задача сразу переходит в requires_intervention
    pid_2 = uuid4()
    task_404 = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'payment_succeeded:{pid_2}:auth_access',
        target_service='auth',
        task_type='sync_access',
        payment_id=pid_2,
        user_id=uuid4(),
        access_version=1,
        payload={'version': 1, 'expires_at': None},
        status='pending',
    )
    repo.outbox_tasks[task_404.dedup_key] = task_404
    auth.unavailable = False
    auth.permanent_error = AuthPermanentDeliveryError(404, 'User not found')
    await dispatcher.try_deliver_payment_tasks(pid_2)
    assert task_404.status == 'requires_intervention'
    assert task_404.attempts == 1
    assert '404' in (task_404.last_error or '')
    assert any(str(task_404.id) in msg for msg in captured_alerts)

    # 4. Исчерпание max_attempts=2 при 503 -> requires_intervention + сигнал
    pid_3 = uuid4()
    task_exhaust = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'payment_succeeded:{pid_3}:auth_access',
        target_service='auth',
        task_type='sync_access',
        payment_id=pid_3,
        user_id=uuid4(),
        access_version=1,
        payload={'version': 1, 'expires_at': None},
        status='pending',
        attempts=1,
    )
    repo.outbox_tasks[task_exhaust.dedup_key] = task_exhaust
    auth.permanent_error = None
    auth.unavailable = True
    await dispatcher.process_due_tasks()
    assert task_exhaust.status == 'requires_intervention'
    assert task_exhaust.attempts == 2
    assert any('exhausted 2 attempts' in msg for msg in captured_alerts)

    # 5. Повреждённый payload (access_version=0) -> requires_intervention
    pid_4 = uuid4()
    task_malformed = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'payment_succeeded:{pid_4}:auth_access',
        target_service='auth',
        task_type='sync_access',
        payment_id=pid_4,
        user_id=uuid4(),
        access_version=0,
        payload={'version': 'invalid'},
        status='pending',
    )
    repo.outbox_tasks[task_malformed.dedup_key] = task_malformed
    auth.unavailable = False
    await dispatcher.process_due_tasks()
    assert task_malformed.status == 'requires_intervention'
    assert 'Malformed outbox payload' in (task_malformed.last_error or '')
