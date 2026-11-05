from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from repositories.purchase import compute_outbox_backoff_seconds
from services.outbox_dispatcher import (
    NotificationsGateway,
    NotificationsPermanentError,
    NotificationsTransientError,
    OutboxDispatcher,
)
from tests.fakes import (
    ConfigurableAuthGateway,
    FakeOutboxTaskRow,
    InMemoryPurchaseRepository,
)


class FakeNotificationsGateway(NotificationsGateway):
    """Тестовый двойник шлюза Notifications для проверки воркера outbox (T-19)."""

    def __init__(self) -> None:
        self.sent_batches: list[dict[str, object]] = []
        self.cancelled_campaigns: list[tuple[UUID, str]] = []
        self.transient_error: NotificationsTransientError | None = None
        self.permanent_error: NotificationsPermanentError | None = None

    async def send_campaign_batch(
        self,
        *,
        campaign_id: UUID,
        batch_id: UUID,
        user_id: UUID,
        template_id: str,
        subject: str,
        text: str,
        available_at: str | None,
        request_id: str,
    ) -> None:
        """Эмулирует приём батча рассылки в Notifications."""
        if self.permanent_error is not None:
            raise self.permanent_error
        if self.transient_error is not None:
            raise self.transient_error
        self.sent_batches.append(
            {
                'campaign_id': campaign_id,
                'batch_id': batch_id,
                'user_id': user_id,
                'template_id': template_id,
                'subject': subject,
                'text': text,
                'available_at': available_at,
                'request_id': request_id,
            }
        )

    async def cancel_campaign(
        self,
        campaign_id: UUID,
        *,
        request_id: str,
    ) -> None:
        """Эмулирует отмену запланированной кампании в Notifications."""
        if self.permanent_error is not None:
            raise self.permanent_error
        if self.transient_error is not None:
            raise self.transient_error
        self.cancelled_campaigns.append((campaign_id, request_id))


def test_compute_outbox_backoff_seconds_exponential_cap() -> None:
    """T-19 (BIL-NFR-R-04): экспоненциальный backoff 2^attempt с потолком 300 с."""
    assert compute_outbox_backoff_seconds(1) == 2
    assert compute_outbox_backoff_seconds(2) == 4
    assert compute_outbox_backoff_seconds(3) == 8
    assert compute_outbox_backoff_seconds(4) == 16
    assert compute_outbox_backoff_seconds(5) == 32
    assert compute_outbox_backoff_seconds(6) == 64
    assert compute_outbox_backoff_seconds(7) == 128
    assert compute_outbox_backoff_seconds(8) == 256
    assert compute_outbox_backoff_seconds(9) == 300
    assert compute_outbox_backoff_seconds(20) == 300


@pytest.mark.asyncio
async def test_outbox_worker_backoff_and_recovery_after_transient_outage() -> None:
    """T-19 (BIL-FR-11, BIL-NFR-R-04): повтор с backoff и доставка после старта."""
    repo = InMemoryPurchaseRepository()
    user_id = uuid4()
    payment_id = uuid4()
    auth = ConfigurableAuthGateway(user_id=user_id, unavailable=True)
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth,
        max_attempts=20,
    )

    task = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'payment_succeeded:{payment_id}:auth_access',
        target_service='auth',
        task_type='sync_access',
        payment_id=payment_id,
        user_id=user_id,
        access_version=1,
        payload={
            'user_id': str(user_id),
            'version': 1,
            'expires_at': '2026-12-05T12:00:00+00:00',
        },
        status='pending',
    )
    repo.outbox_tasks[task.dedup_key] = task

    # 1. Первая попытка при недоступном Auth -> pending, attempts=1, next_retry_at > now
    before = datetime.now(UTC)
    processed_1 = await dispatcher.process_due_tasks()
    assert processed_1 == 1
    assert task.status == 'pending'
    assert task.attempts == 1
    assert task.last_error is not None
    assert task.next_retry_at >= before + timedelta(seconds=2)

    # 2. Немедленный повторный вызов до наступления next_retry_at не берёт задачу
    processed_early = await dispatcher.process_due_tasks()
    assert processed_early == 0
    assert task.attempts == 1

    # 3. Когда время повтора наступило и Auth восстановился -> задача доставляется
    task.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)
    auth.unavailable = False
    processed_2 = await dispatcher.process_due_tasks()
    assert processed_2 == 1
    assert task.status == 'completed'
    assert task.attempts == 1
    assert task.last_error is None
    assert len(auth.synced_access_calls) == 1


@pytest.mark.asyncio
async def test_outbox_worker_exhausts_20_attempts_into_requires_intervention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-19 (BIL-FR-14, BIL-NFR-R-04): после 20 сбоев -> requires_intervention."""
    import sentry_sdk

    alerts: list[str] = []
    monkeypatch.setattr(
        sentry_sdk,
        'capture_message',
        lambda msg, *, level='info': alerts.append(msg) if level == 'error' else None,
    )

    repo = InMemoryPurchaseRepository()
    user_id = uuid4()
    payment_id = uuid4()
    auth = ConfigurableAuthGateway(user_id=user_id, unavailable=True)
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth,
        max_attempts=20,
    )

    task = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'payment_succeeded:{payment_id}:auth_access',
        target_service='auth',
        task_type='sync_access',
        payment_id=payment_id,
        user_id=user_id,
        access_version=1,
        payload={'user_id': str(user_id), 'version': 1, 'expires_at': None},
        status='pending',
        attempts=19,
    )
    repo.outbox_tasks[task.dedup_key] = task

    processed = await dispatcher.process_due_tasks()
    assert processed == 1
    assert task.status == 'requires_intervention'
    assert task.attempts == 20
    assert task.last_error is not None
    assert any('exhausted 20 attempts' in msg for msg in alerts)

    # В состоянии requires_intervention автоповторы остановлены
    task.next_retry_at = datetime.now(UTC) - timedelta(seconds=10)
    auth.unavailable = False
    assert await dispatcher.process_due_tasks() == 0
    assert task.status == 'requires_intervention'


@pytest.mark.asyncio
async def test_outbox_worker_skips_stale_access_version_and_clears_last_error() -> None:
    """T-19 (US-S-02 шаг 3, BIL-FR-11, инв. 11): пропуск устаревшей версии доступа."""
    repo = InMemoryPurchaseRepository()
    user_id = uuid4()
    payment_id = uuid4()
    auth = ConfigurableAuthGateway(user_id=user_id)
    dispatcher = OutboxDispatcher(repository=repo, auth_gateway=auth)

    # Уже доставлена версия 2 (например, отзыв доступа после возврата)
    completed_v2 = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'refund_succeeded:{payment_id}:auth_access',
        target_service='auth',
        task_type='sync_access',
        payment_id=payment_id,
        user_id=user_id,
        access_version=2,
        payload={'user_id': str(user_id), 'version': 2, 'expires_at': None},
        status='completed',
    )
    # А старая задача с версией 1 ожидала повтора после временной ошибки
    stale_v1 = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'payment_succeeded:{payment_id}:auth_access',
        target_service='auth',
        task_type='sync_access',
        payment_id=payment_id,
        user_id=user_id,
        access_version=1,
        payload={
            'user_id': str(user_id),
            'version': 1,
            'expires_at': '2026-12-05T12:00:00+00:00',
        },
        status='pending',
        attempts=2,
        last_error='Auth service returned 503',
    )
    repo.outbox_tasks[completed_v2.dedup_key] = completed_v2
    repo.outbox_tasks[stale_v1.dedup_key] = stale_v1

    processed = await dispatcher.process_due_tasks()
    assert processed == 1
    assert stale_v1.status == 'completed'
    assert stale_v1.last_error is None
    # Вызов в Auth не выполнялся
    assert auth.synced_access_calls == []


@pytest.mark.asyncio
async def test_outbox_worker_dispatches_notifications_and_handles_errors() -> None:
    """T-19 (US-S-02, BIL-FR-11, BIL-FR-14): отправка и отмена писем, 409 и 422."""
    repo = InMemoryPurchaseRepository()
    user_id = uuid4()
    payment_id = uuid4()
    campaign_paid = uuid4()
    batch_paid = uuid4()
    campaign_exp = uuid4()
    auth = ConfigurableAuthGateway(user_id=user_id)
    notifications = FakeNotificationsGateway()
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth,
        notifications_gateway=notifications,
        max_attempts=5,
    )

    email_task = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'payment_succeeded:{payment_id}:notify_paid',
        target_service='notifications',
        task_type='payment_succeeded_email',
        payment_id=payment_id,
        user_id=user_id,
        access_version=None,
        payload={
            'campaign_id': str(campaign_paid),
            'batch_id': str(batch_paid),
            'user_id': str(user_id),
            'template_id': 'billing_notification',
            'subject': 'Подписка «Премиум» оплачена',
            'text': 'Доступ открыт',
            'available_at': None,
        },
        status='pending',
    )
    cancel_task = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'refund_succeeded:{payment_id}:cancel_exp',
        target_service='notifications',
        task_type='cancel_expiration_email',
        payment_id=payment_id,
        user_id=user_id,
        access_version=None,
        payload={'campaign_id': str(campaign_exp)},
        status='pending',
    )
    repo.outbox_tasks[email_task.dedup_key] = email_task
    repo.outbox_tasks[cancel_task.dedup_key] = cancel_task

    # 1. Временная ошибка 409 profile_not_ready -> задача остаётся pending с backoff
    notifications.transient_error = NotificationsTransientError('409 profile_not_ready')
    await dispatcher.process_due_tasks()
    assert email_task.status == 'pending'
    assert email_task.attempts == 1
    assert 'profile_not_ready' in (email_task.last_error or '')

    # 2. После синхронизации профиля оба задания (отправка и отмена) выполняются
    notifications.transient_error = None
    email_task.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)
    cancel_task.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)
    await dispatcher.process_due_tasks()
    assert email_task.status == 'completed'
    assert cancel_task.status == 'completed'
    assert len(notifications.sent_batches) == 1
    assert notifications.sent_batches[0]['campaign_id'] == campaign_paid
    assert len(notifications.cancelled_campaigns) == 1
    assert notifications.cancelled_campaigns[0][0] == campaign_exp

    # 3. Постоянная ошибка 422 от Notifications -> requires_intervention с 1-й попытки
    bad_email_task = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'payment_failed:{payment_id}:notify_failed',
        target_service='notifications',
        task_type='payment_failed_email',
        payment_id=payment_id,
        user_id=user_id,
        access_version=None,
        payload={
            'campaign_id': str(uuid4()),
            'batch_id': str(uuid4()),
            'user_id': str(user_id),
            'template_id': 'billing_notification',
            'subject': 'Оплата не прошла',
            'text': 'Текст',
            'available_at': None,
        },
        status='pending',
    )
    repo.outbox_tasks[bad_email_task.dedup_key] = bad_email_task
    notifications.permanent_error = NotificationsPermanentError(
        422, 'Validation error in campaign batch'
    )
    await dispatcher.process_due_tasks()
    assert bad_email_task.status == 'requires_intervention'
    assert bad_email_task.attempts == 1


@pytest.mark.asyncio
async def test_outbox_worker_handles_unexpected_exception_and_malformed_tasks() -> None:
    """T-19: непредвиденные сбои, некорректный payload и try_deliver_task."""
    repo = InMemoryPurchaseRepository()
    user_id = uuid4()
    payment_id = uuid4()
    task = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key=f'payment_succeeded:{payment_id}:auth_access',
        target_service='auth',
        task_type='sync_access',
        payment_id=payment_id,
        user_id=user_id,
        access_version=1,
        payload={'user_id': str(user_id), 'version': 1, 'expires_at': None},
        status='pending',
    )
    repo.outbox_tasks[task.dedup_key] = task

    async def crashing_deliver(_task: object) -> tuple[str, str | None]:
        raise RuntimeError('unexpected socket error')

    processed = await repo.process_due_outbox_tasks(
        crashing_deliver,  # type: ignore[arg-type]
        max_attempts=3,
    )
    assert processed == 1
    assert task.status == 'pending'
    assert task.attempts == 1
    assert 'unexpected socket error' in (task.last_error or '')

    # Проверяем try_deliver_task и обработку неподдерживаемого task_type / payload
    auth = ConfigurableAuthGateway(user_id=user_id)
    notifications = FakeNotificationsGateway()
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth,
        notifications_gateway=notifications,
    )

    bad_auth_type = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key='bad_auth_type',
        target_service='auth',
        task_type='unknown_auth_task',
        payment_id=payment_id,
        user_id=user_id,
        access_version=1,
        payload={'version': 1},
        status='pending',
    )
    bad_ntf_payload = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key='bad_ntf_payload',
        target_service='notifications',
        task_type='payment_succeeded_email',
        payment_id=payment_id,
        user_id=user_id,
        access_version=None,
        payload={'campaign_id': 'not-a-uuid'},
        status='pending',
    )
    repo.outbox_tasks[bad_auth_type.dedup_key] = bad_auth_type
    repo.outbox_tasks[bad_ntf_payload.dedup_key] = bad_ntf_payload

    assert await dispatcher.try_deliver_task(bad_auth_type.id) == 1
    assert bad_auth_type.status == 'requires_intervention'
    assert 'Unsupported auth task_type' in (bad_auth_type.last_error or '')

    assert await dispatcher.try_deliver_task(bad_ntf_payload.id) == 1
    assert bad_ntf_payload.status == 'requires_intervention'
    assert 'Malformed notifications payload' in (bad_ntf_payload.last_error or '')
