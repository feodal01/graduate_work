from datetime import timedelta
from uuid import UUID, uuid4

import httpx
import pytest

from clients.notifications import HttpNotificationsGateway
from clients.payment_provider_registry import PaymentProviderRegistry
from domain.subscription import (
    SubscriptionState,
    compute_subscription_state,
    deterministic_id,
    format_msk_datetime,
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
async def test_subscription_expiration_email_scheduled_on_exact_end_date_t() -> None:
    """T-22 (US-S-03, BIL-FR-11, NTF-FR-01): планирование письма на дату T."""
    accepted_batches: list[tuple[str, dict[str, object]]] = []

    def ntf_handler(request: httpx.Request) -> httpx.Response:
        import json

        accepted_batches.append(
            (request.url.path, json.loads(request.read().decode('utf-8')))
        )
        return httpx.Response(202, json={'item_count': 1})

    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    user_id = uuid4()
    auth = ConfigurableAuthGateway(user_id=user_id)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(ntf_handler)
    ) as http_client:
        ntf_gateway = HttpNotificationsGateway(
            http_client=http_client,
            base_url='http://notifications-campaign-api:8000',
            api_key='campaign-secret-key',
        )
        dispatcher = OutboxDispatcher(
            repository=repo,
            auth_gateway=auth,
            notifications_gateway=ntf_gateway,
        )
        reconciler = PaymentReconciler(
            payments=repo,
            refunds=repo,
            payment_providers=PaymentProviderRegistry.single(provider),
            public_base_url='https://cinema.example',
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
            provider_payment_id='yk-t22-1',
        )
        provider.get_status = 'succeeded'
        await reconciler.reconcile_payment(
            pid,
            source='webhook',
            respect_throttle=False,
        )

    sub = repo.subscriptions[pid]
    exp_key = f'subscription_expiration:{sub.id}:notify_expiring'
    exp_task = repo.outbox_tasks[exp_key]
    assert exp_task.status == 'completed'

    expected_campaign_id = deterministic_id('subscription_expiration', sub.id)
    expected_batch_id = deterministic_id('subscription_expiration_batch', sub.id)
    expected_ends_msk = format_msk_datetime(sub.ends_at)

    matching = [
        body
        for path, body in accepted_batches
        if path == f'/api/v1/notifications/campaigns/{expected_campaign_id}/batches'
    ]
    assert len(matching) == 1
    sent_body = matching[0]
    assert sent_body['batch_id'] == str(expected_batch_id)
    item = sent_body['items'][0]  # type: ignore[index]
    assert item['user_id'] == str(user_id)
    assert item['template_id'] == 'billing_notification'
    assert item['subject'] == 'Срок подписки «Премиум» закончился'
    assert expected_ends_msk in item['text']
    assert 'https://cinema.example/billing/' in item['text']
    assert item['available_at'] == sub.ends_at.isoformat()

    # Проверяем US-S-03 шаг 1: ровно в момент T состояние становится EXPIRED,
    # а пользователь снова может купить подписку без фоновых задач в Billing
    one_us_before = sub.ends_at - timedelta(microseconds=1)
    assert (
        compute_subscription_state(
            ends_at=sub.ends_at,
            has_succeeded_refund=False,
            now_utc=one_us_before,
        )
        == SubscriptionState.ACTIVE
    )
    assert (
        compute_subscription_state(
            ends_at=sub.ends_at,
            has_succeeded_refund=False,
            now_utc=sub.ends_at,
        )
        == SubscriptionState.EXPIRED
    )


@pytest.mark.asyncio
async def test_expiration_email_rejects_missing_or_naive_available_at() -> None:
    """T-22: письмо об окончании без валидного available_at не уходит немедленно."""
    repo = InMemoryPurchaseRepository()
    user_id = uuid4()
    payment_id = uuid4()
    auth = ConfigurableAuthGateway(user_id=user_id)
    sent_calls = 0

    def ntf_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal sent_calls
        sent_calls += 1
        return httpx.Response(202, json={'item_count': 1})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(ntf_handler)
    ) as http_client:
        ntf_gateway = HttpNotificationsGateway(
            http_client=http_client,
            base_url='http://notifications-campaign-api:8000',
            api_key='campaign-secret-key',
        )
        dispatcher = OutboxDispatcher(
            repository=repo,
            auth_gateway=auth,
            notifications_gateway=ntf_gateway,
        )

        for idx, bad_available_at in enumerate((None, '', '2026-12-05T12:00:00')):
            task = FakeOutboxTaskRow(
                id=uuid4(),
                dedup_key=f'bad_exp:{idx}',
                target_service='notifications',
                task_type='subscription_expiration_email',
                payment_id=payment_id,
                user_id=user_id,
                access_version=None,
                payload={
                    'campaign_id': str(uuid4()),
                    'batch_id': str(uuid4()),
                    'user_id': str(user_id),
                    'template_id': 'billing_notification',
                    'subject': 'Срок подписки «Премиум» закончился',
                    'text': 'Текст письма об окончании',
                    'available_at': bad_available_at,
                },
                status='pending',
            )
            repo.outbox_tasks[task.dedup_key] = task
            await dispatcher.try_deliver_task(task.id)
            assert task.status == 'requires_intervention'
            assert 'available_at' in (task.last_error or '')

    assert sent_calls == 0
