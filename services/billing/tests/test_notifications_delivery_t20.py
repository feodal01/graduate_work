from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest

from clients.notifications import (
    HttpNotificationsGateway,
    NotificationsPermanentError,
    NotificationsTransientError,
)
from domain.subscription import (
    build_payment_failed_email_payload,
    build_payment_succeeded_email_payload,
)
from services.outbox_dispatcher import OutboxDispatcher
from services.reconciler import PaymentReconciler
from tests.fakes import (
    ConfigurableAuthGateway,
    FakePaymentRow,
    FakeProviderGateway,
    InMemoryPurchaseRepository,
)


@pytest.mark.asyncio
async def test_http_notifications_gateway_batch_and_cancel_contract() -> None:
    """T-20 (NTF-API-01, US-S-02): контракт клиента Notifications и коды ошибок."""
    campaign_id = uuid4()
    batch_id = uuid4()
    user_id = uuid4()
    seen_requests: list[tuple[str, str, dict[str, str], str]] = []
    next_status = 202
    next_body: dict[str, object] = {
        'campaign_id': str(campaign_id),
        'batch_id': str(batch_id),
        'item_count': 1,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        headers = {
            'X-API-Key': request.headers.get('X-API-Key', ''),
            'X-Request-Id': request.headers.get('X-Request-Id', ''),
        }
        seen_requests.append(
            (
                request.method,
                request.url.path,
                headers,
                request.read().decode('utf-8'),
            )
        )
        return httpx.Response(next_status, json=next_body)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        gateway = HttpNotificationsGateway(
            http_client=http_client,
            base_url='http://notifications-campaign-api:8000',
            api_key='campaign-secret-key',
        )

        # 1. Успешная отправка батча (202 Accepted)
        await gateway.send_campaign_batch(
            campaign_id=campaign_id,
            batch_id=batch_id,
            user_id=user_id,
            template_id='billing_notification',
            subject='Подписка «Премиум» оплачена',
            text='Доступ открыт до 05.12.2026 15:00 МСК',
            available_at=None,
            request_id='req-ntf-1',
        )
        method, path, headers, raw_body = seen_requests[-1]
        assert method == 'POST'
        assert path == f'/api/v1/notifications/campaigns/{campaign_id}/batches'
        assert headers == {
            'X-API-Key': 'campaign-secret-key',
            'X-Request-Id': 'req-ntf-1',
        }
        import json

        parsed_body = json.loads(raw_body)
        assert parsed_body == {
            'batch_id': str(batch_id),
            'created_by_user_id': str(user_id),
            'items': [
                {
                    'user_id': str(user_id),
                    'template_id': 'billing_notification',
                    'subject': 'Подписка «Премиум» оплачена',
                    'text': 'Доступ открыт до 05.12.2026 15:00 МСК',
                    'type': 'email',
                    'available_at': None,
                }
            ],
        }

        # 2. 409 profile_not_ready -> NotificationsTransientError (повтор с backoff)
        next_status = 409
        next_body = {
            'error_code': 'profile_not_ready',
            'detail': 'profile_not_ready',
            'user_ids': [str(user_id)],
        }
        with pytest.raises(NotificationsTransientError) as transient_exc:
            await gateway.send_campaign_batch(
                campaign_id=campaign_id,
                batch_id=batch_id,
                user_id=user_id,
                template_id='billing_notification',
                subject='Тема',
                text='Текст',
                available_at=None,
                request_id='req-ntf-409-ready',
            )
        assert 'profile_not_ready' in str(transient_exc.value)

        # 3. 409 batch_content_conflict -> NotificationsPermanentError
        next_status = 409
        next_body = {
            'error_code': 'batch_content_conflict',
            'detail': 'batch_content_conflict',
        }
        with pytest.raises(NotificationsPermanentError) as perm_409:
            await gateway.send_campaign_batch(
                campaign_id=campaign_id,
                batch_id=batch_id,
                user_id=user_id,
                template_id='billing_notification',
                subject='Тема',
                text='Текст',
                available_at=None,
                request_id='req-ntf-409-conflict',
            )
        assert perm_409.value.status_code == 409

        # 4. 429 и 5xx -> NotificationsTransientError
        for code in (429, 502, 503):
            next_status = code
            next_body = {'detail': 'Temporary outage'}
            with pytest.raises(NotificationsTransientError):
                await gateway.send_campaign_batch(
                    campaign_id=campaign_id,
                    batch_id=batch_id,
                    user_id=user_id,
                    template_id='billing_notification',
                    subject='Тема',
                    text='Текст',
                    available_at=None,
                    request_id='req-ntf-temp',
                )

        # 5. 401, 413, 422 -> NotificationsPermanentError
        for code in (401, 413, 422):
            next_status = code
            next_body = {'error_code': 'validation_error', 'detail': 'Invalid'}
            with pytest.raises(NotificationsPermanentError) as perm_exc:
                await gateway.send_campaign_batch(
                    campaign_id=campaign_id,
                    batch_id=batch_id,
                    user_id=user_id,
                    template_id='billing_notification',
                    subject='Тема',
                    text='Текст',
                    available_at=None,
                    request_id='req-ntf-perm',
                )
            assert perm_exc.value.status_code == code

        # 6. Отмена кампании (cancel_campaign) -> 200 OK, 503 transient, 401 permanent
        next_status = 200
        next_body = {'campaign_id': str(campaign_id), 'status': 'cancelled'}
        await gateway.cancel_campaign(campaign_id, request_id='req-cancel-ok')
        assert seen_requests[-1][0] == 'POST'
        assert (
            seen_requests[-1][1]
            == f'/api/v1/notifications/campaigns/{campaign_id}/cancel'
        )

        next_status = 503
        with pytest.raises(NotificationsTransientError):
            await gateway.cancel_campaign(campaign_id, request_id='req-cancel-503')

        next_status = 401
        with pytest.raises(NotificationsPermanentError):
            await gateway.cancel_campaign(campaign_id, request_id='req-cancel-401')


@pytest.mark.asyncio
async def test_payment_succeeded_and_failed_emails_dispatched_and_isolated() -> None:
    """T-20 (4.4, US-U-01, US-U-03, US-S-02): письма об успешной и неуспешной оплате."""
    sent_campaigns: list[dict[str, object]] = []
    ntf_status = 202
    ntf_body: dict[str, object] = {'item_count': 1}

    def ntf_handler(request: httpx.Request) -> httpx.Response:
        import json

        if ntf_status in (200, 202):
            sent_campaigns.append(json.loads(request.read().decode('utf-8')))
        return httpx.Response(ntf_status, json=ntf_body)

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
            repository=repo,
            payment_provider=provider,
            public_base_url='http://localhost',
            outbox_dispatcher=dispatcher,
        )

        # 1. Оплата прошла при временно недоступном Notifications (503):
        # доступ в Auth открывается сразу, а письма остаются в pending до восстановления
        ntf_status = 503
        pid_ok = uuid4()
        repo.payments[pid_ok] = FakePaymentRow(
            id=pid_ok,
            user_id=user_id,
            tariff_id=UUID('00000000-0000-0000-0000-000000000001'),
            provider_code='yookassa',
            amount_kopeks=29_900,
            currency='RUB',
            status='pending',
            idempotency_key=pid_ok,
            provider_payment_id='yk-t20-ok',
        )
        provider.get_status = 'succeeded'
        await reconciler.reconcile_payment(
            pid_ok,
            source='webhook',
            respect_throttle=False,
        )

        auth_task = repo.outbox_tasks[f'payment_succeeded:{pid_ok}:auth_access']
        paid_email_task = repo.outbox_tasks[f'payment_succeeded:{pid_ok}:notify_paid']
        assert auth_task.status == 'completed'
        assert paid_email_task.status == 'pending'
        assert paid_email_task.attempts == 1

        # Notifications восстанавливается -> воркер доставляет письма
        ntf_status = 202
        for task in repo.outbox_tasks.values():
            if task.status == 'pending':
                task.next_retry_at = datetime.now(UTC) - timedelta(seconds=1)
        await dispatcher.process_due_tasks()
        assert paid_email_task.status == 'completed'
        assert any(
            item['items'][0]['subject'] == 'Подписка «Премиум» оплачена'  # type: ignore[index]
            and 'МСК' in item['items'][0]['text']  # type: ignore[index]
            and 'http://localhost/billing/' in item['items'][0]['text']  # type: ignore[index]
            and item['items'][0]['available_at'] is None  # type: ignore[index]
            for item in sent_campaigns
        )

        # 2. Оплата не прошла (expired_on_confirmation) -> человекочитаемая причина
        pid_fail = uuid4()
        repo.payments[pid_fail] = FakePaymentRow(
            id=pid_fail,
            user_id=user_id,
            tariff_id=UUID('00000000-0000-0000-0000-000000000001'),
            provider_code='yookassa',
            amount_kopeks=29_900,
            currency='RUB',
            status='pending',
            idempotency_key=pid_fail,
            provider_payment_id='yk-t20-fail',
        )
        provider.get_status = 'failed'
        await reconciler.reconcile_payment(
            pid_fail,
            source='webhook',
            respect_throttle=False,
        )
        failed_email_task = repo.outbox_tasks[
            f'payment_failed:{pid_fail}:notify_failed'
        ]
        assert failed_email_task.status == 'completed'
        last_sent_item = sent_campaigns[-1]['items'][0]  # type: ignore[index]
        assert last_sent_item['subject'] == 'Оплата подписки «Премиум» не прошла'
        assert 'Время на подтверждение оплаты истекло' in last_sent_item['text']
        assert 'expired_on_confirmation' not in last_sent_item['text']
        assert 'http://localhost/billing/' in last_sent_item['text']


def test_email_payload_builders_format_msk_and_humanize_failure_reason() -> None:
    """T-20 (2.2 п. 12, 4.4, BIL-NFR-U-01): МСК в письмах и скрытие сырых ошибок."""
    payment_id = uuid4()
    sub_id = uuid4()
    user_id = uuid4()
    ends_at = datetime(2026, 12, 5, 12, 0, tzinfo=UTC)

    paid_payload = build_payment_succeeded_email_payload(
        payment_id=payment_id,
        subscription_id=sub_id,
        user_id=user_id,
        ends_at=ends_at,
        public_base_url='https://cinema.example/',
    )
    assert paid_payload['template_id'] == 'billing_notification'
    assert paid_payload['available_at'] is None
    assert '05.12.2026 15:00 МСК' in str(paid_payload['text'])
    assert 'https://cinema.example/billing/' in str(paid_payload['text'])

    failed_payload = build_payment_failed_email_payload(
        payment_id=payment_id,
        user_id=user_id,
        failure_reason='HTTP 401: Invalid secret key',
        public_base_url='https://cinema.example/',
    )
    assert 'HTTP 401' not in str(failed_payload['text'])
    assert 'Платёж отклонён платёжной системой' in str(failed_payload['text'])
    assert failed_payload['failure_reason'] == 'HTTP 401: Invalid secret key'


@pytest.mark.asyncio
async def test_http_notifications_gateway_network_error_and_non_json_response() -> None:
    """T-20: обрыв сети и не-JSON ответ от Notifications корректно переводятся."""
    campaign_id = uuid4()
    batch_id = uuid4()
    user_id = uuid4()

    def failing_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError('Notifications unreachable', request=request)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(failing_handler)
    ) as client:
        gateway = HttpNotificationsGateway(
            http_client=client,
            base_url='http://notifications-campaign-api:8000',
            api_key='campaign-secret-key',
        )
        with pytest.raises(NotificationsTransientError):
            await gateway.send_campaign_batch(
                campaign_id=campaign_id,
                batch_id=batch_id,
                user_id=user_id,
                template_id='billing_notification',
                subject='Тема',
                text='Текст',
                available_at=None,
                request_id='req-net-1',
            )
        with pytest.raises(NotificationsTransientError):
            await gateway.cancel_campaign(campaign_id, request_id='req-net-2')

    def non_json_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text='Bad Gateway HTML')

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(non_json_handler)
    ) as client_html:
        gateway_html = HttpNotificationsGateway(
            http_client=client_html,
            base_url='http://notifications-campaign-api:8000',
            api_key='campaign-secret-key',
        )
        with pytest.raises(NotificationsTransientError) as exc_info:
            await gateway_html.send_campaign_batch(
                campaign_id=campaign_id,
                batch_id=batch_id,
                user_id=user_id,
                template_id='billing_notification',
                subject='Тема',
                text='Текст',
                available_at=None,
                request_id='req-html-502',
            )
        assert 'Bad Gateway HTML' in str(exc_info.value)
