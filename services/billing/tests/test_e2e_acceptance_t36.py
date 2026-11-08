from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from clients.auth import SubscriptionAccessSyncResult
from clients.notifications import NotificationsTransientError
from clients.payment_provider import YooKassaProviderAdapter
from clients.payment_provider_registry import PaymentProviderRegistry
from core.config import settings
from dependencies import (
    get_auth_gateway,
    get_notifications_gateway,
    get_outbox_dispatcher,
    get_payment_providers,
    get_payment_reconciler,
    get_purchase_repository,
    get_purchase_service,
    get_service_credentials,
)
from main import create_app
from security.service_credentials import ServiceCredentials
from services.outbox_dispatcher import OutboxDispatcher
from services.purchase import PurchaseService
from services.reconciler import PaymentReconciler
from tests.fakes import FakeProviderGateway, InMemoryPurchaseRepository

SERVICE_KEY = 'billing-e2e-acceptance-t36-service-key-long-enough'
PLACEHOLDER_SHOP_IDS = {'', 'replace-with-test-yookassa-shop-id'}
PLACEHOLDER_SECRETS = {
    '',
    'test_secret_key_change_me',
    'replace-with-test-yookassa-secret-key',
}


class _E2EAuthGateway:
    def __init__(self) -> None:
        self.user_id = uuid4()
        self.login_name = 'e2e_viewer'
        self.tz = 'Asia/Yekaterinburg'
        self.down = False
        self.current_version = 0
        self.expires_at: str | None = None

    async def login(
        self, login: str, password: str, *, request_id: str
    ) -> tuple[str, str]:
        from clients.auth import AuthInvalidCredentialsError

        if login != self.login_name or password != 'ValidPass-2026!':
            raise AuthInvalidCredentialsError()
        return 'e2e-jwt-access', 'e2e-jwt-ref'

    async def logout(
        self, access_token: str, refresh_token: str, *, request_id: str
    ) -> None:
        return None

    async def get_current_user(
        self, access_token: str, *, request_id: str
    ) -> tuple[UUID, str, str]:
        from clients.auth import AuthInvalidCredentialsError

        if access_token != 'e2e-jwt-access':
            raise AuthInvalidCredentialsError()
        return self.user_id, self.login_name, self.tz

    async def put_subscription_access(
        self,
        user_id: UUID,
        *,
        version: int,
        expires_at: str | None,
        request_id: str,
    ) -> SubscriptionAccessSyncResult:
        from clients.auth import AuthServiceUnavailableError

        if self.down:
            raise AuthServiceUnavailableError('Auth temporarily down')
        if version > self.current_version:
            self.current_version = version
            self.expires_at = expires_at
            return SubscriptionAccessSyncResult(
                applied=True, current_version=self.current_version
            )
        return SubscriptionAccessSyncResult(
            applied=False, current_version=self.current_version
        )


class _E2ENotificationsGateway:
    def __init__(self) -> None:
        self.down = False
        self.sent_batches: list[dict[str, Any]] = []
        self.cancelled_ids: list[UUID] = []

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
        if self.down:
            raise NotificationsTransientError('Notifications temporarily down')
        self.sent_batches.append(
            {
                'campaign_id': campaign_id,
                'batch_id': batch_id,
                'user_id': user_id,
                'template_id': template_id,
                'subject': subject,
                'text': text,
                'available_at': available_at,
            }
        )

    async def cancel_campaign(
        self,
        campaign_id: UUID,
        *,
        request_id: str,
    ) -> None:
        if self.down:
            raise NotificationsTransientError('Notifications temporarily down')
        self.cancelled_ids.append(campaign_id)


async def test_e2e_acceptance_criteria_task_1_to_6_and_link_navigation() -> None:
    """T-36 (9.1): сквозная приёмка TASK-1..TASK-6, NFR-U-01..02 и NFR-P-01."""
    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    auth_gw = _E2EAuthGateway()
    ntf_gw = _E2ENotificationsGateway()
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth_gw,
        notifications_gateway=ntf_gw,
    )
    reconciler = PaymentReconciler(
        repository=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        public_base_url='http://localhost',
        outbox_dispatcher=dispatcher,
    )
    purchase_svc = PurchaseService(
        repository=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        reconciler=reconciler,
        public_base_url='http://localhost',
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_payment_providers] = lambda: (
        PaymentProviderRegistry.single(provider)
    )
    app.dependency_overrides[get_auth_gateway] = lambda: auth_gw
    app.dependency_overrides[get_notifications_gateway] = lambda: ntf_gw
    app.dependency_overrides[get_outbox_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler
    app.dependency_overrides[get_purchase_service] = lambda: purchase_svc
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
    ) as client:
        # 0. NFR-U-02: отсутствие X-Request-Id на API возвращает 400 в едином формате
        r_no_req_id = await client.post('/api/v1/billing/payments')
        assert r_no_req_id.status_code == 400
        assert r_no_req_id.json()['error_code'] == 'missing_request_id'
        assert isinstance(r_no_req_id.json()['detail'], str)

        # 1. Навигация по ссылкам (NFR-U-01): вход -> экран «Моя подписка» (Э1 -> Э2)
        r_root = await client.get('/billing/', follow_redirects=False)
        assert r_root.status_code in (302, 303)
        assert '/billing/login' in r_root.headers['location']

        r_login_page = await client.get('/billing/login')
        assert r_login_page.status_code == 200
        assert 'action="/billing/login"' in r_login_page.text

        r_bad_login = await client.post(
            '/billing/login',
            data={'login': 'e2e_viewer', 'password': 'wrong'},
            follow_redirects=False,
        )
        assert r_bad_login.status_code == 401

        r_login = await client.post(
            '/billing/login',
            data={'login': 'e2e_viewer', 'password': 'ValidPass-2026!'},
            follow_redirects=False,
        )
        assert r_login.status_code == 303
        assert r_login.headers['location'] == '/billing/'
        client.cookies.update(r_login.cookies)

        r_sub_empty = await client.get('/billing/')
        assert r_sub_empty.status_code == 200
        assert 'Нет подписки' in r_sub_empty.text
        assert 'action="/billing/subscribe"' in r_sub_empty.text

        # 1b. Ветка неуспешной оплаты (US-U-03, TASK-6): отказ карты -> экран
        #     «Оплата не прошла» с кнопкой повтора и транзакционное письмо об отказе
        r_buy_fail = await client.post('/billing/subscribe', follow_redirects=False)
        assert r_buy_fail.status_code == 303
        failed_pay_id = provider.create_calls[0].payment_id
        failed_yk_id = repo.payments[failed_pay_id].provider_payment_id
        assert failed_yk_id is not None

        provider.get_status = 'failed'
        await client.post(
            '/api/v1/billing/webhooks/yookassa',
            json={'object': {'id': failed_yk_id}},
            headers={'X-Request-Id': 'e2e-wh-fail'},
        )
        r_res_failed = await client.get(f'/billing/payments/{failed_pay_id}')
        assert r_res_failed.status_code == 200
        assert 'Оплата не прошла' in r_res_failed.text
        assert 'action="/billing/subscribe"' in r_res_failed.text
        assert len(ntf_gw.sent_batches) == 1
        assert (
            ntf_gw.sent_batches[0]['subject'] == 'Оплата подписки «Премиум» не прошла'
        )

        # 2. Повторная покупка и защита от двойного списания (TASK-1, TASK-5)
        provider.get_status = 'pending'
        r_buy_1 = await client.post('/billing/subscribe', follow_redirects=False)
        assert r_buy_1.status_code == 303
        assert 'yoomoney.ru/checkout/' in r_buy_1.headers['location']
        assert len(provider.create_calls) == 2
        payment_id = provider.create_calls[1].payment_id
        provider_payment_id = repo.payments[payment_id].provider_payment_id
        assert provider_payment_id is not None

        # Повторный запрос создания покупки возвращает тот же платёж без дубля в ЮKassa
        r_buy_dup = await client.post(
            '/api/v1/billing/payments',
            headers={'X-Request-Id': 'e2e-dup-buy'},
        )
        assert r_buy_dup.status_code == 200
        assert r_buy_dup.json()['payment_id'] == str(payment_id)
        assert len(provider.create_calls) == 2

        # 3. Экран результата до и после оплаты, доступ и письма (TASK-1, TASK-6)
        get_calls_before_result = len(provider.get_calls)
        r_res_pending = await client.get(f'/billing/payments/{payment_id}')
        assert r_res_pending.status_code == 200
        assert 'Проверяем оплату' in r_res_pending.text
        assert f'/billing/payments/{payment_id}/continue' in r_res_pending.text
        assert len(provider.get_calls) == get_calls_before_result

        # Вебхук от ЮKassa запускает сверку GET -> создаётся подписка, доступ
        # в Auth (версия 1) и отправляются/планируются письма в Notifications
        provider.get_status = 'succeeded'
        r_wh = await client.post(
            '/api/v1/billing/webhooks/yookassa',
            json={
                'type': 'notification',
                'event': 'payment.succeeded',
                'object': {'id': provider_payment_id},
            },
            headers={'X-Request-Id': 'e2e-wh-1'},
        )
        assert r_wh.status_code == 200
        assert auth_gw.current_version == 1
        assert auth_gw.expires_at is not None
        assert len(ntf_gw.sent_batches) == 3
        succ_batch = next(
            b
            for b in ntf_gw.sent_batches[1:]
            if b['available_at'] is None and 'Подписка' in b['subject']
        )
        exp_batch = next(
            b for b in ntf_gw.sent_batches[1:] if b['available_at'] is not None
        )
        assert succ_batch['subject'] == 'Подписка «Премиум» оплачена'
        exp_campaign_id = exp_batch['campaign_id']

        r_res_ok = await client.get(f'/billing/payments/{payment_id}')
        assert r_res_ok.status_code == 200
        assert 'Оплата прошла' in r_res_ok.text
        assert 'href="/billing/"' in r_res_ok.text

        # Переход по ссылке на экран «Моя подписка» (TASK-1, NFR-U-01): «Активна до …»
        r_sub_active = await client.get('/billing/')
        assert r_sub_active.status_code == 200
        assert 'Активна до' in r_sub_active.text

        r_summary_active = await client.get(
            '/api/v1/billing/me/summary',
            headers={'X-Request-Id': 'e2e-summary-active'},
        )
        assert r_summary_active.status_code == 200
        assert r_summary_active.json()['status_key'] == 'active'

        # Повторная покупка при активной подписке -> 409 в едином формате (NFR-U-02)
        r_buy_active = await client.post(
            '/api/v1/billing/payments',
            headers={'X-Request-Id': 'e2e-active-409'},
        )
        assert r_buy_active.status_code == 409
        assert r_buy_active.json()['error_code'] == 'subscription_already_active'
        assert isinstance(r_buy_active.json()['detail'], str)

        # 4. Контроль в админке, возврат, отзыв доступа и отмена письма (TASK-2..5)
        svc_headers = {
            'X-Service-Api-Key': SERVICE_KEY,
            'X-Request-Id': 'e2e-admin',
        }
        r_subs_list = await client.get(
            '/api/v1/internal/billing/subscriptions',
            params={'user_id': str(auth_gw.user_id), 'status': 'active'},
            headers=svc_headers,
        )
        assert r_subs_list.status_code == 200
        assert r_subs_list.json()['total'] == 1

        r_card = await client.get(
            f'/api/v1/internal/billing/payments/{payment_id}',
            headers=svc_headers,
        )
        assert r_card.status_code == 200
        assert r_card.json()['can_refund'] is True

        admin_id = uuid4()
        # Пустая причина возврата -> 422 в едином формате (NFR-U-02)
        r_ref_422 = await client.post(
            f'/api/v1/internal/billing/payments/{payment_id}/refunds',
            json={'admin_user_id': str(admin_id), 'reason': '   '},
            headers=svc_headers,
        )
        assert r_ref_422.status_code == 422
        assert r_ref_422.json()['error_code'] == 'validation_error'
        assert isinstance(r_ref_422.json()['errors'], list)

        # Возврат с причиной -> succeeded, 1 вызов в ЮKassa с ключом идемпотентности,
        # доступ отозван (expires_at=None), письмо отменено, запись в admin_actions
        r_ref_ok = await client.post(
            f'/api/v1/internal/billing/payments/{payment_id}/refunds',
            json={'admin_user_id': str(admin_id), 'reason': 'Обращение клиента №101'},
            headers=svc_headers,
        )
        assert r_ref_ok.status_code == 200
        assert r_ref_ok.json()['status'] == 'succeeded'
        assert len(provider.create_refund_calls) == 1
        assert (
            provider.create_refund_calls[0].idempotency_key
            == provider.create_refund_calls[0].refund_id
        )
        assert auth_gw.current_version == 2
        assert auth_gw.expires_at is None
        assert ntf_gw.cancelled_ids == [exp_campaign_id]
        assert len(ntf_gw.sent_batches) == 4
        assert 'Возврат средств' in ntf_gw.sent_batches[-1]['subject']

        # Повторная попытка возврата уже возвращённого платежа -> 409 (TASK-5, NFR-U-02)
        r_ref_dup = await client.post(
            f'/api/v1/internal/billing/payments/{payment_id}/refunds',
            json={'admin_user_id': str(admin_id), 'reason': 'Повтор'},
            headers=svc_headers,
        )
        assert r_ref_dup.status_code == 409
        assert r_ref_dup.json()['error_code'] == 'refund_already_succeeded'
        assert isinstance(r_ref_dup.json()['detail'], str)
        assert len(provider.create_refund_calls) == 1

        # Список подписок и платежей в админке после возврата (TASK-3)
        r_subs_ref = await client.get(
            '/api/v1/internal/billing/subscriptions',
            params={'user_id': str(auth_gw.user_id), 'status': 'refunded'},
            headers=svc_headers,
        )
        assert r_subs_ref.status_code == 200
        assert r_subs_ref.json()['total'] == 1

        r_pays_ref = await client.get(
            '/api/v1/internal/billing/payments',
            params={'user_id': str(auth_gw.user_id), 'has_refund': 'true'},
            headers=svc_headers,
        )
        assert r_pays_ref.status_code == 200
        assert r_pays_ref.json()['total'] == 1

        # На экране пользователя статус стал «Возвращена»
        r_sub_refunded = await client.get('/billing/')
        assert r_sub_refunded.status_code == 200
        assert 'Возвращена' in r_sub_refunded.text

        # В журнале действий администраторов (BIL-API-14) есть запись о возврате
        r_audit = await client.get(
            '/api/v1/internal/billing/admin-actions',
            headers=svc_headers,
        )
        assert r_audit.status_code == 200
        assert r_audit.json()['total'] == 1
        assert r_audit.json()['items'][0]['reason'] == 'Обращение клиента №101'

        # 5. Устойчивость к перебоям (TASK-4):
        #    а) закрытая вкладка (без вебхука) -> итог фиксируется фоновой сверкой;
        #    б) при остановленном Auth и Notifications ни оплата, ни возврат не падают
        #       с 5xx, а доставляют последствия после восстановления сервисов
        provider.get_status = 'pending'
        r_buy_2 = await client.post(
            '/api/v1/billing/payments',
            headers={'X-Request-Id': 'e2e-buy-outage'},
        )
        assert r_buy_2.status_code == 200
        pay_2_id = UUID(r_buy_2.json()['payment_id'])

        auth_gw.down = True
        ntf_gw.down = True
        provider.get_status = 'succeeded'
        repo.payments[pay_2_id].created_at = datetime.now(UTC) - timedelta(seconds=30)
        reconciled_count = await reconciler.reconcile_due_pending_payments(
            min_age_seconds=5.0
        )
        assert reconciled_count == 1
        assert repo.payments[pay_2_id].status == 'succeeded'
        assert auth_gw.current_version == 2  # ещё не доставлено (Auth down)

        # До доставки в Auth экран результата показывает «Оплачено, доступ активируется»
        auth_gw.down = False
        r_res_activating = await client.get(f'/billing/payments/{pay_2_id}')
        assert r_res_activating.status_code == 200
        assert 'Оплачено, доступ активируется' in r_res_activating.text

        # Восстанавливаем Auth и Notifications -> фоновый проход доставляет задачи
        ntf_gw.down = False
        for t in repo.outbox_tasks.values():
            if t.status == 'pending':
                t.next_retry_at = datetime.now(UTC)
        await dispatcher.process_due_tasks()
        assert auth_gw.current_version == 3
        assert auth_gw.expires_at is not None
        assert len(ntf_gw.sent_batches) == 6

        r_res_activated = await client.get(f'/billing/payments/{pay_2_id}')
        assert r_res_activated.status_code == 200
        assert 'Оплата прошла' in r_res_activated.text

        # Возврат при остановленном Auth и Notifications также не падает с 5xx
        auth_gw.down = True
        ntf_gw.down = True
        r_ref_outage = await client.post(
            f'/api/v1/internal/billing/payments/{pay_2_id}/refunds',
            json={
                'admin_user_id': str(admin_id),
                'reason': 'Возврат во время сбоя Auth и Notifications',
            },
            headers=svc_headers,
        )
        assert r_ref_outage.status_code == 200
        assert r_ref_outage.json()['status'] == 'succeeded'
        assert auth_gw.current_version == 3

        auth_gw.down = False
        ntf_gw.down = False
        for t in repo.outbox_tasks.values():
            if t.status == 'pending':
                t.next_retry_at = datetime.now(UTC)
        await dispatcher.process_due_tasks()
        assert auth_gw.current_version == 4
        assert auth_gw.expires_at is None
        assert len(ntf_gw.cancelled_ids) == 2
        assert len(ntf_gw.sent_batches) == 7

        r_outbox_clean = await client.get(
            '/api/v1/internal/billing/outbox-tasks',
            headers=svc_headers,
        )
        assert r_outbox_clean.status_code == 200
        assert r_outbox_clean.json()['total'] == 0

        r_metrics = await client.get(
            '/api/v1/internal/billing/metrics',
            headers=svc_headers,
        )
        assert r_metrics.status_code == 200
        assert r_metrics.json()['undelivered_outbox_count'] == 0

        # 6. Критерий NFR-P-01: время ответа собственных эндпоинтов p95 <= 300 мс
        latencies_ms: list[float] = []
        for _ in range(20):
            t0 = perf_counter()
            res = await client.get('/billing/')
            assert res.status_code == 200
            latencies_ms.append((perf_counter() - t0) * 1000.0)
        latencies_ms.sort()
        p95_ms = latencies_ms[int(len(latencies_ms) * 0.95) - 1]
        assert p95_ms <= 300.0, f'p95 latency {p95_ms:.2f}ms exceeds 300ms SLO'


@pytest.mark.integration
async def test_live_yookassa_test_shop_sandbox_e2e() -> None:
    """T-36 (6.4, 9.1): сквозной проход приложения с тестовым магазином ЮKassa."""
    shop_id = settings.payment_providers.yookassa.shop_id.strip()
    secret_key = (
        settings.payment_providers.yookassa.secret_key.get_secret_value().strip()
    )
    if shop_id in PLACEHOLDER_SHOP_IDS or secret_key in PLACEHOLDER_SECRETS:
        pytest.skip('Live YooKassa test shop credentials are not configured')

    repo = InMemoryPurchaseRepository()
    auth_gw = _E2EAuthGateway()
    ntf_gw = _E2ENotificationsGateway()

    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as yk_http:
        adapter = YooKassaProviderAdapter(
            http_client=yk_http,
            shop_id=shop_id,
            secret_key=secret_key,
            base_url=str(settings.payment_providers.yookassa.api_base_url),
        )
        dispatcher = OutboxDispatcher(
            repository=repo,
            auth_gateway=auth_gw,
            notifications_gateway=ntf_gw,
        )
        reconciler = PaymentReconciler(
            repository=repo,
            payment_providers=PaymentProviderRegistry.single(adapter),
            public_base_url='http://localhost',
            outbox_dispatcher=dispatcher,
        )
        purchase_svc = PurchaseService(
            repository=repo,
            payment_providers=PaymentProviderRegistry.single(adapter),
            reconciler=reconciler,
            public_base_url='http://localhost',
        )

        app = create_app(app_lifespan=None)
        app.dependency_overrides[get_purchase_repository] = lambda: repo
        app.dependency_overrides[get_payment_providers] = lambda: (
            PaymentProviderRegistry.single(adapter)
        )
        app.dependency_overrides[get_auth_gateway] = lambda: auth_gw
        app.dependency_overrides[get_notifications_gateway] = lambda: ntf_gw
        app.dependency_overrides[get_outbox_dispatcher] = lambda: dispatcher
        app.dependency_overrides[get_payment_reconciler] = lambda: reconciler
        app.dependency_overrides[get_purchase_service] = lambda: purchase_svc

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url='http://test',
            headers={'Authorization': 'Bearer e2e-jwt-access'},
        ) as client:
            # 1. Создаём покупку через приложение -> реальный POST /v3/payments в ЮKassa
            r_create_1 = await client.post(
                '/api/v1/billing/payments',
                headers={'X-Request-Id': 't36-live-yk-1'},
            )
            assert r_create_1.status_code == 200
            body_1 = r_create_1.json()
            payment_id = UUID(body_1['payment_id'])
            assert 'yoomoney.ru' in body_1['confirmation_url']
            provider_payment_id = repo.payments[payment_id].provider_payment_id
            assert provider_payment_id is not None

            # 2. Повтор покупки возвращает тот же платёж и номер в ЮKassa (TASK-5)
            r_create_2 = await client.post(
                '/api/v1/billing/payments',
                headers={'X-Request-Id': 't36-live-yk-2'},
            )
            assert r_create_2.status_code == 200
            assert r_create_2.json()['payment_id'] == str(payment_id)
            assert repo.payments[payment_id].provider_payment_id == provider_payment_id

            # 3. Вебхук с реальным provider_payment_id вызывает GET /v3/payments/{id}
            #    в тестовом магазине ЮKassa с проверкой суммы, валюты и metadata
            r_wh = await client.post(
                '/api/v1/billing/webhooks/yookassa',
                json={
                    'type': 'notification',
                    'event': 'payment.waiting_for_capture',
                    'object': {'id': provider_payment_id, 'status': 'succeeded'},
                },
                headers={'X-Request-Id': 't36-live-yk-wh'},
            )
            assert r_wh.status_code == 200
            # Без ввода карты платёж в ЮKassa остаётся pending (не веря телу вебхука!)
            assert repo.payments[payment_id].status == 'pending'
