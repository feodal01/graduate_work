from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import sentry_sdk

from clients.auth import (
    AuthInvalidCredentialsError,
    AuthPermanentDeliveryError,
    AuthServiceUnavailableError,
    SubscriptionAccessSyncResult,
)
from clients.notifications import (
    NotificationsPermanentError,
    NotificationsTransientError,
)
from clients.payment_provider import (
    CreateProviderPaymentRequest,
    CreateProviderRefundRequest,
    ProviderIntegrityMismatchError,
    ProviderPermanentError,
    ProviderProtocolAnomalyError,
    ProviderReconciliationError,
    ProviderTransientError,
    YooKassaProviderAdapter,
)
from core.config import settings
from db.health import HealthCheck
from dependencies import (
    ACCESS_TOKEN_COOKIE,
    get_auth_gateway,
    get_database_health,
    get_notifications_gateway,
    get_outbox_dispatcher,
    get_payment_provider,
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

SERVICE_KEY = 'billing-degradation-matrix-t37-service-key-long-enough'


class _StubHealth(HealthCheck):
    def __init__(self, *, healthy: bool = True) -> None:
        self.healthy = healthy

    async def ping(self) -> bool:
        return self.healthy


class _DegradableAuthGateway:
    def __init__(self) -> None:
        self.user_id = uuid4()
        self.login_name = 'matrix_user'
        self.tz = 'Europe/Moscow'
        self.ui_mode = 'ok'  # 'ok' | 'down'
        self.sync_mode = 'ok'  # 'ok' | 'down' | 'perm_404'
        self.current_version = 0
        self.expires_at: str | None = None
        self.sync_calls = 0

    async def login(
        self, login: str, password: str, *, request_id: str
    ) -> tuple[str, str]:
        if self.ui_mode == 'down':
            raise AuthServiceUnavailableError('Auth unavailable on login')
        if login != self.login_name or password != 'MatrixPass-2026!':
            raise AuthInvalidCredentialsError()
        return 'matrix-jwt-access', 'matrix-jwt-ref'

    async def logout(
        self, access_token: str, refresh_token: str, *, request_id: str
    ) -> None:
        if self.ui_mode == 'down':
            raise AuthServiceUnavailableError('Auth unavailable on logout')

    async def get_current_user(
        self, access_token: str, *, request_id: str
    ) -> tuple[UUID, str, str]:
        if self.ui_mode == 'down':
            raise AuthServiceUnavailableError('Auth unavailable on token check')
        if access_token != 'matrix-jwt-access':
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
        self.sync_calls += 1
        if self.sync_mode == 'down':
            raise AuthServiceUnavailableError('Auth 503 on subscription-access')
        if self.sync_mode == 'perm_404':
            raise AuthPermanentDeliveryError(404, 'User not found in Auth')
        if version > self.current_version:
            self.current_version = version
            self.expires_at = expires_at
            return SubscriptionAccessSyncResult(
                applied=True, current_version=self.current_version
            )
        return SubscriptionAccessSyncResult(
            applied=False, current_version=self.current_version
        )


class _DegradableNotificationsGateway:
    def __init__(self) -> None:
        self.mode = 'ok'  # 'ok' | 'transient' | 'perm_422'
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
        if self.mode == 'transient':
            raise NotificationsTransientError('409 profile_not_ready or 503')
        if self.mode == 'perm_422':
            raise NotificationsPermanentError(422, 'Invalid template payload')
        self.sent_batches.append({'campaign_id': campaign_id, 'subject': subject})

    async def cancel_campaign(
        self,
        campaign_id: UUID,
        *,
        request_id: str,
    ) -> None:
        if self.mode == 'transient':
            raise NotificationsTransientError('Notifications 503 on cancel')
        if self.mode == 'perm_422':
            raise NotificationsPermanentError(422, 'Invalid cancel request')
        self.cancelled_ids.append(campaign_id)


async def test_degradation_matrix_rows_1_to_4_yookassa_failures(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T-37 (раздел 6.3 ТЗ, строки 1–4): деградация при всех видах сбоев ЮKassa."""
    monkeypatch.setattr(settings.worker, 'billing_stuck_threshold_seconds', 600)
    captured_sentry: list[tuple[str, str]] = []

    def fake_capture_message(message: str, level: str = 'info', **_: object) -> str:
        captured_sentry.append((message, level))
        return 'evt-id'

    monkeypatch.setattr(sentry_sdk, 'capture_message', fake_capture_message)

    # Проверяем реальный YooKassaProviderAdapter для строк 3 и 4 матрицы 6.3:
    # отправка сигнала в GlitchTip (sentry_sdk.capture_message) и ERROR-лог (caplog)
    yk_post_code = 400
    yk_get_mode = '401'
    sample_pid = uuid4()
    sample_rid = uuid4()

    def yk_mock_handler(request: httpx.Request) -> httpx.Response:
        if request.method == 'POST':
            return httpx.Response(
                yk_post_code,
                json={'code': f'err_{yk_post_code}', 'description': 'Permanent error'},
            )
        if yk_get_mode in ('401', '403'):
            return httpx.Response(
                int(yk_get_mode),
                json={'code': f'http_{yk_get_mode}', 'description': 'Auth error'},
            )
        if yk_get_mode == 'wfc':
            return httpx.Response(
                200,
                json={
                    'id': 'yk_1',
                    'status': 'waiting_for_capture',
                    'amount': {'value': '299.00', 'currency': 'RUB'},
                    'metadata': {'payment_id': str(sample_pid)},
                },
            )
        if yk_get_mode == 'bad_amount':
            return httpx.Response(
                200,
                json={
                    'id': 'yk_1',
                    'status': 'succeeded',
                    'amount': {'value': '199.00', 'currency': 'RUB'},
                    'metadata': {'payment_id': str(sample_pid)},
                },
            )
        if yk_get_mode == 'bad_currency':
            return httpx.Response(
                200,
                json={
                    'id': 'yk_1',
                    'status': 'succeeded',
                    'amount': {'value': '299.00', 'currency': 'USD'},
                    'metadata': {'payment_id': str(sample_pid)},
                },
            )
        return httpx.Response(
            200,
            json={
                'id': 'yk_1',
                'status': 'succeeded',
                'amount': {'value': '299.00', 'currency': 'RUB'},
                'metadata': {'payment_id': str(uuid4())},
            },
        )

    real_adapter = YooKassaProviderAdapter(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(yk_mock_handler)),
        shop_id='1518689',
        secret_key='test_secret',
    )
    for code in (400, 401, 403):
        yk_post_code = code
        captured_sentry.clear()
        with pytest.raises(ProviderPermanentError):
            await real_adapter.create_payment(
                CreateProviderPaymentRequest(
                    payment_id=sample_pid,
                    idempotency_key=sample_pid,
                    amount_kopeks=29900,
                    currency='RUB',
                    description='Подписка',
                    return_url='http://localhost/billing',
                )
            )
        assert len(captured_sentry) == 1 and captured_sentry[0][1] == 'error'
        assert f'HTTP {code}' in caplog.text

        captured_sentry.clear()
        with pytest.raises(ProviderPermanentError):
            await real_adapter.create_refund(
                CreateProviderRefundRequest(
                    refund_id=sample_rid,
                    idempotency_key=sample_rid,
                    provider_payment_id='yk_1',
                    amount_kopeks=29900,
                    currency='RUB',
                    description='Возврат',
                )
            )
        assert len(captured_sentry) == 1 and captured_sentry[0][1] == 'error'

    for mode, exc_cls in (
        ('401', ProviderReconciliationError),
        ('403', ProviderReconciliationError),
        ('wfc', ProviderProtocolAnomalyError),
        ('bad_amount', ProviderIntegrityMismatchError),
        ('bad_currency', ProviderIntegrityMismatchError),
        ('bad_meta', ProviderIntegrityMismatchError),
    ):
        yk_get_mode = mode
        captured_sentry.clear()
        with pytest.raises(exc_cls):
            await real_adapter.get_payment(
                'yk_1',
                expected_payment_id=sample_pid,
                expected_amount_kopeks=29900,
                expected_currency='RUB',
            )
        assert len(captured_sentry) == 1 and captured_sentry[0][1] == 'error'

    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    auth_gw = _DegradableAuthGateway()
    ntf_gw = _DegradableNotificationsGateway()
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth_gw,
        notifications_gateway=ntf_gw,
    )
    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
        outbox_dispatcher=dispatcher,
    )
    purchase_svc = PurchaseService(
        repository=repo,
        payment_provider=provider,
        reconciler=reconciler,
        public_base_url='http://localhost',
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_payment_provider] = lambda: provider
    app.dependency_overrides[get_auth_gateway] = lambda: auth_gw
    app.dependency_overrides[get_notifications_gateway] = lambda: ntf_gw
    app.dependency_overrides[get_outbox_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler
    app.dependency_overrides[get_purchase_service] = lambda: purchase_svc
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    svc_headers = {'X-Service-Api-Key': SERVICE_KEY, 'X-Request-Id': 't37-admin'}
    user_headers = {
        'Authorization': 'Bearer matrix-jwt-access',
        'X-Request-Id': 't37-user',
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
    ) as client:
        # --- Строка 1 матрицы 6.3: ЮKassa POST /v3/payments (тайм-аут, 429, 5xx) ---
        for transient_err in (
            ProviderTransientError('YooKassa timeout', status_code=None),
            ProviderTransientError('YooKassa 429', status_code=429),
            ProviderTransientError('YooKassa 503', status_code=503),
        ):
            provider.create_error = transient_err
            r_ui_503 = await client.post(
                '/billing/subscribe',
                headers={'Cookie': f'{ACCESS_TOKEN_COOKIE}=matrix-jwt-access'},
                follow_redirects=False,
            )
            assert r_ui_503.status_code == 503
            assert 'попробуйте ещё раз' in r_ui_503.text.lower()

            r_api_503 = await client.post(
                '/api/v1/billing/payments',
                headers=user_headers,
            )
            assert r_api_503.status_code == 503
            assert r_api_503.json()['error_code'] == 'payment_provider_unavailable'

        # В БД создан ровно 1 pending-платёж без provider_payment_id, и все попытки
        # использовали один и тот же Idempotence-Key == payment.id (BIL-FR-02, TASK-5)
        assert len(repo.payments) == 1
        pending_pay = next(iter(repo.payments.values()))
        assert pending_pay.status == 'pending'
        assert pending_pay.provider_payment_id is None
        assert {c.idempotency_key for c in provider.create_calls} == {pending_pay.id}

        # После восстановления ЮKassa повтор досоздаёт платёж по тому же ключу
        provider.create_error = None
        r_recover = await client.post('/api/v1/billing/payments', headers=user_headers)
        assert r_recover.status_code == 200
        assert r_recover.json()['payment_id'] == str(pending_pay.id)
        assert pending_pay.provider_payment_id is not None

        # --- Строки 2 и 4 матрицы 6.3: ЮKassa GET сверки (тайм-аут, 429, 503,
        #     несовпадение суммы/валюты/metadata, waiting_for_capture, 401/403) ---
        pending_pay.created_at = datetime.now(UTC) - timedelta(minutes=15)
        for get_err in (
            ProviderTransientError('Timeout on GET payment', status_code=None),
            ProviderTransientError('429 on GET payment', status_code=429),
            ProviderTransientError('503 on GET payment', status_code=503),
            ProviderIntegrityMismatchError('Amount mismatch'),
            ProviderIntegrityMismatchError('Currency mismatch'),
            ProviderIntegrityMismatchError('Metadata payment_id mismatch'),
            ProviderProtocolAnomalyError('Unexpected waiting_for_capture'),
            ProviderReconciliationError(401, 'Unauthorized on GET'),
            ProviderReconciliationError(403, 'Forbidden on GET'),
        ):
            provider.get_error = get_err
            pending_pay.last_checked_at = None
            await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)
            assert repo.payments[pending_pay.id].status == 'pending'

        r_stuck = await client.get(
            '/api/v1/internal/billing/payments',
            params={'stuck': 'true'},
            headers=svc_headers,
        )
        assert r_stuck.status_code == 200
        assert [i['id'] for i in r_stuck.json()['items']] == [str(pending_pay.id)]
        provider.get_error = None

        # Завершаем платёж неуспехом, чтобы проверить строку 3 (400/401/403 на POST)
        provider.get_status = 'failed'
        await reconciler.reconcile_payment(
            pending_pay.id, source='webhook', respect_throttle=False
        )
        assert repo.payments[pending_pay.id].status == 'failed'

        # --- Строка 3 матрицы 6.3: ЮKassa POST создания вернул 400/401/403 ->
        #     повторов нет, платёж сразу переводится в failed, возврат — в rejected ---
        for perm_code in (400, 401, 403):
            provider.create_error = ProviderPermanentError(
                perm_code, f'Permanent error {perm_code}'
            )
            r_perm_pay = await client.post(
                '/api/v1/billing/payments', headers=user_headers
            )
            assert r_perm_pay.status_code == 503
        assert all(p.status == 'failed' for p in repo.payments.values())
        provider.create_error = None

        # Проверяем «Повторов нет»: фоновая сверка не опрашивает failed-платежи
        calls_before = len(provider.get_calls) + len(provider.create_calls)
        await reconciler.reconcile_due_pending_payments(min_age_seconds=0.0)
        assert (len(provider.get_calls) + len(provider.create_calls)) == calls_before

        # Создаём и оплачиваем подписку, чтобы проверить строки 2 и 3 для возвратов
        provider.get_status = 'pending'
        r_ok_pay = await client.post('/api/v1/billing/payments', headers=user_headers)
        assert r_ok_pay.status_code == 200
        active_pay_id = UUID(r_ok_pay.json()['payment_id'])
        provider.get_status = 'succeeded'
        await reconciler.reconcile_payment(
            active_pay_id, source='webhook', respect_throttle=False
        )
        assert repo.payments[active_pay_id].status == 'succeeded'

        # Строка 3 для возврата: 400/401/403 на POST /v3/refunds -> сразу rejected
        for ref_perm_code in (400, 401, 403):
            provider.create_refund_error = ProviderPermanentError(
                ref_perm_code, f'Refund rejected {ref_perm_code}'
            )
            r_perm_ref = await client.post(
                f'/api/v1/internal/billing/payments/{active_pay_id}/refunds',
                json={
                    'admin_user_id': str(uuid4()),
                    'reason': f'Попытка возврата при коде {ref_perm_code}',
                },
                headers=svc_headers,
            )
            assert r_perm_ref.status_code == 200
            assert r_perm_ref.json()['status'] == 'rejected'
        provider.create_refund_error = None

        ref_calls_before = len(provider.get_refund_calls) + len(
            provider.create_refund_calls
        )
        await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)
        assert (
            len(provider.get_refund_calls) + len(provider.create_refund_calls)
        ) == ref_calls_before

        # --- Строка 2 матрицы 6.3: тайм-аут/429/5xx на POST /v3/refunds и GET сверки ->
        #     возврат сохраняется как processing, повторяется и виден в «Зависших» ---
        for trans_ref_err in (
            ProviderTransientError('Refund POST timeout', status_code=None),
            ProviderTransientError('Refund POST 429', status_code=429),
            ProviderTransientError('Refund POST 503', status_code=503),
        ):
            provider.create_refund_error = trans_ref_err
            r_trans_ref = await client.post(
                f'/api/v1/internal/billing/payments/{active_pay_id}/refunds',
                json={
                    'admin_user_id': str(uuid4()),
                    'reason': 'Возврат при сбое ЮKassa',
                },
                headers=svc_headers,
            )
            assert r_trans_ref.status_code == 200
            assert r_trans_ref.json()['status'] == 'processing'
            proc_ref_id = UUID(r_trans_ref.json()['id'])
            assert repo.refunds[proc_ref_id].provider_refund_id is None

        # Если возврат в processing старше 10 мин, платёж виден в «Зависшие операции»
        repo.refunds[proc_ref_id].created_at = datetime.now(UTC) - timedelta(minutes=15)
        r_stuck_ref = await client.get(
            '/api/v1/internal/billing/payments',
            params={'stuck': 'true'},
            headers=svc_headers,
        )
        assert r_stuck_ref.status_code == 200
        assert [i['id'] for i in r_stuck_ref.json()['items']] == [str(active_pay_id)]

        # При временном сбое GET /v3/refunds/{id} возврат остаётся processing
        provider.create_refund_error = None
        for get_ref_err in (
            ProviderTransientError('Refund GET timeout', status_code=None),
            ProviderTransientError('Refund GET 429', status_code=429),
            ProviderTransientError('Refund GET 503', status_code=503),
        ):
            provider.get_refund_error = get_ref_err
            repo.refunds[proc_ref_id].last_checked_at = None
            await reconciler.reconcile_due_processing_refunds(min_age_seconds=5.0)
            assert repo.refunds[proc_ref_id].status == 'processing'

        # После восстановления ЮKassa фоновая сверка проводит возврат в succeeded
        provider.get_refund_error = None
        provider.next_get_refund_status = 'succeeded'
        repo.refunds[proc_ref_id].last_checked_at = None
        await reconciler.reconcile_due_processing_refunds(min_age_seconds=5.0)
        assert repo.refunds[proc_ref_id].status == 'succeeded'


async def test_degradation_matrix_rows_5_to_9_auth_notifications_and_db_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-37 (6.3, строки 5–9): деградация при отказе Auth, Notifications и БД."""
    monkeypatch.setattr(settings.worker, 'billing_stuck_threshold_seconds', 600)
    captured_sentry: list[str] = []
    monkeypatch.setattr(
        sentry_sdk,
        'capture_message',
        lambda m, **_: captured_sentry.append(m),
    )

    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    auth_gw = _DegradableAuthGateway()
    ntf_gw = _DegradableNotificationsGateway()
    health = _StubHealth(healthy=True)
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth_gw,
        notifications_gateway=ntf_gw,
        max_attempts=3,
    )
    reconciler = PaymentReconciler(
        repository=repo,
        payment_provider=provider,
        public_base_url='http://localhost',
        outbox_dispatcher=dispatcher,
    )
    purchase_svc = PurchaseService(
        repository=repo,
        payment_provider=provider,
        reconciler=reconciler,
        public_base_url='http://localhost',
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_payment_provider] = lambda: provider
    app.dependency_overrides[get_auth_gateway] = lambda: auth_gw
    app.dependency_overrides[get_notifications_gateway] = lambda: ntf_gw
    app.dependency_overrides[get_outbox_dispatcher] = lambda: dispatcher
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler
    app.dependency_overrides[get_purchase_service] = lambda: purchase_svc
    app.dependency_overrides[get_database_health] = lambda: health
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    svc_headers = {'X-Service-Api-Key': SERVICE_KEY, 'X-Request-Id': 't37-admin'}
    user_headers = {
        'Authorization': 'Bearer matrix-jwt-access',
        'X-Request-Id': 't37-user',
    }
    cookie_headers = {'Cookie': f'{ACCESS_TOKEN_COOKIE}=matrix-jwt-access'}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
    ) as client:
        # --- Строка 5 матрицы 6.3: Auth (AUTH-API-01) недоступен / 429 / 5xx ->
        #     приём оплаты не блокируется; видно «Оплачено, доступ активируется»;
        #     задача в outbox_tasks повторяется до восстановления Auth ---
        r_buy = await client.post('/api/v1/billing/payments', headers=user_headers)
        assert r_buy.status_code == 200
        pay_id = UUID(r_buy.json()['payment_id'])
        yk_id = repo.payments[pay_id].provider_payment_id
        assert yk_id is not None

        auth_gw.sync_mode = 'down'
        ntf_gw.mode = 'transient'
        provider.get_status = 'succeeded'
        r_wh = await client.post(
            '/api/v1/billing/webhooks/yookassa',
            json={'object': {'id': yk_id}},
            headers={'X-Request-Id': 't37-wh-auth-down'},
        )
        assert r_wh.status_code == 200
        assert auth_gw.current_version == 0
        assert ntf_gw.sent_batches == []

        # Экран результата и «Моя подписка» показывают «Оплачено, доступ активируется»
        r_res_act = await client.get(
            f'/billing/payments/{pay_id}', headers=cookie_headers
        )
        assert r_res_act.status_code == 200
        assert 'Оплачено, доступ активируется' in r_res_act.text

        r_sub_act = await client.get('/billing/', headers=cookie_headers)
        assert r_sub_act.status_code == 200
        assert 'Оплачено, доступ активируется' in r_sub_act.text

        r_sum_act = await client.get('/api/v1/billing/me/summary', headers=user_headers)
        assert r_sum_act.status_code == 200
        assert r_sum_act.json()['status_key'] == 'activating'

        # Восстанавливаем Auth и Notifications -> задачи доставляются, статус active
        auth_gw.sync_mode = 'ok'
        ntf_gw.mode = 'ok'
        for t in repo.outbox_tasks.values():
            if t.status == 'pending':
                t.next_retry_at = datetime.now(UTC)
        await dispatcher.process_due_tasks()
        assert auth_gw.current_version == 1
        assert auth_gw.expires_at is not None
        assert len(ntf_gw.sent_batches) == 2

        # --- Строка 7 матрицы 6.3: Auth недоступен на пользовательских экранах
        #     при наличии активной подписки -> 503 и нет утечки данных ---
        auth_gw.ui_mode = 'down'
        r_login_503 = await client.post(
            '/billing/login',
            data={'login': 'matrix_user', 'password': 'MatrixPass-2026!'},
            follow_redirects=False,
        )
        assert r_login_503.status_code == 503

        for protected_path in (
            '/billing/',
            f'/billing/payments/{pay_id}',
        ):
            r_ui_503 = await client.get(protected_path, headers=cookie_headers)
            assert r_ui_503.status_code == 503
            assert str(pay_id) not in r_ui_503.text
            assert 'Активна до' not in r_ui_503.text
            assert '299 ₽' not in r_ui_503.text

        r_api_sum_503 = await client.get(
            '/api/v1/billing/me/summary', headers=user_headers
        )
        assert r_api_sum_503.status_code == 503
        auth_gw.ui_mode = 'ok'

        # --- Строка 8 матрицы 6.3: Notifications недоступен / 409 / 5xx при возврате ->
        #     возврат и закрытие доступа в Auth выполняются штатно, а задачи писем
        #     ждут в outbox и доставляются после восстановления ---
        ntf_gw.mode = 'transient'
        r_ref_ntf_down = await client.post(
            f'/api/v1/internal/billing/payments/{pay_id}/refunds',
            json={'admin_user_id': str(uuid4()), 'reason': 'Возврат при сбое NTF'},
            headers=svc_headers,
        )
        assert r_ref_ntf_down.status_code == 200
        assert r_ref_ntf_down.json()['status'] == 'succeeded'
        assert auth_gw.current_version == 2
        assert auth_gw.expires_at is None
        assert ntf_gw.cancelled_ids == []

        ntf_gw.mode = 'ok'
        for t in repo.outbox_tasks.values():
            if t.status == 'pending':
                t.next_retry_at = datetime.now(UTC)
        await dispatcher.process_due_tasks()
        assert len(ntf_gw.cancelled_ids) == 1
        assert len(ntf_gw.sent_batches) == 3

        # --- Строка 6 матрицы 6.3: постоянная ошибка 4xx и исчерпание лимита при 5xx ->
        #     requires_intervention, сигнал в мониторинг, автоповторы остановлены,
        #     в админке работают «Повторить» и «Закрыть с причиной» ---
        provider.get_status = 'pending'
        r_buy_2 = await client.post('/api/v1/billing/payments', headers=user_headers)
        pay_2_id = UUID(r_buy_2.json()['payment_id'])
        yk_2_id = repo.payments[pay_2_id].provider_payment_id
        assert yk_2_id is not None

        auth_gw.sync_mode = 'down'  # будет исчерпан лимит 3 попыток на 5xx
        ntf_gw.mode = 'perm_422'  # сразу перейдёт в requires_intervention на 422
        captured_sentry.clear()
        provider.get_status = 'succeeded'
        await client.post(
            '/api/v1/billing/webhooks/yookassa',
            json={'object': {'id': yk_2_id}},
            headers={'X-Request-Id': 't37-wh-row6'},
        )
        # Задачи Notifications сразу перешли в requires_intervention;
        # доводим задачу Auth до 3 попыток (max_attempts=3)
        for _ in range(2):
            for t in repo.outbox_tasks.values():
                if t.status == 'pending':
                    t.next_retry_at = datetime.now(UTC)
            await dispatcher.process_due_tasks()

        intervene_tasks = [
            t
            for t in repo.outbox_tasks.values()
            if t.payment_id == pay_2_id and t.status == 'requires_intervention'
        ]
        assert len(intervene_tasks) == 3
        assert any('requires intervention' in m for m in captured_sentry)

        # Автоматические повторы для requires_intervention остановлены
        sync_calls_before = auth_gw.sync_calls
        await dispatcher.process_due_tasks()
        assert auth_gw.sync_calls == sync_calls_before

        # В списке проблемных задач (BIL-API-11) у всех трёх can_retry_or_close == True
        r_outbox_list = await client.get(
            '/api/v1/internal/billing/outbox-tasks',
            params={'payment_id': str(pay_2_id)},
            headers=svc_headers,
        )
        assert r_outbox_list.status_code == 200
        assert r_outbox_list.json()['total'] == 3
        assert all(i['can_retry_or_close'] for i in r_outbox_list.json()['items'])

        auth_intervene = next(t for t in intervene_tasks if t.target_service == 'auth')
        ntf_intervene = next(
            t for t in intervene_tasks if t.target_service == 'notifications'
        )

        # Кнопка «Повторить» для Auth после восстановления доставляет доступ
        auth_gw.sync_mode = 'ok'
        r_retry = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{auth_intervene.id}/retry',
            json={'admin_user_id': str(uuid4())},
            headers=svc_headers,
        )
        assert r_retry.status_code == 200
        assert r_retry.json()['status'] == 'completed'
        assert auth_gw.current_version == 3

        # Кнопка «Закрыть с причиной» для Notifications переводит задачу в closed
        r_close = await client.post(
            f'/api/v1/internal/billing/outbox-tasks/{ntf_intervene.id}/close',
            json={
                'admin_user_id': str(uuid4()),
                'reason': 'Закрыто администратором по строке 6 матрицы 6.3',
            },
            headers=svc_headers,
        )
        assert r_close.status_code == 200
        assert r_close.json()['status'] == 'closed'

        # --- Строка 9 матрицы 6.3: billing-db недоступна -> healthcheck и запросы 503,
        #     вебхуку ЮKassa отдаётся 503, а после восстановления БД итог фиксирует
        #     сверка при старте ---
        # Создаём pending-платёж до падения БД
        await reconciler.create_or_reconcile_refund(
            pay_2_id, admin_user_id=uuid4(), reason='Освобождаем подписку для строки 9'
        )
        provider.get_status = 'pending'
        r_buy_3 = await client.post('/api/v1/billing/payments', headers=user_headers)
        pay_3_id = UUID(r_buy_3.json()['payment_id'])
        yk_3_id = repo.payments[pay_3_id].provider_payment_id
        assert yk_3_id is not None

        health.healthy = False
        repo.db_unavailable = True

        r_health_503 = await client.get(
            '/api/v1/health/', headers={'X-Request-Id': 't37-h-503'}
        )
        assert r_health_503.status_code == 503

        r_ui_db_503 = await client.get('/billing/', headers=cookie_headers)
        assert r_ui_db_503.status_code == 503

        r_int_db_503 = await client.get(
            '/api/v1/internal/billing/payments', headers=svc_headers
        )
        assert r_int_db_503.status_code == 503

        provider.get_status = 'succeeded'
        r_wh_503 = await client.post(
            '/api/v1/billing/webhooks/yookassa',
            json={'object': {'id': yk_3_id}},
            headers={'X-Request-Id': 't37-wh-503'},
        )
        assert r_wh_503.status_code == 503
        assert r_wh_503.json()['error_code'] == 'database_unavailable'
        assert repo.payments[pay_3_id].status == 'pending'

        # После восстановления БД сверка при старте фиксирует итог succeeded!
        health.healthy = True
        repo.db_unavailable = False
        repo.payments[pay_3_id].last_checked_at = None
        await reconciler.reconcile_due_pending_payments(min_age_seconds=0.0)
        assert repo.payments[pay_3_id].status == 'succeeded'
