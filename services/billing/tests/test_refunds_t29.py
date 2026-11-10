from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from clients.auth import LogoutOutcome, SubscriptionAccessSyncResult
from clients.payment_provider import (
    CreateProviderRefundRequest,
    ProviderIntegrityMismatchError,
    ProviderProtocolAnomalyError,
    ProviderReconciliationError,
    ProviderTransientError,
    YooKassaProviderAdapter,
)
from clients.payment_provider_registry import PaymentProviderRegistry
from dependencies import (
    get_payment_reconciler,
    get_repositories,
    get_service_credentials,
)
from domain.subscription import build_subscription_expiration_email_payload
from main import create_app
from security.service_credentials import ServiceCredentials
from services.outbox_dispatcher import OutboxDispatcher
from services.reconciler import PaymentReconciler
from tests.fakes import (
    FakePaymentRow,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
    in_memory_repositories,
)

SERVICE_KEY = 'billing-internal-refund-service-key-long-enough'


class _RecordingAuthGateway:
    def __init__(self) -> None:
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
        self.calls.append((user_id, version, expires_at))
        return SubscriptionAccessSyncResult(applied=True, current_version=version)


class _RecordingNotificationsGateway:
    def __init__(self) -> None:
        self.batches: list[dict[str, Any]] = []
        self.cancelled_campaigns: list[UUID] = []

    async def send_campaign_batch(self, **kwargs: Any) -> None:
        self.batches.append(dict(kwargs))

    async def cancel_campaign(self, campaign_id: UUID, *, request_id: str) -> None:
        self.cancelled_campaigns.append(campaign_id)


async def test_yookassa_adapter_refund_create_get_and_metadata_search() -> None:
    """T-29 (ACL ЮKassa refunds, инв. 14, 15, 17): POST/GET и поиск по metadata."""
    refund_id = uuid4()
    other_refund_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == 'POST' and request.url.path == '/v3/refunds':
            assert request.headers.get('Idempotence-Key') == str(refund_id)
            return httpx.Response(
                200,
                json={
                    'id': 'yk_ref_001',
                    'payment_id': 'yk_pay_001',
                    'status': 'pending',
                    'amount': {'value': '299.00', 'currency': 'RUB'},
                    'metadata': {'refund_id': str(refund_id)},
                },
            )
        if request.method == 'GET' and request.url.path == '/v3/refunds/yk_ref_001':
            return httpx.Response(
                200,
                json={
                    'id': 'yk_ref_001',
                    'payment_id': 'yk_pay_001',
                    'status': 'succeeded',
                    'amount': {'value': '299.00', 'currency': 'RUB'},
                    'metadata': {'refund_id': str(refund_id)},
                },
            )
        if request.method == 'GET' and request.url.path == '/v3/refunds/yk_ref_canc':
            return httpx.Response(
                200,
                json={
                    'id': 'yk_ref_canc',
                    'payment_id': 'yk_pay_001',
                    'status': 'canceled',
                    'cancellation_details': {'reason': 'rejected_by_timeout'},
                    'amount': {'value': '299.00', 'currency': 'RUB'},
                    'metadata': {'refund_id': str(refund_id)},
                },
            )
        if request.method == 'GET' and request.url.path == '/v3/refunds':
            pay_param = request.url.params.get('payment_id')
            if pay_param == 'yk_pay_001':
                return httpx.Response(
                    200,
                    json={
                        'type': 'list',
                        'items': [
                            {
                                'id': 'yk_ref_other',
                                'payment_id': 'yk_pay_001',
                                'status': 'succeeded',
                                'amount': {'value': '299.00', 'currency': 'RUB'},
                                'metadata': {'refund_id': str(other_refund_id)},
                            },
                            {
                                'id': 'yk_ref_001',
                                'payment_id': 'yk_pay_001',
                                'status': 'succeeded',
                                'amount': {'value': '299.00', 'currency': 'RUB'},
                                'metadata': {'refund_id': str(refund_id)},
                            },
                        ],
                    },
                )
            return httpx.Response(200, json={'type': 'list', 'items': []})
        return httpx.Response(400, json={'description': 'Bad refund request'})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as hc:
        adapter = YooKassaProviderAdapter(
            http_client=hc,
            shop_id='shop-1',
            secret_key='secret-1',
        )
        created = await adapter.create_refund(
            CreateProviderRefundRequest(
                refund_id=refund_id,
                idempotency_key=refund_id,
                provider_payment_id='yk_pay_001',
                amount_kopeks=29900,
                currency='RUB',
                description='Возврат подписки Премиум',
            )
        )
        assert created.provider_refund_id == 'yk_ref_001'
        assert created.domain_status == 'processing'

        fetched = await adapter.get_refund(
            'yk_ref_001',
            expected_refund_id=refund_id,
            expected_amount_kopeks=29900,
            expected_currency='RUB',
        )
        assert fetched.domain_status == 'succeeded'

        canceled = await adapter.get_refund(
            'yk_ref_canc',
            expected_refund_id=refund_id,
            expected_amount_kopeks=29900,
            expected_currency='RUB',
        )
        assert canceled.domain_status == 'rejected'
        assert canceled.rejection_reason == 'rejected_by_timeout'

        # Розыск по metadata в списке возвратов платежа (инвариант 14)
        found = await adapter.find_refund_by_metadata(
            'yk_pay_001',
            refund_id=refund_id,
            expected_amount_kopeks=29900,
            expected_currency='RUB',
        )
        assert found is not None
        assert found.provider_refund_id == 'yk_ref_001'
        assert found.domain_status == 'succeeded'

        not_found = await adapter.find_refund_by_metadata(
            'yk_pay_empty',
            refund_id=refund_id,
            expected_amount_kopeks=29900,
            expected_currency='RUB',
        )
        assert not_found is None

        # Unhappy paths: несовпадение суммы, валюты или metadata.refund_id (инв. 17)
        with pytest.raises(ProviderIntegrityMismatchError):
            await adapter.get_refund(
                'yk_ref_001',
                expected_refund_id=uuid4(),
                expected_amount_kopeks=29900,
                expected_currency='RUB',
            )
        with pytest.raises(ProviderIntegrityMismatchError):
            await adapter.get_refund(
                'yk_ref_001',
                expected_refund_id=refund_id,
                expected_amount_kopeks=19900,
                expected_currency='RUB',
            )
        with pytest.raises(ProviderProtocolAnomalyError):
            await adapter.get_refund('../bad_id')
        with pytest.raises(ProviderReconciliationError):
            await adapter.get_refund('yk_unknown_400')


async def test_refund_api_full_lifecycle_consequences_and_unhappy_paths() -> None:
    """T-29 (BIL-FR-10, BIL-API-09, US-A-04, инв. 4, 5, 7, 9, 11, 12): возврат."""
    from tests.fakes import FakeProviderGateway

    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    auth_gw = _RecordingAuthGateway()
    ntf_gw = _RecordingNotificationsGateway()
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth_gw,
        notifications_gateway=ntf_gw,
    )
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        public_base_url='http://localhost',
        outbox_dispatcher=dispatcher,
    )

    now_utc = datetime.now(UTC)
    user_id = uuid4()
    admin_id = uuid4()
    tariff_id = uuid4()
    pay_id = uuid4()
    sub_id = uuid4()

    repo.payments[pay_id] = FakePaymentRow(
        id=pay_id,
        user_id=user_id,
        tariff_id=tariff_id,
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='succeeded',
        idempotency_key=pay_id,
        provider_payment_id='yk_pay_for_refund_01',
        created_at=now_utc - timedelta(days=1),
        updated_at=now_utc - timedelta(days=1),
    )
    repo.subscriptions[sub_id] = FakeSubscriptionRow(
        id=sub_id,
        user_id=user_id,
        tariff_id=tariff_id,
        payment_id=pay_id,
        starts_at=now_utc - timedelta(days=1),
        ends_at=now_utc + timedelta(days=29),
        created_at=now_utc - timedelta(days=1),
    )
    repo.access_versions[user_id] = 1

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_repositories] = lambda: in_memory_repositories(repo)
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={'X-Request-Id': 't29-api'},
    ) as client:
        # 1. Unhappy: без сервисного ключа -> 401
        r401 = await client.post(
            f'/api/v1/internal/billing/payments/{pay_id}/refunds',
            json={'admin_user_id': str(admin_id), 'reason': 'Причина'},
        )
        assert r401.status_code == 401

        auth_headers = {
            'X-Request-Id': 't29-api',
            'X-Service-Api-Key': SERVICE_KEY,
        }

        # 2. Unhappy: пустая причина -> 422 (инвариант 9, BIL-FR-13)
        for bad_reason in ('', '   '):
            r422 = await client.post(
                f'/api/v1/internal/billing/payments/{pay_id}/refunds',
                headers=auth_headers,
                json={'admin_user_id': str(admin_id), 'reason': bad_reason},
            )
            assert r422.status_code == 422

        # 3. Unhappy: неизвестный платёж -> 404
        r404 = await client.post(
            f'/api/v1/internal/billing/payments/{uuid4()}/refunds',
            headers=auth_headers,
            json={'admin_user_id': str(admin_id), 'reason': 'Причина'},
        )
        assert r404.status_code == 404
        assert r404.json()['error_code'] == 'payment_not_found'

        # 4. Unhappy: первая попытка отменяется провайдером (canceled -> rejected),
        #    при этом доступ и письма не меняются
        provider.next_get_refund_status = 'rejected'
        provider.next_get_refund_rejection_reason = 'rejected_by_provider'
        r_rej = await client.post(
            f'/api/v1/internal/billing/payments/{pay_id}/refunds',
            headers=auth_headers,
            json={
                'admin_user_id': str(admin_id),
                'reason': 'Первая попытка возврата',
            },
        )
        assert r_rej.status_code == 200
        rej_json = r_rej.json()
        assert rej_json['status'] == 'rejected'
        assert rej_json['rejection_reason'] == 'rejected_by_provider'
        assert auth_gw.calls == []
        assert ntf_gw.cancelled_campaigns == []
        assert ntf_gw.batches == []
        assert len(repo.admin_actions) == 1
        assert repo.admin_actions[0].admin_user_id == admin_id
        assert repo.admin_actions[0].reason == 'Первая попытка возврата'

        # 5. Happy: после rejected администратор оформляет новую попытку (инвариант 4);
        #    сначала провайдер возвращает processing -> повторный запрос идемпотентно
        #    возвращает ту же попытку без второго вызова create_refund (BIL-NFR-R-02)
        provider.next_get_refund_status = 'processing'
        provider.next_get_refund_rejection_reason = None
        r_proc_1 = await client.post(
            f'/api/v1/internal/billing/payments/{pay_id}/refunds',
            headers=auth_headers,
            json={'admin_user_id': str(admin_id), 'reason': 'Повторная попытка'},
        )
        assert r_proc_1.status_code == 200
        proc_id = UUID(r_proc_1.json()['id'])
        assert proc_id != UUID(rej_json['id'])
        assert r_proc_1.json()['status'] == 'processing'
        create_refund_calls_after_1 = len(provider.create_refund_calls)

        # Двойная отправка формы при processing: возвращает тот же возврат и сверяет GET
        provider.next_get_refund_status = 'succeeded'
        r_proc_2 = await client.post(
            f'/api/v1/internal/billing/payments/{pay_id}/refunds',
            headers=auth_headers,
            json={'admin_user_id': str(admin_id), 'reason': 'Повторная попытка'},
        )
        assert r_proc_2.status_code == 200
        assert UUID(r_proc_2.json()['id']) == proc_id
        assert len(provider.create_refund_calls) == create_refund_calls_after_1
        assert r_proc_2.json()['status'] == 'succeeded'

        # Проверяем последствия выполненного возврата:
        # - в Auth отправлена версия 2 с expires_at=None (отзыв доступа)
        assert auth_gw.calls == [(user_id, 2, None)]
        # - в Notifications отменено запланированное письмо с тем же campaign_id
        exp_payload = build_subscription_expiration_email_payload(
            subscription_id=sub_id,
            user_id=user_id,
            ends_at=repo.subscriptions[sub_id].ends_at,
            public_base_url='http://localhost',
            tariff_title='Премиум',
        )
        assert ntf_gw.cancelled_campaigns == [UUID(str(exp_payload['campaign_id']))]
        # - в Notifications отправлено письмо о выполненном возврате
        assert len(ntf_gw.batches) == 1
        assert '299 ₽' in ntf_gw.batches[0]['text']

        # 6. Повтор после succeeded: вторая попытка не создаётся, возвращается
        #    выполненный возврат (инв. 4, BIL-NFR-R-02)
        r_already = await client.post(
            f'/api/v1/internal/billing/payments/{pay_id}/refunds',
            headers=auth_headers,
            json={'admin_user_id': str(admin_id), 'reason': 'Третья попытка'},
        )
        assert r_already.status_code == 200
        assert r_already.json()['status'] == 'succeeded'
        assert r_already.json()['reason'] != 'Третья попытка'


async def test_background_refund_reconcile_24h_metadata_lookup_and_webhook() -> None:
    """T-29 (BIL-FR-05, BIL-FR-07, инв. 14): фоновая сверка, розыск >24ч и вебхук."""
    from tests.fakes import FakeProviderGateway

    repo = InMemoryPurchaseRepository()
    provider = FakeProviderGateway()
    auth_gw = _RecordingAuthGateway()
    ntf_gw = _RecordingNotificationsGateway()
    dispatcher = OutboxDispatcher(
        repository=repo,
        auth_gateway=auth_gw,
        notifications_gateway=ntf_gw,
    )
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(provider),
        public_base_url='http://localhost',
        outbox_dispatcher=dispatcher,
    )

    now_utc = datetime.now(UTC)
    user_1 = uuid4()
    pay_1 = uuid4()
    sub_1 = uuid4()
    repo.payments[pay_1] = FakePaymentRow(
        id=pay_1,
        user_id=user_1,
        tariff_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='succeeded',
        idempotency_key=pay_1,
        provider_payment_id='yk_pay_bg_1',
        created_at=now_utc - timedelta(days=2),
    )
    repo.subscriptions[sub_1] = FakeSubscriptionRow(
        id=sub_1,
        user_id=user_1,
        tariff_id=repo.payments[pay_1].tariff_id,
        payment_id=pay_1,
        starts_at=now_utc - timedelta(days=2),
        ends_at=now_utc + timedelta(days=28),
        created_at=now_utc - timedelta(days=2),
    )

    # 1. Создание возврата при сбое ЮKassa (503) оставляет его в processing без номера
    provider.create_refund_error = ProviderTransientError(
        'YooKassa 503', status_code=503
    )
    outcome_initial = await reconciler.create_or_reconcile_refund(
        pay_1,
        admin_user_id=uuid4(),
        reason='Сбой при создании',
    )
    assert outcome_initial.status == 'processing'
    ref_1 = repo.refunds[outcome_initial.refund_id]
    assert ref_1.provider_refund_id is None

    # 2. Возраст > 24 часов: фоновая сверка ищет возврат через find_refund_by_metadata
    provider.create_refund_error = None
    ref_1.created_at = now_utc - timedelta(hours=25)

    # 2a. Если запрос списка завершился сбоем (503), остаётся processing (инв. 14)
    provider.find_refund_error = ProviderTransientError(
        'List refunds 503', status_code=503
    )
    await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)
    assert repo.refunds[ref_1.id].status == 'processing'

    # 2b. Если возврат не найден в списке (>24ч) -> переводится в rejected (инв. 14)
    provider.find_refund_error = None
    ref_1.last_checked_at = None
    await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)
    assert repo.refunds[ref_1.id].status == 'rejected'
    assert repo.refunds[ref_1.id].rejection_reason == 'not_created_in_provider'

    # 3. Новая попытка возврата, найденная в списке по metadata (>24ч), и вебхук
    provider.create_refund_error = ProviderTransientError(
        'YooKassa timeout', status_code=504
    )
    outcome_2 = await reconciler.create_or_reconcile_refund(
        pay_1,
        admin_user_id=uuid4(),
        reason='Вторая попытка (розыск по metadata)',
    )
    ref_2 = repo.refunds[outcome_2.refund_id]
    ref_2.created_at = now_utc - timedelta(hours=25)
    provider.create_refund_error = None
    provider.metadata_refunds[ref_2.id] = 'yk_ref_found_by_meta'
    provider.next_get_refund_status = 'processing'

    await reconciler.reconcile_due_pending_payments(min_age_seconds=5.0)
    assert repo.refunds[ref_2.id].provider_refund_id == 'yk_ref_found_by_meta'
    assert repo.refunds[ref_2.id].status == 'processing'

    # Входящий вебхук ЮKassa с object.id = 'yk_ref_found_by_meta' завершает возврат!
    provider.next_get_refund_status = 'succeeded'
    wh_outcome = await reconciler.reconcile_by_provider_operation_id(
        'yk_ref_found_by_meta',
        provider_code='yookassa',
        source='webhook',
    )
    assert wh_outcome is not None
    assert repo.refunds[ref_2.id].status == 'succeeded'
