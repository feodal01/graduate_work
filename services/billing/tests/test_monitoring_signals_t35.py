from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
import sentry_sdk
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from clients.payment_provider import (
    CreateProviderPaymentRequest,
    CreateProviderRefundRequest,
    ProviderIntegrityMismatchError,
    ProviderPermanentError,
    ProviderProtocolAnomalyError,
    ProviderReconciliationError,
    YooKassaProviderAdapter,
)
from core.config import settings
from core.observability import (
    NO_REQUEST_ID,
    correlation_request_id,
    get_current_request_id,
)
from dependencies import get_purchase_repository, get_service_credentials
from main import create_app
from security.service_credentials import ServiceCredentials
from services.monitoring import OperationalMonitor
from tests.fakes import (
    FakeOutboxTaskRow,
    FakePaymentRow,
    FakeRefundRow,
    InMemoryPurchaseRepository,
)

SERVICE_KEY = 'billing-internal-metrics-key-long-enough'
METRICS_PATH = '/api/v1/internal/billing/metrics'


async def test_all_five_mandatory_monitoring_signals_section_6_4(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T-35 (6.4): проверка всех 5 обязательных сигналов в GlitchTip и ERROR-лог."""
    captured_sentry: list[tuple[str, str]] = []
    sentry_tags: dict[str, str] = {}

    def fake_capture_message(message: str, level: str = 'info', **_: object) -> str:
        captured_sentry.append((message, level))
        return 'evt-id'

    def fake_set_tag(key: str, value: str) -> None:
        sentry_tags[key] = value

    monkeypatch.setattr(sentry_sdk, 'capture_message', fake_capture_message)
    monkeypatch.setattr(sentry_sdk, 'set_tag', fake_set_tag)
    pay_id = uuid4()
    ref_id = uuid4()
    refund_post_status = 400

    # 1. Сигнал №1 (инв. 15): 400/401/403 от ЮKassa на создание и 401/403 при сверке
    def yk_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith('/payments') and request.method == 'POST':
            return httpx.Response(
                401,
                json={'code': 'invalid_credentials', 'description': 'Bad secret key'},
            )
        if request.url.path.endswith('/refunds') and request.method == 'POST':
            return httpx.Response(
                refund_post_status,
                json={'code': 'invalid_request', 'description': 'Bad refund request'},
            )
        if '/payments/yk_403' in request.url.path:
            return httpx.Response(
                403,
                json={'code': 'forbidden', 'description': 'Shop blocked'},
            )
        if '/refunds/yk_ref_401' in request.url.path:
            return httpx.Response(
                401,
                json={'code': 'invalid_credentials', 'description': 'Unauthorized'},
            )
        if request.url.path.endswith('/refunds') and request.method == 'GET':
            if request.url.params.get('payment_id') == 'yk_pay_403':
                return httpx.Response(
                    403,
                    json={'code': 'forbidden', 'description': 'Forbidden list'},
                )
            return httpx.Response(
                200,
                json={
                    'items': [
                        {
                            'id': 'yk_ref_list_bad',
                            'payment_id': 'yk_pay_1',
                            'status': 'succeeded',
                            'amount': {'value': '100.00', 'currency': 'RUB'},
                            'metadata': {'refund_id': str(ref_id)},
                        }
                    ]
                },
            )
        if '/payments/yk_wfc' in request.url.path:
            return httpx.Response(
                200,
                json={
                    'id': 'yk_wfc',
                    'status': 'waiting_for_capture',
                    'amount': {'value': '299.00', 'currency': 'RUB'},
                    'metadata': {'payment_id': str(pay_id)},
                },
            )
        if '/payments/yk_mismatch_amount' in request.url.path:
            return httpx.Response(
                200,
                json={
                    'id': 'yk_mismatch_amount',
                    'status': 'succeeded',
                    'amount': {'value': '199.00', 'currency': 'RUB'},
                    'metadata': {'payment_id': str(pay_id)},
                },
            )
        if '/payments/yk_mismatch_curr' in request.url.path:
            return httpx.Response(
                200,
                json={
                    'id': 'yk_mismatch_curr',
                    'status': 'succeeded',
                    'amount': {'value': '299.00', 'currency': 'USD'},
                    'metadata': {'payment_id': str(pay_id)},
                },
            )
        if '/payments/yk_mismatch_meta' in request.url.path:
            return httpx.Response(
                200,
                json={
                    'id': 'yk_mismatch_meta',
                    'status': 'succeeded',
                    'amount': {'value': '299.00', 'currency': 'RUB'},
                    'metadata': {'payment_id': str(uuid4())},
                },
            )
        if '/refunds/yk_ref_mismatch_curr' in request.url.path:
            return httpx.Response(
                200,
                json={
                    'id': 'yk_ref_mismatch_curr',
                    'payment_id': 'yk_pay_1',
                    'status': 'succeeded',
                    'amount': {'value': '299.00', 'currency': 'USD'},
                    'metadata': {'refund_id': str(ref_id)},
                },
            )
        if '/refunds/yk_ref_mismatch_meta' in request.url.path:
            return httpx.Response(
                200,
                json={
                    'id': 'yk_ref_mismatch_meta',
                    'payment_id': 'yk_pay_1',
                    'status': 'succeeded',
                    'amount': {'value': '299.00', 'currency': 'RUB'},
                    'metadata': {'refund_id': str(uuid4())},
                },
            )
        return httpx.Response(500)

    adapter = YooKassaProviderAdapter(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(yk_handler)),
        shop_id='1518689',
        secret_key='test_secret',
    )

    with pytest.raises(ProviderPermanentError):
        await adapter.create_payment(
            CreateProviderPaymentRequest(
                payment_id=pay_id,
                idempotency_key=pay_id,
                amount_kopeks=29900,
                currency='RUB',
                description='Подписка',
                return_url='http://localhost/billing',
            )
        )
    assert any(
        'YooKassa permanent HTTP 401' in m and lvl == 'error'
        for m, lvl in captured_sentry
    )

    for status_code in (400, 401, 403):
        refund_post_status = status_code
        with pytest.raises(ProviderPermanentError):
            await adapter.create_refund(
                CreateProviderRefundRequest(
                    refund_id=ref_id,
                    idempotency_key=ref_id,
                    provider_payment_id='yk_pay_1',
                    amount_kopeks=29900,
                    currency='RUB',
                    description='Возврат',
                )
            )
        assert any(
            f'YooKassa permanent HTTP {status_code}' in m and lvl == 'error'
            for m, lvl in captured_sentry
        )

    with pytest.raises(ProviderReconciliationError):
        await adapter.get_payment('yk_403', expected_payment_id=pay_id)
    assert any(
        'YooKassa reconciliation HTTP 403' in m and lvl == 'error'
        for m, lvl in captured_sentry
    )

    with pytest.raises(ProviderReconciliationError):
        await adapter.get_refund('yk_ref_401', expected_refund_id=ref_id)
    assert any(
        'YooKassa reconciliation HTTP 401' in m and lvl == 'error'
        for m, lvl in captured_sentry
    )

    with pytest.raises(ProviderReconciliationError):
        await adapter.find_refund_by_metadata('yk_pay_403', refund_id=ref_id)

    # 2. Сигнал №2 (инвариант 16): статус waiting_for_capture у платежа в ЮKassa
    with pytest.raises(ProviderProtocolAnomalyError):
        await adapter.get_payment(
            'yk_wfc',
            expected_payment_id=pay_id,
            expected_amount_kopeks=29900,
            expected_currency='RUB',
        )
    assert any(
        'waiting_for_capture' in m and lvl == 'error' for m, lvl in captured_sentry
    )

    # 3. Сигнал №3 (инвариант 17): несовпадение суммы, валюты или UUID в metadata
    for yk_pid in ('yk_mismatch_amount', 'yk_mismatch_curr', 'yk_mismatch_meta'):
        captured_sentry.clear()
        with pytest.raises(ProviderIntegrityMismatchError):
            await adapter.get_payment(
                yk_pid,
                expected_payment_id=pay_id,
                expected_amount_kopeks=29900,
                expected_currency='RUB',
            )
        assert len(captured_sentry) == 1
        assert captured_sentry[0][1] == 'error'

    for yk_rid in ('yk_ref_mismatch_curr', 'yk_ref_mismatch_meta'):
        captured_sentry.clear()
        with pytest.raises(ProviderIntegrityMismatchError):
            await adapter.get_refund(
                yk_rid,
                expected_refund_id=ref_id,
                expected_amount_kopeks=29900,
                expected_currency='RUB',
            )
        assert len(captured_sentry) == 1
        assert captured_sentry[0][1] == 'error'

    captured_sentry.clear()
    with pytest.raises(ProviderIntegrityMismatchError):
        await adapter.find_refund_by_metadata(
            'yk_pay_1',
            refund_id=ref_id,
            expected_amount_kopeks=29900,
            expected_currency='RUB',
        )
    assert len(captured_sentry) == 1
    assert captured_sentry[0][1] == 'error'

    # 4. Сигнал №4: переход задачи outbox_tasks в requires_intervention
    #    (и при permanent_error, и при исчерпании max_attempts на transient_error)
    repo = InMemoryPurchaseRepository()
    t_perm_id = uuid4()
    t_exhaust_id = uuid4()
    repo.outbox_tasks['k_perm'] = FakeOutboxTaskRow(
        id=t_perm_id,
        dedup_key='k_perm',
        target_service='auth',
        task_type='sync_access',
        payment_id=pay_id,
        user_id=uuid4(),
        access_version=1,
        payload={'version': 1},
    )
    repo.outbox_tasks['k_exhaust'] = FakeOutboxTaskRow(
        id=t_exhaust_id,
        dedup_key='k_exhaust',
        target_service='notifications',
        task_type='payment_succeeded_email',
        payment_id=pay_id,
        user_id=uuid4(),
        access_version=None,
        payload={},
        attempts=19,
    )

    async def fail_both(task: FakeOutboxTaskRow) -> tuple[str, str]:
        if task.id == t_perm_id:
            return 'permanent_error', 'Auth HTTP 404 user not found'
        return 'transient_error', 'Notifications HTTP 503'

    captured_sentry.clear()
    await repo.process_due_outbox_tasks(
        fail_both,  # type: ignore[arg-type]
        target_services=('auth', 'notifications'),
        max_attempts=20,
    )
    assert repo.outbox_tasks['k_perm'].status == 'requires_intervention'
    assert repo.outbox_tasks['k_exhaust'].status == 'requires_intervention'
    assert any(
        str(t_perm_id) in m and 'requires intervention' in m and lvl == 'error'
        for m, lvl in captured_sentry
    )
    assert any(
        str(t_exhaust_id) in m and 'exhausted 20 attempts' in m and lvl == 'error'
        for m, lvl in captured_sentry
    )
    # Проверяем, что correlation_request_id восстановил NO_REQUEST_ID после выхода
    assert get_current_request_id() == NO_REQUEST_ID
    assert sentry_tags.get('request_id') == NO_REQUEST_ID

    # 5. Сигнал №5: наличие операций в выборках «Зависшие операции» (> 10 мин)
    #    и «Не доставлено в Auth или Notifications» + запись всех 7 атрибутов спана
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer('test.t35')

    now_utc = datetime.now(UTC)
    stuck_pay_id = uuid4()
    repo.payments[stuck_pay_id] = FakePaymentRow(
        id=stuck_pay_id,
        user_id=uuid4(),
        tariff_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='pending',
        idempotency_key=stuck_pay_id,
        created_at=now_utc - timedelta(minutes=15),
    )

    captured_sentry.clear()
    monitor = OperationalMonitor(
        repository=repo,
        stuck_threshold_seconds=600,
        alert_cooldown_seconds=300.0,
    )
    with (
        trace.use_span(tracer.start_span('monitor_span'), end_on_exit=True),
        correlation_request_id('parent-req-id'),
    ):
        snapshot = await monitor.check_and_emit_signals(now_utc=now_utc)
        assert sentry_tags.get('request_id') == 'parent-req-id'

    assert sentry_tags.get('request_id') == NO_REQUEST_ID
    assert snapshot.stuck_payments_count == 1
    assert snapshot.stuck_refunds_count == 0
    assert snapshot.stuck_operations_count == 1
    assert snapshot.outbox_requires_intervention_count == 2
    assert snapshot.undelivered_outbox_count == 2
    assert len(captured_sentry) == 2
    assert any('Зависшие операции' in m for m, _ in captured_sentry)
    assert any('Не доставлено' in m for m, _ in captured_sentry)
    assert 'Зависшие операции' in caplog.text
    assert 'Не доставлено' in caplog.text

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    span_attrs = dict(spans[0].attributes or {})
    for attr_key in (
        'billing.stuck_payments_count',
        'billing.stuck_refunds_count',
        'billing.stuck_operations_count',
        'billing.outbox_pending_count',
        'billing.outbox_stuck_pending_count',
        'billing.outbox_requires_intervention_count',
        'billing.undelivered_outbox_count',
    ):
        assert attr_key in span_attrs

    # Повторная проверка через 5 с с теми же счётчиками не спамит
    captured_sentry.clear()
    await monitor.check_and_emit_signals(now_utc=now_utc + timedelta(seconds=5))
    assert captured_sentry == []

    # Повторная проверка после истечения alert_cooldown (301 с) снова поднимает сигнал
    await monitor.check_and_emit_signals(now_utc=now_utc + timedelta(seconds=301))
    assert len(captured_sentry) == 2

    # Если зависший платёж завершился (1 -> 0), но одновременно завис возврат (0 -> 1),
    # сумма stuck_operations_count осталась 1, но сигнал о возврате уходит сразу!
    captured_sentry.clear()
    repo.payments[stuck_pay_id].status = 'succeeded'
    stuck_ref_id = uuid4()
    repo.refunds[stuck_ref_id] = FakeRefundRow(
        id=stuck_ref_id,
        payment_id=stuck_pay_id,
        subscription_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29900,
        status='processing',
        reason='Зависший возврат',
        admin_user_id=uuid4(),
        idempotency_key=stuck_ref_id,
        created_at=now_utc - timedelta(minutes=20),
    )
    await monitor.check_and_emit_signals(now_utc=now_utc + timedelta(seconds=310))
    assert len(captured_sentry) == 1
    assert 'возвратов processing: 1' in captured_sentry[0][0]

    # Переход 1 -> 0 -> 1 внутри окна cooldown сразу поднимает сигнал,
    # а для outbox сигнал работает и при outbox_requires_intervention_count == 0,
    # когда есть зависшая pending-задача старше 10 минут
    captured_sentry.clear()
    repo.refunds.clear()
    repo.outbox_tasks.clear()
    await monitor.check_and_emit_signals(now_utc=now_utc + timedelta(seconds=315))
    assert captured_sentry == []

    repo.outbox_tasks['k_stuck_pending_only'] = FakeOutboxTaskRow(
        id=uuid4(),
        dedup_key='k_stuck_pending_only',
        target_service='notifications',
        task_type='payment_succeeded_email',
        payment_id=pay_id,
        user_id=uuid4(),
        access_version=None,
        payload={},
        status='pending',
        created_at=now_utc - timedelta(minutes=20),
    )
    await monitor.check_and_emit_signals(now_utc=now_utc + timedelta(seconds=320))
    assert len(captured_sentry) == 1
    assert 'зависших pending: 1' in captured_sentry[0][0]


async def test_internal_metrics_endpoint_is_side_effect_free_and_handles_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """T-35: GET /api/v1/internal/billing/metrics не спамит в GlitchTip при чтении."""
    captured_sentry: list[str] = []
    monkeypatch.setattr(
        sentry_sdk,
        'capture_message',
        lambda m, **_: captured_sentry.append(m),
    )
    monkeypatch.setattr(settings.worker, 'billing_stuck_threshold_seconds', 600)
    repo = InMemoryPurchaseRepository()
    now_utc = datetime.now(UTC)
    pid = uuid4()
    repo.payments[pid] = FakePaymentRow(
        id=pid,
        user_id=uuid4(),
        tariff_id=uuid4(),
        provider_code='yookassa',
        amount_kopeks=29900,
        currency='RUB',
        status='pending',
        idempotency_key=pid,
        created_at=now_utc - timedelta(minutes=20),
    )

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_purchase_repository] = lambda: repo
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={'X-Request-Id': 't35-metrics'},
    ) as client:
        # 1. Без сервисного ключа -> 401
        r_unauth = await client.get(METRICS_PATH)
        assert r_unauth.status_code == 401

        # 2. С валидным ключом -> 200 и чтение метрик не вызывает capture_message
        r_ok = await client.get(
            METRICS_PATH,
            headers={'X-Service-Api-Key': SERVICE_KEY},
        )
        assert r_ok.status_code == 200
        assert r_ok.json() == {
            'stuck_payments_count': 1,
            'stuck_refunds_count': 0,
            'stuck_operations_count': 1,
            'outbox_pending_count': 0,
            'outbox_stuck_pending_count': 0,
            'outbox_requires_intervention_count': 0,
            'undelivered_outbox_count': 0,
        }
        assert captured_sentry == []

        # 3. При сбое БД -> 503
        repo.db_unavailable = True
        r_503 = await client.get(
            METRICS_PATH,
            headers={'X-Service-Api-Key': SERVICE_KEY},
        )
        assert r_503.status_code == 503
        assert r_503.json()['error_code'] == 'database_unavailable'
