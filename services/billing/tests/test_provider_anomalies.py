"""Аномалии ЮKassa, незнакомые вебхуки и поздние номера (инв. 13, 14, 16, 6.4).

- Аномалию, которая повторяется на каждом чтении (`waiting_for_capture`,
  неизвестный статус, незавершённый или неоднозначный список возвратов),
  сигналит владелец операции — сверка, один раз на операцию; адаптер пишет
  только WARNING.
- Пока у ожидающего платежа есть отметка аномалии, «Продолжить оплату» ведёт
  на экран «Проверяем оплату…», а не на страницу оплаты.
- Вебхук с незнакомым номером может принести номер нашей операции, ответ на
  создание которой потерялся (BIL-API-05).
"""

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import sentry_sdk

from clients.payment_provider import (
    DuplicateRefundsError,
    ProviderPermanentError,
    ProviderStatusAnomalyError,
    ProviderTransientError,
    RefundListIncompleteError,
    YooKassaProviderAdapter,
)
from clients.payment_provider_registry import PaymentProviderRegistry
from dependencies import (
    get_payment_reconciler,
    get_repositories,
    get_service_credentials,
)
from main import create_app
from repositories.records import ANOMALIES_KEY, LOOKUP_NOT_FOUND_KEY
from security.service_credentials import ServiceCredentials
from services.purchase import PurchaseService
from services.reconciler import PaymentReconciler
from services.webhook_probe import WebhookProbeLimiter, WebhookProbePolicy
from tests.fakes import (
    FakePaymentRow,
    FakeProviderGateway,
    FakeRefundRow,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
    in_memory_repositories,
)

SERVICE_KEY = 'billing-internal-anomaly-key-long-enough'


@pytest.fixture
def signals(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Сообщения, отправленные в GlitchTip через sentry_sdk.capture_message."""
    captured: list[str] = []

    def fake_capture_message(message: str, level: str = 'info', **_: object) -> str:
        if level == 'error':
            captured.append(message)
        return 'evt-id'

    monkeypatch.setattr(sentry_sdk, 'capture_message', fake_capture_message)
    return captured


def _env(
    provider: Any = None,
) -> tuple[InMemoryPurchaseRepository, Any, PaymentReconciler, PurchaseService]:
    repo = InMemoryPurchaseRepository()
    gateway = provider or FakeProviderGateway()
    registry = PaymentProviderRegistry.single(gateway)
    reconciler = PaymentReconciler(
        payments=repo, refunds=repo, payment_providers=registry
    )
    service = PurchaseService(
        payments=repo,
        user_views=repo,
        payment_providers=registry,
        public_base_url='http://localhost',
        reconciler=reconciler,
    )
    return repo, gateway, reconciler, service


def _payment(
    repo: InMemoryPurchaseRepository,
    *,
    status: str = 'pending',
    provider_payment_id: str | None = 'yk-hold-1',
    confirmation_url: str | None = 'https://yoomoney.ru/checkout/hold-1',
    user_id: UUID | None = None,
) -> FakePaymentRow:
    pid = uuid4()
    row = FakePaymentRow(
        id=pid,
        user_id=user_id or uuid4(),
        tariff_id=repo.tariff.id,
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status=status,
        idempotency_key=pid,
        provider_payment_id=provider_payment_id,
        confirmation_url=confirmation_url,
    )
    repo.payments[pid] = row
    return row


def _paid_with_refund(
    repo: InMemoryPurchaseRepository,
    *,
    refund_provider_id: str | None = None,
    refund_age: timedelta = timedelta(hours=25),
) -> FakeRefundRow:
    payment = _payment(
        repo,
        status='succeeded',
        provider_payment_id='yk-paid-1',
        confirmation_url=None,
    )
    sub_id = uuid4()
    now = datetime.now(UTC)
    repo.subscriptions[sub_id] = FakeSubscriptionRow(
        id=sub_id,
        user_id=payment.user_id,
        tariff_id=payment.tariff_id,
        payment_id=payment.id,
        starts_at=now - timedelta(days=2),
        ends_at=now + timedelta(days=28),
    )
    repo.access_versions[payment.user_id] = 1
    rid = uuid4()
    refund = FakeRefundRow(
        id=rid,
        payment_id=payment.id,
        subscription_id=sub_id,
        provider_code='yookassa',
        amount_kopeks=29_900,
        status='processing',
        reason='Возврат',
        admin_user_id=uuid4(),
        idempotency_key=rid,
        provider_refund_id=refund_provider_id,
        created_at=now - refund_age,
    )
    repo.refunds[rid] = refund
    return refund


# --- waiting_for_capture и прочие аномалии статуса платежа ---------------------


async def test_waiting_for_capture_never_sends_user_to_payment_page(
    signals: list[str],
) -> None:
    """Инв. 16: два клика «Продолжить» подряд ведут на «Проверяем оплату…»."""
    repo, provider, _, service = _env()
    payment = _payment(repo)
    provider.get_error = ProviderStatusAnomalyError(
        'waiting_for_capture', provider_status='waiting_for_capture'
    )

    first = await service.continue_payment_for_user(
        payment_id=payment.id, user_id=payment.user_id
    )
    # Второй клик внутри 5-секундного окна: ЮKassa не опрашивается, но отметка
    # аномалии в записи платежа не даёт увести на сохранённую ссылку.
    second = await service.continue_payment_for_user(
        payment_id=payment.id, user_id=payment.user_id
    )

    result_url = f'/billing/payments/{payment.id}'
    assert (first.redirect_url, second.redirect_url) == (result_url, result_url)
    assert first.confirmation_url is None and second.confirmation_url is None
    assert len(provider.get_calls) == 1
    assert repo.payments[payment.id].status == 'pending'
    assert len([m for m in signals if str(payment.id) in m]) == 1

    view = await service.get_payment_result_view(
        payment_id=payment.id, user_id=payment.user_id, user_tz='UTC'
    )
    assert view.block == 'pending'
    assert view.auto_refresh_seconds == 5


async def test_status_anomaly_signal_is_raised_once_per_payment(
    signals: list[str], caplog: pytest.LogCaptureFixture
) -> None:
    """Сверка каждые 30 с не засыпает мониторинг: дальше только WARNING."""
    repo, provider, reconciler, _ = _env()
    payment = _payment(repo)
    provider.get_error = ProviderStatusAnomalyError(
        "Unknown YooKassa payment status 'frozen'", provider_status='frozen'
    )

    with caplog.at_level(logging.WARNING, logger='services.reconciler'):
        for _ in range(3):
            with pytest.raises(ProviderStatusAnomalyError):
                await reconciler.reconcile_payment(
                    payment.id, source='reconcile', respect_throttle=False
                )

    assert len([m for m in signals if str(payment.id) in m]) == 1
    repeated = [r for r in caplog.records if 'Repeated provider anomaly' in r.message]
    assert len(repeated) == 2


async def test_normal_status_clears_anomaly_and_resumes_redirect(
    signals: list[str],
) -> None:
    """Нормальный ответ ЮKassa снимает отметку: снова ведём на оплату."""
    repo, provider, reconciler, service = _env()
    payment = _payment(repo)
    provider.get_error = ProviderStatusAnomalyError(
        'waiting_for_capture', provider_status='waiting_for_capture'
    )
    with pytest.raises(ProviderStatusAnomalyError):
        await reconciler.reconcile_payment(
            payment.id, source='reconcile', respect_throttle=False
        )
    assert ANOMALIES_KEY in (payment.provider_details or {})

    provider.get_error = None
    payment.last_checked_at = None
    outcome = await service.continue_payment_for_user(
        payment_id=payment.id, user_id=payment.user_id
    )

    assert ANOMALIES_KEY not in (payment.provider_details or {})
    assert outcome.redirect_url.startswith('https://yoomoney.ru/checkout/')


async def test_real_adapter_leaves_waiting_for_capture_signal_to_reconciler(
    signals: list[str],
) -> None:
    """Адаптер у `waiting_for_capture` пишет WARNING; сигнал — один, от сверки."""
    pay_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                'id': 'yk-wfc',
                'status': 'waiting_for_capture',
                'amount': {'value': '299.00', 'currency': 'RUB'},
                'metadata': {'payment_id': str(pay_id)},
            },
        )

    adapter = YooKassaProviderAdapter(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        shop_id='s',
        secret_key='k',
    )
    repo, _, reconciler, _ = _env(adapter)
    repo.payments[pay_id] = FakePaymentRow(
        id=pay_id,
        user_id=uuid4(),
        tariff_id=repo.tariff.id,
        provider_code='yookassa',
        amount_kopeks=29_900,
        currency='RUB',
        status='pending',
        idempotency_key=pay_id,
        provider_payment_id='yk-wfc',
    )

    for _ in range(2):
        with pytest.raises(ProviderStatusAnomalyError):
            await reconciler.reconcile_payment(
                pay_id, source='reconcile', respect_throttle=False
            )

    assert len(signals) == 1
    assert str(pay_id) in signals[0] and 'waiting_for_capture' in signals[0]


# --- Список возвратов: несколько совпадений и незавершённый обход -------------


def _list_adapter(
    pages: dict[str | None, dict[str, Any]],
) -> YooKassaProviderAdapter:
    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get('cursor')
        return httpx.Response(200, json=pages[cursor])

    return YooKassaProviderAdapter(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        shop_id='s',
        secret_key='k',
    )


def _list_item(provider_refund_id: str, refund_id: UUID | None) -> dict[str, Any]:
    return {
        'id': provider_refund_id,
        'status': 'succeeded',
        'payment_id': 'yk-pay-list',
        'amount': {'value': '299.00', 'currency': 'RUB'},
        'metadata': {'refund_id': str(refund_id or uuid4())},
    }


async def test_adapter_scans_all_pages_and_reports_every_duplicate(
    caplog: pytest.LogCaptureFixture, signals: list[str]
) -> None:
    """Совпадения на разных страницах — не привязка первого, а ошибка с номерами."""
    refund_id = uuid4()
    adapter = _list_adapter(
        {
            None: {
                'items': [_list_item('yk-ref-A', refund_id), _list_item('yk-x', None)],
                'next_cursor': 'page-2',
            },
            'page-2': {'items': [_list_item('yk-ref-B', refund_id)]},
        }
    )

    with (
        caplog.at_level(logging.WARNING),
        pytest.raises(DuplicateRefundsError) as exc_info,
    ):
        await adapter.find_refund_by_metadata('yk-pay-list', refund_id=refund_id)

    assert exc_info.value.provider_refund_ids == ['yk-ref-A', 'yk-ref-B']
    assert signals == []
    assert any('yk-ref-B' in r.getMessage() for r in caplog.records)


async def test_single_match_on_later_page_is_still_attached() -> None:
    """Одно совпадение после полного обхода — как раньше."""
    refund_id = uuid4()
    adapter = _list_adapter(
        {
            None: {'items': [_list_item('yk-x', None)], 'next_cursor': 'p2'},
            'p2': {'items': [_list_item('yk-ref-only', refund_id)]},
        }
    )

    found = await adapter.find_refund_by_metadata(
        'yk-pay-list', refund_id=refund_id, expected_amount_kopeks=29_900
    )

    assert found is not None and found.provider_refund_id == 'yk-ref-only'


async def test_duplicate_refunds_keep_attempt_processing_with_one_signal(
    signals: list[str],
) -> None:
    """Инв. 14: двойной возврат у ЮKassa не привязывается и не считается «пусто»."""
    repo, provider, reconciler, _ = _env()
    refund = _paid_with_refund(repo)
    provider.find_refund_error = DuplicateRefundsError(
        'two refunds', provider_refund_ids=['yk-ref-A', 'yk-ref-B']
    )

    for _ in range(2):
        refund.last_checked_at = None
        await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    assert refund.status == 'processing'
    assert refund.provider_refund_id is None
    assert LOOKUP_NOT_FOUND_KEY not in (refund.provider_details or {})
    mine = [m for m in signals if str(refund.id) in m]
    assert len(mine) == 1 and 'two refunds' in mine[0]


async def test_incomplete_list_signals_once_and_admin_gets_accurate_message(
    signals: list[str],
) -> None:
    """Инв. 14, 7.2: незавершённый обход — один сигнал и понятный ответ кнопке."""
    repo, provider, reconciler, _ = _env()
    refund = _paid_with_refund(repo)
    provider.find_refund_error = RefundListIncompleteError('Repeated next_cursor')

    for _ in range(2):
        refund.last_checked_at = None
        await reconciler.reconcile_due_processing_refunds(min_age_seconds=0.0)

    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_repositories] = lambda: in_memory_repositories(repo)
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler
    app.dependency_overrides[get_service_credentials] = lambda: ServiceCredentials(
        SERVICE_KEY
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url='http://test',
        headers={'X-Service-Api-Key': SERVICE_KEY, 'X-Request-Id': 'p12'},
    ) as client:
        resp = await client.post(
            f'/api/v1/internal/billing/operations/refund/{refund.id}/reconcile'
        )

    assert resp.status_code == 409
    assert resp.json()['error_code'] == 'refund_lookup_incomplete'
    assert 'не до конца' in resp.json()['detail']
    assert 'не удалось связаться' not in resp.json()['detail'].lower()
    assert refund.status == 'processing'
    assert len([m for m in signals if str(refund.id) in m]) == 1


# --- Вебхук с незнакомым номером ----------------------------------------------


def _object_adapter(
    objects: dict[str, dict[str, Any]],
    calls: list[str],
) -> YooKassaProviderAdapter:
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        object_id = request.url.path.rsplit('/', 1)[-1]
        if object_id not in objects:
            return httpx.Response(404, json={'description': 'not found'})
        return httpx.Response(200, json=objects[object_id])

    return YooKassaProviderAdapter(
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        shop_id='s',
        secret_key='k',
    )


def _webhook_client(
    reconciler: PaymentReconciler,
) -> Callable[[dict[str, Any]], Any]:
    app = create_app(app_lifespan=None)
    app.dependency_overrides[get_payment_reconciler] = lambda: reconciler

    async def post(body: dict[str, Any]) -> httpx.Response:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url='http://test',
            headers={'X-Request-Id': 'webhook-unknown-object'},
        ) as client:
            return await client.post('/api/v1/billing/webhooks/yookassa', json=body)

    return post


async def test_webhook_attaches_lost_refund_number_by_metadata() -> None:
    """Ответ на POST возврата потерялся; вебхук приносит номер — привязываем."""
    calls: list[str] = []
    repo = InMemoryPurchaseRepository()
    refund = _paid_with_refund(repo, refund_age=timedelta(minutes=10))
    objects = {
        'yk-ref-lost': {
            'id': 'yk-ref-lost',
            'status': 'succeeded',
            'payment_id': 'yk-paid-1',
            'amount': {'value': '299.00', 'currency': 'RUB'},
            'metadata': {'refund_id': str(refund.id)},
        }
    }
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(
            _object_adapter(objects, calls)
        ),
    )

    resp = await _webhook_client(reconciler)(
        {'event': 'refund.succeeded', 'object': {'id': 'yk-ref-lost'}}
    )

    assert resp.status_code == 200
    assert refund.provider_refund_id == 'yk-ref-lost'
    assert refund.status == 'succeeded'
    # Тип из `event`: только чтения возврата (подсказка + проверка), без платежей.
    assert calls == ['/v3/refunds/yk-ref-lost', '/v3/refunds/yk-ref-lost']


async def test_webhook_attaches_lost_payment_number_and_keeps_it_pending() -> None:
    """Ответ на POST платежа потерялся; вебхук приносит номер и ссылку."""
    calls: list[str] = []
    repo = InMemoryPurchaseRepository()
    payment = _payment(repo, provider_payment_id=None, confirmation_url=None)
    objects = {
        'yk-pay-lost': {
            'id': 'yk-pay-lost',
            'status': 'pending',
            'amount': {'value': '299.00', 'currency': 'RUB'},
            'metadata': {'payment_id': str(payment.id)},
            'confirmation': {'confirmation_url': 'https://yoomoney.ru/lost'},
        }
    }
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(
            _object_adapter(objects, calls)
        ),
    )

    resp = await _webhook_client(reconciler)(
        {'event': 'payment.waiting', 'object': {'id': 'yk-pay-lost'}}
    )

    assert resp.status_code == 200
    assert payment.provider_payment_id == 'yk-pay-lost'
    assert payment.confirmation_url == 'https://yoomoney.ru/lost'
    assert payment.status == 'pending'
    assert calls == ['/v3/payments/yk-pay-lost', '/v3/payments/yk-pay-lost']


async def test_webhook_does_not_attach_object_failing_integrity_checks(
    signals: list[str],
) -> None:
    """Сумма не совпала — номер не привязывается (инв. 17), вебхук 200."""
    calls: list[str] = []
    repo = InMemoryPurchaseRepository()
    refund = _paid_with_refund(repo, refund_age=timedelta(minutes=10))
    objects = {
        'yk-ref-alien': {
            'id': 'yk-ref-alien',
            'status': 'succeeded',
            'payment_id': 'yk-paid-1',
            'amount': {'value': '1.00', 'currency': 'RUB'},
            'metadata': {'refund_id': str(refund.id)},
        }
    }
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(
            _object_adapter(objects, calls)
        ),
    )

    resp = await _webhook_client(reconciler)(
        {'event': 'refund.succeeded', 'object': {'id': 'yk-ref-alien'}}
    )

    assert resp.status_code == 200
    assert refund.provider_refund_id is None
    assert refund.status == 'processing'


def _probe_env(
    objects: dict[str, dict[str, Any]],
    *,
    policy: WebhookProbePolicy | None = None,
    with_candidate: bool = True,
) -> tuple[InMemoryPurchaseRepository, list[str], Callable[[dict[str, Any]], Any]]:
    """Вебхук-клиент с настоящим адаптером; при with_candidate — платёж без номера."""
    calls: list[str] = []
    repo = InMemoryPurchaseRepository()
    if with_candidate:
        _payment(repo, provider_payment_id=None, confirmation_url=None)
    reconciler = PaymentReconciler(
        payments=repo,
        refunds=repo,
        payment_providers=PaymentProviderRegistry.single(
            _object_adapter(objects, calls)
        ),
        webhook_probe_limiter=WebhookProbeLimiter(policy),
    )
    return repo, calls, _webhook_client(reconciler)


FOREIGN_PAYMENT = {
    'id': 'yk-foreign',
    'status': 'succeeded',
    'amount': {'value': '10.00', 'currency': 'RUB'},
    'metadata': {'order': 'other-shop'},
}


async def test_unknown_id_without_candidate_operation_costs_no_provider_call(
    caplog: pytest.LogCaptureFixture, signals: list[str]
) -> None:
    """Нет нашей операции без номера — нет запроса к ЮKassa, один запрос к БД."""
    repo, calls, post = _probe_env(
        {'yk-foreign': FOREIGN_PAYMENT}, with_candidate=False
    )

    with caplog.at_level(logging.INFO):
        for _ in range(5):
            resp = await post(
                {'event': 'payment.succeeded', 'object': {'id': 'yk-foreign'}}
            )
            assert resp.status_code == 200

    assert calls == []
    assert repo.awaiting_number_queries == 5
    assert signals == []
    assert [r for r in caplog.records if r.name == 'services.reconciler'] == []


async def test_webhook_without_event_is_not_read_at_all() -> None:
    """Без `event` уведомление не от ЮKassa: ни запроса к ЮKassa, ни к БД."""
    repo, calls, post = _probe_env({'yk-foreign': FOREIGN_PAYMENT})

    resp = await post({'object': {'id': 'yk-foreign'}})

    assert resp.status_code == 200
    assert calls == []
    assert repo.awaiting_number_queries == 0


async def test_foreign_object_is_read_once_per_window_with_warning_only(
    caplog: pytest.LogCaptureFixture, signals: list[str]
) -> None:
    """Повторы того же номера в окне ЮKassa не читают; чужой объект — WARNING."""
    _, calls, post = _probe_env({'yk-foreign': FOREIGN_PAYMENT})

    with caplog.at_level(logging.DEBUG):
        for _ in range(3):
            resp = await post(
                {'event': 'payment.succeeded', 'object': {'id': 'yk-foreign'}}
            )
            assert resp.status_code == 200

    assert calls == ['/v3/payments/yk-foreign']
    assert signals == []
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any(
        'unknown provider object yk-foreign' in r.getMessage() for r in caplog.records
    )


async def test_global_cap_on_speculative_reads_is_honoured() -> None:
    """Всех пробных чтений в окне — не больше лимита, разные номера не помогают."""
    _, calls, post = _probe_env(
        {}, policy=WebhookProbePolicy(window=timedelta(minutes=10), max_per_window=2)
    )

    for i in range(5):
        resp = await post(
            {'event': 'payment.succeeded', 'object': {'id': f'yk-random-{i}'}}
        )
        assert resp.status_code == 200

    assert calls == ['/v3/payments/yk-random-0', '/v3/payments/yk-random-1']


async def test_404_on_speculative_read_raises_no_signal(
    caplog: pytest.LogCaptureFixture, signals: list[str]
) -> None:
    """404 на чтение чужого номера — только WARNING, без ERROR и GlitchTip."""
    _, calls, post = _probe_env({})

    with caplog.at_level(logging.DEBUG):
        resp = await post(
            {'event': 'refund.succeeded', 'object': {'id': 'yk-not-found'}}
        )
        resp_payment = await post(
            {'event': 'payment.succeeded', 'object': {'id': 'yk-not-found-2'}}
        )

    assert (resp.status_code, resp_payment.status_code) == (200, 200)
    # Возврата без номера нет — его номер не читается; платёж без номера есть.
    assert calls == ['/v3/payments/yk-not-found-2']
    assert signals == []
    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any(
        r.levelno == logging.WARNING and 'could not be read' in r.getMessage()
        for r in caplog.records
    )


def test_probe_limiter_forgets_ids_after_window() -> None:
    """Через окно номер снова можно прочитать, а лимит освобождается."""
    limiter = WebhookProbeLimiter(
        WebhookProbePolicy(window=timedelta(minutes=10), max_per_window=1)
    )
    start = datetime.now(UTC)

    assert limiter.allow('yookassa', 'yk-1', now_utc=start) is True
    assert limiter.allow('yookassa', 'yk-1', now_utc=start) is False
    assert limiter.allow('yookassa', 'yk-2', now_utc=start) is False
    later = start + timedelta(minutes=10, seconds=1)
    assert limiter.allow('yookassa', 'yk-2', now_utc=later) is True


async def test_webhook_provider_failure_on_unknown_object_answers_200(
    caplog: pytest.LogCaptureFixture, signals: list[str]
) -> None:
    """Сбой ЮKassa при чтении незнакомого объекта: повторять нечего — 200."""
    repo, provider, reconciler, _ = _env()
    _paid_with_refund(repo, refund_age=timedelta(minutes=10))
    provider.get_refund_error = ProviderTransientError('YooKassa 503')

    with caplog.at_level(logging.WARNING, logger='services.reconciler'):
        resp = await _webhook_client(reconciler)(
            {'event': 'refund.succeeded', 'object': {'id': 'yk-ref-unknown'}}
        )

    assert resp.status_code == 200
    assert provider.get_refund_calls == ['yk-ref-unknown']
    assert any('could not be read' in r.getMessage() for r in caplog.records)
    assert signals == []


def test_webhook_probe_settings_reach_api_and_are_validated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`BILLING_WEBHOOK_PROBE_*` доходят до общего ограничителя billing-api."""
    from pydantic import ValidationError

    from core.config import WorkerSettings, settings
    from dependencies import get_webhook_probe_limiter

    monkeypatch.setattr(settings.worker, 'billing_webhook_probe_window_seconds', 60)
    monkeypatch.setattr(settings.worker, 'billing_webhook_probe_max_per_window', 3)
    get_webhook_probe_limiter.cache_clear()
    try:
        limiter = get_webhook_probe_limiter()
        assert limiter is get_webhook_probe_limiter()
        assert limiter.policy == WebhookProbePolicy(
            window=timedelta(seconds=60), max_per_window=3
        )
    finally:
        get_webhook_probe_limiter.cache_clear()
    assert (
        WebhookProbePolicy.from_settings(WorkerSettings.model_construct())
        == WebhookProbePolicy()
    )
    for field, value in (
        ('billing_webhook_probe_window_seconds', 0),
        ('billing_webhook_probe_max_per_window', 0),
    ):
        with pytest.raises(ValidationError):
            WorkerSettings.model_validate({field: value})


# --- Поздний номер платежа и отказ ЮKassa в создании --------------------------


async def test_late_payment_number_for_closed_payment_is_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Инв. 13: номер по закрытому платежу не хранится, но остаётся в логе."""
    repo, provider, _, service = _env()
    payment = _payment(repo, provider_payment_id=None, confirmation_url=None)
    original_create = provider.create_payment

    async def create_while_closed(request: Any) -> Any:
        snapshot = await original_create(request)
        if request.payment_id == payment.id:
            await repo.finalize_payment_failed(
                payment.id,
                failure_reason='idempotency_key_expired',
                source='reconcile',
                notify_user=False,
            )
        return snapshot

    provider.create_payment = create_while_closed

    with caplog.at_level(logging.WARNING, logger='services.reconciler'):
        await service.continue_payment_for_user(
            payment_id=payment.id, user_id=payment.user_id
        )

    records = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and 'is not stored' in r.getMessage()
    ]
    assert len(records) == 1
    message = records[0].getMessage()
    assert str(payment.id) in message
    assert "'failed'" in message
    assert 'yk-pay-1' in message
    assert payment.provider_payment_id is None


async def test_background_create_rejected_by_provider_queues_no_email() -> None:
    """Инв. 15, BIL-FR-06: отказ ЮKassa в создании — платёж закрыт без письма."""
    repo, provider, reconciler, _ = _env()
    payment = _payment(repo, provider_payment_id=None, confirmation_url=None)
    payment.created_at = datetime.now(UTC) - timedelta(minutes=5)
    provider.create_error = ProviderPermanentError(401, 'invalid credentials')

    await reconciler.reconcile_due_pending_payments(min_age_seconds=0.0)

    assert payment.status == 'failed'
    assert [t for t in repo.outbox_tasks.values() if t.payment_id == payment.id] == []
