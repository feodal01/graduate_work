import base64
import json
from uuid import uuid4

import httpx
import pytest


@pytest.mark.asyncio
async def test_create_payment_sends_capture_true_idempotence_key_and_metadata() -> None:
    """BIL-FR-01, BIL-NFR-R-01: POST /v3/payments передаёт capture=true и metadata."""
    from clients.payment_provider import (
        CreateProviderPaymentRequest,
        YooKassaProviderAdapter,
    )

    payment_id = uuid4()
    idempotency_key = uuid4()
    captured_request: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured_request['method'] = request.method
        captured_request['path'] = request.url.path
        captured_request['headers'] = dict(request.headers)
        captured_request['body'] = json.loads(request.read().decode('utf-8'))
        return httpx.Response(
            200,
            json={
                'id': '2e9b9b60-000f-5000-8000-18db351245c7',
                'status': 'pending',
                'amount': {'value': '299.00', 'currency': 'RUB'},
                'confirmation': {
                    'type': 'redirect',
                    'confirmation_url': 'https://yoomoney.ru/checkout/payments/v2/contract?orderId=2e9b',
                },
                'metadata': {'payment_id': str(payment_id)},
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = YooKassaProviderAdapter(
            http_client=http_client,
            shop_id='1518689',
            secret_key='test_secret_key',
            base_url='https://api.yookassa.ru/v3',
        )
        result = await adapter.create_payment(
            CreateProviderPaymentRequest(
                payment_id=payment_id,
                idempotency_key=idempotency_key,
                amount_kopeks=29_900,
                currency='RUB',
                description='Подписка «Премиум» на 30 дней',
                return_url=f'http://localhost/billing/payments/{payment_id}',
            )
        )

    assert captured_request['method'] == 'POST'
    assert captured_request['path'] == '/v3/payments'
    headers = captured_request['headers']
    assert isinstance(headers, dict)
    assert headers['idempotence-key'] == str(idempotency_key)
    expected_basic = base64.b64encode(b'1518689:test_secret_key').decode('ascii')
    assert headers['authorization'] == f'Basic {expected_basic}'

    body = captured_request['body']
    assert body == {
        'amount': {'value': '299.00', 'currency': 'RUB'},
        'capture': True,
        'confirmation': {
            'type': 'redirect',
            'return_url': f'http://localhost/billing/payments/{payment_id}',
        },
        'description': 'Подписка «Премиум» на 30 дней',
        'metadata': {'payment_id': str(payment_id)},
    }

    assert result.provider_code == 'yookassa'
    assert result.provider_payment_id == '2e9b9b60-000f-5000-8000-18db351245c7'
    assert result.domain_status == 'pending'
    assert (
        result.confirmation_url
        == 'https://yoomoney.ru/checkout/payments/v2/contract?orderId=2e9b'
    )
    assert result.amount_kopeks == 29_900
    assert result.currency == 'RUB'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('yk_status', 'expected_domain_status', 'expected_reason'),
    [
        ('pending', 'pending', None),
        ('succeeded', 'succeeded', None),
        ('canceled', 'failed', 'insufficient_funds'),
    ],
)
async def test_get_payment_translates_statuses_and_validates_integrity(
    yk_status: str,
    expected_domain_status: str,
    expected_reason: str | None,
) -> None:
    """ACL (3.6), инв. 17: GET /v3/payments/{id} переводит статусы и сверяет данные."""
    from clients.payment_provider import YooKassaProviderAdapter

    payment_id = uuid4()
    provider_payment_id = '2e9b9b60-000f-5000-8000-18db351245c7'

    def handler(_request: httpx.Request) -> httpx.Response:
        payload: dict[str, object] = {
            'id': provider_payment_id,
            'status': yk_status,
            'amount': {'value': '299.00', 'currency': 'RUB'},
            'metadata': {'payment_id': str(payment_id)},
        }
        if yk_status == 'canceled':
            payload['cancellation_details'] = {
                'party': 'payment_network',
                'reason': 'insufficient_funds',
            }
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = YooKassaProviderAdapter(
            http_client=http_client,
            shop_id='1518689',
            secret_key='test_secret_key',
            base_url='https://api.yookassa.ru/v3',
        )
        result = await adapter.get_payment(
            provider_payment_id,
            expected_payment_id=payment_id,
            expected_amount_kopeks=29_900,
            expected_currency='RUB',
        )

    assert result.provider_payment_id == provider_payment_id
    assert result.domain_status == expected_domain_status
    assert result.failure_reason == expected_reason


@pytest.mark.asyncio
async def test_get_payment_raises_anomaly_on_waiting_for_capture() -> None:
    """Инвариант 16: статус waiting_for_capture даёт ProviderProtocolAnomalyError."""
    from clients.payment_provider import (
        ProviderProtocolAnomalyError,
        YooKassaProviderAdapter,
    )

    payment_id = uuid4()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                'id': 'yk-anomaly-1',
                'status': 'waiting_for_capture',
                'amount': {'value': '299.00', 'currency': 'RUB'},
                'metadata': {'payment_id': str(payment_id)},
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = YooKassaProviderAdapter(
            http_client=http_client,
            shop_id='1518689',
            secret_key='test_secret_key',
            base_url='https://api.yookassa.ru/v3',
        )
        with pytest.raises(ProviderProtocolAnomalyError, match='waiting_for_capture'):
            await adapter.get_payment(
                'yk-anomaly-1',
                expected_payment_id=payment_id,
                expected_amount_kopeks=29_900,
                expected_currency='RUB',
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('amount_value', 'currency', 'metadata_payment_id'),
    [
        ('199.00', 'RUB', 'MATCH'),
        ('299.00', 'USD', 'MATCH'),
        ('299.00', 'RUB', str(uuid4())),
    ],
)
async def test_get_payment_raises_integrity_error_on_mismatched_fields(
    amount_value: str,
    currency: str,
    metadata_payment_id: str,
) -> None:
    """Инвариант 17: несовпадение суммы, валюты или metadata блокирует операцию."""
    from clients.payment_provider import (
        ProviderIntegrityMismatchError,
        YooKassaProviderAdapter,
    )

    payment_id = uuid4()
    meta_id = str(payment_id) if metadata_payment_id == 'MATCH' else metadata_payment_id

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                'id': 'yk-mismatch-1',
                'status': 'succeeded',
                'amount': {'value': amount_value, 'currency': currency},
                'metadata': {'payment_id': meta_id},
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = YooKassaProviderAdapter(
            http_client=http_client,
            shop_id='1518689',
            secret_key='test_secret_key',
            base_url='https://api.yookassa.ru/v3',
        )
        with pytest.raises(ProviderIntegrityMismatchError):
            await adapter.get_payment(
                'yk-mismatch-1',
                expected_payment_id=payment_id,
                expected_amount_kopeks=29_900,
                expected_currency='RUB',
            )


@pytest.mark.asyncio
@pytest.mark.parametrize('status_code', [400, 401, 403])
async def test_permanent_http_errors_400_401_403_raise_permanent_error(
    status_code: int,
) -> None:
    """Инвариант 15: коды 400, 401, 403 дают ProviderPermanentError."""
    from clients.payment_provider import (
        CreateProviderPaymentRequest,
        ProviderPermanentError,
        YooKassaProviderAdapter,
    )

    payment_id = uuid4()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code,
            json={
                'code': 'invalid_credentials',
                'description': 'Invalid shopId or key',
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = YooKassaProviderAdapter(
            http_client=http_client,
            shop_id='1518689',
            secret_key='bad_key',
            base_url='https://api.yookassa.ru/v3',
        )
        with pytest.raises(ProviderPermanentError) as exc_info:
            await adapter.create_payment(
                CreateProviderPaymentRequest(
                    payment_id=payment_id,
                    idempotency_key=payment_id,
                    amount_kopeks=29_900,
                    currency='RUB',
                    description='Подписка Премиум',
                    return_url='http://localhost/billing/',
                )
            )
        assert exc_info.value.status_code == status_code


@pytest.mark.asyncio
@pytest.mark.parametrize('status_code', [429, 500, 502, 503])
async def test_transient_http_errors_429_5xx_and_timeout_raise_transient_error(
    status_code: int,
) -> None:
    """Инвариант 10: коды 429, 5xx и тайм-ауты дают ProviderTransientError."""
    from clients.payment_provider import (
        CreateProviderPaymentRequest,
        ProviderTransientError,
        YooKassaProviderAdapter,
    )

    payment_id = uuid4()

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={'description': 'Temporary error'})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = YooKassaProviderAdapter(
            http_client=http_client,
            shop_id='1518689',
            secret_key='test_secret_key',
            base_url='https://api.yookassa.ru/v3',
        )
        with pytest.raises(ProviderTransientError):
            await adapter.create_payment(
                CreateProviderPaymentRequest(
                    payment_id=payment_id,
                    idempotency_key=payment_id,
                    amount_kopeks=29_900,
                    currency='RUB',
                    description='Подписка Премиум',
                    return_url='http://localhost/billing/',
                )
            )

    def timeout_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout('Read timed out', request=request)

    timeout_transport = httpx.MockTransport(timeout_handler)
    async with httpx.AsyncClient(transport=timeout_transport) as http_client:
        adapter = YooKassaProviderAdapter(
            http_client=http_client,
            shop_id='1518689',
            secret_key='test_secret_key',
            base_url='https://api.yookassa.ru/v3',
        )
        with pytest.raises(ProviderTransientError):
            await adapter.get_payment('yk-123')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'bad_amount', ['299.005', 'NaN', 'Infinity', '-10.00', '0.00', 'abc']
)
async def test_parse_decimal_to_kopeks_rejects_fractional_kopeks_nan_and_non_positive(
    bad_amount: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Инвариант 17: дробные копейки, NaN, Infinity и <=0 дают аномалию и алерт."""
    import sentry_sdk

    from clients.payment_provider import (
        ProviderProtocolAnomalyError,
        parse_decimal_to_kopeks,
    )

    sentry_messages: list[str] = []
    monkeypatch.setattr(
        sentry_sdk,
        'capture_message',
        lambda msg, **_kw: sentry_messages.append(msg),
    )

    with pytest.raises(ProviderProtocolAnomalyError):
        parse_decimal_to_kopeks(bad_amount)
    assert len(sentry_messages) == 1


@pytest.mark.asyncio
async def test_create_payment_requires_confirmation_url_when_status_is_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Инвариант 13: pending без confirmation_url вызывает аномалию и сигнал."""
    import sentry_sdk

    from clients.payment_provider import (
        CreateProviderPaymentRequest,
        ProviderProtocolAnomalyError,
        YooKassaProviderAdapter,
    )

    payment_id = uuid4()
    sentry_messages: list[str] = []
    monkeypatch.setattr(
        sentry_sdk,
        'capture_message',
        lambda msg, **_kw: sentry_messages.append(msg),
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                'id': 'yk-no-url-1',
                'status': 'pending',
                'amount': {'value': '299.00', 'currency': 'RUB'},
                'metadata': {'payment_id': str(payment_id)},
            },
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = YooKassaProviderAdapter(
            http_client=http_client,
            shop_id='1518689',
            secret_key='test_secret_key',
            base_url='https://api.yookassa.ru/v3',
        )
        with pytest.raises(ProviderProtocolAnomalyError, match='confirmation_url'):
            await adapter.create_payment(
                CreateProviderPaymentRequest(
                    payment_id=payment_id,
                    idempotency_key=payment_id,
                    amount_kopeks=29_900,
                    currency='RUB',
                    description='Подписка Премиум',
                    return_url='http://localhost/billing/',
                )
            )
    assert len(sentry_messages) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('status_code', [401, 403, 404])
async def test_get_payment_401_403_404_raises_reconciliation_alert(
    status_code: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Инвариант 15 (п. 2): 401/403/404 при GET не дают ProviderPermanentError."""
    import sentry_sdk

    from clients.payment_provider import (
        ProviderPermanentError,
        ProviderReconciliationError,
        YooKassaProviderAdapter,
    )

    sentry_messages: list[str] = []
    monkeypatch.setattr(
        sentry_sdk,
        'capture_message',
        lambda msg, **_kw: sentry_messages.append(msg),
    )

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={'description': 'Auth or not found'})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = YooKassaProviderAdapter(
            http_client=http_client,
            shop_id='1518689',
            secret_key='test_secret_key',
            base_url='https://api.yookassa.ru/v3',
        )
        with pytest.raises(ProviderReconciliationError) as exc_info:
            await adapter.get_payment('yk-existing-1')
        assert not isinstance(exc_info.value, ProviderPermanentError)
        assert exc_info.value.status_code == status_code
    assert len(sentry_messages) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'unsafe_id', ['', '   ', '../refunds', 'yk-1/../../other', 'yk-1?foo=bar']
)
async def test_get_payment_rejects_unsafe_or_empty_provider_payment_id(
    unsafe_id: str,
) -> None:
    """BIL-NFR-SEC-04: невалидный provider_payment_id отклоняется до HTTP-вызова."""
    from clients.payment_provider import (
        ProviderProtocolAnomalyError,
        YooKassaProviderAdapter,
    )

    called = False

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal called
        called = True
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        adapter = YooKassaProviderAdapter(
            http_client=http_client,
            shop_id='1518689',
            secret_key='test_secret_key',
            base_url='https://api.yookassa.ru/v3',
        )
        with pytest.raises(ProviderProtocolAnomalyError):
            await adapter.get_payment(unsafe_id)
    assert called is False
