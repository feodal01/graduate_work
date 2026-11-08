"""Реестр платёжных провайдеров и маршрутизация по provider_code (BIL-NFR-S-02)."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest

from clients.payment_provider import ProviderTransientError, YooKassaProviderAdapter
from clients.payment_provider_registry import (
    PaymentProviderRegistry,
    UnknownPaymentProviderError,
    build_payment_provider_registry,
)
from core.config import PaymentProvidersSettings
from services.purchase import PurchaseService
from services.reconciler import PaymentReconciler
from tests.fakes import (
    FakePaymentRow,
    FakeProviderGateway,
    FakeSubscriptionRow,
    InMemoryPurchaseRepository,
)


class _SecondProviderGateway(FakeProviderGateway):
    """Второй провайдер: тот же стаб, но со своим кодом."""

    provider_code = 'cloudpayments'

    def __init__(self) -> None:
        super().__init__(id_prefix='cp-pay')


def _two_providers(
    *,
    active_code: str,
) -> tuple[PaymentProviderRegistry, FakeProviderGateway, _SecondProviderGateway]:
    yookassa = FakeProviderGateway()
    second = _SecondProviderGateway()
    registry = PaymentProviderRegistry(
        {yookassa.provider_code: yookassa, second.provider_code: second},
        active_code=active_code,
    )
    return registry, yookassa, second


def _pending_payment(provider_code: str, provider_payment_id: str) -> FakePaymentRow:
    payment_id = uuid4()
    return FakePaymentRow(
        id=payment_id,
        user_id=uuid4(),
        tariff_id=uuid4(),
        provider_code=provider_code,
        amount_kopeks=29_900,
        currency='RUB',
        status='pending',
        idempotency_key=payment_id,
        provider_payment_id=provider_payment_id,
        created_at=datetime.now(UTC) - timedelta(minutes=5),
    )


def test_registry_rejects_unconfigured_active_provider() -> None:
    """Активный провайдер без адаптера — ошибка конфигурации при старте."""
    with pytest.raises(ValueError, match='not configured'):
        PaymentProviderRegistry(
            {'yookassa': FakeProviderGateway()},
            active_code='cloudpayments',
        )


def test_registry_rejects_adapter_registered_under_foreign_code() -> None:
    """Ключ реестра обязан совпадать с provider_code адаптера."""
    with pytest.raises(ValueError, match='reports provider_code'):
        PaymentProviderRegistry(
            {'cloudpayments': FakeProviderGateway()},
            active_code='cloudpayments',
        )


def test_registry_unknown_code_is_transient_config_error() -> None:
    """Неизвестный provider_code даёт временную ошибку, а не тихий фолбэк."""
    registry = PaymentProviderRegistry.single(FakeProviderGateway())

    assert registry.active_code == 'yookassa'
    assert registry.codes == ('yookassa',)
    with pytest.raises(UnknownPaymentProviderError, match='legacy'):
        registry.get('legacy')
    # Наследник временной ошибки: операция не финализируется, а ждёт сверки.
    assert issubclass(UnknownPaymentProviderError, ProviderTransientError)


def test_payment_providers_settings_read_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Активный провайдер и реквизиты ЮKassa читаются из окружения."""
    monkeypatch.setenv('PAYMENT_PROVIDER_ACTIVE', 'yookassa')
    monkeypatch.setenv('YOOKASSA_SHOP_ID', 'shop-from-env')

    cfg = PaymentProvidersSettings()

    assert cfg.active == 'yookassa'
    assert cfg.yookassa.shop_id == 'shop-from-env'


async def test_build_registry_creates_yookassa_adapter_and_closes_clients() -> None:
    """Фабрика собирает адаптер ЮKassa и закрывает его HTTP-клиент."""
    registry = build_payment_provider_registry(PaymentProvidersSettings())

    assert registry.active_code == 'yookassa'
    assert isinstance(registry.active, YooKassaProviderAdapter)
    await registry.aclose()
    client = registry._http_clients[0]
    assert isinstance(client, httpx.AsyncClient)
    assert client.is_closed


def test_build_registry_fails_fast_on_unsupported_active_provider() -> None:
    """Неподдерживаемый активный провайдер не даёт сервису стартовать."""
    with pytest.raises(ValueError, match='not supported'):
        build_payment_provider_registry(
            PaymentProvidersSettings(active='cloudpayments')
        )


async def test_new_purchase_goes_to_active_provider_and_records_its_code() -> None:
    """Новый платёж создаётся у активного провайдера с его provider_code."""
    registry, yookassa, second = _two_providers(active_code='cloudpayments')
    repo = InMemoryPurchaseRepository()
    service = PurchaseService(
        payments=repo,
        user_views=repo,
        payment_providers=registry,
        public_base_url='http://localhost',
        reconciler=PaymentReconciler(
            payments=repo,
            refunds=repo,
            payment_providers=registry,
            public_base_url='http://localhost',
        ),
    )

    outcome = await service.start_or_continue_purchase(uuid4())

    payment = repo.payments[outcome.payment_id]
    assert payment.provider_code == 'cloudpayments'
    assert payment.provider_payment_id == 'cp-pay-1'
    assert len(second.create_calls) == 1
    assert yookassa.create_calls == []


async def test_existing_payment_reconciles_via_its_own_provider() -> None:
    """Старый платёж сверяется у провайдера, который его создал."""
    registry, yookassa, second = _two_providers(active_code='cloudpayments')
    repo = InMemoryPurchaseRepository()
    payment = _pending_payment('yookassa', 'yk-pay-old')
    repo.payments[payment.id] = payment
    reconciler = PaymentReconciler(
        payments=repo, refunds=repo, payment_providers=registry
    )

    await reconciler.reconcile_payment(
        payment.id,
        source='admin',
        respect_throttle=False,
    )

    assert yookassa.get_calls == ['yk-pay-old']
    assert second.get_calls == []


async def test_webhook_lookup_is_scoped_to_provider_code() -> None:
    """Вебхук ищет операцию только среди операций своего провайдера."""
    registry, yookassa, second = _two_providers(active_code='yookassa')
    repo = InMemoryPurchaseRepository()
    payment = _pending_payment('cloudpayments', 'shared-id-1')
    repo.payments[payment.id] = payment
    reconciler = PaymentReconciler(
        payments=repo, refunds=repo, payment_providers=registry
    )

    # Тот же внешний id, пришедший вебхуком другого провайдера, не находится.
    foreign = await reconciler.reconcile_by_provider_operation_id(
        'shared-id-1',
        provider_code='yookassa',
    )
    own = await reconciler.reconcile_by_provider_operation_id(
        'shared-id-1',
        provider_code='cloudpayments',
    )

    assert foreign is None
    assert own is not None
    assert second.get_calls == ['shared-id-1']
    assert yookassa.get_calls == []


async def test_refund_goes_to_provider_of_the_paid_payment() -> None:
    """Возврат создаётся у провайдера, через которого прошла оплата."""
    registry, yookassa, second = _two_providers(active_code='cloudpayments')
    repo = InMemoryPurchaseRepository()
    now_utc = datetime.now(UTC)
    payment = _pending_payment('yookassa', 'yk-pay-paid')
    payment.status = 'succeeded'
    repo.payments[payment.id] = payment
    sub_id = uuid4()
    repo.subscriptions[sub_id] = FakeSubscriptionRow(
        id=sub_id,
        user_id=payment.user_id,
        tariff_id=payment.tariff_id,
        payment_id=payment.id,
        starts_at=now_utc - timedelta(days=1),
        ends_at=now_utc + timedelta(days=29),
        created_at=now_utc - timedelta(days=1),
    )
    reconciler = PaymentReconciler(
        payments=repo, refunds=repo, payment_providers=registry
    )

    outcome = await reconciler.create_or_reconcile_refund(
        payment.id,
        admin_user_id=uuid4(),
        reason='Проверка маршрутизации',
    )

    assert repo.refunds[outcome.refund_id].provider_code == 'yookassa'
    assert len(yookassa.create_refund_calls) == 1
    assert second.create_refund_calls == []


async def test_payment_of_unconfigured_provider_stays_pending() -> None:
    """Платёж провайдера без адаптера не финализируется и ждёт сверки."""
    registry = PaymentProviderRegistry.single(FakeProviderGateway())
    repo = InMemoryPurchaseRepository()
    payment = _pending_payment('legacy', 'legacy-1')
    repo.payments[payment.id] = payment
    reconciler = PaymentReconciler(
        payments=repo, refunds=repo, payment_providers=registry
    )

    with pytest.raises(UnknownPaymentProviderError):
        await reconciler.reconcile_payment(
            payment.id,
            source='admin',
            respect_throttle=False,
        )

    assert repo.payments[payment.id].status == 'pending'
